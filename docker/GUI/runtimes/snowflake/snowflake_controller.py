import os
import sys
import time
import subprocess
import streamlit as st

from pathlib import Path
from typing import Optional

from constants import __RUNNING__, __STOPPED__, SNOWFLAKE, SNOWFLAKE_PIDS
from utils.pipeline_state import set_all_stages, set_stage_state
from runtimes.snowflake.snowflake_client import SnowflakeClient
from runtimes.snowflake.snowflake_config import SnowflakeConfig
from utils.logging_utils import log
from state import set_runtime_state

# module-level storage
_processes: dict[str, subprocess.Popen] = {}
_log_files: dict[str, str] = {}

def _get_client() -> tuple[Optional[SnowflakeClient], Optional[SnowflakeConfig]]:
    config = SnowflakeConfig.from_env()
    if not config.is_configured:
        log("[HDMAS SNOWFLAKE] Snowflake is not configured. Missing account/user/password in env.")
        return None, None
    return SnowflakeClient(config), config

def _build_env(config: SnowflakeConfig) -> dict:
    # returns a copy of the os.environ with the Snowflake credentials
    env = dict(os.environ)
    env.update({
        "SNOWFLAKE_ACCOUNT": config.account,
        "SNOWFLAKE_USER": config.user,
        "SNOWFLAKE_PASSWORD": config.password,
        "SNOWFLAKE_DATABASE": config.database,
        "SNOWFLAKE_SCHEMA": config.schema,
        "SNOWFLAKE_WAREHOUSE": config.warehouse,
        "SNOWFLAKE_ROLE": config.role,
        "SNOWFLAKE_BRONZE_TABLE": config.bronze_table,
        "SNOWFLAKE_SILVER_TABLE": config.silver_table,
        "SNOWFLAKE_GOLD_TABLE": config.gold_table,
        "SNOWFLAKE_BATCH_CSV_PATH": config.batch_csv_local_path
    })
    return env

def get_status() -> str:
    config = SnowflakeConfig.from_env()
    if not config.is_configured:
        return __STOPPED__
    try:
        return __RUNNING__ if SnowflakeClient(config).is_connected() else __STOPPED__
    except Exception:
        return __STOPPED__
    
def on_selected():
    config = SnowflakeConfig.from_env()
    if not config.is_configured:
        log(
            "[HDMAS SNOWFLAKE] Snowflake is not configured. Please set the required environment variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD."
        )
        set_runtime_state(SNOWFLAKE, __STOPPED__)
        return
    
    log(f"[HDMAS SNOWFLAKE] Account: {config.account} | User: {config.user} |"
        f"Database: {config.database} | Schema: {config.schema} |"
        f"Warehouse: {config.warehouse} | Role: {config.role}"
    )
    log("[HDMAS SNOWFLAKE] Press 'Start Pipeline' to connect.")
    set_runtime_state(SNOWFLAKE, __STOPPED__)
    
def start_pipeline():
    log("[HDMAS SNOWFLAKE] Attempting to connect to Snowflake...")
    if get_status() != __RUNNING__:
        log("[HDMAS SNOWFLAKE] Connection failed.")
        set_runtime_state(SNOWFLAKE, __STOPPED__)
        return
    
    set_runtime_state(SNOWFLAKE, __RUNNING__)
    
    client, config = _get_client()
    if not client or not config:
        return
    
    version = client.get_version()
    log(f"[HDMAS SNOWFLAKE] Connected to Snowflake - Snowflake: {version}")
    log(
        f"[HDMAS SNOWFLAKE] Target: "
        f"{config.database}.{config.schema} | Tables: "
        f"{config.bronze_table} / {config.silver_table} / {config.gold_table}"
    )

    # reset runtime tracking
    st.session_state[SNOWFLAKE_PIDS] = {}
    _processes.clear()
    _log_files.clear()
    set_all_stages(__STOPPED__)
    log("[HDMAS SNOWFLAKE] Pipeline is ready. Use the stage controls to run individual stages.")
    
def stop_pipeline():
    log("[HDMAS SNOWFLAKE] Stopping pipeline...")
    for stage in ("Gold", "Silver", "Batch"):
        stop_stage(stage)
    set_runtime_state(SNOWFLAKE, __STOPPED__)
    log("[HDMAS SNOWFLAKE] Pipeline stopped.")
    
def start_stage(stage: str):
    client, config = _get_client()
    if client is None or config is None:
        log("[HDMAS SNOWFLAKE] Cannot start stage - Snowflake is not properly configured.")
        return
    
    script = config.script_for(stage)
    if not script:
        log(f"[HDMAS SNOWFLAKE] No script configured for stage '{stage}'. Cannot start stage.")
        return
    
    if not os.path.isfile(script):
        log(f"[HDMAS SNOWFLAKE] Script for stage '{stage}' not found at path: {script}")
        return
    
    if stage == 'Batch' and not config.batch_csv_local_path:
        log(f"[HDMAS SNOWFLAKE] Batch stage requires SNOWFLAKE_BATCH_CSV_PATH to be set. Cannot start stage.")
        return
    
    log_path = Path("/tmp") / f"snowflake_{stage.lower()}_{int(time.time())}_job.log"
    
    try:
        env = _build_env(config)
        with open(log_path, "w") as log_file:
            process = subprocess.Popen(
                [sys.executable, script],
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT
            )
            
        _processes[stage] = process
        _log_files[stage] = str(log_path)
        
        pids = dict(st.session_state.get(SNOWFLAKE_PIDS, {}))
        pids[stage] = process.pid
        st.session_state[SNOWFLAKE_PIDS] = pids
        
        set_stage_state(stage, __RUNNING__)
        log(f"[HDMAS SNOWFLAKE] {stage} started (pid={process.pid}) - Logs at {log_path}")
        
    except Exception as e:
        log(f"[HDMAS SNOWFLAKE] Failed to start stage '{stage}' - [ERR]: {e}")
        set_stage_state(stage, __STOPPED__)
        
def stop_stage(stage: str):
    process = _processes.get(stage)
    
    if process is not None:
        try:
            process.terminate()
            log(f"[HDMAS SNOWFLAKE] Terminated {stage} (pid={process.pid})")
        except Exception as e:
            log(f"[HDMAS SNOWFLAKE] Failed to terminate {stage} (pid={process.pid}) - [ERR]: {e}")
        finally:
            _processes.pop(stage, None)
            _log_files.pop(stage, None)
    else:
        # kill by PID
        pid = st.session_state.get(SNOWFLAKE_PIDS, {}).get(stage)
        if pid:
            try:
                import signal
                os.kill(pid, signal.SIGTERM)
                log(f"[HDMAS SNOWFLAKE] Terminated {stage} (pid={pid})")
            except Exception as e:
                log(f"[HDMAS SNOWFLAKE] Failed to terminate {stage} (pid={pid}) - [ERR]: {e}")
    
    pids = dict(st.session_state.get(SNOWFLAKE_PIDS, {}))
    pids.pop(stage, None)
    st.session_state[SNOWFLAKE_PIDS] = pids
    set_stage_state(stage, __STOPPED__)
    
    