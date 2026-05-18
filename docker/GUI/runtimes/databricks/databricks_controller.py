import os
import streamlit as st  # type: ignore

from utils.pipeline_state import set_all_stages, set_stage_state
from runtimes.databricks.databricks_config import DatabricksConfig
from utils.logging_utils import log
from runtimes.databricks.databricks_client import DatabricksClient
from constants import (
    __IDLE__,
    __RUNNING__,
    __STOPPED__,
    DATABRICKS,
    DATABRICKS_RUN_IDS,
    DATABRICKS,
)
from state import set_runtime_state


# internal helpers
def _get_client() -> tuple[DatabricksClient | None, DatabricksConfig | None]:
    # returns (DatabricksClient, DatabricksConfig) or (None, None)
    config = DatabricksConfig.from_env()
    if not config.is_configured:
        log(
            "[HDMAS DABARICKS] Not configured. Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env"
        )
        return None, None

    return DatabricksClient(config.host, config.token), config

def _check_dbfs_paths(client: DatabricksClient, config: DatabricksConfig):
    layers = ["bronze", "silver", "gold"]
    
    for layer in layers:
        path = config.dbfs_path_for(layer)
        try:
            if client.dbfs_exists(path):
                log(f"[HDMAS DATABRICKS] DBFS path exists: {path}")
            else:
                client.dbfs_mkdirs(path)
                log(f"[HDMAS DATABRICKS] DBFS path not found, creating: {path}")
        except Exception as e:
            log(f"[HDMAS DATABRICKS] Could not create path {path} - [ERR]: {e}")
            
def _upload_batch_csv(client: DatabricksClient, config: DatabricksConfig) -> bool:
    local_csv = config.batch_csv_local_path
    
    if not local_csv:
        log("[HDMAS DATABRICKS] No local CSV path configured for batch layer. Batch layer will be skipped.")
        return False
    
    if not os.path.isfile(local_csv):
        log(f"[HDMAS DATABRICKS] Local CSV file not found: {local_csv}. Batch layer will be skipped.")
        return False
    
    dbfs_destination = config.batch_csv_dbfs_path
    if not dbfs_destination:
        log("[HDMAS DATABRICKS] No DBFS destination path configured for batch CSV. Batch layer will be skipped.")
        return False
    
    # check if file is already uploaded to DBFS
    if client.dbfs_exists(dbfs_destination):
        log(f"[HDMAS DATABRICKS] Batch CSV already exists on DBFS at {dbfs_destination}. Skipping upload.")
        return True
    
    log(f"[HDMAS DATABRICKS] Uploading batch CSV to DBFS: {local_csv} → {dbfs_destination}")
    try:
        client.dbfs_upload(local_csv, dbfs_destination, overwrite=False)
        log(f"[HDMAS DATABRICKS] Batch CSV uploaded successfully to {dbfs_destination}")
        return True
    except Exception as e:
        log(f"[HDMAS DATABRICKS] Failed to upload batch CSV: {e}")
        return False

# public interface
def get_status():
    config = DatabricksConfig.from_env()

    if not config.is_configured:
        return __STOPPED__

    try:
        client = DatabricksClient(config.host, config.token)
        return __RUNNING__ if client.is_connected() else __STOPPED__
    except Exception:
        return __STOPPED__


def on_selected():
    config = DatabricksConfig.from_env()
    
    if not config.is_configured:
        log("[HDMAS DATABRICKS] Not configured. Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env")
        set_runtime_state(DATABRICKS, __STOPPED__)
        return
    
    log(f"[HDMAS DATABRICKS] Workspace: {config.host}")
    log("[HDMAS DATABRICKS] Press <Start Pipeline> to connect and prepare the workspace.")
    set_runtime_state(DATABRICKS, __IDLE__)
    


def start_pipeline() -> None:
    log("[HDMAS DATABRICKS] Connecting to Databricks workspace...")
    
    status = get_status()
    if status != __RUNNING__:
        log("[HDMAS DATABRICKS] Connection failed. Check configuration and try again.")
        set_runtime_state(DATABRICKS, __STOPPED__)
        return
    
    set_runtime_state(DATABRICKS, __RUNNING__)
    log("[HDMAS DATABRICKS] Connected successfully.")
    
    client, config = _get_client()
    if client is None:
        return
    
    st.session_state[DATABRICKS_RUN_IDS] = {}
    set_all_stages(__IDLE__)
    
    _check_dbfs_paths(client, config)
    _upload_batch_csv(client, config)
    
    log("[HDMAS DATABRICKS] Workspace is ready. Start each stage to run the pipeline.")


def stop_pipeline() -> None:
    log("[HDMAS DATABRICKS] Stopping pipeline...")
    for stage in ("Gold", "Silver", "Bronze", "Batch", "Kafka"):
        stop_stage(stage)
        
    set_runtime_state(DATABRICKS, __STOPPED__)
    log("[HDMAS DATABRICKS] Pipeline stopped.")


def start_stage(stage: str) -> None:
    client, config = _get_client()
    if client is None:
        return

    job_id = config.job_id_for(stage)
    if not job_id:
        log(f"[HDMAS DATABRICKS] No DATABRICKS_JOB_ID_{stage.upper()} configured.")
        return

    try:
        run_id = client.run_now(job_id)
        run_ids = dict(st.session_state.get(DATABRICKS_RUN_IDS, {}))
        run_ids[stage] = run_id
        st.session_state[DATABRICKS_RUN_IDS] = run_ids
        set_stage_state(stage, __RUNNING__)
        log(f"[HDMAS DATABRICKS] {stage} triggered → run_id={run_id}")
    except Exception as e:
        log(f"[HDMAS DATABRICKS] Failed to start {stage}: {e}")
        set_stage_state(stage, __STOPPED__)


def stop_stage(stage: str) -> None:
    client, _ = _get_client()
    if client is None:
        return

    run_ids = st.session_state.get(DATABRICKS_RUN_IDS, {})
    run_id = run_ids.get(stage)

    if not run_id:
        log(f"[HDMAS DATABRICKS] No active run_id found for {stage}.")
        set_stage_state(stage, __STOPPED__)
        return

    try:
        client.cancel_run(run_id)
        log(f"[HDMAS DATABRICKS] {stage} cancel requested (run_id={run_id})")
    except Exception as e:
        log(f"[HDMAS DATABRICKS] Cancel failed for {stage}: {e}")
    finally:
        run_ids = dict(st.session_state.get(DATABRICKS_RUN_IDS, {}))
        run_ids.pop(stage, None)
        st.session_state[DATABRICKS_RUN_IDS] = run_ids
        set_stage_state(stage, __STOPPED__)
