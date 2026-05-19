import os
import time
import streamlit as st  # type: ignore
import runtimes.snowflake.snowflake_controller as snowflake_ctrl

from state import get_runtime_state
from utils.logging_utils import log
from utils.pipeline_state import set_stage_state
from runtimes.databricks.databricks_client import DatabricksClient
from runtimes.databricks.databricks_config import DatabricksConfig
from runtimes.docker.actions import BATCH_SPARK_APP_PATTERN
from runtimes.docker.pipeline_runtime import run_command
from runtimes.hadoop.hadoop_client import HadoopClient
from runtimes.hadoop.hadoop_config import HadoopConfig

from constants import (
    __DOCKER__,
    __IDLE__,
    __RUNNING__,
    __STOPPED__,
    COMPOSE_FILE,
    DATABRICKS,
    DATABRICKS_RUN_IDS,
    SNOWFLAKE,
    HADOOP,
    HADOOP_RUN_IDS,
    SNOWFLAKE_PIDS,
)


def _command_has_output(command: str):
    ok, output = run_command(command)
    return ok and bool(output and output.strip())


def _spark_app_running(pattern: str) -> bool:
    python_script = (
        f"import sys, json; data=json.load(sys.stdin); "
        f"apps=[a.get('name','') for a in data.get('activeapps',[])]; "
        f"print('found') if any('{pattern}' in a for a in apps) else None"
    )
    cmd = [
        "docker",
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "exec",
        "spark-master",
        "bash",
        "-lc",
        f'curl -s http://localhost:8080/json/ | python3 -c "{python_script}"',
    ]
    ok, output = run_command(cmd)
    return ok and bool(output and output.strip())


def _check_batch_completion() -> str | None:
    """
    Inspects completedapps in the Spark master JSON for the batch app.
    Returns 'success' if the app finished cleanly, 'failed' if it errored,
    or None if no completed entry is found yet.
    """
    python_script = (
        "import sys, json; "
        "data=json.load(sys.stdin); "
        f"apps=[a for a in data.get('completedapps',[]) if '{BATCH_SPARK_APP_PATTERN}' in a.get('name','')]; "
        "latest=sorted(apps, key=lambda a: a.get('endtime', 0))[-1] if apps else None; "
        "print('success' if latest and latest.get('state','').upper()=='FINISHED' else 'failed') if latest else print('none')"
    )

    cmd = [
        "docker",
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "exec",
        "spark-master",
        "bash",
        "-lc",
        f'curl -s http://localhost:8080/json/ | python3 -c "{python_script}"',
    ]

    ok, output = run_command(cmd)
    if not ok or not output:
        return None

    result = output.strip().lower()
    return result if result in ("success", "failed") else None


def _is_kafka_running() -> bool:
    cmd = "docker compose ps --services --status running | grep '^crypto-producer$'"
    return _command_has_output(cmd)


def _is_batch_running() -> bool:
    return _spark_app_running(BATCH_SPARK_APP_PATTERN)


def _is_bronze_running() -> bool:
    return _spark_app_running("kafka-to-delta-bronze")


def _is_silver_running() -> bool:
    return _spark_app_running("bronze-to-silver-crypto-trades")


def _is_gold_running() -> bool:
    return _spark_app_running("delta-silver-to-gold")


# --------------------------------------------------
# Databricks stage polling
# --------------------------------------------------
def _refresh_databricks_stage_states() -> None:
    """
    Poll each stage's active run_id against the Databricks Runs Get API.
    Updates stage_states in session state accordingly.
    """
    config = DatabricksConfig.from_env()
    if not config.is_configured:
        return

    try:
        client = DatabricksClient(config.host, config.token)
    except Exception as e:
        log(f"[HDMAS DATABRICKS MONITOR] Failed to build client: {e}")
        return

    run_ids: dict = dict(st.session_state.get(DATABRICKS_RUN_IDS, {}))
    updated_run_ids = dict(run_ids)

    for stage, run_id in run_ids.items():
        try:
            active = client.is_run_active(run_id)

            if active:
                set_stage_state(stage, __RUNNING__)
                log(f"[HDMAS DATABRICKS] Stage {stage} -> RUNNING (run_id={run_id})")
            else:
                # run has terminated - check result
                result = client.get_run_result(run_id)

                if result == "SUCCESS":
                    log(
                        f"[HDMAS DATABRICKS] {stage} completed successfully (run_id={run_id})"
                    )
                elif result in ("FAILED", "TIMEDOUT", "CANCELED"):
                    log(
                        f"[HDMAS DATABRICKS] {stage} ended with {result} (run_id={run_id})"
                    )
                else:
                    log(
                        f"[HDMAS DATABRICKS] {stage} run_id={run_id} state unknown ({result})"
                    )

                set_stage_state(stage, __STOPPED__)
                updated_run_ids.pop(stage, None)
        except Exception as e:
            log(
                f"[HDMAS DATABRICKS MONITOR] Error polling {stage} (run_id={run_id}): {e}"
            )
    st.session_state[DATABRICKS_RUN_IDS] = updated_run_ids


# --------------------------------------------------
# Hadoop stage polling
# --------------------------------------------------
def _refresh_hadoop_stage_states() -> None:
    config = HadoopConfig.from_env()
    if not config.is_configured:
        return

    try:
        client = HadoopClient(config)
    except Exception as e:
        log(f"[HDMAS HADOOP MONITOR] Failed to build client - ERR: {e}")
        return

    app_ids: dict = dict(st.session_state.get(HADOOP_RUN_IDS, {}))
    updated_app_ids = dict(app_ids)

    # Kafka always uses Docker regardless of runtime.
    # Apply the same 20-second grace period used in the LOCAL path so the
    # monitor does not flip Kafka back to STOPPED before the container registers.
    stage_start_times = st.session_state.get("stage_start_times", {})
    now = time.time()

    kafka_running = _is_kafka_running()
    if kafka_running:
        set_stage_state("Kafka", __RUNNING__)
    else:
        if now - stage_start_times.get("Kafka", 0) >= 20:
            set_stage_state("Kafka", __STOPPED__)

    for stage, app_id in app_ids.items():
        try:
            active = client.is_app_active(app_id)
            if active:
                set_stage_state(stage, __RUNNING__)
                log(f"[HDMAS HADOOP MONITOR] {stage} is RUNNING (app_id={app_id})")
            else:
                result = client.get_app_result(app_id)
                if result == "SUCCESS":
                    log(
                        f"[HDMAS HADOOP MONITOR] {stage} completed successfully (app_id={app_id})"
                    )
                elif result in ("FAILED", "KILLED"):
                    log(
                        f"[HDMAS HADOOP MONITOR] {stage} ended with {result} (app_id={app_id})"
                    )
                else:
                    log(
                        f"[HDMAS HADOOP MONITOR] {stage} app_id={app_id} state unknown ({result})"
                    )

                set_stage_state(stage, __STOPPED__)
                updated_app_ids.pop(stage, None)
        except Exception as e:
            log(
                f"[HDMAS HADOOP MONITOR] Error polling {stage} (app_id={app_id}). ERR: {e}"
            )

    st.session_state[HADOOP_RUN_IDS] = updated_app_ids


# --------------------------------------------------
# Snowflake stage polling
# --------------------------------------------------
def _refresh_snowflake_stage_states():
    pids: dict = dict(st.session_state.get(SNOWFLAKE_PIDS, {}))
    updated_pids = dict(pids)
    
    for stage, pid in list(pids.items()):
        process = snowflake_ctrl._processes.get(stage)
        
        if process is not None:
            item = process.poll()
            if item is not None:
                log(f"[HDMAS SNOWFLAKE MONITOR] {stage} process (pid={pid}) has terminated.")
                set_stage_state(stage, __STOPPED__)
                updated_pids.pop(stage, None)
            
            # CASE: process is finished - tail log file for GUI output
            log_path = snowflake_ctrl._log_files.get(stage, "")
            if log_path and os.path.isfile(log_path):
                try:
                    with open(log_path) as log_file:
                        lines = log_file.readlines()
                    for line in lines:
                        line = line.strip()
                        if line:
                            log(f"[HDMAS SNOWFLAKE] {stage} LOG - {line}")
                except Exception as e:
                    log(f"[HDMAS SNOWFLAKE] Failed to read log file for {stage} (pid={pid}) - ERR: {e}")
                    pass
                
            if item == 0:
                log(f"[HDMAS SNOWFLAKE] {stage} (pid={pid}) completed successfully.")
            else:
                log(f"[HDMAS SNOWFLAKE] {stage} exited with code {item}. (pid={pid}). Check /tmp/snowflake_{stage.lower()}_*.log")
        
            set_stage_state(stage, __STOPPED__)
            updated_pids.pop(stage, None)
            snowflake_ctrl._processes.pop(stage, None)
            snowflake_ctrl._log_files.pop(stage, None)
        else:
            # check PID liveness directly
            try:
                os.kill(pid, 0)
                set_stage_state(stage, __RUNNING__)
            except (ProcessLookupError, PermissionError):
                log(f"[HDMAS SNOWFLAKE] {stage} (pid={pid}) is no longer running.")
                set_stage_state(stage, __STOPPED__)
                updated_pids.pop(stage, None)
    st.session_state[SNOWFLAKE_PIDS] = updated_pids


# --------------------------------------------------
# Main entry point — called on every Streamlit rerun
# --------------------------------------------------
def refresh_pipeline_runtime_status() -> None:
    runtime = st.session_state.get("selected_runtime", "LOCAL")

    if runtime == DATABRICKS:
        db_state = get_runtime_state(DATABRICKS)
        if db_state != __RUNNING__:
            # databreacks is not reachable - mark all stages as stopped
            for stage in ("kafka", "bronze", "silver", "gold"):
                set_stage_state(stage, __STOPPED__)
            return

        _refresh_databricks_stage_states()
        return

    if runtime == HADOOP:
        hadoop_state = get_runtime_state(HADOOP)
        if hadoop_state != __RUNNING__:
            for stage in ("kafka", "bronze", "silver", "gold"):
                set_stage_state(stage, __STOPPED__)
            return

        _refresh_hadoop_stage_states()
        return
    
    if runtime == SNOWFLAKE:
        snowflake_state = get_runtime_state(SNOWFLAKE)
        if snowflake_state != __RUNNING__:
            for stage in ("Batch", "Silver", "Gold"):
                set_stage_state(stage, __IDLE__)
            return
        _refresh_snowflake_stage_states()
        return

    # LOCAL / Docker path (original logic)
    docker_state = get_runtime_state(__DOCKER__)

    if docker_state != __RUNNING__:
        for stage in ("kafka", "batch", "bronze", "silver", "gold"):
            set_stage_state(stage, __STOPPED__)
        return

    checks = {
        "Kafka": _is_kafka_running(),
        "Batch": _is_batch_running(),
        "Bronze": _is_bronze_running(),
        "Silver": _is_silver_running(),
        "Gold": _is_gold_running(),
    }

    stage_start_times = st.session_state.get("stage_start_times", {})
    now = time.time()

    for stage, is_running in checks.items():
        if is_running:
            set_stage_state(stage, __RUNNING__ if is_running else __STOPPED__)
            log(
                f"[DEBUG] PIPELINE RUNTIME DETECTED - {stage} -> {__RUNNING__ if is_running else __STOPPED__}"
            )
        else:
            # If the stage was started recently, leave the RUNNING state
            # in place until the grace period expires. This prevents the
            # probe from flipping the button back to "Start" while spark-submit
            # is still initialising and hasn't registered with the master yet.
            started_at = stage_start_times.get(stage, 0)
            if now - started_at < 20:
                continue

            # [Batch]: a transition from RUNNING → not running means the job finished.
            if stage == "Batch":
                was_running = st.session_state.stage_states.get("Batch") == __RUNNING__

                if was_running and st.session_state.get("batch_exhausted") is None:
                    result = _check_batch_completion()
                    if result in ("success", "failed"):
                        st.session_state.batch_exhausted = result

                        if result == "success":
                            log(
                                "[HDMAS BATCH] All CSV data processed successfully. Batch job exited cleanly."
                            )
                        else:
                            log(
                                "[HDMAS BATCH] Batch job exited with an error. Check logs for details."
                            )

            set_stage_state(stage, __STOPPED__)
