import streamlit as st  # type: ignore

from utils.pipeline_state import set_stage_state
from runtimes.databricks.databricks_config import DatabricksConfig
from utils.logging_utils import log
from runtimes.databricks.databricks_client import DatabricksClient
from constants import (
    __RUNNING__,
    __STOPPED__,
    DATABRICKS,
    DATABRICKS_RUN_IDS,
    DATABRICKS,
)
from state import set_runtime_state


# --------------------------------------------------
# Internal helpers
# --------------------------------------------------
def _get_client():
    # returns (DatabricksClient, DatabricksConfig) or (None, None)
    config = DatabricksConfig.from_env()
    if not config.is_configured:
        log(
            "[HDMAS DABARICKS] Not configured. Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env"
        )
        return None, None

    return DatabricksClient(config.host, config.token), config


# --------------------------------------------------
# Public interface
# --------------------------------------------------
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
    status = get_status()
    set_runtime_state(DATABRICKS, status)

    if status == __RUNNING__:
        log("[HDMAS] Databricks environment successfully selected")
    else:
        log(
            "[HDMAS] Databricks runtime not reachable. Check DATABRICKS_HOST and DATABRICKS_TOKEN in .env"
        )


def start_pipeline() -> None:
    log("[HDMAS DATABRICKS] Starting pipeline...")
    for stage in ("Kafka", "Bronze", "Silver", "Gold"):
        start_stage(stage)


def stop_pipeline() -> None:
    log("[HDMAS DATABRICKS] Stopping pipeline...")
    for stage in ("Gold", "Silver", "Bronze", "Kafka"):
        stop_stage(stage)


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
