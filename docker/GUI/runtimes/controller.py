import streamlit as st
import runtimes.docker.docker_controller as docker_ctrl
import runtimes.hadoop.hadoop_controller as hadoop_ctrl
import runtimes.databricks.databricks_controller as databricks_ctrl
import runtimes.snowflake.snowflake_controller as snowflake_ctrl

from constants import (
    __DOCKER__,
    __IDLE__,
    __RUNNING__,
    __STOPPED__,
    DATABRICKS,
    HADOOP,
    LOCAL,
    PIPELINE_ACTIONS,
    SNOWFLAKE,
    START_PIPELINE_ACTION,
    STOP_PIPELINE_ACTION,
)
from runtimes.docker.actions import DOCKER_ACTIONS
from runtimes.runtime_monitor import refresh_pipeline_runtime_status
from state import set_runtime_state

from utils.logging_utils import log


# --------------------------------------------------
# Runtime selection — called when the dropdown changes
# --------------------------------------------------
def on_runtime_selected(runtime: str) -> None:
    log(f"[HDMAS] Switching runtime to: {runtime}")
    _runtime_ctrl(runtime).on_selected()


# --------------------------------------------------
# Dispatcher — single entry point for all UI actions
# --------------------------------------------------
def dispatch_action(action: str):
    action = action.lower().strip()
    runtime = st.session_state.get("selected_runtime", LOCAL)
    ctrl = _runtime_ctrl(runtime)

    if action == START_PIPELINE_ACTION:
        ctrl.start_pipeline()
        
    elif action == STOP_PIPELINE_ACTION:
        ctrl.stop_pipeline()
        
    elif action in PIPELINE_ACTIONS:
        # stage-level actions
        verb, stage = action.split("_", 1)
        if verb == "start":
            ctrl.start_stage(stage.capitalize())
        else:
            ctrl.stop_stage(stage.capitalize())
            
    elif action in DOCKER_ACTIONS:
        # docker actions only allowed in LOCAL
        if runtime != LOCAL:
            log(f"[HDMAS] Docker not available in {runtime} runtime.")
            return
        docker_ctrl.handle_action(action)

    else:
        log("[HDMAS] - Unsupported command.")

    refresh_runtime_status()


# Status refresh
def refresh_runtime_status():
    runtime = st.session_state.get("selected_runtime", LOCAL)

    if runtime == LOCAL:
        status = _runtime_ctrl(runtime).get_status()
        _set_status(runtime, status)

    refresh_pipeline_runtime_status()


# --------------------------------------------------
# Individual status getters (used by runtime_monitor)
# --------------------------------------------------
def get_docker_runtime_status() -> str:
    return docker_ctrl.get_status()


def get_hadoop_runtime_status() -> str:
    return hadoop_ctrl.get_status()


def get_databricks_runtime_status() -> str:
    return databricks_ctrl.get_status()

def get_snowflake_runtime_status() -> str:
    return snowflake_ctrl.get_status()

# --------------------------------------------------
# Internal
# --------------------------------------------------
def _runtime_ctrl(runtime: str):
    # Return the right controller module for the active runtime
    if runtime == HADOOP:
        return hadoop_ctrl
    if runtime == DATABRICKS:
        return databricks_ctrl
    if runtime == SNOWFLAKE:
        return snowflake_ctrl
    return docker_ctrl


def _set_status(runtime: str, status: str):
    key = {HADOOP: HADOOP, DATABRICKS: DATABRICKS, SNOWFLAKE: SNOWFLAKE}.get(runtime, __DOCKER__)
    set_runtime_state(key, status)
