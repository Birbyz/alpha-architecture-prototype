import streamlit as st  # pyright: ignore[reportMissingImports]

from state import get_runtime_state
from runtimes.controller import dispatch_action
from sections.pipeline_flow import render_stage
from constants import (
    __DOCKER__,
    __IDLE__,
    __RUNNING__,
    __STOPPED__,
    _DATABRICKS_NA_STAGES,
    _SNOWFLAKE_NA_STAGES,
    DATABRICKS,
    HADOOP,
    LIGHT_GREEN,
    LOCAL,
    RED,
    ORANGE,
    PIPELINE_STAGES_ARRAY,
    SNOWFLAKE,
)


def color_status(state: str) -> str:
    color = "color: "

    if state == __RUNNING__:
        return color + LIGHT_GREEN

    if state == __STOPPED__:
        return color + RED

    return color + ORANGE


def is_stage_running(states: dict, stage: str) -> tuple[bool, str]:
    stage_name = stage.capitalize()

    if states.get(stage_name) != __RUNNING__:
        return False, f"{stage_name} is not running."

    return True, ""


def can_start_stage(stage: str) -> tuple[bool, str]:
    states = st.session_state.stage_states
    runtime = st.session_state.get("selected_runtime", LOCAL)

    # Only LOCAL depends on Docker
    if runtime == LOCAL:
        docker_state = get_runtime_state(__DOCKER__)
        if docker_state != __RUNNING__:
            return False, "Local runtime not ready."

    # HADOOP
    elif runtime == HADOOP:
        if get_runtime_state(HADOOP) != __RUNNING__:
            return False, "HADOOP is not connected. Press 'Start Pipeline' first."
    
    # DATABRICKS
    elif runtime == DATABRICKS:
        if get_runtime_state(DATABRICKS) != __RUNNING__:
            return False, "DATABRICKS is not connected. Press 'Start Pipeline' first."
        if stage in _DATABRICKS_NA_STAGES:
            return False, f"{stage} stage is not supported on Databricks runtime."
        
        if stage == "Batch" or stage == "Silver" or stage == "Gold":
            return True, ""
        
        if stage == "ML":
            return is_stage_running(states, "gold") 
        return False, "Unsupported stage for Databricks runtime."
    
    # SNOWFLAKE
    elif runtime == SNOWFLAKE:
        if get_runtime_state(SNOWFLAKE) != __RUNNING__:
            return False, "SNOWFLAKE is not connected. Press 'Start Pipeline' first."
        
        if stage in _SNOWFLAKE_NA_STAGES:
            return False, f"{stage} stage is not supported on Snowflake runtime."
        
        if stage == "Batch":
            return True, ""
        if stage == "Silver":
            return True, ""
        if stage == "Gold":
            return True, ""
        if stage == "ML":
            return is_stage_running(states, "gold")
        return False, "Unsupported stage for Snowflake runtime."

    if stage == "Kafka":
        return True, ""

    if stage == "Batch":
        if runtime != LOCAL:
            return (
                False,
                f"Batch CSV ingestion is only supported on LOCAL (got {runtime}).",
            )
        return True, ""

    if stage == "Bronze":
        kafka_running = states.get("Kafka") == __RUNNING__
        batch_running = states.get("Batch") == __RUNNING__

        if not kafka_running and not batch_running:
            return False, "Kafka or Batch layers must be running."

        return True, ""

    if stage == "Silver":
        return is_stage_running(states, "bronze")

    if stage == "Gold":
        return is_stage_running(states, "silver")

    if stage == "ML":
        return is_stage_running(states, "gold")

    return False, "Unsupported stage."


def can_stop_stage(stage: str) -> tuple[bool, str]:
    states = st.session_state.stage_states
    runtime = st.session_state.get("selected_runtime", LOCAL)
    
    # DATABRICKS skips streaming data due to cluster payments
    if runtime == DATABRICKS:
        if stage in _DATABRICKS_NA_STAGES:
            return False, f"{stage} is not available in the Databricks runtime."
 
        # databricks allows stopping stages in any order since they don't have interdependencies in this runtime
        # also the processing time is really short which may cause consecutive starting/stopping impossible
        return True, ""
    
    # SNOWFLAKE allows stopping in any order
    if runtime == SNOWFLAKE:
        if stage in _SNOWFLAKE_NA_STAGES:
            return False, f"{stage} is not available in the Snowflake runtime."
        return True, ""

    
    # Stopping KAFKA is only allowed if Bronze is not running,
    # since Bronze depends on it as its data source.
    if stage == "Kafka":
        if states.get("Bronze") == __RUNNING__:
            return False, "Stop Bronze before stopping Kafka."
        return True, ""

    if stage == "Batch":
        if states.get("Bronze") == __RUNNING__:
            return False, "Stop Bronze before stopping Batch."
        return True, ""

    if stage == "Bronze":
        if states.get("Silver") == __RUNNING__:
            return False, "Stop Silver before stopping Bronze."
        return True, ""

    if stage == "Silver":
        if states.get("Gold") == __RUNNING__:
            return False, "Stop Gold before stopping Silver."
        return True, ""

    if stage == "Gold":
        if states.get("ML") == __RUNNING__:
            return False, "Stop ML before stopping Gold."
        return True, ""

    return True, ""


def render_pipeline_status():
    st.subheader("Pipeline Status")
    init_pipeline_status_state()
    refresh_stage_states()  # always refresh statuses before painting the table
    runtime = st.session_state.get("selected_runtime", LOCAL)

    # --- Batch completion notifications ---
    batch_exhausted = st.session_state.get("batch_exhausted")
    if batch_exhausted == "success":
        st.success(
            "✅ Batch complete — all CSV data has been written to the Bronze Delta layer. "
            "You can stop the Batch stage or continue with Silver/Gold.",
            icon=None,
        )
    elif batch_exhausted == "failed":
        st.error(
            "❌ Batch job exited with an error before all data was processed. "
            "Check the Logs tab for details.",
            icon=None,
        )

    header_cols = st.columns([2, 1, 1])
    header_cols[0].markdown("**Component**")
    header_cols[1].markdown("**State**")
    header_cols[2].markdown("**Action**")

    for stage in PIPELINE_STAGES_ARRAY:
        # In Databricks, skip N/A stages — no row rendered at all
        if runtime == DATABRICKS and stage in _DATABRICKS_NA_STAGES:
            continue
        
        if runtime == SNOWFLAKE and stage in _SNOWFLAKE_NA_STAGES:
            continue

        cols = st.columns([2, 1, 1], vertical_alignment="center")
        state = st.session_state.stage_states.get(stage, __IDLE__)

        with cols[0]:
            st.write(stage)

        with cols[1]:
            render_stage(stage, state, show_label=False)

        with cols[2]:
            label = get_stage_toggle_label(stage)
            clicked = st.button(
                label,
                key=f"{stage.lower()}_toggle",
                use_container_width=True,
            )

            if clicked:
                if label == "Start":
                    allowed, reason = can_start_stage(stage)
                    if not allowed:
                        st.warning(f"Cannot start {stage}: {reason}")
                    else:
                        dispatch_action(f"start_{stage.lower()}")
                        st.rerun()
                else:
                    allowed, reason = can_stop_stage(stage)
                    if not allowed:
                        st.warning(f"Cannot stop {stage}: {reason}")
                    else:
                        dispatch_action(f"stop_{stage.lower()}")
                        st.rerun()
    st.divider()


def refresh_stage_states():
    if "stage_states" not in st.session_state:
        st.session_state.stage_states = {
            stage: __IDLE__ for stage in PIPELINE_STAGES_ARRAY
        }
        return


def get_stage_toggle_label(stage: str) -> str:
    state = st.session_state.stage_states.get(stage, __IDLE__)
    return "Stop" if state == __RUNNING__ else "Start"


def init_pipeline_status_state():
    if "selected_stage_logs" not in st.session_state:
        st.session_state.selected_stage_logs = None

    if "stage_states" not in st.session_state:
        st.session_state.stage_states = {
            stage: __IDLE__ for stage in PIPELINE_STAGES_ARRAY
        }

    if "batch_exhausted" not in st.session_state:
        st.session_state.batch_exhausted = None
