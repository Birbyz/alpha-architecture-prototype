import streamlit as st

from constants import (
    __DOCKER__,
    __IDLE__,
    __RUNNING__,
    __STOPPED__,
    DATABRICKS,
    HADOOP,
    LOCAL,
    SNOWFLAKE,
    START_PIPELINE_ACTION,
    START_PIPELINE_BUTTON,
    STOP_PIPELINE_ACTION,
    STOP_PIPELINE_BUTTON,
)
from state import get_runtime_state
from runtimes.controller import dispatch_action


def render_pipeline_controls():
    st.subheader("Pipeline")

    runtime = st.session_state.get("selected_runtime", LOCAL)

    top = st.columns([1.5, 2])

    with top[0]:
        st.markdown(f"**Runtime:** `{runtime}`")

    with top[1]:
        label, color = _runtime_status_badge(runtime)
        st.markdown(
            f"<span style='color:{color}; font-size:0.85rem'>{label}</span>",
            unsafe_allow_html=True,
        )

    is_runtime_ready = _runtime_status_badge(runtime)[0].startswith("Connected.")
    cols = st.columns([1, 1, 1])

    with cols[0]:
        start_clicked = st.button(
            START_PIPELINE_BUTTON,
            type="primary",
            use_container_width=True,
            key=START_PIPELINE_ACTION,
        )

    with cols[1]:
        stop_clicked = st.button(
            STOP_PIPELINE_BUTTON, use_container_width=True, key=STOP_PIPELINE_ACTION
        )

    if start_clicked:
        dispatch_action(START_PIPELINE_ACTION)
        st.rerun()

    if stop_clicked:
        dispatch_action(STOP_PIPELINE_ACTION)
        st.rerun()

    badge_label, _ = _runtime_status_badge(runtime)
    if badge_label == "Unable to connect." and runtime != LOCAL:
        st.caption("⚠️ HADOOP is not reachable ...")

    st.divider()


def _runtime_status_badge(runtime: str) -> tuple[str, str]:
    # returns (label, color) for the current runtime connection status
    if runtime == LOCAL:
        state = get_runtime_state(__DOCKER__)
    elif runtime == DATABRICKS:
        state = get_runtime_state(DATABRICKS)
    elif runtime == HADOOP:
        state = get_runtime_state(HADOOP)
    elif runtime == SNOWFLAKE:
        state = get_runtime_state(SNOWFLAKE)
    else:
        state = __STOPPED__

    if state == __RUNNING__:
        return "Connected.", "green"

    if runtime == LOCAL:
        return "Docker is currently not working.", "orange"

    if state == __IDLE__:
        return "Not connected. Press 'Start Pipeline' to connect.", "orange"

    return "Unable to connect.", "red"
