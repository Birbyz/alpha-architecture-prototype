import streamlit as st

from constants import (
    __IDLE__,
    LAST_PREDICTION,
    LOG_LINES,
    ML_STATE,
    PIPELINE_STATE,
    STAGE_STATES,
    __ML__,
    PIPELINE_STAGES_ARRAY,
)


def ensure_session_state() -> None:
    if PIPELINE_STATE not in st.session_state:
        st.session_state.pipeline_state = __IDLE__

    if LOG_LINES not in st.session_state:
        st.session_state.log_lines = []

    if STAGE_STATES not in st.session_state:
        st.session_state.stage_states = {
            stage: __IDLE__ for stage in PIPELINE_STAGES_ARRAY
        }
        st.session_state.stage_states[__ML__] = __IDLE__

    if ML_STATE not in st.session_state:
        st.session_state.ml_state = __IDLE__

    if LAST_PREDICTION not in st.session_state:
        st.session_state.last_prediction = None


def set_pipeline_state(state: str) -> None:
    st.session_state.pipeline_state = state


def set_stage_state(stage: str, state: str) -> None:
    st.session_state.stage_states[stage] = state


def set_all_stages(state: str, include_ml: bool = False) -> None:
    for stage in PIPELINE_STAGES_ARRAY:
        st.session_state.stage_states[stage] = state

    if include_ml:
        st.session_state.stage_states[__ML__] = state
