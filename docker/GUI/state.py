import streamlit as st  # type: ignore # pip install streamlit

from constants import (
    __IDLE__,
    __STOPPED__,
    LAST_PREDICTION,
    LOG_LINES,
    ML_STATE,
    PIPELINE_STATE,
    STAGE_STATES,
    __DOCKER__,
    RUNTIME_STATES,
    DATABRICKS_RUN_IDS,
    HADOOP_RUN_IDS,
    DATABRICKS,
    HADOOP,
)
from utils.logging_utils import log


def init_session_state():
    # ---------- session state ----------
    if PIPELINE_STATE not in st.session_state:
        st.session_state.pipeline_state = __STOPPED__

    if LOG_LINES not in st.session_state:
        st.session_state.log_lines = []

    if STAGE_STATES not in st.session_state:
        st.session_state.stage_states = {
            "Kafka": __IDLE__,
            "Batch": __IDLE__,
            "Bronze": __IDLE__,
            "Silver": __IDLE__,
            "Gold": __IDLE__,
            "ML": __IDLE__,
        }

    if ML_STATE not in st.session_state:
        st.session_state.ml_state = __IDLE__

    if LAST_PREDICTION not in st.session_state:
        st.session_state.last_prediction = {
            "timestamp": "-",
            "model_version": "-",
            "prediction_target": "-",
            "predicted_price": "-",
            "confidence": "-",
        }

    # Databricks: {stage_name: run_id} populated when jobs are triggered
    if DATABRICKS_RUN_IDS not in st.session_state:
        st.session_state[DATABRICKS_RUN_IDS] = {}

    # Hadoop: {stage_name: yarn_application_id} populated when the jobs are submitted
    if HADOOP_RUN_IDS not in st.session_state:
        st.session_state[HADOOP_RUN_IDS] = {}


def init_runtime_state() -> None:
    if RUNTIME_STATES not in st.session_state:
        st.session_state.runtime_states = {
            __DOCKER__: __STOPPED__,
            DATABRICKS: __STOPPED__,
            HADOOP: __STOPPED__,
        }


def get_runtime_state(name: str) -> str:
    init_runtime_state()
    return st.session_state[RUNTIME_STATES].get(name, __STOPPED__)


def set_runtime_state(name: str, value: str) -> None:
    init_runtime_state()

    current = dict(st.session_state[RUNTIME_STATES])
    current[name] = value
    st.session_state[RUNTIME_STATES] = current

    # log(f"[DEBUG] SET RUNTIME STATE - {name} -> {value}")
