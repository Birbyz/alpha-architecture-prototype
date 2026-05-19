import streamlit as st

from constants import DATABRICKS, HADOOP, LOCAL, SNOWFLAKE
from runtimes.controller import on_runtime_selected

RUNTIMES = [LOCAL, HADOOP, DATABRICKS, SNOWFLAKE]


def render_environment():
    st.caption("Runtime")

    if "selected_runtime" not in st.session_state:
        st.session_state.selected_runtime = LOCAL

    previous = st.session_state.selected_runtime

    selected = st.selectbox(
        "Runtime",
        RUNTIMES,
        index=RUNTIMES.index(previous),
        label_visibility="collapsed",
    )

    if selected != previous:
        st.session_state.selected_runtime = selected
        on_runtime_selected(selected)
    else:
        st.session_state.selected_runtime = selected

    st.divider()
