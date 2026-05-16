import streamlit as st

from datetime import datetime
from constants import LOG_LINES


def log(msg: str):
    if LOG_LINES not in st.session_state:
        st.session_state.log_lines = []

    ts = datetime.now().strftime("%H:%M:%S")
    st.session_state.log_lines.append(f"[{ts}] {msg}")

    # keep it bounded
    if len(st.session_state.log_lines) > 500:
        st.session_state.log_lines = st.session_state.log_lines[-500:]
