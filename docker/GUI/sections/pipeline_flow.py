import streamlit as st

from constants import (
    __IDLE__,
    __RUNNING__,
    __STOPPED__,
)


def render_stage(label: str, state: str, show_label: bool = True):
    if show_label:
        st.markdown(f"### {label}")

    if state == __RUNNING__:
        st.success(f"**{__RUNNING__}** 🟢")
    elif state == __STOPPED__:
        st.error(f"**{__STOPPED__}** 🔴")
    else:
        st.info(f"**{__IDLE__}** ⚪")


def render_pipeline_flow():
    # --------------------------------------------------
    # KAFKA → BRONZE → SILVER → GOLD
    #           ↑                 ↓
    #         BATCH               ML
    # --------------------------------------------------
    st.subheader("Pipeline Flow")
    st.markdown("")

    layout = [1.6, 2.2, 0.5, 2.2, 0.5, 2.2, 0.5, 2.2, 1.6]

    # Row 1: Kafka -> Bronze -> Silver -> Gold
    row1 = st.columns(layout)
    with row1[1]:
        render_stage("Kafka", st.session_state.stage_states["Kafka"])

    with row1[2]:
        st.markdown(
            "<div style='text-align:center; font-size:30px;'>→</div>",
            unsafe_allow_html=True,
        )

    with row1[3]:
        render_stage("Bronze", st.session_state.stage_states["Bronze"])

    with row1[4]:
        st.markdown(
            "<div style='text-align:center; font-size:30px;'>→</div>",
            unsafe_allow_html=True,
        )

    with row1[5]:
        render_stage("Silver", st.session_state.stage_states["Silver"])

    with row1[6]:
        st.markdown(
            "<div style='text-align:center; font-size:30px;'>→</div>",
            unsafe_allow_html=True,
        )

    with row1[7]:
        render_stage("Gold", st.session_state.stage_states["Gold"])

    # Row 2: up arrow from Batch + down arrow for ML
    row2 = st.columns(layout)
    with row2[3]:
        st.markdown(
            "<div style='text-align:center; font-size:30px;'>↑</div>",
            unsafe_allow_html=True,
        )

    with row2[7]:
        st.markdown(
            "<div style='text-align:center; font-size:30px;'>↓</div>",
            unsafe_allow_html=True,
        )

    # Row 3: Batch + ML
    row3 = st.columns(layout)
    with row3[3]:
        render_stage("Batch", st.session_state.stage_states["Batch"])

    with row3[7]:
        render_stage("ML", st.session_state.stage_states["ML"])

    st.divider()
