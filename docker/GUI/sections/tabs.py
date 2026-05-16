import pandas as pd
import streamlit as st

from datetime import datetime
from utils.logging_utils import log
from constants import __IDLE__, __RUNNING__, HADOOP
from utils.data_builders import (
    build_gold_latest_df,
    build_gold_sample_df,
    build_hdfs_layer_stats,
    build_metrics_series,
)


def is_pipeline_running():
    stage_states = st.session_state.get("stage_states", {})
    return any(state == __RUNNING__ for state in stage_states.values())


def render_logs_tab():
    st.subheader("Logs")
    logs_text = "\n".join(st.session_state.get("log_lines", [])[-300:])
    st.text_area("Console", logs_text, height=450)


def render_gold_tab():
    st.subheader("Gold Latest Rows")

    runtime = st.session_state.get("selected_runtime", "LOCAL")

    if runtime == HADOOP:
        st.caption("HADOOP runtime - data is stored on HDFS, not locally.")

        # HDFS layer stats
        col_refresh, _ = st.columns([1, 3])
        with col_refresh:
            refresh_stats = st.button("Refresh HDFS stats", use_container_width=True)

        if refresh_stats:
            with st.spinner("Querying HDFS..."):
                st.session_state["hdfs_layer_stats"] = build_hdfs_layer_stats()

        stats = st.session_state.get("hdfs_layer_stats")
        if stats is not None:
            st.dataframe(stats, hide_index=True, use_container_width=True)
        else:
            st.info(
                "Press **Refresh HDFS stats** to see file the latest updates upon the Delta layers."
            )

        st.divider()

        # GOLD sample data
        st.markdown("#### Sample Gold Rows")
        st.caption(
            "Reads the latest rows from the Gold Delta table on HDFS via a Spark client job (~30s)."
        )

        sample_cols = st.columns([1, 1, 2])
        with sample_cols[0]:
            sample_n = st.selectbox("Rows", [10, 20, 50], index=1, key="gold_sample_n")
        with sample_cols[1]:
            fetch_clicked = st.button("Fetch Gold Data", use_container_width=True)

        if fetch_clicked:
            with st.spinner("Running Spark job to read Gold Delta from HDFS..."):
                st.session_state["gold_sample_df"] = build_gold_sample_df(sample_n)

        gold_sample = st.session_state.get("gold_sample_df")
        if gold_sample is not None:
            st.dataframe(gold_sample, use_container_width=True, hide_index=True)
        return

    gold_controls = st.columns([1, 3])
    with gold_controls[0]:
        row_count = st.selectbox("Rows", [20, 50], index=0)
    with gold_controls[1]:
        st.caption("Placeholder preview of the latest Gold-layer rows")

    gold_df = build_gold_latest_df(row_count)
    st.dataframe(gold_df, width="stretch", hide_index=True)


def render_metrics_tab():
    st.subheader("Metrics")

    running = is_pipeline_running()
    k1, k2, k3, k4 = st.columns(4)

    if running:
        with k1:
            st.metric("Rows Processed", "12,480", "+640")
        with k2:
            st.metric("Pipeline Lag", "1.1s", "-0.2s")
        with k3:
            st.metric("Duplicate Rate", "0.3%", "-0.1%")
        with k4:
            st.metric("Last Gold Update", "now", "active")
    else:
        with k1:
            st.metric("Rows Processed", "0", "0")
        with k2:
            st.metric("Pipeline Lag", "0s", "0s")
        with k3:
            st.metric("Duplicate Rate", "0%", "0%")
        with k4:
            st.metric("Last Gold Update", "-", "inactive")

    st.markdown("#### Processing Trend")
    metrics_df = build_metrics_series()
    st.line_chart(metrics_df.set_index("time")[["rows_processed"]], height=250)

    st.markdown("#### Pipeline Lag Trend")
    st.line_chart(metrics_df.set_index("time")[["pipeline_lag_sec"]], height=250)


def render_ml_tab():
    if "ml_state" not in st.session_state:
        st.session_state.ml_state = __IDLE__

    if "last_prediction" not in st.session_state:
        st.session_state.last_prediction = {
            "timestamp": "-",
            "model_version": "-",
            "prediction_target": "-",
            "predicted_price": "-",
            "confidence": "-",
        }

    st.subheader("ML")

    ml_buttons = st.columns(3)

    with ml_buttons[0]:
        train_clicked = st.button("TRAIN MODEL", use_container_width=True)

    with ml_buttons[1]:
        predict_clicked = st.button("RUN PREDICTION", use_container_width=True)

    with ml_buttons[2]:
        reset_ml_clicked = st.button("RESET ML STATE", use_container_width=True)

    if train_clicked:
        st.session_state.ml_state = __RUNNING__
        st.session_state.stage_states["ML"] = __RUNNING__
        log("ML training started (placeholder)")

        st.session_state.ml_state = __IDLE__
        st.session_state.stage_states["ML"] = __IDLE__
        log("ML training completed (placeholder)")

    if predict_clicked:
        st.session_state.ml_state = __RUNNING__
        st.session_state.stage_states["ML"] = __RUNNING__
        log("Prediction job started (placeholder)")
        st.session_state.last_prediction = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "model_version": "v0.1-placeholder",
            "prediction_target": "BTCUSDT / next 1 day",
            "predicted_price": "69245.80",
            "confidence": "0.78",
        }
        st.session_state.ml_state = __IDLE__
        st.session_state.stage_states["ML"] = __IDLE__
        log("Prediction job completed (placeholder)")

    if reset_ml_clicked:
        st.session_state.ml_state = __IDLE__
        st.session_state.stage_states["ML"] = __IDLE__
        st.session_state.last_prediction = {
            "timestamp": "-",
            "model_version": "-",
            "prediction_target": "-",
            "predicted_price": "-",
            "confidence": "-",
        }
        log("ML state reset")

    ml_info = st.columns(4)

    with ml_info[0]:
        st.metric("ML State", st.session_state.ml_state)

    with ml_info[1]:
        st.metric("Model Version", st.session_state.last_prediction["model_version"])

    with ml_info[2]:
        st.metric(
            "Prediction Target", st.session_state.last_prediction["prediction_target"]
        )

    with ml_info[3]:
        st.metric("Confidence", st.session_state.last_prediction["confidence"])

    st.markdown("#### Latest Prediction")
    prediction_df = pd.DataFrame([st.session_state.last_prediction])
    st.dataframe(prediction_df, width="stretch", hide_index=True)


def render_tabs():
    # ---------- tabs ----------
    tabs = st.tabs(["Logs", "Gold Latest Rows", "Metrics", "ML"])

    with tabs[0]:
        render_logs_tab()

    with tabs[1]:
        render_gold_tab()

    with tabs[2]:
        render_metrics_tab()

    with tabs[3]:
        render_ml_tab()
