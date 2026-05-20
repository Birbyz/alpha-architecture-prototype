import pandas as pd
import streamlit as st

from datetime import datetime
from runtimes.ml import ml_predictor
from runtimes.snowflake.snowflake_client import SnowflakeClient
from runtimes.snowflake.snowflake_config import SnowflakeConfig
from utils.logging_utils import log
from constants import __IDLE__, __RUNNING__, HADOOP, SNOWFLAKE
from utils.data_builders import (
    build_gold_latest_df,
    build_gold_sample_df,
    build_hdfs_layer_stats,
    build_metrics_series,
    build_snowflake_gold_df,
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

    # HADOOP
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
    
    # SNOWFLAKE
    if runtime == SNOWFLAKE:
        config = SnowflakeConfig.from_env()
        st.caption(
            f"SNOWFLAKE runtime - data is stored in **{config.database}.{config.schema}**."
            f"Data is also accessible here - [Snowsight ↗](https://{config.account}.snowflakecomputing.com)."
        )
        
        # row count summary
        col_counts, col_fetch = st.columns([2, 1])
        with col_counts:
            if st.button("Refresh Table Row Counts", use_container_width=True):
                with st.spinner("Querying Snowflake..."):
                    _show_snowflake_table_counts(config)
                    
        sf_counts = st.session_state.get("sf_table_counts")
        if sf_counts is not None:
            st.dataframe(sf_counts, hide_index=True, use_container_width=True)
            
        st.divider()
        
        st.markdown("#### Latest Gold Rows")
        st.caption("Queries the Gold table directly via snowflake-connector-python.")
        
        fetch_cols = st.columns([1, 2])
        with fetch_cols[0]:
            gold_n = st.selectbox("Rows", [10, 20, 50], index=0, key="sf_gold_n")
        with fetch_cols[1]:
            fetch_gold_clicked = st.button("Fetch Gold Data", use_container_width=True)
            
        if fetch_gold_clicked:
            with st.spinner("Querying Snowflake for latest Gold rows..."):
                df = build_snowflake_gold_df(gold_n)
                st.session_state["sf_gold_df"] = df
        
        df = st.session_state.get("sf_gold_df")
        if df is not None:
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("Press **Fetch Gold Data** to query the latest rows from the Gold table in Snowflake.")
        return
        

    # LOCAL
    gold_controls = st.columns([1, 3])
    with gold_controls[0]:
        row_count = st.selectbox("Rows", [20, 50], index=0)
    with gold_controls[1]:
        st.caption("Placeholder preview of the latest Gold-layer rows")

    gold_df = build_gold_latest_df(row_count)
    st.dataframe(gold_df, width="stretch", hide_index=True)
    
def _show_snowflake_table_counts(config: SnowflakeConfig):
    try:
        client = SnowflakeClient(config)
        rows = []
        for layer, table in [
            ("Bronze", config.bronze_table),
            ("Silver", config.silver_table),
            ("Gold", config.gold_table)
        ]:
            count = client.get_table_row_count(table)
            rows.append({
                "Layer": layer,
                "Table": f"{config.database}.{config.schema}.{table}",
                "Rows": count if count is not None else "-"
            })
        st.session_state["sf_table_counts"] = pd.DataFrame(rows)
    except Exception as e:
        log(f"[HDMAS SNOWFLAKE] Error fetching table counts - [ERR]: {e}")
        st.session_state["sf_table_counts"] = pd.DataFrame([{"error": str(e)}])

# METRICS TAB
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


# ML tab
def _init_ml_state():
    if "ml_state" not in st.session_state:
        st.session_state["ml_state"] = __IDLE__
    if "ml_train_result" not in st.session_state:
        st.session_state["ml_train_result"] = None
    if "ml_predict_result" not in st.session_state:
        st.session_state["ml_predict_result"] = None
    if "ml_error" not in st.session_state:
        st.session_state["ml_error"] = None
   
    
def render_ml_tab():
    _init_ml_state()
    st.subheader("ML Predictions")
    st.caption(
        "Multi-horizon VWAP forecasting trained on the Gold layer. "
        "Predicts the next 5 / 15 / 30 minute VWAP and price direction using a Random Forest."
    )

    # control rows
    control_rows = st.columns([1, 1, 1, 2])
    with control_rows[0]:
        source = st.selectbox(
            "Data Source",
            ["snowflake", "local"],
            index=0,
            key="ml_source",
            help="Snowflake: queries the Gold table directy. Local reads Delta parquet files"
        )
        
    with control_rows[1]:
        train_clicked = st.button("Train Model", use_container_width=True, type="primary")
        
    with control_rows[2]:
        predict_clicked = st.button(
            "Run Prediction",
            use_container_width=True,
            disabled=not ml_predictor.is_model_trained()
        )
        
    with control_rows[3]:
        reset_clicked = st.button("Reset ML State", use_container_width=True)
        
    # ACTIONS
    if predict_clicked:
        st.session_state["ml_state"] = __RUNNING__
        st.session_state["ml_error"] = None
        log("[HDMAS ML] Running prediction on latest gold row...")
        with st.spinner("Running prediction..."):
            try:
                result = ml_predictor.predict()
                st.session_state["ml_predict_result"] = result
                st.session_state["ml_state"] = __IDLE__
                log(
                    f"[HDMAS ML] Prediction complete — current VWAP ${result['current_vwap']:,.2f}, "
                    f"5-min forecast ${result['predictions']['5']['predicted_vwap']:,.2f} "
                    f"({result['predictions']['5']['direction']} {result['predictions']['5']['confidence']}%)."
                )
            except Exception as e:
                st.session_state["ml_error"] = str(e)
                st.session_state["ml_state"] = __IDLE__
                log(f"[HDMAS ML] Prediction failed: {e}")
                
    if train_clicked:
        st.session_state["ml_state"] = __RUNNING__
        st.session_state["ml_error"] = None
        log(f"[HDMAS ML] Training started (source={source})...")
        with st.spinner("Training model — this may take a moment..."):
            try:
                meta = ml_predictor.train_model(source=source)
                st.session_state["ml_train_result"] = meta
                st.session_state["ml_state"] = __IDLE__
                log(
                    f"[HDMAS ML] Training complete — {meta['n_rows_engineered']} rows, "
                    f"version {meta['model_version']}."
                )
            except Exception as e:
                st.session_state["ml_error"] = str(e)
                st.session_state["ml_state"] = __IDLE__
                log(f"[HDMAS ML] Training failed: {e}")

    
    if reset_clicked:
        ml_predictor.reset()
        st.session_state["ml_state"] = __IDLE__
        st.session_state["ml_train_result"] = None
        st.session_state["ml_predict_result"] = None
        st.session_state["ml_error"] = None
        log("[HDMAS ML] State reset — model and metadata removed.")
        st.rerun()
        
        
    # error banner
    if st.session_state.get("ml_error"):
        st.error(f"**ML Error** {st.session_state['ml_error']}")
    st.divider()
    
    # load persisted metadata
    meta = st.session_state.get("ml_train_result") or ml_predictor.load_meta()
    
    # CASE: no model created
    if meta is None:
        st.info(
            "No trained model found. Select a data source and press **Train Model** to get started. "
            "The model learns from your Gold layer VWAP time series and predicts future prices."
        )
        return

    # model summary cards
    st.markdown("#### Model")
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Version", meta.get("model_version", "-"))
    with m2:
        st.metric("Trained at", meta.get("trained_at", "-"))
    with m3:
        st.metric("Training rows", f"{meta.get('n_train', '-'):,}" if isinstance(meta.get("n_train"), int) else "-")
    with m4:
        st.metric("Source", meta.get("source", "-").capitalize())

    
    # metrics
        st.markdown("#### Evaluation metrics (test set)")
    raw_metrics = meta.get("metrics", {})
    horizons_labels = {"5": "5 min", "15": "15 min", "30": "30 min"}
    h_cols = st.columns(len(horizons_labels))
 
    for i, (h_key, h_label) in enumerate(horizons_labels.items()):
        hm = raw_metrics.get(h_key, {})
        with h_cols[i]:
            st.markdown(f"**{h_label}**")
            st.metric("MAE", f"${hm.get('mae', '-'):,.2f}" if isinstance(hm.get("mae"), (int, float)) else "-")
            st.metric("RMSE", f"${hm.get('rmse', '-'):,.2f}" if isinstance(hm.get("rmse"), (int, float)) else "-")
            r2_val = hm.get("r2")
            st.metric("R²", f"{r2_val:.4f}" if isinstance(r2_val, (int, float)) else "-")

    
    # test-set forecast chart
    chart_data = meta.get("chart", {})
    if chart_data.get("actual") and chart_data.get("pred_5min"):
        st.markdown("#### Actual vs predicted VWAP (test set, 5-min horizon)")
        chart_df = pd.DataFrame(
            {
                "Actual VWAP": chart_data["actual"],
                "Predicted VWAP (5 min)": chart_data["pred_5min"],
            },
            index=chart_data.get("timestamps", range(len(chart_data["actual"]))),
        )
        st.line_chart(chart_df, height=280)

    # feature importance
    fi = meta.get("feature_importance", {})
    if fi:
        st.markdown("#### Feature importance (5-min model)")
        fi_df = (
            pd.DataFrame({"Feature": list(fi.keys()), "Importance": list(fi.values())})
            .sort_values("Importance", ascending=False)
            .head(15)
            .reset_index(drop=True)
        )
        st.bar_chart(fi_df.set_index("Feature")["Importance"], height=260)

    # latest predictions
    pred_result = st.session_state.get("ml_predict_result")
    if pred_result is None:
        st.divider()
        st.info("Press **Run Prediction** to forecast the next 5 / 15 / 30 minutes.")
        return
 
    st.divider()
    st.markdown("#### Latest prediction")
 
    p_cols = st.columns(4)
    with p_cols[0]:
        st.metric("Predicted at", pred_result.get("timestamp", "-"))
    with p_cols[1]:
        st.metric("Current VWAP", f"${pred_result['current_vwap']:,.2f}")
    with p_cols[2]:
        st.metric("Model", pred_result.get("model_version", "-"))
    with p_cols[3]:
        st.metric("Source", pred_result.get("source", "-").capitalize())
 
    st.markdown("#### Per-horizon forecasts")
    pred_cols = st.columns(3)
    horizon_map = [("5", "5 min"), ("15", "15 min"), ("30", "30 min")]
 
    for col, (h_key, h_label) in zip(pred_cols, horizon_map):
        hp = pred_result.get("predictions", {}).get(h_key, {})
        direction = hp.get("direction", "?")
        confidence = hp.get("confidence", 0)
        predicted = hp.get("predicted_vwap", 0)
        std = hp.get("std", 0)
        lower = hp.get("lower", 0)
        upper = hp.get("upper", 0)
 
        arrow = "↑" if direction == "UP" else "↓"
        delta_usd = predicted - pred_result["current_vwap"]
        delta_str = f"{'+' if delta_usd >= 0 else ''}{delta_usd:,.2f}"
 
        with col:
            st.markdown(f"**{h_label}**")
            st.metric(
                label="VWAP forecast",
                value=f"${predicted:,.2f}",
                delta=delta_str,
            )
            st.markdown(
                f"{arrow} **{direction}** &nbsp; `{confidence:.1f}% confidence`",
                unsafe_allow_html=True,
            )
            st.caption(f"±${std:,.2f} std  |  range ${lower:,.2f} - ${upper:,.2f}")


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