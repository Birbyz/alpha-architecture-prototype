import glob
import json
import os
import joblib
import logging as _logging

from datetime import datetime
from typing import Optional, Callable, Dict, Any

import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.metrics import (
    mean_squared_error,
    r2_score,
    mean_absolute_error,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)

from constants import SNOWFLAKE
from runtimes.snowflake.snowflake_client import SnowflakeClient
from runtimes.snowflake.snowflake_config import SnowflakeConfig


# --------------------------------------------------
# Paths
# --------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(_HERE, "model.joblib")
META_PATH = os.path.join(_HERE, "model_meta.json")


# --------------------------------------------------
# Model purpose
# --------------------------------------------------
# This model no longer tries to predict only the absolute BTC VWAP price.
# For unstable regimes, absolute price prediction can fail badly because the
# model learns a price level instead of movement/risk.
#
# It trains two models per horizon:
#   1. Return regressor: predicts future log return.
#   2. Drop classifier: predicts probability of a dangerous drop.
#
# The UI/API can still display a predicted future VWAP, derived as:
#   predicted_vwap = current_vwap * exp(predicted_log_return)


# --------------------------------------------------
# Adaptive parameter tiers
# --------------------------------------------------
_TIERS = [
    {
        "name": "full",
        "min_raw": 500,
        "min_engineered": 80,
        "lag_steps": 30,
        "vol_lag_steps": 10,
        "rolling_windows": [5, 15, 30, 60],
        "horizons": [5, 15, 30],
        "min_periods": None,
        "training_window_days": 30,
    },
    {
        "name": "medium",
        "min_raw": 150,
        "min_engineered": 40,
        "lag_steps": 10,
        "vol_lag_steps": 5,
        "rolling_windows": [5, 15, 30],
        "horizons": [5, 15, 30],
        "min_periods": 3,
        "training_window_days": 14,
    },
    {
        "name": "sparse",
        "min_raw": 0,
        "min_engineered": 20,
        "lag_steps": 5,
        "vol_lag_steps": 3,
        "rolling_windows": [3, 5, 10],
        "horizons": [1, 3, 5],
        "min_periods": 1,
        "training_window_days": None,
    },
]


# Backward-compatible module-level constants
HORIZONS = [5, 15, 30]
LAG_STEPS = 30
VOL_LAG_STEPS = 10
ROLLING_WINDOWS = [5, 15, 30, 60]
N_ESTIMATORS_REGRESSOR = 250
N_ESTIMATORS_CLASSIFIER = 300
MAX_DEPTH_REGRESSOR = 12
MAX_DEPTH_CLASSIFIER = 10
RANDOM_STATE = 42


# Dangerous-drop labels.
# Tune these for your app. These defaults are intentionally moderate:
#   5 min:  -0.30%
#   15 min: -0.75%
#   30 min: -1.50%
DROP_THRESHOLDS = {
    1: -0.0010,
    3: -0.0020,
    5: -0.0030,
    15: -0.0075,
    30: -0.0150,
}


# Alert thresholds based on classifier probability.
RISK_PROBABILITIES = {
    "LOW": 0.00,
    "MEDIUM": 0.45,
    "HIGH": 0.65,
    "CRITICAL": 0.80,
}


# --------------------------------------------------
# Utility helpers
# --------------------------------------------------
def _get_tier(n_rows: int) -> dict:
    for tier in _TIERS:
        if n_rows >= tier["min_raw"]:
            return tier
    return _TIERS[-1]


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.replace(0, np.nan)
    return numerator / denominator


def _clean_infinite_values(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace([np.inf, -np.inf], np.nan)


def _risk_label(probability: float) -> str:
    if probability >= RISK_PROBABILITIES["CRITICAL"]:
        return "CRITICAL"
    if probability >= RISK_PROBABILITIES["HIGH"]:
        return "HIGH"
    if probability >= RISK_PROBABILITIES["MEDIUM"]:
        return "MEDIUM"
    return "LOW"


def _drop_threshold_for_horizon(horizon: int) -> float:
    # Fallback scales approximately by horizon if a custom horizon is used.
    return DROP_THRESHOLDS.get(horizon, -0.0005 * horizon)


# --------------------------------------------------
# Data loading
# --------------------------------------------------
def _load_snowflake_environment() -> pd.DataFrame:
    config = SnowflakeConfig.from_env()
    if not config.is_configured:
        raise RuntimeError("Snowflake configuration is missing.")

    client = SnowflakeClient(config)
    rows = client.query_gold_data_for_ml(config.gold_table)
    if not rows:
        raise ValueError("No data returned from Snowflake query.")

    df = pd.DataFrame(rows)
    df.columns = [c.lower() for c in df.columns]
    return df


def _load_local_environment() -> pd.DataFrame:
    gold_path = os.getenv("DELTA_PATH_GOLD", "/data/gold/delta")

    if not os.path.exists(gold_path):
        project_root = os.getenv("HDMAS_PROJECT_ROOT", "")
        if project_root:
            gold_path = os.path.join(
                project_root, "docker", "volumes", "data", "gold", "delta"
            )

    if not os.path.exists(gold_path):
        raise FileNotFoundError(
            f"Gold delta path not found at '{gold_path}'. "
            "Ensure HDMAS_PROJECT_ROOT is set in your .env and the Gold pipeline "
            "has produced at least one parquet file."
        )

    files = glob.glob(os.path.join(gold_path, "**", "*.parquet"), recursive=True)
    if not files:
        raise FileNotFoundError(f"No parquet files found under '{gold_path}'.")

    parts = []
    for path in files:
        part_df = pd.read_parquet(path)
        parent_dir = os.path.basename(os.path.dirname(path))
        if "=" in parent_dir:
            key, val = parent_dir.split("=", 1)
            part_df[key.lower()] = val
        parts.append(part_df)

    df = pd.concat(parts, ignore_index=True)
    df.columns = [c.lower() for c in df.columns]
    return df


def load_gold_data(source: str = SNOWFLAKE.lower()) -> pd.DataFrame:
    df = (
        _load_snowflake_environment()
        if source == SNOWFLAKE.lower()
        else _load_local_environment()
    )

    if "aggregation_window_start" not in df.columns:
        raise ValueError("Gold data must contain 'aggregation_window_start'.")
    if "vwap_price_usd" not in df.columns:
        raise ValueError("Gold data must contain 'vwap_price_usd'.")

    df["aggregation_window_start"] = pd.to_datetime(df["aggregation_window_start"])

    numeric_cols = [
        "vwap_price_usd",
        "volume_btc",
        "volume_usd",
        "trade_count",
        "sell_volume_btc",
        "buy_volume_btc",
        "sell_pressure",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # These fields may not exist yet if your Gold layer has not been upgraded.
    # Add neutral defaults so the predictor remains compatible with your current table.
    if "volume_btc" not in df.columns:
        df["volume_btc"] = 0.0
    if "volume_usd" not in df.columns:
        df["volume_usd"] = 0.0
    if "trade_count" not in df.columns:
        df["trade_count"] = 0.0
    if "sell_pressure" not in df.columns:
        df["sell_pressure"] = 0.5
    if "sell_volume_btc" not in df.columns:
        df["sell_volume_btc"] = df["volume_btc"] * df["sell_pressure"]
    if "buy_volume_btc" not in df.columns:
        df["buy_volume_btc"] = df["volume_btc"] * (1.0 - df["sell_pressure"])

    df = (
        df.sort_values("aggregation_window_start")
        .drop_duplicates(subset=["aggregation_window_start"])
        .dropna(subset=["vwap_price_usd"])
        .reset_index(drop=True)
    )

    # Avoid invalid prices.
    df = df[df["vwap_price_usd"] > 0].reset_index(drop=True)
    return df


def _filter_recent_training_window(df: pd.DataFrame, tier: dict) -> pd.DataFrame:
    training_window_days = tier.get("training_window_days")
    if not training_window_days:
        return df

    max_ts = df["aggregation_window_start"].max()
    cutoff = max_ts - pd.Timedelta(days=training_window_days)
    recent = df[df["aggregation_window_start"] >= cutoff].reset_index(drop=True)

    # If the recent slice is too small, use all data rather than failing.
    if len(recent) >= tier["min_raw"]:
        return recent
    return df


# --------------------------------------------------
# Feature engineering
# --------------------------------------------------
def _feature_columns(tier: Optional[dict] = None) -> list:
    if tier is None:
        tier = _TIERS[0]

    cols = []

    for lag in range(1, tier["lag_steps"] + 1):
        cols.append(f"vwap_lag_{lag}")
        cols.append(f"return_lag_{lag}")

    for lag in range(1, tier["vol_lag_steps"] + 1):
        cols.append(f"volume_btc_lag_{lag}")
        cols.append(f"trade_count_lag_{lag}")
        cols.append(f"sell_pressure_lag_{lag}")

    for w in tier["rolling_windows"]:
        cols += [
            f"vwap_roll_mean_{w}",
            f"vwap_roll_std_{w}",
            f"vwap_zscore_{w}",
            f"return_mean_{w}",
            f"return_std_{w}",
            f"volume_zscore_{w}",
            f"trade_count_zscore_{w}",
            f"sell_pressure_mean_{w}",
        ]

    cols += [
        "log_price",
        "return_1",
        "return_3",
        "return_5",
        "return_15",
        "return_30",
        "vwap_pct_change_1",
        "vwap_pct_change_5",
        "trade_count_delta",
        "volume_btc_pct_change",
        "volume_usd_pct_change",
        "buy_sell_imbalance",
        "sell_pressure",
        "hour_sin",
        "hour_cos",
        "minute_sin",
        "minute_cos",
        "day_sin",
        "day_cos",
    ]

    return cols


def engineer_features(df: pd.DataFrame, tier: Optional[dict] = None) -> pd.DataFrame:
    if tier is None:
        tier = _TIERS[0]

    df = df.copy()
    p = "vwap_price_usd"
    v = "volume_btc"
    vu = "volume_usd"
    c = "trade_count"
    sp = "sell_pressure"
    mp = tier["min_periods"]

    required = ["aggregation_window_start", p, v, vu, c, sp, "sell_volume_btc", "buy_volume_btc"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after loading defaults: {missing}")

    df[p] = pd.to_numeric(df[p], errors="coerce")
    df[v] = pd.to_numeric(df[v], errors="coerce").fillna(0.0)
    df[vu] = pd.to_numeric(df[vu], errors="coerce").fillna(0.0)
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df[sp] = pd.to_numeric(df[sp], errors="coerce").fillna(0.5).clip(0.0, 1.0)

    df["log_price"] = np.log(df[p])
    df["return_1"] = np.log(df[p] / df[p].shift(1))
    df["return_3"] = np.log(df[p] / df[p].shift(3))
    df["return_5"] = np.log(df[p] / df[p].shift(5))
    df["return_15"] = np.log(df[p] / df[p].shift(15))
    df["return_30"] = np.log(df[p] / df[p].shift(30))

    for lag in range(1, tier["lag_steps"] + 1):
        df[f"vwap_lag_{lag}"] = df[p].shift(lag)
        df[f"return_lag_{lag}"] = df["return_1"].shift(lag)

    for lag in range(1, tier["vol_lag_steps"] + 1):
        df[f"volume_btc_lag_{lag}"] = df[v].shift(lag)
        df[f"trade_count_lag_{lag}"] = df[c].shift(lag)
        df[f"sell_pressure_lag_{lag}"] = df[sp].shift(lag)

    shifted_price = df[p].shift(1)
    shifted_return = df["return_1"].shift(1)
    shifted_volume = df[v].shift(1)
    shifted_trade_count = df[c].shift(1)
    shifted_sell_pressure = df[sp].shift(1)

    for w in tier["rolling_windows"]:
        roll_min = mp if mp else w

        price_mean = shifted_price.rolling(window=w, min_periods=roll_min).mean()
        price_std = shifted_price.rolling(window=w, min_periods=roll_min).std()
        return_mean = shifted_return.rolling(window=w, min_periods=roll_min).mean()
        return_std = shifted_return.rolling(window=w, min_periods=roll_min).std()
        volume_mean = shifted_volume.rolling(window=w, min_periods=roll_min).mean()
        volume_std = shifted_volume.rolling(window=w, min_periods=roll_min).std()
        trade_mean = shifted_trade_count.rolling(window=w, min_periods=roll_min).mean()
        trade_std = shifted_trade_count.rolling(window=w, min_periods=roll_min).std()

        df[f"vwap_roll_mean_{w}"] = price_mean
        df[f"vwap_roll_std_{w}"] = price_std
        df[f"vwap_zscore_{w}"] = _safe_divide(df[p] - price_mean, price_std)
        df[f"return_mean_{w}"] = return_mean
        df[f"return_std_{w}"] = return_std
        df[f"volume_zscore_{w}"] = _safe_divide(df[v] - volume_mean, volume_std)
        df[f"trade_count_zscore_{w}"] = _safe_divide(df[c] - trade_mean, trade_std)
        df[f"sell_pressure_mean_{w}"] = shifted_sell_pressure.rolling(
            window=w, min_periods=roll_min
        ).mean()

    df["vwap_pct_change_1"] = df[p].pct_change(1)
    df["vwap_pct_change_5"] = df[p].pct_change(5)
    df["trade_count_delta"] = df[c].diff(1)
    df["volume_btc_pct_change"] = df[v].pct_change(1)
    df["volume_usd_pct_change"] = df[vu].pct_change(1)

    df["buy_sell_imbalance"] = _safe_divide(
        df["buy_volume_btc"] - df["sell_volume_btc"],
        df["buy_volume_btc"] + df["sell_volume_btc"],
    )

    ts = df["aggregation_window_start"]
    hour = ts.dt.hour
    minute = ts.dt.minute
    day = ts.dt.dayofweek

    # Cyclical timestamp features are safer than raw hour/minute integers.
    df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    df["minute_sin"] = np.sin(2 * np.pi * minute / 60)
    df["minute_cos"] = np.cos(2 * np.pi * minute / 60)
    df["day_sin"] = np.sin(2 * np.pi * day / 7)
    df["day_cos"] = np.cos(2 * np.pi * day / 7)

    # Targets: future returns and dangerous-drop labels.
    for h in tier["horizons"]:
        future_price = df[p].shift(-h)
        future_log_return = np.log(future_price / df[p])
        threshold = _drop_threshold_for_horizon(h)

        df[f"target_return_{h}"] = future_log_return
        df[f"target_vwap_{h}"] = future_price
        df[f"target_drop_{h}"] = (future_log_return <= threshold).astype(float)

    df = _clean_infinite_values(df)
    df = df.dropna().reset_index(drop=True)

    return df


# --------------------------------------------------
# Training weights
# --------------------------------------------------
def _recency_weights(df: pd.DataFrame, half_life_days: float = 7.0) -> pd.Series:
    max_ts = df["aggregation_window_start"].max()
    age_days = (max_ts - df["aggregation_window_start"]).dt.total_seconds() / 86400.0
    weights = np.exp(-age_days / half_life_days)
    return pd.Series(weights, index=df.index).clip(lower=0.05)


# --------------------------------------------------
# Metrics
# --------------------------------------------------
def _regression_metrics(y_true: pd.Series, y_pred: np.ndarray) -> Dict[str, float]:
    return {
        "mae_return": round(float(mean_absolute_error(y_true, y_pred)), 8),
        "rmse_return": round(float(mean_squared_error(y_true, y_pred) ** 0.5), 8),
        "r2": round(float(r2_score(y_true, y_pred)), 4),
    }


def _classifier_metrics(y_true: pd.Series, y_prob: np.ndarray) -> Dict[str, Any]:
    y_pred = (y_prob >= 0.5).astype(int)

    metrics: Dict[str, Any] = {
        "positive_rate": round(float(np.mean(y_true)), 4),
        "precision": round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_true, y_pred, zero_division=0)), 4),
        "f1": round(float(f1_score(y_true, y_pred, zero_division=0)), 4),
    }

    # ROC AUC is undefined if y_true has only one class.
    if len(np.unique(y_true)) >= 2:
        metrics["roc_auc"] = round(float(roc_auc_score(y_true, y_prob)), 4)
    else:
        metrics["roc_auc"] = None

    return metrics


# --------------------------------------------------
# Train
# --------------------------------------------------
def train_model(
    source: str = SNOWFLAKE.lower(),
    on_progress: Optional[Callable[[int, int, str], None]] = None,
) -> dict:
    def _progress(step: int, total: int, msg: str) -> None:
        if on_progress:
            on_progress(step, total, msg)

    _progress(1, 5, f"Loading gold data from {source.capitalize()}...")
    df_raw_all = load_gold_data(source)

    tier = _get_tier(len(df_raw_all))
    df_raw = _filter_recent_training_window(df_raw_all, tier)

    total_steps = len(tier["horizons"]) * 2 + 3

    _progress(
        1,
        total_steps,
        f"Loaded {len(df_raw_all)} gold rows, training on {len(df_raw)} rows — tier: {tier['name']}...",
    )

    _logging.getLogger(__name__).info(
        "[HDMAS ML] Loaded %d raw gold rows from %s; training rows=%d; tier=%s",
        len(df_raw_all),
        source,
        len(df_raw),
        tier["name"],
    )

    df = engineer_features(df_raw, tier)

    if len(df) < tier["min_engineered"]:
        raise ValueError(
            f"Only {len(df)} usable rows after feature engineering "
            f"({len(df_raw)} raw rows from {source}, tier='{tier['name']}'). "
            f"Need at least {tier['min_engineered']} engineered rows for this tier. "
            "Run the Gold pipeline longer or reduce feature windows."
        )

    feature_cols = _feature_columns(tier)
    missing_feature_cols = [col for col in feature_cols if col not in df.columns]
    if missing_feature_cols:
        raise ValueError(f"Missing engineered feature columns: {missing_feature_cols}")

    x = df[feature_cols]

    split_idx = int(len(df) * 0.80)
    split_idx = min(max(split_idx, 1), len(df) - 1)

    x_train = x.iloc[:split_idx]
    x_test = x.iloc[split_idx:]

    weights = _recency_weights(df)
    train_weights = weights.iloc[:split_idx]

    regressors: Dict[int, RandomForestRegressor] = {}
    classifiers: Dict[int, RandomForestClassifier] = {}
    metrics: Dict[int, Dict[str, Any]] = {}

    step = 2
    for h in tier["horizons"]:
        _progress(step, total_steps, f"Training {h}-min return regressor...")

        y_train_return = df[f"target_return_{h}"].iloc[:split_idx]
        y_test_return = df[f"target_return_{h}"].iloc[split_idx:]

        regressor = RandomForestRegressor(
            n_estimators=N_ESTIMATORS_REGRESSOR,
            max_depth=MAX_DEPTH_REGRESSOR,
            min_samples_leaf=max(1, split_idx // 50),
            random_state=RANDOM_STATE,
            n_jobs=1,
        )
        regressor.fit(x_train, y_train_return, sample_weight=train_weights)
        pred_return = regressor.predict(x_test)
        regressors[h] = regressor

        step += 1
        _progress(step, total_steps, f"Training {h}-min drop-risk classifier...")

        y_train_drop = df[f"target_drop_{h}"].iloc[:split_idx].astype(int)
        y_test_drop = df[f"target_drop_{h}"].iloc[split_idx:].astype(int)

        classifier = RandomForestClassifier(
            n_estimators=N_ESTIMATORS_CLASSIFIER,
            max_depth=MAX_DEPTH_CLASSIFIER,
            min_samples_leaf=max(1, split_idx // 50),
            class_weight="balanced_subsample",
            random_state=RANDOM_STATE,
            n_jobs=1,
        )

        classifier.fit(x_train, y_train_drop, sample_weight=train_weights)

        if len(classifier.classes_) == 1:
            # Extremely rare in normal data, but can happen with tiny/sparse samples.
            # If only one class exists, fake a deterministic probability.
            only_class = int(classifier.classes_[0])
            drop_prob = np.ones(len(x_test)) if only_class == 1 else np.zeros(len(x_test))
        else:
            positive_class_index = list(classifier.classes_).index(1)
            drop_prob = classifier.predict_proba(x_test)[:, positive_class_index]

        classifiers[h] = classifier

        # Keep the original UI/API metrics (mae, rmse, r2) in BTC/USD terms,
        # while also exposing the new return-regression and drop-classifier metrics.
        current_test_vwap = df["vwap_price_usd"].iloc[split_idx:].to_numpy()
        y_test_vwap = df[f"target_vwap_{h}"].iloc[split_idx:]
        pred_vwap = current_test_vwap * np.exp(pred_return)

        metrics[h] = {
            # Backward-compatible metrics expected by the existing output/UI.
            "mae": round(float(mean_absolute_error(y_test_vwap, pred_vwap)), 2),
            "rmse": round(float(mean_squared_error(y_test_vwap, pred_vwap) ** 0.5), 2),
            "r2": round(float(r2_score(y_test_vwap, pred_vwap)), 4),

            # Additional metrics for the improved predictor.
            "return_regression": _regression_metrics(y_test_return, pred_return),
            "drop_classifier": _classifier_metrics(y_test_drop, drop_prob),
            "drop_threshold_return": _drop_threshold_for_horizon(h),
            "drop_threshold_pct": round(_drop_threshold_for_horizon(h) * 100, 4),
        }

        step += 1

    _progress(step, total_steps, "Saving model and metadata...")

    primary_horizon = tier["horizons"][0]
    primary_regressor = regressors[primary_horizon]
    primary_classifier = classifiers[primary_horizon]

    regressor_feature_importance = {
        col: round(float(imp), 6)
        for col, imp in zip(feature_cols, primary_regressor.feature_importances_)
    }
    classifier_feature_importance = {
        col: round(float(imp), 6)
        for col, imp in zip(feature_cols, primary_classifier.feature_importances_)
    }

    joblib.dump(
        {
            "regressors": regressors,
            "classifiers": classifiers,
            "tier": tier,
            "feature_cols": feature_cols,
            "drop_thresholds": DROP_THRESHOLDS,
            "risk_probabilities": RISK_PROBABILITIES,
        },
        MODEL_PATH,
    )

    chart_slice = slice(max(0, len(x_test) - 120), None)
    chart_df = df.iloc[split_idx:].iloc[chart_slice]
    chart_x = x_test.iloc[chart_slice]

    chart_pred_return = regressors[primary_horizon].predict(chart_x)
    chart_current = chart_df["vwap_price_usd"].to_numpy()
    chart_pred_vwap = chart_current * np.exp(chart_pred_return)

    if len(classifiers[primary_horizon].classes_) == 1:
        only_class = int(classifiers[primary_horizon].classes_[0])
        chart_drop_prob = (
            np.ones(len(chart_x)) if only_class == 1 else np.zeros(len(chart_x))
        )
    else:
        positive_class_index = list(classifiers[primary_horizon].classes_).index(1)
        chart_drop_prob = classifiers[primary_horizon].predict_proba(chart_x)[:, positive_class_index]

    version = f"v2.0-return-risk-rf-{datetime.now().strftime('%Y%m%d-%H%M')}"

    sparse_warning = None
    if tier["name"] != "full":
        sparse_warning = (
            f"Limited data ({len(df_raw)} raw rows). Trained in '{tier['name']}' mode — "
            f"horizons: {tier['horizons']} min. "
            "For better accuracy, run the Gold pipeline until more 1-minute rows accumulate."
        )

    meta = {
        "trained_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": source,
        "tier": tier["name"],
        "horizons": tier["horizons"],
        "training_window_days": tier.get("training_window_days"),
        # Backward-compatible row fields expected by the existing output/UI.
        "n_rows_total": len(df_raw),
        "n_rows_total_loaded": len(df_raw_all),
        "n_rows_total_used": len(df_raw),
        "n_rows_engineered": len(df),
        "n_train": split_idx,
        "n_test": len(df) - split_idx,
        "model_version": version,
        "model_type": "return_regressor_plus_drop_classifier",
        "sparse_warning": sparse_warning,
        "drop_thresholds": {str(k): v for k, v in DROP_THRESHOLDS.items()},
        "risk_probabilities": RISK_PROBABILITIES,
        "metrics": {str(k): v for k, v in metrics.items()},
        # Backward-compatible feature_importance expected by the old output/UI.
        "feature_importance": regressor_feature_importance,
        "regressor_feature_importance": regressor_feature_importance,
        "classifier_feature_importance": classifier_feature_importance,
        "chart": {
            # Backward-compatible chart fields expected by the old output/UI.
            "timestamps": chart_df["aggregation_window_start"].dt.strftime("%H:%M").tolist(),
            "actual": chart_df[f"target_vwap_{primary_horizon}"].round(2).tolist(),
            "pred_5min": np.round(chart_pred_vwap, 2).tolist(),

            # Additional chart fields for the improved predictor.
            "horizon": primary_horizon,
            "current_vwap": np.round(chart_current, 2).tolist(),
            "actual_future_vwap": chart_df[f"target_vwap_{primary_horizon}"].round(2).tolist(),
            "predicted_future_vwap": np.round(chart_pred_vwap, 2).tolist(),
            "drop_probability_pct": np.round(chart_drop_prob * 100, 1).tolist(),
        },
    }

    with open(META_PATH, "w") as fh:
        json.dump(meta, fh, indent=2)

    _progress(total_steps, total_steps, "Training complete.")
    return meta


# --------------------------------------------------
# Predict
# --------------------------------------------------
def predict() -> dict:
    if not is_model_trained():
        raise FileNotFoundError("No trained model found. Train model first.")

    meta = load_meta()
    assert meta is not None, "model_meta.json missing despite is_model_trained() returning True"

    source = meta.get("source", SNOWFLAKE.lower())
    saved = joblib.load(MODEL_PATH)

    regressors: Dict[int, RandomForestRegressor] = saved["regressors"]
    classifiers: Dict[int, RandomForestClassifier] = saved["classifiers"]
    tier = saved["tier"]
    feature_cols = saved.get("feature_cols", _feature_columns(tier))

    df_raw = load_gold_data(source)
    df_raw = _filter_recent_training_window(df_raw, tier)
    df = engineer_features(df_raw, tier)

    if df.empty:
        raise ValueError("Feature engineering produced an empty dataset.")

    latest_x = df[feature_cols].iloc[[-1]]
    latest_row = df.iloc[-1]
    current_vwap = float(latest_row["vwap_price_usd"])
    latest_data_ts = latest_row["aggregation_window_start"]

    predictions: Dict[str, Dict[str, Any]] = {}

    for h in tier["horizons"]:
        regressor = regressors[h]
        classifier = classifiers[h]

        # Regressor uncertainty from individual trees.
        tree_returns = np.array([tree.predict(latest_x)[0] for tree in regressor.estimators_])
        pred_return_mean = float(np.mean(tree_returns))
        pred_return_std = float(np.std(tree_returns))

        predicted_tree_vwaps = current_vwap * np.exp(tree_returns)
        predicted_vwap = float(np.mean(predicted_tree_vwaps))
        price_std = float(np.std(predicted_tree_vwaps))
        lower_vwap = predicted_vwap - price_std
        upper_vwap = predicted_vwap + price_std

        direction = "UP" if predicted_vwap > current_vwap else "DOWN"
        confidence = (
            float(np.mean(predicted_tree_vwaps > current_vwap))
            if direction == "UP"
            else float(np.mean(predicted_tree_vwaps <= current_vwap))
        )

        if len(classifier.classes_) == 1:
            only_class = int(classifier.classes_[0])
            drop_probability = 1.0 if only_class == 1 else 0.0
        else:
            positive_class_index = list(classifier.classes_).index(1)
            drop_probability = float(classifier.predict_proba(latest_x)[0, positive_class_index])

        threshold = _drop_threshold_for_horizon(h)
        risk = _risk_label(drop_probability)

        predictions[str(h)] = {
            # Backward-compatible fields expected by the existing output/UI.
            "predicted_vwap": round(predicted_vwap, 2),
            "std": round(price_std, 2),
            "direction": direction,
            "confidence": round(confidence * 100, 1),
            "lower": round(lower_vwap, 2),
            "upper": round(upper_vwap, 2),

            # Additional fields for the improved risk predictor.
            "predicted_return_pct": round(pred_return_mean * 100, 4),
            "return_std_pct": round(pred_return_std * 100, 4),
            "drop_probability": round(drop_probability * 100, 1),
            "drop_threshold_pct": round(threshold * 100, 4),
            "risk": risk,
            "alert": risk in {"HIGH", "CRITICAL"},
        }

    highest_risk = max(
        predictions.values(),
        key=lambda item: item["drop_probability"],
    )

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latest_data_timestamp": latest_data_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "current_vwap": round(current_vwap, 2),
        "predictions": predictions,
        "overall_risk": highest_risk["risk"],
        "should_warn_user": any(item["alert"] for item in predictions.values()),
        "horizons": tier["horizons"],
        "tier": tier["name"],
        "model_version": meta.get("model_version", "-"),
        "source": source,
    }


# --------------------------------------------------
# Optional backtesting helper
# --------------------------------------------------
def backtest_latest_model(source: Optional[str] = None) -> dict:
    """
    Lightweight backtest using the saved model and the latest available Gold data.
    Useful for debugging whether alerts are too noisy or too quiet.
    """
    if not is_model_trained():
        raise FileNotFoundError("No trained model found. Train model first.")

    meta = load_meta()
    assert meta is not None

    saved = joblib.load(MODEL_PATH)
    regressors = saved["regressors"]
    classifiers = saved["classifiers"]
    tier = saved["tier"]
    feature_cols = saved.get("feature_cols", _feature_columns(tier))

    use_source = source or meta.get("source", SNOWFLAKE.lower())
    df_raw = load_gold_data(use_source)
    df = engineer_features(df_raw, tier)

    if len(df) < tier["min_engineered"]:
        raise ValueError("Not enough engineered rows to backtest.")

    x = df[feature_cols]
    results: Dict[str, Any] = {}

    for h in tier["horizons"]:
        regressor = regressors[h]
        classifier = classifiers[h]

        y_return = df[f"target_return_{h}"]
        y_drop = df[f"target_drop_{h}"].astype(int)

        pred_return = regressor.predict(x)

        if len(classifier.classes_) == 1:
            only_class = int(classifier.classes_[0])
            drop_prob = np.ones(len(x)) if only_class == 1 else np.zeros(len(x))
        else:
            positive_class_index = list(classifier.classes_).index(1)
            drop_prob = classifier.predict_proba(x)[:, positive_class_index]

        high_alert = (drop_prob >= RISK_PROBABILITIES["HIGH"]).astype(int)

        results[str(h)] = {
            "return_regression": _regression_metrics(y_return, pred_return),
            "drop_classifier_50pct_threshold": _classifier_metrics(y_drop, drop_prob),
            "high_alert_precision": round(
                float(precision_score(y_drop, high_alert, zero_division=0)), 4
            ),
            "high_alert_recall": round(
                float(recall_score(y_drop, high_alert, zero_division=0)), 4
            ),
            "high_alert_count": int(high_alert.sum()),
            "actual_drop_count": int(y_drop.sum()),
        }

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": use_source,
        "tier": tier["name"],
        "n_rows_engineered": len(df),
        "results": results,
    }


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def is_model_trained() -> bool:
    return os.path.exists(MODEL_PATH) and os.path.exists(META_PATH)


def load_meta() -> Optional[dict]:
    if not os.path.exists(META_PATH):
        return None
    with open(META_PATH) as fh:
        return json.load(fh)


def reset() -> None:
    for path in (MODEL_PATH, META_PATH):
        if os.path.exists(path):
            os.remove(path)


if __name__ == "__main__":
    # Simple manual run:
    #   python ml_predictor.py train local
    #   python ml_predictor.py predict
    #   python ml_predictor.py backtest local
    import sys

    command = sys.argv[1].lower() if len(sys.argv) > 1 else "predict"
    cli_source = sys.argv[2].lower() if len(sys.argv) > 2 else SNOWFLAKE.lower()

    if command == "train":
        print(json.dumps(train_model(source=cli_source), indent=2))
    elif command == "predict":
        print(json.dumps(predict(), indent=2))
    elif command == "backtest":
        print(json.dumps(backtest_latest_model(source=cli_source), indent=2))
    elif command == "reset":
        reset()
        print("Model files removed.")
    else:
        raise SystemExit(f"Unknown command: {command}")
