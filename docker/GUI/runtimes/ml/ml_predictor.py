import glob
import json
import os
import joblib
import numpy as np
import pandas as pd
import logging as _logging

from datetime import datetime
from typing import Optional, Callable
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error

from constants import SNOWFLAKE
from runtimes.snowflake.snowflake_client import SnowflakeClient
from runtimes.snowflake.snowflake_config import SnowflakeConfig


# --------------------------------------------------
# Paths
# --------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(_HERE, "model.joblib")
META_PATH  = os.path.join(_HERE, "model_meta.json")

# --------------------------------------------------
# Adaptive parameter tiers
# Chosen so that (max_lag + max_horizon) < n_rows for each tier.
#
#   FULL   (>= 500 rows): original windows — best model quality
#   MEDIUM (>= 100 rows): shorter windows — still good
#   SPARSE (<  100 rows): minimal windows — lets the flow be tested end-to-end
#
# For each tier the minimum engineered rows required after dropna() is listed.
# --------------------------------------------------
_TIERS = [
    {
        "name": "full",
        "min_raw": 500,
        "min_engineered": 60,
        "lag_steps": 10,
        "vol_lag_steps": 5,
        "rolling_windows": [5, 15, 30],
        "horizons": [5, 15, 30],
        "min_periods": None,   # strict rolling — NaN until window fills
    },
    {
        "name": "medium",
        "min_raw": 100,
        "min_engineered": 30,
        "lag_steps": 5,
        "vol_lag_steps": 3,
        "rolling_windows": [3, 7, 15],
        "horizons": [3, 7, 15],
        "min_periods": 2,
    },
    {
        "name": "sparse",
        "min_raw": 0,          # fallback — always matches
        "min_engineered": 15,
        "lag_steps": 3,
        "vol_lag_steps": 2,
        "rolling_windows": [3, 5],
        "horizons": [1, 3, 5],
        "min_periods": 1,      # compute rolling with whatever rows exist
    },
]

# Kept as module-level constants for backward-compatibility (predict / feature cols)
# — overridden per-call via _get_tier()
HORIZONS        = [5, 15, 30]
LAG_STEPS       = 10
VOL_LAG_STEPS   = 5
ROLLING_WINDOWS = [5, 15, 30]
N_ESTIMATORS    = 150
MAX_DEPTH       = 12
RANDOM_STATE    = 42
TRAIN_TOTAL_STEPS = len(HORIZONS) + 2


def _get_tier(n_rows: int) -> dict:
    for tier in _TIERS:
        if n_rows >= tier["min_raw"]:
            return tier
    return _TIERS[-1]   # sparse is always the fallback


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
            gold_path = os.path.join(project_root, "docker", "volumes", "data", "gold", "delta")

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
    for f in files:
        part_df = pd.read_parquet(f)
        parent_dir = os.path.basename(os.path.dirname(f))
        if "=" in parent_dir:
            key, val = parent_dir.split("=", 1)
            part_df[key.lower()] = val
        parts.append(part_df)

    df = pd.concat(parts, ignore_index=True)
    df.columns = [c.lower() for c in df.columns]
    return df


def load_gold_data(source: str = SNOWFLAKE.lower()) -> pd.DataFrame:
    df = _load_snowflake_environment() if source == SNOWFLAKE.lower() else _load_local_environment()
    df["aggregation_window_start"] = pd.to_datetime(df["aggregation_window_start"])

    for col in ("vwap_price_usd", "volume_btc", "volume_usd", "trade_count"):
        if col in df.columns:
            try:
                df[col] = df[col].apply(lambda x: float(x) if x is not None else None)
            except (TypeError, ValueError):
                df[col] = pd.to_numeric(df[col], errors="coerce")

    df = (
        df.sort_values("aggregation_window_start")
        .drop_duplicates(subset=["aggregation_window_start"])
        .dropna(subset=["vwap_price_usd"])
        .reset_index(drop=True)
    )
    return df


# --------------------------------------------------
# Feature engineering  (tier-aware)
# --------------------------------------------------

def _feature_columns(tier: Optional[dict] = None) -> list:
    if tier is None:
        tier = _TIERS[0]   # full by default
    cols = []
    for lag in range(1, tier["lag_steps"] + 1):
        cols.append(f"vwap_lag_{lag}")
    for lag in range(1, tier["vol_lag_steps"] + 1):
        cols.append(f"volume_btc_lag_{lag}")
    for w in tier["rolling_windows"]:
        cols += [f"vwap_roll_mean_{w}", f"vwap_roll_std_{w}"]
    cols += [
        "vwap_pct_change_1",
        "vwap_pct_change_5",
        "trade_count_delta",
        "volume_btc_pct_change",
        "hour_of_day",
        "day_of_week",
        "minute_of_hour",
    ]
    return cols


def engineer_features(df: pd.DataFrame, tier: Optional[dict] = None) -> pd.DataFrame:
    if tier is None:
        tier = _TIERS[0]

    df = df.copy()
    p  = "vwap_price_usd"
    v  = "volume_btc"
    c  = "trade_count"
    mp = tier["min_periods"]   # None = strict, int = lenient

    for lag in range(1, tier["lag_steps"] + 1):
        df[f"vwap_lag_{lag}"] = df[p].shift(lag)
    for lag in range(1, tier["vol_lag_steps"] + 1):
        df[f"volume_btc_lag_{lag}"] = df[v].shift(lag)

    for w in tier["rolling_windows"]:
        shifted = df[p].shift(1)
        roll_kwargs = {"window": w, "min_periods": mp if mp else w}
        df[f"vwap_roll_mean_{w}"] = shifted.rolling(**roll_kwargs).mean()
        df[f"vwap_roll_std_{w}"]  = shifted.rolling(**roll_kwargs).std()

    df["vwap_pct_change_1"]    = df[p].pct_change(1)
    df["vwap_pct_change_5"]    = df[p].pct_change(5)
    df["trade_count_delta"]    = df[c].diff(1) if c in df.columns else 0.0
    df["volume_btc_pct_change"] = df[v].pct_change(1)

    ts = df["aggregation_window_start"]
    df["hour_of_day"]   = ts.dt.hour
    df["day_of_week"]   = ts.dt.dayofweek
    df["minute_of_hour"] = ts.dt.minute

    for h in tier["horizons"]:
        df[f"target_vwap_{h}"] = df[p].shift(-h)

    df = df.dropna().reset_index(drop=True)
    return df


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
    df_raw = load_gold_data(source)

    tier = _get_tier(len(df_raw))
    total_steps = len(tier["horizons"]) + 2

    _progress(1, total_steps, f"Loaded {len(df_raw)} rows — using '{tier['name']}' parameter tier...")
    _logging.getLogger(__name__).info(
        "[HDMAS ML] Loaded %d raw gold rows from %s — tier: %s",
        len(df_raw), source, tier["name"],
    )

    df = engineer_features(df_raw, tier)

    if len(df) < tier["min_engineered"]:
        raise ValueError(
            f"Only {len(df)} usable rows after feature engineering "
            f"({len(df_raw)} raw rows from {source}, tier='{tier['name']}'). "
            f"Need at least {tier['min_engineered']} engineered rows for this tier. "
            f"Run the Gold pipeline longer — at least "
            f"{tier['lag_steps'] + max(tier['rolling_windows']) + max(tier['horizons']) + tier['min_engineered']} "
            f"raw rows are required."
        )

    feature_cols = _feature_columns(tier)
    x = df[feature_cols]
    split_idx = int(len(df) * 0.80)
    # Ensure at least 1 test row
    split_idx = min(split_idx, len(df) - 1)
    x_train, x_test = x.iloc[:split_idx], x.iloc[split_idx:]

    models  = {}
    metrics = {}

    for step, h in enumerate(tier["horizons"], start=2):
        _progress(step, total_steps, f"Training {h}-min horizon model ({step - 1}/{len(tier['horizons'])})...")
        y_train = df[f"target_vwap_{h}"].iloc[:split_idx]
        y_test  = df[f"target_vwap_{h}"].iloc[split_idx:]

        model = RandomForestRegressor(
            n_estimators=N_ESTIMATORS,
            max_depth=MAX_DEPTH,
            min_samples_leaf=max(1, split_idx // 20),  # scale leaf size to dataset
            random_state=RANDOM_STATE,
            n_jobs=1,
        )
        model.fit(x_train, y_train)

        y_pred = model.predict(x_test)
        models[h] = model
        metrics[h] = {
            "mae":  round(float(mean_absolute_error(y_test, y_pred)), 2),
            "rmse": round(float(mean_squared_error(y_test, y_pred) ** 0.5), 2),
            "r2":   round(float(r2_score(y_test, y_pred)), 4),
        }

    _progress(total_steps, total_steps, "Saving model and metadata...")

    primary = models[tier["horizons"][0]]
    feature_importance = {
        col: round(float(imp), 6)
        for col, imp in zip(feature_cols, primary.feature_importances_)
    }

    joblib.dump({"models": models, "tier": tier}, MODEL_PATH)

    chart_slice = slice(max(0, len(x_test) - 120), None)
    test_timestamps = (
        df["aggregation_window_start"].iloc[split_idx:].iloc[chart_slice]
        .dt.strftime("%H:%M").tolist()
    )
    test_actual  = df["vwap_price_usd"].iloc[split_idx:].iloc[chart_slice].round(2).tolist()
    test_pred    = models[tier["horizons"][0]].predict(x_test.iloc[chart_slice]).round(2).tolist()

    version = f"v1.0-rf-{datetime.now().strftime('%Y%m%d-%H%M')}"
    sparse_warning = (
        tier["name"] != "full"
        and f"Limited data ({len(df_raw)} raw rows). Trained in '{tier['name']}' mode — "
            f"horizons: {tier['horizons']} min. "
            f"For full accuracy run the Gold pipeline until ~500+ rows accumulate."
        or None
    )

    meta = {
        "trained_at":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source":            source,
        "tier":              tier["name"],
        "horizons":          tier["horizons"],
        "n_rows_total":      len(df_raw),
        "n_rows_engineered": len(df),
        "n_train":           split_idx,
        "n_test":            len(df) - split_idx,
        "model_version":     version,
        "sparse_warning":    sparse_warning,
        "metrics":           {str(k): v for k, v in metrics.items()},
        "feature_importance": feature_importance,
        "chart": {
            "timestamps": test_timestamps,
            "actual":     test_actual,
            "pred_5min":  test_pred,
        },
    }

    with open(META_PATH, "w") as fh:
        json.dump(meta, fh, indent=2)

    return meta


# --------------------------------------------------
# Predict
# --------------------------------------------------

def predict() -> dict:
    if not is_model_trained():
        raise FileNotFoundError("No trained model found. Train model first.")

    meta = load_meta()
    assert meta is not None, "model_meta.json missing despite is_model_trained() returning True"

    source  = meta.get("source", SNOWFLAKE.lower())
    saved   = joblib.load(MODEL_PATH)
    models  = saved["models"]
    tier    = saved["tier"]

    df_raw = load_gold_data(source)
    df     = engineer_features(df_raw, tier)

    if df.empty:
        raise ValueError("Feature engineering produced an empty dataset.")

    feature_cols = _feature_columns(tier)
    latest_x     = df[feature_cols].iloc[[-1]]
    current_vwap = float(df_raw["vwap_price_usd"].iloc[-1])

    predictions = {}
    for h in tier["horizons"]:
        model      = models[h]
        tree_preds = np.array([t.predict(latest_x)[0] for t in model.estimators_])
        pred_mean  = float(np.mean(tree_preds))
        pred_std   = float(np.std(tree_preds))
        direction  = "UP" if pred_mean > current_vwap else "DOWN"
        confidence = (
            float(np.mean(tree_preds > current_vwap))
            if direction == "UP"
            else float(np.mean(tree_preds <= current_vwap))
        )
        predictions[str(h)] = {
            "predicted_vwap": round(pred_mean, 2),
            "std":            round(pred_std, 2),
            "direction":      direction,
            "confidence":     round(confidence * 100, 1),
            "lower":          round(pred_mean - pred_std, 2),
            "upper":          round(pred_mean + pred_std, 2),
        }

    return {
        "timestamp":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "current_vwap":  round(current_vwap, 2),
        "predictions":   predictions,
        "horizons":      tier["horizons"],
        "tier":          tier["name"],
        "model_version": meta.get("model_version", "-"),
        "source":        source,
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