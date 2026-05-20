import glob
import json
import os 
import joblib
import numpy as np
import pandas as pd

from datetime import datetime
from typing import Optional, Callable
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error

from constants import SNOWFLAKE
from runtimes.snowflake.snowflake_client import SnowflakeClient
from runtimes.snowflake.snowflake_config import SnowflakeConfig



# paths
_HERE = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.abspath(
    os.path.join(_HERE, '..', 'trained_models')
)
MODEL_PATH = os.path.join(MODELS_DIR, 'model.joblib')
META_PATH = os.path.join(MODELS_DIR, 'model_meta.json')

# config
HORIZONS = [5, 15, 30]          # minutes ahead to predict
LAG_STEPS = 10                  # VWAP lag features t-1 to t-10
VOL_LAG_STEPS = 5               # Volume lag features t-1 to t-5
ROLLING_WINDOWS = [5, 15, 30]   # rolling mean windows in minutes
N_ESTIMATORS = 150              # Random Forest trees
MAX_DEPTH = 12                  # Random Forest max depth
RANDOM_STATE = 42               # for reproducibility
TRAIN_TOTAL_STEPS = len(HORIZONS) + 2

# data loading
def _load_snowflake_environment() -> pd.DataFrame:
    # pull the gold table from Snowflake and normalize it as a  DataFrame
    config = SnowflakeConfig.from_env()
    if not config.is_configured:
        raise RuntimeError("Snowflake configuration is missing.")
    
    client = SnowflakeClient(config)
    rows = client.query_gold_data_for_ml(config.gold_table)
    if not rows:
        raise ValueError("No data returned from Snowflake query.")
    
    df = pd.DataFrame(rows)
    df.columns = [column.lower() for column in df.columns]  # standardize column names
    return df

def _load_local_environment() -> pd.DataFrame:
    # read local gold files directly for training
    gold_path = os.getenv("DELTA_PATH_GOLD", "/data/gold/delta")
    files = glob.glob(os.path.join(gold_path, "**", "*.parquet"), recursive=True)
    if not files:
        raise FileNotFoundError(f"No Parquet files found in {gold_path}")
    
    df = pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
    df.columns = [column.lower() for column in df.columns]  # standardize column names
    return df

def load_gold_data(source: str = SNOWFLAKE.lower()) -> pd.DataFrame:
    # load gold data from the requested source
    df = _load_snowflake_environment() if source == SNOWFLAKE.lower() else _load_local_environment()
    df["aggregation_window_start"] = pd.to_datetime(df["aggregation_window_start"])
    
    for col in ("vwap_price_usd", "volume_btc", "volume_usd", "trade_count"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
    df = (
        df.sort_values("aggregation_window_start")
        .drop_duplicates(subset=["aggregation_window_start"])
        .dropna(subset=["vwap_price_usd"])
        .reset_index(drop=True)
    )
    
    return df

# feature engineering
def _feature_columns() -> list:
    cols = []
    for lag in range(1, LAG_STEPS + 1):
        cols.append(f"vwap_lag_{lag}")
    for lag in range(1, VOL_LAG_STEPS + 1):
        cols.append(f"volume_btc_lag_{lag}")
    for window in ROLLING_WINDOWS:
        cols += [f"vwap_roll_mean_{window}", f"vwap_roll_std_{window}"]
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

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    # add lag, rolling, momentum and temporal features
    df = df.copy()
    
    p = "vwap_price_usd"
    v = "volume_btc"
    c = "trade_count"
    
    for lag in range(1, LAG_STEPS + 1):
        df[f"vwap_lag_{lag}"] = df[p].shift(lag)
    for lag in range(1, VOL_LAG_STEPS + 1):
        df[f"volume_btc_lag_{lag}"] = df[v].shift(lag)
        
    for window in ROLLING_WINDOWS:
        shifted = df[p].shift(1)
        df[f"vwap_roll_mean_{window}"] = shifted.rolling(window=window).mean()
        df[f"vwap_roll_std_{window}"] = shifted.rolling(window=window).std()
        
    df["vwap_pct_change_1"] = df[p].pct_change(1)
    df["vwap_pct_change_5"] = df[p].pct_change(5)
    df["trade_count_delta"] = df[c].diff(1) if c in df.columns else 0.0
    df["volume_btc_pct_change"] = df[v].pct_change(1)
    
    ts = df["aggregation_window_start"]
    df["hour_of_day"] = ts.dt.hour
    df["day_of_week"] = ts.dt.dayofweek
    df["minute_of_hour"] = ts.dt.minute
    
    for h in HORIZONS:
        df[f"target_vwap_{h}"] = df[p].shift(-h)
    
    df = df.dropna().reset_index(drop=True)
    return df

# train
def train_model(source: str = SNOWFLAKE.lower(), on_progress: Optional[Callable[[int, int, str], None]] = None) -> dict:
    def _progress(step: int, msg: str):
        if on_progress:
            on_progress(step, TRAIN_TOTAL_STEPS, msg)
            
    _progress(1, f"Loading gold data from {source.capitalize()}")
    df_raw = load_gold_data(source)
    
    _progress(1, "Engineering features...")
    df = engineer_features(df_raw)
    
    if len(df) < 60:
        raise ValueError(f"Only {len(df)} usable rows after engineering. Need at least 60.")
    
    feature_cols = _feature_columns()
    x = df[feature_cols]
    split_idx = int(len(df) * 0.80)
    x_train, x_test = x.iloc[:split_idx], x.iloc[split_idx:]
    
    models = {}
    metrics = {}
    
    # steps 2 - 4: one model per horizon
    for step, h in enumerate(HORIZONS, start=2):
        _progress(step, f"Training {h}-min(s) horizon model ({step-1}/{len(HORIZONS)})...")
        y_train = df[f"target_vwap_{h}"].iloc[:split_idx]
        y_test = df[f"target_vwap_{h}"].iloc[split_idx:]
        
        model = RandomForestRegressor(
            n_estimators=N_ESTIMATORS,
            max_depth=MAX_DEPTH,
            min_samples_leaf=3,
            random_state=RANDOM_STATE,
            n_jobs=1
        )
        model.fit(x_train, y_train)
        
        y_pred = model.predict(x_test)
        models[h] = model
        metrics[h] = {
            "mae": round(float(mean_absolute_error(y_test, y_pred)), 2),
            "rmse": round(float(mean_squared_error(y_test, y_pred) ** 0.5), 2),
            "r2": round(float(r2_score(y_test, y_pred)), 4)

        }
        
    # step 5 - persist
    _progress(TRAIN_TOTAL_STEPS, "Saving model and metadata...")
    primary = models[HORIZONS[0]]
    feature_importance = {
        col: round(float(imp), 6)
        for col, imp in zip(feature_cols, primary.feature_importances_)
    }
    
    joblib.dump(models, MODEL_PATH)
    
    # chart data - last 120 test rows
    chart_slice = slice(max(0, len(x_test) - 120), None)
    test_timestamps = (
        df["aggregation_window_start"].iloc[split_idx:].iloc[chart_slice]
        .dt.strftime("%H:%M").tolist()
    )
    
    test_actual = df["vwap_price_usd"].iloc[split_idx:].iloc[chart_slice].round(2).tolist()
    test_pred_5 = models[5].predict(x_test.iloc[chart_slice]).round(2).tolist()
    
    version = f"v1.0-rf-{datetime.now().strftime('%Y%m%d-%H%M')}"
    meta = {
        "trained_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": source,
        "n_rows_total": len(df_raw),
        "n_rows_engineered": len(df),
        "n_train": split_idx,
        "n_test": len(df) - split_idx,
        "model_version": version,
        "metrics": {str(k): v for k, v in metrics.items()},
        "feature_importance": feature_importance,
        "chart": {
            "timestamps": test_timestamps,
            "actual": test_actual,
            "pred_5min": test_pred_5,
        },
    }
    
    with open(META_PATH, "w") as fh:
        json.dump(meta, fh, indent=2)
 
    return meta

# predict
def predict() -> dict:
    if not is_model_trained():
        raise FileNotFoundError("No trained model found. Train model first.")
    
    meta = load_meta()
    assert meta is not None, "model_meta.json missing despite is_model_trained() returning True"
    
    source = meta.get("source", SNOWFLAKE.lower())
    
    df_raw = load_gold_data(source)
    df = engineer_features(df_raw)
    
    if df.empty:
        raise ValueError("Feature engineering produced an empty dataset.")
    
    feature_cols = _feature_columns()
    models = joblib.load(MODEL_PATH)
    latest_x = df[feature_cols].iloc[[-1]]
    current_vwap = float(df_raw["vwap_price_usd"].iloc[-1])
    
    predictions = {}
    for h in HORIZONS:
        model = models[h]
        tree_preds = np.array([t.predict(latest_x)[0] for t in model.estimators_])
        pred_mean = float(np.mean(tree_preds))
        pred_std = float(np.std(tree_preds))
        direction = "UP" if pred_mean > current_vwap else "DOWN"
        confidence = (
            float(np.mean(tree_preds > current_vwap))
            if direction == "UP"
            else float(np.mean(tree_preds <= current_vwap))
        )
        predictions[str(h)] = {
            "predicted_vwap": round(pred_mean, 2),
            "std": round(pred_std, 2),
            "direction": direction,
            "confidence": round(confidence * 100, 1),
            "lower": round(pred_mean - pred_std, 2),
            "upper": round(pred_mean + pred_std, 2),
        }
    
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "current_vwap": round(current_vwap, 2),
        "predictions": predictions,
        "model_version": meta.get("model_version", "-"),
        "source": source,
    }


# helpers
def is_model_trained() -> bool:
    return os.path.exists(MODEL_PATH) and os.path.exists(META_PATH)

def load_meta() -> Optional[dict]:
    if not os.path.exists(META_PATH):
        return None
    
    with open(META_PATH) as fh:
        return json.load(fh)
    
def reset():
    # remove persisted model and metadata
    for path in (MODEL_PATH, META_PATH):
        if os.path.exists(path):
            os.remove(path)

 

    