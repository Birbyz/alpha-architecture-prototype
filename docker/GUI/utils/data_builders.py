import os
import json
import subprocess
import pandas as pd

from datetime import datetime, timedelta

from runtimes.docker.docker_status import get_service_status


def build_services_df():
    return pd.DataFrame(get_service_status())


def build_hdfs_layer_stats() -> pd.DataFrame:
    """Returns file count, total size and last-modified time for each Delta layer on HDFS."""
    hadoop_home = os.getenv("HADOOP_HOME", "")
    hdfs_bin = f"{hadoop_home}/bin/hdfs" if hadoop_home else "hdfs"
    namenode = os.getenv("HADOOP_NAMENODE", "localhost:9000")

    layers = {
        "Bronze": os.getenv("DELTA_PATH_BRONZE", "/data/bronze/delta"),
        "Silver": os.getenv("DELTA_PATH_SILVER", "/data/silver/delta"),
        "Gold": os.getenv("DELTA_PATH_GOLD", "/data/gold/delta"),
    }

    rows = []
    for layer, path in layers.items():
        hdfs_path = (
            f"hdfs://{namenode}{path}" if not path.startswith("hdfs://") else path
        )
        try:
            r = subprocess.run(
                [hdfs_bin, "dfs", "-count", hdfs_path],
                capture_output=True,
                text=True,
                timeout=6,
            )
            if r.returncode == 0:
                parts = r.stdout.strip().split()
                files = int(parts[1]) if len(parts) >= 2 else 0
                size_mb = (
                    round(int(parts[2]) / 1024 / 1024, 2) if len(parts) >= 3 else 0
                )
            else:
                files, size_mb = "—", "—"

            r2 = subprocess.run(
                [hdfs_bin, "dfs", "-ls", "-R", hdfs_path],
                capture_output=True,
                text=True,
                timeout=6,
            )
            last_mod = "—"
            if r2.returncode == 0:
                timestamps = []
                for line in r2.stdout.splitlines():
                    p = line.split()
                    if len(p) >= 7 and p[0].startswith("-"):
                        try:
                            timestamps.append(
                                datetime.strptime(f"{p[5]} {p[6]}", "%Y-%m-%d %H:%M")
                            )
                        except ValueError:
                            pass
                if timestamps:
                    last_mod = max(timestamps).strftime("%Y-%m-%d %H:%M")

            rows.append(
                {
                    "Layer": layer,
                    "Files": files,
                    "Size (MB)": size_mb,
                    "Last Modified": last_mod,
                    "HDFS Path": hdfs_path,
                }
            )
        except Exception as e:
            rows.append(
                {
                    "Layer": layer,
                    "Files": "err",
                    "Size (MB)": "err",
                    "Last Modified": str(e)[:40],
                    "HDFS Path": hdfs_path,
                }
            )

    return pd.DataFrame(rows)


def build_gold_sample_df(n: int = 20) -> pd.DataFrame:
    """Reads the latest Gold Delta rows from HDFS using a client-mode Spark job."""
    import tempfile

    spark_home = os.getenv("HADOOP_SPARK_HOME", "/opt/spark")
    spark_submit = f"{spark_home}/bin/spark-submit"
    namenode = os.getenv("HADOOP_NAMENODE", "localhost:9000")
    gold_path = os.getenv("DELTA_PATH_GOLD", "/data/gold/delta")
    hdfs_gold = (
        f"hdfs://{namenode}{gold_path}"
        if not gold_path.startswith("hdfs://")
        else gold_path
    )
    hadoop_conf = os.getenv("HADOOP_CONF_DIR", "")

    script = f"""
import json
from pyspark.sql import SparkSession
spark = (SparkSession.builder
    .appName("hdmas-gold-sample")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
try:
    df = spark.read.format("delta").load("{hdfs_gold}")
    rows = df.orderBy("aggregation_window_start", ascending=False).limit({n}).toJSON().collect()
    for r in rows:
        print("JSON_ROW:" + r)
except Exception as e:
    print("JSON_ROW:" + json.dumps({{"error": str(e)}}))
spark.stop()
"""

    env = dict(os.environ)
    if hadoop_conf:
        env["HADOOP_CONF_DIR"] = hadoop_conf
        env["YARN_CONF_DIR"] = hadoop_conf

    tmp_script = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, prefix="hdmas_gold_"
        ) as f:
            f.write(script)
            tmp_script = f.name

        result = subprocess.run(
            [
                spark_submit,
                "--master",
                "yarn",
                "--deploy-mode",
                "client",
                "--packages",
                "io.delta:delta-spark_2.13:4.0.1",
                "--conf",
                "spark.jars.ivy=/tmp/.ivy2",
                tmp_script,
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        rows = []
        for line in result.stdout.splitlines():
            if line.startswith("JSON_ROW:"):
                try:
                    rows.append(json.loads(line[len("JSON_ROW:") :]))
                except Exception:
                    pass
        if rows:
            return pd.DataFrame(rows)
        return pd.DataFrame(
            [{"info": "No Gold rows yet", "detail": result.stderr[-300:]}]
        )
    except subprocess.TimeoutExpired:
        return pd.DataFrame([{"info": "Timed out reading Gold data (>120s)"}])
    except Exception as e:
        return pd.DataFrame([{"info": f"Error: {e}"}])
    finally:
        if tmp_script and os.path.exists(tmp_script):
            os.unlink(tmp_script)


def build_gold_latest_df(n: int = 20):
    now = datetime.now()
    rows = []

    for i in range(n):
        rows.append(
            {
                "event_time": (now - timedelta(seconds=i * 5)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "symbol": "BTCUSDT",
                "price": round(68000 + (i * 3.17), 2),
                "quantity": round(0.001 + (i * 0.0001), 6),
                "trade_count": 1 + i,
                "source": "gold_layer",
            }
        )

    return pd.DataFrame(rows)


def build_metrics_series():
    now = datetime.now()
    rows = []
    for i in range(12):
        rows.append(
            {
                "time": (now - timedelta(minutes=11 - i)).strftime("%H:%M"),
                "rows_processed": 800 + (i * 35),
                "pipeline_lag_sec": max(0.2, 2.4 - (i * 0.12)),
            }
        )
    return pd.DataFrame(rows)


def color_state(val):
    if val == "running":
        return "color: lightgreen"
    if val == "restarting":
        return "color: orange"
    if val == "exited":
        return "color: red"
    return ""
