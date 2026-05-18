import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    expr,
    date_format,
    to_timestamp,
    to_json,
    struct,
)
from pyspark.sql.types import StructType, StructField, LongType, StringType

_DEFAULTS = {
    "DELTA_PATH_BRONZE": "dbfs:/user/hdmas/delta/bronze",
    "BATCH_DATA_PATH":   "dbfs:/user/hdmas/batch",
}


# replaces: from common.utils import get_env_var, build_spark
def get_env_var(key: str) -> str:
    value = os.getenv(key, _DEFAULTS.get(key, ""))
    
    if not value:
        raise RuntimeError(f"Missing required env variable: {key}")
    return value
 
 
def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# JOB
def main():
    spark = build_spark("batch-data-to-delta-bronze-crypto-trades")

    DELTA_PATH_BRONZE = get_env_var("DELTA_PATH_BRONZE")
    BATCH_DATA_PATH = get_env_var("BATCH_DATA_PATH")

    # Binance CSV structure
    #  id, price, qty, quote_qty, time, is_buyer_maker, is_best_match
    binance_trade_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("price", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("quote_qty", StringType(), True),
            StructField("time_raw", StringType(), True),
            StructField("is_buyer_maker_raw", StringType(), True),  # True/False
            StructField("is_best_match_raw", StringType(), True),  # True/False
        ]
    )

    raw = (
        spark.read.format("csv")
        .schema(binance_trade_schema)
        .option("header", "false")
        .option("sep", ",")
        .load(BATCH_DATA_PATH)
    )

    # time value normalization
    df = (
        raw.withColumn("trade_id", col("id").cast(StringType()))
        .withColumn(
            "event_ts",
            expr(
                """
                CASE
                  WHEN CAST(CAST(time_raw AS DOUBLE) AS BIGINT) >= 10000000000000
                  THEN CAST(CAST(time_raw AS DOUBLE) AS BIGINT) / 1000
                  ELSE CAST(CAST(time_raw AS DOUBLE) AS BIGINT)
                END
                """
            ).cast("bigint"),
        )  # us -> ms
        .withColumn("is_maker", expr("lower(is_buyer_maker_raw) = 'true'"))
        # .withColumn("is_best_match", expr("lower(is_best_match_raw) = 'true'"))
        .withColumn("symbol", lit("BTCUSDT"))
        .withColumn("quantity", col("qty"))
    )

    # ==============================
    # Build RAW trade JSON (Binance shape)
    # ==============================
    raw_json_obj = struct(
        lit("trade").alias("e"),
        col("event_ts").cast("long").alias("E"),
        col("symbol").alias("s"),
        col("trade_id").cast("long").alias("t"),
        col("price").cast("string").alias("p"),
        col("quantity").cast("string").alias("q"),
        col("event_ts").cast("long").alias("T"),
        col("is_maker").alias("m"),
        expr("lower(is_best_match_raw) = 'true'").alias("M"),
        # col("is_best_match_raw").alias("is_best_match"),
    )

    df = df.withColumn("raw", to_json(raw_json_obj))

    # ==============================
    # Build PAYLOAD (Kafka-style envelope)
    # ==============================
    df = (
        df.withColumn("source", lit("binance_vision_csv"))
        .withColumn("stream", lit("btcusdt@trade"))
        .withColumn("event_type", lit("trade"))
        .withColumn(
            "ingestion_time",
            expr("date_format(current_timestamp(), \"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")"),
        )
    )

    payload_obj = struct(
        col("source"),
        col("stream"),
        col("event_ts"),
        col("event_type"),
        col("price"),
        col("quantity"),
        col("symbol"),
        col("ingestion_time"),
        col("is_maker"),
        col("raw"),  # raw already JSON string
    )

    df = df.withColumn("_raw_json", to_json(payload_obj)).withColumn(
        "raw_json", col("raw")
    )  # MUST equal raw (streaming contract)

    bronze_like = (
        df.withColumn("event_time", expr("timestamp_millis(event_ts)"))
        .withColumn("ingestion_ts", to_timestamp(col("ingestion_time")))
        .withColumn("price_dec", col("price").cast("decimal(38,18)"))
        .withColumn("quantity_dec", col("qty").cast("decimal(38,18)"))
        .withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd"))
        .withColumn("bronze_ingested_at", current_timestamp())
        .withColumn(
            "bronze_is_valid",
            col("event_ts").isNotNull()
            & col("symbol").isNotNull()
            & col("price").isNotNull()
            & col("qty").isNotNull()
            & (col("price_dec").isNotNull() & (col("price_dec") > 0))
            & (col("quantity_dec").isNotNull() & (col("quantity_dec") > 0)),
        )
    )

    # Write ONLY the Bronze table schema columns
    bronze_out = bronze_like.select(
        "source",
        "stream",
        "event_ts",
        "event_type",
        "price",
        "quantity",
        "symbol",
        "ingestion_time",
        "is_maker",
        "raw",
        "_raw_json",
        "event_time",
        "ingestion_ts",
        "trade_id",
        "price_dec",
        "quantity_dec",
        "event_date",
        "bronze_ingested_at",
        "raw_json",
        "bronze_is_valid",
    )

    (
        bronze_out.write.format("delta")
        .mode("append")
        .partitionBy("symbol", "event_date")
        .save(DELTA_PATH_BRONZE)
    )

    print(f"Batch append complete: {BATCH_DATA_PATH} -> {DELTA_PATH_BRONZE}")
    print(f"\n=== FINISHED ===\n\n\n")


if __name__ == "__main__":
    main()


# docker compose exec spark-client bash -lc '
# export BATCH_DATA_PATH=/data/batch/BTCUSDT-trades-test.csv
# export DELTA_PATH_BRONZE=/data/delta/bronze/_test_crypto_trades_raw

# mkdir -p /tmp/ivy

# /opt/spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   --conf spark.jars.ivy=/tmp/ivy \
#   --conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/ivy\ -Divy.home=/tmp/ivy \
#   --packages io.delta:delta-spark_2.13:4.0.1 \
#   --driver-memory 2g \
#   --executor-memory 2g \
#   --conf spark.executor.cores=1 \
#   /workspace/jobs/bronze/batch_to_delta_bronze.py
# '
