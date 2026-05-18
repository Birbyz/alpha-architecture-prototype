import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (expr, col, window, current_timestamp, count as _count, sum as _sum)

_DEFAULTS = {
    "SILVER_TABLE":         "main.default.hdmas_silver",
    "GOLD_TABLE":           "main.default.hdmas_gold",
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
    print(f"\n=== GOLD DELTA LAYER START ===")
    
    SILVER_TABLE = get_env_var("SILVER_TABLE")
    GOLD_TABLE = get_env_var("GOLD_TABLE")
    
    spark = build_spark("delta-silver-to-gold-crypto-trades")
    
    # read from silver delta table as a stream
    silver_df = spark.read.table(SILVER_TABLE)
    
    # enrich and normalize data
    normalize = (
        silver_df
        .withColumn("event_timestamp", expr("timestamp_millis(event_ts)"))
        .withColumn("quantity_btc", col("quantity").cast("decimal(38, 18)"))
        .withColumn("notional_usd", (col("price").cast("decimal(38, 8)") * col("quantity_btc")))
    )
    
    # gold aggregation: total notional traded per symbol per day
    gold_aggregation = (
        normalize
        .groupBy(
            col("symbol"), 
            window(col("event_timestamp"), "1 minute")
        )
        .agg(
            _count("*").alias("trade_count"),
            _sum("quantity_btc").alias("volume_btc"),
            _sum("notional_usd").alias("volume_usd")
        )
        .withColumn(
            "vwap_price_usd",
            col("volume_usd") / col("volume_btc")
        )
        .withColumn(
            "gold_ingested_at",
            current_timestamp()
        )
        .select(
            col("symbol"),
            col("window.start").alias("aggregation_window_start"),
            col("window.end").alias("aggregation_window_end"),
            col("trade_count"),
            col("volume_btc"),
            col("volume_usd"),
            col("vwap_price_usd"),
            col("gold_ingested_at")
        )
    )
    
    (
        gold_aggregation
        .write
        .format("delta")
        .mode("append")
        .option("overwriteSchema", "true")
        .partitionBy("symbol")
        .saveAsTable(GOLD_TABLE)
    )
    
    print("=== GOLD DELTA LAYER FINISHED ===")
    
    
    
if __name__ == "__main__":
    main()
    

