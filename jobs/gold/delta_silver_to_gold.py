from common.utils import build_spark, get_env_var

from pyspark.sql.functions import (expr, col, window, current_timestamp, count as _count, sum as _sum)

def main():
    DELTA_PATH_SILVER = get_env_var("DELTA_PATH_SILVER")
    DELTA_PATH_GOLD = get_env_var("DELTA_PATH_GOLD")
    CHECKPOINT_PATH_GOLD = get_env_var("CHECKPOINT_PATH_GOLD")
    
    spark = build_spark("delta-silver-to-gold-crypto-trades")
    
    # read from silver delta table as a stream
    silver_stream = spark.readStream.format("delta").load(DELTA_PATH_SILVER)
    
    # enrich and normalize data
    normalize = (
        silver_stream
        .withColumn("event_timestamp", expr("timestamp_millis(event_ts)"))
        .withColumn("quantity_btc", col("quantity") / expr("pow(10, 8)"))
        .withColumn("notional_usd", (col("price") * col("quantity")) / expr("pow(10, 8)"))
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
    
    gold_stream = (
        gold_aggregation
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH_GOLD)
        .partitionBy("symbol")
        .start(DELTA_PATH_GOLD)
    )
    
    gold_stream.awaitTermination()
    
    
if __name__ == "__main__":
    main()
    
    