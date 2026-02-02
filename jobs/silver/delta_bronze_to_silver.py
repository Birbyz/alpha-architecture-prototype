import time

from delta.tables import DeltaTable
from common.utils import build_spark, get_env_var
from pyspark.sql.functions import (
    col,
    expr,
    to_date,
    sha2,
    concat_ws,
    coalesce,
    lit,
    when,
    format_number,
)


def upsert_bronze_to_silver(batch_df, batch_id: int):
    print(f"\n=== SILVER BATCH START | batch_id={batch_id} ===")
    print(f"rows_in_batch = {batch_df.count()}")

    # extract trade ID from raw_json
    df = (
        batch_df
        .filter(col("bronze_is_valid") == True)
        .filter(col("price_dec").isNotNull() & (col("price_dec") > 0))
        .filter(col("quantity_dec").isNotNull() & (col("quantity_dec") > 0))
        .withColumn("trade_id", expr("get_json_object(raw_json, '$.t')"))
        .withColumn("trade_date", to_date(col("event_time")))
    )

    # rather choose trades where trade_id exists
    # otherwise, fallback to a composite fingerprint
    df = df.withColumn(
        "trade_key",
        when(
            col("trade_id").isNotNull(),
            sha2(concat_ws("||", col("symbol"), col("trade_id")), 256),
        ).otherwise(
            sha2(
                concat_ws(
                    "||",
                    col("symbol"),
                    col("event_ts").cast("string"),
                    coalesce(col("trade_id"), lit("")),
                    format_number(col("price_dec"), 18),
                    format_number(col("quantity_dec"), 18),
                ),
                256,
            ),
        ),
    )

    # keep only the necessary columns
    silver_df = df.select(
        "trade_key",
        "trade_id",
        "symbol",
        "event_ts",
        col("price_dec").alias("price"),
        col("quantity_dec").alias("quantity"),
        # "is_maker",
        "ingestion_ts",
        "trade_date",
        # "source",
        # "stream",
    )

    DELTA_PATH_SILVER = get_env_var("DELTA_PATH_SILVER")
    if DeltaTable.isDeltaTable(batch_df.sparkSession, DELTA_PATH_SILVER):
        target = DeltaTable.forPath(batch_df.sparkSession, DELTA_PATH_SILVER)
        (
            target.alias("t")
            .merge(silver_df.alias("s"), "t.trade_key = s.trade_key")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            silver_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("symbol", "trade_date")
            .save(DELTA_PATH_SILVER)
        )

    print(f"Batch {batch_id} finished\n\n\n")
    time.sleep(10)


def main():
    DELTA_PATH_BRONZE = get_env_var("DELTA_PATH_BRONZE")
    CHECKPOINT_PATH_SILVER = get_env_var("CHECKPOINT_PATH_SILVER")
    DATA_BATCH_TIMER_SILVER = get_env_var("DATA_BATCH_TIMER_SILVER")

    spark = build_spark("bronze-to-silver-crypto-trades")

    bronze_stream = spark.readStream.format("delta").load(DELTA_PATH_BRONZE)
    # bronze_stream.select("raw_json").show(5, truncate=False)

    query = (
        bronze_stream.writeStream.foreachBatch(
            lambda df, bid: upsert_bronze_to_silver(df, bid)
        )
        .option("checkpointLocation", CHECKPOINT_PATH_SILVER)
        .trigger(
            processingTime=DATA_BATCH_TIMER_SILVER
        )  # every X seconds data is processed as a new batch
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()


# docker compose exec spark-client bash -lc '
# mkdir -p /tmp/ivy && \
# /opt/spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   --conf spark.jars.ivy=/tmp/ivy \
#   --packages \
# io.delta:delta-spark_2.13:4.0.1 \
#   /workspace/jobs/silver/delta_bronze_to_silver.py
# '
