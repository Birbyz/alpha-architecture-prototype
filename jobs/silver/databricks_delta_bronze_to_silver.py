import os
import time

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    to_date,
    sha2,
    concat_ws,
)

_DEFAULTS = {
    "BRONZE_TABLE": "main.default.hdmas_bronze",
    "SILVER_TABLE": "main.default.hdmas_silver",
}


# replaces: from common.utils import build_spark, get_env_var
def get_env_var(key: str) -> str:
    value = os.getenv(key, _DEFAULTS.get(key, ""))
    if not value:
        raise RuntimeError(f"Missing required env variable: {key}")
    return value
 
 
def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()
 

# JOB
def upsert_bronze_to_silver(batch_df, batch_id: int):
    print(f"\n=== SILVER BATCH {batch_id} ===")
    print("ROWS IN BATCH:", batch_df.count())
    batch_df.select("price_dec", "quantity_dec", "bronze_is_valid").show(5, False)


    # extract trade ID from raw_json
    df = (
        batch_df.filter(col("bronze_is_valid") == True)
        .filter(col("trade_id").isNotNull())
        .filter(col("price_dec").isNotNull() & (col("price_dec") > 0))
        .filter(col("quantity_dec").isNotNull() & (col("quantity_dec") > 0))
        .withColumn("trade_date", to_date(col("event_time")))
    )

    # rather choose trades where trade_id exists
    # otherwise, fallback to a composite fingerprint
    df = df.withColumn(
        "trade_key",
        sha2(concat_ws("||", col("symbol"), col("trade_id")), 256),
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


    SILVER_TABLE = get_env_var("SILVER_TABLE")
    
    if batch_df.sparkSession.catalog.tableExists(SILVER_TABLE):
        (
            DeltaTable
            .forName(batch_df.sparkSession, SILVER_TABLE)
            .alias("t")
            .merge(silver_df.alias("s"), "t.trade_key = s.trade_key")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            silver_df
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("symbol", "trade_date")
            .saveAsTable(SILVER_TABLE)
        )

    print(f"Batch {batch_id} finished\n\n\n")


def main():
    BRONZE_TABLE = get_env_var("BRONZE_TABLE")
    spark = build_spark("bronze-to-silver-crypto-trades")

    bronze_stream = spark.read.table(BRONZE_TABLE)
    # bronze_stream.select("raw_json").show(5, truncate=False)

    upsert_bronze_to_silver(bronze_stream, 0)


if __name__ == "__main__":
    main()

