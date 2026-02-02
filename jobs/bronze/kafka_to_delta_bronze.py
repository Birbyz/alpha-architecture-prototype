from common.utils import get_env_var, build_spark
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    DecimalType,
    LongType,
    MapType,
)
from pyspark.sql.functions import (
    col,
    expr,
    from_json,
    to_json,
    to_timestamp,
    date_format,
    current_timestamp,
)


def main():
    KAFKA_TOPIC = get_env_var("KAFKA_TOPIC")
    DELTA_PATH_BRONZE = get_env_var("DELTA_PATH_BRONZE")
    KAFKA_SERVER = get_env_var("KAFKA_BOOTSTRAP_SERVERS")
    CHECKPOINT_PATH_BRONZE = get_env_var("CHECKPOINT_PATH_BRONZE")

    spark = build_spark("kafka-to-delta-bronze-crypto-trades")
    # spark.sparkContext.setLogLevel("WARN")

    # match producer.py payload
    payload_schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("stream", StringType(), True),
            StructField("event_ts", LongType(), True),  # binance 'E' in ms
            StructField("event_type", StringType(), True),  # binance 'e'
            StructField("price", StringType(), True),   # BTC PRICE at the time of the transaction
            StructField("quantity", StringType(), True),    # BTC QUANTITY traded
            StructField("symbol", StringType(), True),
            StructField("ingestion_time", StringType(), True),  # ISO string
            StructField("is_maker", BooleanType(), True),
            StructField(
                "raw", StringType(), True
            ),  # stored as a JSON string
        ]
    )

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # parse JSON
    parsed = (
        df.select(
            expr("CAST(value AS STRING)").alias("value_str"),
        )
        .withColumn("json", from_json(col("value_str"), payload_schema))
        .select("json.*", col("value_str").alias("_raw_json"))
    )

    normalize = (
        parsed.withColumn("event_time", expr("timestamp_millis(event_ts)"))
        .withColumn("ingestion_ts", to_timestamp(col("ingestion_time")))
        .withColumn("trade_id", expr("get_json_object(raw, '$.t')"))
        .withColumn("price_dec", col("price").cast(DecimalType(38, 18)))
        .withColumn("quantity_dec", col("quantity").cast(DecimalType(38, 18)))
        .withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd"))
        .withColumn("bronze_ingested_at", current_timestamp())
        .withColumn("raw_json", col("raw"))
        .withColumn(
            "bronze_is_valid",
            (col("event_ts").isNotNull())
            & (col("symbol").isNotNull())
            & (col("price").isNotNull())
            & (col("quantity").isNotNull()),
        )
    )

    query = (
        normalize.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH_BRONZE)
        .partitionBy("symbol", "event_date")
        .start(DELTA_PATH_BRONZE)
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
# io.delta:delta-spark_2.13:4.0.1,\
# org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,\
# org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1 \
#   /workspace/jobs/bronze/kafka_to_delta_bronze.py
# '
