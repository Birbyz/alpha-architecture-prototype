from constants import (
    START_DOCKER_COMMAND,
    STOP_DOCKER_COMMAND,
)

# --------------------------------------------------
# Runtime names
# --------------------------------------------------
LOCAL = "LOCAL"
HADOOP = "HADOOP"
DATABRICKS = "DATABRICKS"

RUNTIMES = {
    LOCAL,
    HADOOP,
    DATABRICKS,
}

# --------------------------------------------------
# Generic pipeline actions
# --------------------------------------------------
START_PIPELINE_ACTION = "start_pipeline"
STOP_PIPELINE_ACTION = "stop_pipeline"

# --------------------------------------------------
# Local Docker actions
# --------------------------------------------------
START_DOCKER_ACTION = "start_docker"
STOP_DOCKER_ACTION = "stop_docker"
RESTART_DOCKER_ACTION = "restart_docker"
PENDING_DOCKER_ACTION = "pending_docker_action"

DOCKER_ACTIONS = {
    START_DOCKER_ACTION,
    STOP_DOCKER_ACTION,
    RESTART_DOCKER_ACTION,
}

ACTIONS = {
    "start": {
        "command": START_DOCKER_COMMAND,
        "label": "Starting",
    },
    "stop": {
        "command": STOP_DOCKER_COMMAND,
        "label": "Stopping",
    },
}

# --------------------------------------------------
# Stage actions
# --------------------------------------------------
START_KAFKA_ACTION = "start_kafka"
STOP_KAFKA_ACTION = "stop_kafka"

START_BATCH_ACTION = "start_batch"
STOP_BATCH_ACTION = "stop_batch"

START_BRONZE_ACTION = "start_bronze"
STOP_BRONZE_ACTION = "stop_bronze"

START_SILVER_ACTION = "start_silver"
STOP_SILVER_ACTION = "stop_silver"

START_GOLD_ACTION = "start_gold"
STOP_GOLD_ACTION = "stop_gold"

PIPELINE_ACTIONS = {
    START_KAFKA_ACTION,
    STOP_KAFKA_ACTION,
    START_BATCH_ACTION,
    STOP_BATCH_ACTION,
    START_BRONZE_ACTION,
    STOP_BRONZE_ACTION,
    START_SILVER_ACTION,
    STOP_SILVER_ACTION,
    START_GOLD_ACTION,
    STOP_GOLD_ACTION,
}

# --------------------------------------------------
# Local runtime commands
# --------------------------------------------------
KAFKA_START_COMMAND = "docker compose up -d crypto-producer"
KAFKA_STOP_COMMAND = "docker compose stop crypto-producer"

BATCH_SPARK_APP_PATTERN = "batch-data-to-delta-bronze-crypto-trades"
BATCH_START_COMMAND = """
docker compose exec -d spark-client bash -lc '
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name batch-data-to-delta-bronze-crypto-trades \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --driver-memory 512m \
  --executor-memory 1g \
  --executor-cores 1 \
  --total-executor-cores 1 \
  --packages "io.delta:delta-spark_2.13:4.0.1" \
  /workspace/jobs/bronze/batch_to_delta_bronze.py
'
"""

BATCH_STOP_COMMAND = (
    "docker compose exec spark-client pkill -SIGTERM -f batch_to_delta_bronze.py"
)

BRONZE_START_COMMAND = """
docker compose exec spark-client bash -lc '
nohup /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name kafka-to-delta-bronze-crypto-trades \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --driver-memory 512m \
  --executor-memory 1g \
  --executor-cores 1 \
  --total-executor-cores 1 \
  --packages "io.delta:delta-spark_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1" \
  /workspace/jobs/bronze/kafka_to_delta_bronze.py
'
"""

BRONZE_STOP_COMMAND = (
    "docker compose exec spark-client pkill -SIGTERM -f kafka_to_delta_bronze.py"
)

SILVER_START_COMMAND = """
docker compose exec -T spark-client bash -lc '
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name bronze-to-silver-crypto-trades \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --driver-memory 512m \
  --executor-memory 1g \
  --executor-cores 1 \
  --total-executor-cores 1 \
  --packages "io.delta:delta-spark_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1" \
  /workspace/jobs/silver/delta_bronze_to_silver.py
'
"""

SILVER_STOP_COMMAND = (
    "docker compose exec spark-client pkill -SIGTERM -f delta_bronze_to_silver.py"
)

GOLD_START_COMMAND = """
docker compose exec -T spark-client bash -lc '
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name delta-silver-to-gold-crypto-trades \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --driver-memory 512m \
  --executor-memory 1g \
  --executor-cores 1 \
  --total-executor-cores 1 \
  --packages "io.delta:delta-spark_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1" \
  /workspace/jobs/gold/delta_silver_to_gold.py
'
"""
GOLD_STOP_COMMAND = (
    "docker compose exec spark-client pkill -SIGTERM -f delta_silver_to_gold.py"
)

PIPELINE_STAGE_NAMES = {
    "kafka": "Kafka",
    "batch": "Batch",
    "bronze": "Bronze",
    "silver": "Silver",
    "gold": "Gold",
}

core_services = {
    "kafka",
    "kafka-ui",
    "spark-master",
    "spark-worker",
    "spark-client",
}
