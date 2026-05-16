from pathlib import Path

# --------------------------------------------------
# Pipeline stage states
# --------------------------------------------------
__IDLE__ = "IDLE"
__RUNNING__ = "RUNNING"
__STOPPED__ = "STOPPED"
__ML__ = "ML"
__ERROR__ = "Error. Something went wrong."

# --------------------------------------------------
# Runtime names
# --------------------------------------------------
__DOCKER__ = "DOCKER"
LOCAL = "LOCAL"
HADOOP = "HADOOP"
DATABRICKS = "DATABRICKS"
SNOWFLAKE = "SNOWFLAKE"

# --------------------------------------------------
# Session state keys
# --------------------------------------------------
PIPELINE_STATE = "pipeline_state"
LOG_LINES = "log_lines"
STAGE_STATES = "stage_states"
ML_STATE = "ml_state"
LAST_PREDICTION = "last_prediction"
RUNTIME_STATES = "runtime_states"

# Databricks: {stage_name: run_id}
DATABRICKS_RUN_IDS = "databricks_run_ids"

# Hadoop: {stage_name: yarn_application_id}
HADOOP_RUN_IDS = "hadoop_run_ids"

# --------------------------------------------------
# UI button labels
# --------------------------------------------------
START_DOCKER_BUTTON = "Start Docker"
STOP_DOCKER_BUTTON = "Stop Docker"
CLEAR_BUTTON = "Clear Logs"
START_PIPELINE_BUTTON = "Start Pipeline"
STOP_PIPELINE_BUTTON = "Stop Pipeline"
REFRESH_PIPELINE_BUTTON = "Refresh"

# --------------------------------------------------
# UI colors
# --------------------------------------------------
LIGHT_GREEN = "lightgreen"
RED = "red"
ORANGE = "orange"
DEEP_SKY_BLUE = "deepskyblue"

# --------------------------------------------------
# Pipeline
# --------------------------------------------------
PIPELINE_STAGES_ARRAY = ["Kafka", "Batch", "Bronze", "Silver", "Gold"]

PIPELINE_STAGE_NAMES = {
    "kafka": "Kafka",
    "batch": "Batch",
    "bronze": "Bronze",
    "silver": "Silver",
    "gold": "Gold",
}

# --------------------------------------------------
# Pipeline action name constants  (shared across all runtimes)
# --------------------------------------------------
START_PIPELINE_ACTION = "start_pipeline"
STOP_PIPELINE_ACTION = "stop_pipeline"

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
# Docker / compose
# --------------------------------------------------
COMPOSE_DIR = Path(__file__).resolve().parents[1]
COMPOSE_FILE = COMPOSE_DIR / "docker-compose.yml"

START_DOCKER_COMMAND = "docker compose up -d"
STOP_DOCKER_COMMAND = "docker compose stop"
DOCKER_STATUS_COMMAND = "docker compose ps --services --status running"

SPARK_MASTER = "spark-master"
SPARK_WORKER = "spark-worker"

# --------------------------------------------------
# YARN application states
# --------------------------------------------------
HADOOP_ACTIVE_STATES = {"NEW", "NEW_SAVING", "SUBMITTED", "ACCEPTED", "RUNNING"}
HADOOP_TERMINAL_STATES = {"FINISHED", "FAILED", "KILLED"}
HADOOP_SUCCESS_FINAL = "SUCCEEDED"  # finalStatus when state == FINISHED

# --------------------------------------------------
# Spark packages (must match Docker image versions)
# --------------------------------------------------
DELTA_PKG = "io.delta:delta-spark_2.13:4.0.1"
KAFKA_PKGS = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,"
    "org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1"
)

# Stages that need Kafka jars in addition to Delta
KAFKA_STAGES = {"Kafka", "Bronze", "Silver", "Gold"}

# --------------------------------------------------
# Env vars forwarded to YARN ApplicationMaster
# --------------------------------------------------
# Path vars are converted to HDFS paths automatically
HADOOP_PATH_ENV_VARS = {
    "DELTA_PATH_BRONZE",
    "CHECKPOINT_PATH_BRONZE",
    "DELTA_PATH_SILVER",
    "CHECKPOINT_PATH_SILVER",
    "DELTA_PATH_GOLD",
    "CHECKPOINT_PATH_GOLD",
    "BATCH_DATA_PATH",
}

# Non-path vars forwarded as-is
HADOOP_NON_PATH_ENV_VARS = {
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
    "DATA_BATCH_TIMER_SILVER",
    "EXTENSION_KEY_CONFIG",
    "EXTENSION_VALUE_CONFIG",
    "CATALOG_KEY_CONFIG",
    "CATALOG_VALUE_CONFIG",
}

# --------------------------------------------------
# Databricks job run states
# --------------------------------------------------
DATABRICKS_RUNNING_STATES = {"PENDING", "RUNNING", "TERMINATING"}
DATABRICKS_TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
