import os

from pyspark.sql import SparkSession


# return the env value of a given key
def get_env_var(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(f"Missing required env variable: {key}")

    return value


# build spark session
def build_spark(
    app_name: str, *, extra_configs: dict[str, str] | None = None
) -> SparkSession:
    EXTENSION_KEY_CONFIG = get_env_var("EXTENSION_KEY_CONFIG")
    EXTENSION_VALUE_CONFIG = get_env_var("EXTENSION_VALUE_CONFIG")
    CATALOG_KEY_CONFIG = get_env_var("CATALOG_KEY_CONFIG")
    CATALOG_VALUE_CONFIG = get_env_var("CATALOG_VALUE_CONFIG")

    spark_builder = (
        SparkSession.builder.appName(app_name)
        .config(EXTENSION_KEY_CONFIG, EXTENSION_VALUE_CONFIG)
        .config(CATALOG_KEY_CONFIG, CATALOG_VALUE_CONFIG)
    )

    if extra_configs:
        for key, value in extra_configs.items():
            spark_builder = spark_builder.config(key, value)

    return spark_builder.getOrCreate()
