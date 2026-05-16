import os

from typing import Optional
from dataclasses import dataclass, field


@dataclass
class HadoopConfig:
    # YARN ResourceManager REST endpoint
    rm_host: str
    rm_port: str

    # SSH edge-node used to spark-submit jobs
    ssh_host: str
    ssh_user: str
    ssh_key_path: str  # path to PEM/RSA private key
    ssh_password: str

    # spark on the cluster
    spark_home: str
    spark_master: str
    deploy_mode: str

    # paths to PySpark job script on the cluster / HDFS
    job_script_batch: str
    job_script_kafka: str
    job_script_bronze: str
    job_script_silver: str
    job_script_gold: str

    # base HDFS path for delta tables
    hdfs_delta_base: str

    hadoop_conf_dir: str = ""

    # SSH connection options
    ssh_connect_timeout: int = 10

    @classmethod
    def from_env(cls) -> "HadoopConfig":
        def _int(key: str, default: int) -> int:
            val = os.getenv(key, "").strip()
            return int(val) if val else default

        return cls(
            rm_host=os.getenv("HADOOP_RM_HOST", "").strip(),
            rm_port=_int("HADOOP_RM_PORT", 8088),
            ssh_host=os.getenv(
                "HADOOP_SSH_HOST", os.getenv("HADOOP_RM_HOST", "")
            ).strip(),
            ssh_user=os.getenv("HADOOP_SSH_USER", "hadoop").strip(),
            ssh_key_path=os.getenv("HADOOP_SSH_KEY_PATH", "").strip(),
            ssh_password=os.getenv("HADOOP_SSH_PASSWORD", "").strip(),
            spark_home=os.getenv("HADOOP_SPARK_HOME", "/opt/spark").strip(),
            spark_master=os.getenv("HADOOP_SPARK_MASTER", "yarn").strip(),
            deploy_mode=os.getenv("HADOOP_DEPLOY_MODE", "cluster").strip(),
            job_script_kafka=os.getenv("HADOOP_JOB_SCRIPT_KAFKA", "").strip(),
            job_script_batch=os.getenv("HADOOP_JOB_SCRIPT_BATCH", "").strip(),
            job_script_bronze=os.getenv("HADOOP_JOB_SCRIPT_BRONZE", "").strip(),
            job_script_silver=os.getenv("HADOOP_JOB_SCRIPT_SILVER", "").strip(),
            job_script_gold=os.getenv("HADOOP_JOB_SCRIPT_GOLD", "").strip(),
            hdfs_delta_base=os.getenv(
                "HADOOP_HDFS_DELTA_BASE", "hdfs:///user/hdmas/delta"
            ).strip(),
            hadoop_conf_dir=os.getenv(
                "HADOOP_CONF_DIR", os.getenv("YARN_CONF_DIR", "")
            ).strip(),
        )

    @property
    def is_configured(self) -> bool:
        # True when minimum required fields are present
        return bool(self.rm_host and self.ssh_host and self.ssh_user)

    @property
    def rm_base_url(self) -> str:
        return f"http://{self.rm_host}:{self.rm_port}"

    @property
    def batch_csv_local_path(self) -> str:
        return os.getenv("HADOOP_BATCH_CSV_PATH", "").strip()

    def script_for(self, stage: str) -> Optional[str]:
        return {
            "Kafka": self.job_script_kafka,
            "Batch": self.job_script_batch,
            "Bronze": self.job_script_bronze,
            "Silver": self.job_script_silver,
            "Gold": self.job_script_gold,
        }.get(stage)
