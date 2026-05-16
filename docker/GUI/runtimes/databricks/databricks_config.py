import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabricksConfig:
    # connection
    host: str
    token: str
    
    # job IDs for each stage
    job_id_kafka: Optional[int]
    job_id_batch: Optional[int]
    job_id_bronze: Optional[int]
    job_id_silver: Optional[int]
    job_id_gold: Optional[int]
    
    # Storage configuration
    batch_csv_local_path: str
    
    #DBFS destination path for uploaded CSV
    dbfs_delta_base: str
    
    # batch CSV
    batch_csv_dbfs_path: str
    
    # job cluster ID
    cluster_id: str

    @classmethod
    def from_env(cls) -> "DatabricksConfig":
        def _int(key: str) -> Optional[int]:
            val = os.getenv(key, "").strip()
            return int(val) if val else None

        return cls(
            host=os.getenv("DATABRICKS_HOST", "").rstrip("/"),
            token=os.getenv("DATABRICKS_TOKEN", ""),
            
            job_id_kafka=_int("DATABRICKS_JOB_ID_KAFKA"),
            job_id_batch=_int("DATABRICKS_JOB_ID_BATCH"),
            job_id_bronze=_int("DATABRICKS_JOB_ID_BRONZE"),
            job_id_silver=_int("DATABRICKS_JOB_ID_SILVER"),
            job_id_gold=_int("DATABRICKS_JOB_ID_GOLD"),
            
            dbfs_delta_base=os.getenv("DATABRICKS_DBFS_DELTA_BASE", "dbfs:/user/hdmas/delta").strip(),
            batch_csv_local_path=os.getenv("DATABRICKS_BATCH_CSV_LOCAL_PATH", "").strip(),
            batch_csv_dbfs_path=os.getenv("DATABRICKS_BATCH_CSV_DBFS_PATH", "").strip(),
            cluster_id=os.getenv("DATABRICKS_CLUSTER_ID", "").strip(),
        )

    @property
    def is_configured(self) -> bool:
        return bool(self.host and self.token)

    def job_id_for(self, stage: str) -> Optional[int]:
        return {
            "Kafka": self.job_id_kafka,
            "Batch": self.job_id_batch,
            "Bronze": self.job_id_bronze,
            "Silver": self.job_id_silver,
            "Gold": self.job_id_gold,
        }.get(stage)
        
    def dbfs_path_for(self, layer: str) -> str:
        # resolve the DBFS path for a given layer (e.g., "Bronze", "Silver", "Gold")
        return f"{self.dbfs_delta_base.rstrip('/')}/{layer.lower()}"
