import os

from dataclasses import dataclass

@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    database: str
    schema: str
    warehouse: str
    role: str
    
    # table names
    bronze_table: str
    silver_table: str
    gold_table: str
    
    # local CSV for batch ingestion
    batch_csv_local_path: str
    
    # path to standalone job scripts
    batch_job_script_path: str
    silver_job_script_path: str
    gold_job_script_path: str
    
    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        project_root = os.getenv("HDMAS_PROJECT_ROOT", "").strip()
        
        def _script(env_key: str, layer: str, filename: str) -> str:
            explicit = os.getenv(env_key, "").strip()
            if explicit:
                return explicit
            if project_root:
                return os.path.join(project_root, "jobs", layer, filename)
            return ""
        
        return cls(
            account = os.getenv("SNOWFLAKE_ACCOUNT", "").strip(),
            user = os.getenv("SNOWFLAKE_USER", "").strip(),
            password = os.getenv("SNOWFLAKE_PASSWORD", "").strip(),
            database = os.getenv("SNOWFLAKE_DATABASE", "HDMAS").strip(),
            schema = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC").strip(),
            warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH").strip(),
            role = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN").strip(),
            bronze_table = os.getenv("SNOWFLAKE_BRONZE_TABLE", "SNOWFLAKE_BRONZE_TABLE").strip(),
            silver_table = os.getenv("SNOWFLAKE_SILVER_TABLE", "SNOWFLAKE_SILVER_TABLE").strip(),
            gold_table = os.getenv("SNOWFLAKE_GOLD_TABLE", "SNOWFLAKE_GOLD_TABLE").strip(),
            batch_csv_local_path = os.getenv("SNOWFLAKE_BATCH_CSV_PATH", "").strip(),
            batch_job_script_path = _script("SNOWFLAKE_BATCH_JOB_SCRIPT_PATH", "batch", "batch_job.py"),
            silver_job_script_path = _script("SNOWFLAKE_SILVER_JOB_SCRIPT_PATH", "silver", "silver_job.py"),
            gold_job_script_path = _script("SNOWFLAKE_GOLD_JOB_SCRIPT_PATH", "gold", "gold_job.py")
        )
        
    @property
    def is_configured(self) -> bool:
        return bool(self.account and self.user and self.password)
    
    def connection_kwargs(self) -> dict:
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
            "role": self.role
        }
        
    def script_for(self, stage: str) -> str:
        return {
            "Batch": self.batch_job_script_path,
            "Silver": self.silver_job_script_path,
            "Gold": self.gold_job_script_path
        }.get(stage, "")
        
    def table_for(self, layer: str) -> str:
        return {
            "Bronze": self.bronze_table,
            "Silver": self.silver_table,
            "Gold": self.gold_table
        }.get(layer, "")