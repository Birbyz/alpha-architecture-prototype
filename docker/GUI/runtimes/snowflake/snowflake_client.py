import snowflake.connector

from typing import Any, Dict, Optional, List
from runtimes.snowflake.snowflake_config import SnowflakeConfig

class SnowflakeClient:
    def __init__(self, config: SnowflakeConfig, timeout: int = 15):
        self._config = config
        self._timeout = timeout
        
    def _connect(self):
        return snowflake.connector.connect(
            **self._config.connection_kwargs(),
            network_timeout=self._timeout,
            login_timeout=self._timeout
        )
    
    def is_connected(self) -> bool:
        try:
            connection = self._connect()
            cursor = connection.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            cursor.fetchone()
            connection.close()

            return True
        except Exception:
            return False
        
    def get_version(self) -> Optional[str]:
        try:
            connection = self._connect()
            cursor = connection.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            row = cursor.fetchone()
            connection.close()
            
            return row[0] if row else None
        except Exception:
            return None
        
    def table_exists(self, table_name: str) -> bool:
        try:
            connection = self._connect()
            cursor = connection.cursor()
            # SHOW TABLES is faster than querying INFORMATION_SCHEMA for existence checks
            cursor.execute(f"SHOW TABLES LIKE '{table_name.upper()}'")
            rows = cursor.fetchall()
            connection.close()
            return len(rows) > 0
        except Exception:
            return False
        
    def get_table_row_count(self, table_name: str) -> Optional[int]:
        try:
            connection = self._connect()
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row = cursor.fetchone()
            connection.close()
            return row[0] if row else None
        except Exception:
            return None
        
    # gold data query
    def query_gold_latest_data(self, table_name: str, limit: int = 20) -> List[Dict[str, Any]]:
        try:
            connection = self._connect()
            cursor = connection.cursor()
            cursor.execute(
                f"""
                SELECT
                    SYMBOL,
                    AGGREGATION_WINDOW_START,
                    AGGREGATION_WINDOW_END,
                    TRADE_COUNT,
                    VOLUME_BTC,
                    VOLUME_USD,
                    VWAP_PRICE_USD,
                    GOLD_INGESTED_AT
                FROM {table_name}
                ORDER BY AGGREGATION_WINDOW_START DESC
                LIMIT {int(limit)}
            """)
            
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            connection.close()
            return rows
        except Exception as e:
            return [{"error": str(e)}]