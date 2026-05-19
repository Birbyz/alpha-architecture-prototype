import os
import sys
import snowflake.connector

def _env(key: str, default: str = "") -> str:
    val = os.getenv(key, default).strip()
    if not val and not default:
        print(f"[HDMAS SNOWFLAKE] Warning: Environment variable '{key}' is not set.")
        sys.exit(1)
    return val

def main():
    account = _env("SNOWFLAKE_ACCOUNT")
    user = _env("SNOWFLAKE_USER")
    password = _env("SNOWFLAKE_PASSWORD")
    database = _env("SNOWFLAKE_DATABASE", "HDMAS")
    schema = _env("SNOWFLAKE_SCHEMA", "PUBLIC")
    warehouse = _env("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    role = _env("SNOWFLAKE_ROLE", "SYSADMIN")
    silver_table = _env("SNOWFLAKE_SILVER_TABLE", "SNOWFLAKE_SILVER_TABLE")
    gold_table = _env("SNOWFLAKE_GOLD_TABLE", "SNOWFLAKE_GOLD_TABLE")

    print(f"[HDMAS SNOWFLAKE] Connecting to account: {account} | user: {user}...")
    connection = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role
    )
    cursor = connection.cursor()
    print(f"[HDMAS SNOWFLAKE] Connected. Processing data into {database}.{schema}.{gold_table}")


    # gold table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            SYMBOL                   VARCHAR,
            AGGREGATION_WINDOW_START TIMESTAMP_NTZ,
            AGGREGATION_WINDOW_END   TIMESTAMP_NTZ,
            TRADE_COUNT              INTEGER,
            VOLUME_BTC               NUMBER(38,18),
            VOLUME_USD               NUMBER(38,8),
            VWAP_PRICE_USD           NUMBER(38,8),
            GOLD_INGESTED_AT         TIMESTAMP_NTZ
        )
    """)
    print(f"[SNOWFLAKE GOLD] Table ready: {gold_table}")
    
    # truncate + insert
    cursor.execute(f"TRUNCATE TABLE {gold_table}")
    print("[SNOWFLAKE GOLD] Table truncated.")
    
    cursor.execute(f"""
                INSERT INTO {gold_table} (
            SYMBOL,
            AGGREGATION_WINDOW_START,
            AGGREGATION_WINDOW_END,
            TRADE_COUNT,
            VOLUME_BTC,
            VOLUME_USD,
            VWAP_PRICE_USD,
            GOLD_INGESTED_AT
        )
        SELECT
            SYMBOL,
 
            -- 1-minute window start — derived from EVENT_TS (millisecond epoch)
            DATE_TRUNC('MINUTE', TO_TIMESTAMP_NTZ(EVENT_TS, 3))
                AS AGGREGATION_WINDOW_START,
 
            DATEADD('MINUTE', 1, DATE_TRUNC('MINUTE', TO_TIMESTAMP_NTZ(EVENT_TS, 3)))
                AS AGGREGATION_WINDOW_END,
 
            COUNT(*)                                        AS TRADE_COUNT,
            SUM(QUANTITY)                                   AS VOLUME_BTC,
            SUM(PRICE * QUANTITY)                           AS VOLUME_USD,
 
            -- VWAP — DIV0 avoids division-by-zero
            DIV0(SUM(PRICE * QUANTITY), SUM(QUANTITY))      AS VWAP_PRICE_USD,
 
            CURRENT_TIMESTAMP()                             AS GOLD_INGESTED_AT
 
        FROM {silver_table}
        WHERE SYMBOL   IS NOT NULL
          AND EVENT_TS IS NOT NULL
        GROUP BY
            SYMBOL,
            DATE_TRUNC('MINUTE', TO_TIMESTAMP_NTZ(EVENT_TS, 3))
        ORDER BY AGGREGATION_WINDOW_START DESC
    """)

    # row count for verification
    cursor.execute(f"SELECT COUNT(*) FROM {gold_table}")
    row_count = cursor.fetchone()
    print(f"[SNOWFLAKE GOLD] Insert complete — total rows in Gold table: {row_count[0] if row_count else '?'}")
    
    cursor.close()
    connection.close()
    print("[HDMAS SNOWFLAKE] === FINISHED ===")
    
if __name__ == "__main__":
    main()
    
