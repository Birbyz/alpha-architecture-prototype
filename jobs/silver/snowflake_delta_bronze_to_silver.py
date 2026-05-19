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
    bronze_table = _env("SNOWFLAKE_BRONZE_TABLE", "SNOWFLAKE_BRONZE_TABLE")
    silver_table = _env("SNOWFLAKE_SILVER_TABLE", "SNOWFLAKE_SILVER_TABLE")
    
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
    print(f"[HDMAS SNOWFLAKE] Connected. Processing data into {database}.{schema}.{silver_table}")
    
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {silver_table} (
            TRADE_KEY    VARCHAR        NOT NULL,
            TRADE_ID     VARCHAR,
            SYMBOL       VARCHAR,
            EVENT_TS     NUMBER,
            PRICE        NUMBER(38,18),
            QUANTITY     NUMBER(38,18),
            INGESTION_TS TIMESTAMP_NTZ,
            TRADE_DATE   DATE
        )
    """)
    print(f"[SNOWFLAKE SILVER] Table ready: {silver_table}")

    # merge bronze to silver
    cursor.execute(f"""
                           MERGE INTO {silver_table} AS t
        USING (
            SELECT
                SHA2(SYMBOL || '||' || TRADE_ID, 256) AS TRADE_KEY,
                TRADE_ID,
                SYMBOL,
                EVENT_TS,
                PRICE        AS PRICE,
                QUANTITY     AS QUANTITY,
                INGESTION_TS,
                TRADE_DATE
            FROM {bronze_table}
            WHERE BRONZE_IS_VALID = TRUE
              AND TRADE_ID        IS NOT NULL
              AND PRICE           IS NOT NULL
              AND QUANTITY        IS NOT NULL
              AND PRICE           > 0
              AND QUANTITY        > 0
        ) AS s
        ON  t.TRADE_KEY = s.TRADE_KEY
        WHEN NOT MATCHED THEN
            INSERT (TRADE_KEY, TRADE_ID, SYMBOL, EVENT_TS,
                    PRICE, QUANTITY, INGESTION_TS, TRADE_DATE)
            VALUES (s.TRADE_KEY, s.TRADE_ID, s.SYMBOL, s.EVENT_TS,
                    s.PRICE, s.QUANTITY, s.INGESTION_TS, s.TRADE_DATE)
    """)

    merge_result = cursor.fetchone()
    inserted = merge_result[0] if merge_result else "?"
    print(f"[SNOWFLAKE SILVER] MERGE complete — rows inserted: {inserted}")
    
    # count rows for validation
    cursor.execute(f"SELECT COUNT(*) FROM {silver_table}")
    row_count = cursor.fetchone()
    print(f"[SNOWFLAKE SILVER] Silver table total rows: {row_count[0] if row_count else '?'}")
    
    cursor.close()
    connection.close()
    print("[HDMAS SNOWFLAKE BATCH] === FINISHED ===")
    
if __name__ == "__main__":
    main()