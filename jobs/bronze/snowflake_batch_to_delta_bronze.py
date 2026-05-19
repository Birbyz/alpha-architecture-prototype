import os
import sys
import snowflake.connector


from pathlib import Path


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
    batch_csv_local_path = _env("SNOWFLAKE_BATCH_CSV_PATH")
    
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
    print(f"[HDMAS SNOWFLAKE] Connected. Loading → {database}.{schema}.{bronze_table}")
    
    # bronze table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {bronze_table} (
            TRADE_ID            VARCHAR,
            SYMBOL              VARCHAR        DEFAULT 'BTCUSDT',
            PRICE               NUMBER(38,18),
            QUANTITY            NUMBER(38,18),
            EVENT_TS            NUMBER,
            IS_MAKER            BOOLEAN,
            EVENT_TIME          TIMESTAMP_NTZ,
            TRADE_DATE          DATE,
            SOURCE              VARCHAR,
            INGESTION_TS        TIMESTAMP_NTZ,
            BRONZE_IS_VALID     BOOLEAN,
            BRONZE_INGESTED_AT  TIMESTAMP_NTZ
        )
    """)
    print(f"[HDMAS SNOWFLAKE BATCH] Table ready: {bronze_table}")
    
    # load batch CSV data
    staging_table = f"{bronze_table}_STAGE_RAW"
    cursor.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE {staging_table} (
            RAW_ID              VARCHAR,
            RAW_PRICE           VARCHAR,
            RAW_QTY             VARCHAR,
            RAW_QUOTE_QTY       VARCHAR,
            RAW_TIME            VARCHAR,
            RAW_IS_BUYER_MAKER  VARCHAR,
            RAW_IS_BEST_MATCH   VARCHAR
        )
    """)
    print(f"[HDMAS SNOWFLAKE BATCH] Staging table ready: {staging_table}")

    # file format for batch CSV
    cursor.execute("""
        CREATE OR REPLACE FILE FORMAT hdmas_binance_csv_fmt
            TYPE                         = 'CSV'
            FIELD_DELIMITER              = ','
            SKIP_HEADER                  = 0
            NULL_IF                      = ('NULL', 'null', '')
            EMPTY_FIELD_AS_NULL          = TRUE
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    """)

    
    cursor.execute("CREATE STAGE IF NOT EXISTS hdmas_batch_stage")
    
    # local files
    posix = Path(batch_csv_local_path).as_posix()
    file_uri = f"file://{posix}" if posix.startswith("/") else f"file:///{posix}"
    print(f"[HDMAS SNOWFLAKE BATCH] PUT {file_uri} at @hdmas_batch_stage ...")
    cursor.execute(f"PUT {file_uri} @hdmas_batch_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    
    for row in cursor.fetchall():
        print(f"[HDMAS SNOWFLAKE BATCH] PUT result: {row}")
        
    # copy into staging table
    cursor.execute(f"""
        COPY INTO {staging_table} (
            RAW_ID, RAW_PRICE, RAW_QTY, RAW_QUOTE_QTY,
            RAW_TIME, RAW_IS_BUYER_MAKER, RAW_IS_BEST_MATCH
        )
        FROM (
            SELECT $1, $2, $3, $4, $5, $6, $7
            FROM @hdmas_batch_stage
        )
        FILE_FORMAT = (FORMAT_NAME = 'hdmas_binance_csv_fmt')
        ON_ERROR    = 'CONTINUE'
    """)

    copy_result = cursor.fetchall()
    loaded = sum(
        r[3] for r in copy_result
        if len(r) > 3 and isinstance(r[3], (int, float)) and r[3] is not None
    )
    print(f"[HDMAS SNOWFLAKE BATCH] COPY INTO staging done — rows loaded: {loaded}")
    
    # insert into bronze with transformations
    cursor.execute(f"""
        INSERT INTO {bronze_table} (
            TRADE_ID, SYMBOL, PRICE, QUANTITY, EVENT_TS, IS_MAKER,
            EVENT_TIME, TRADE_DATE, SOURCE, INGESTION_TS,
            BRONZE_IS_VALID, BRONZE_INGESTED_AT
        )
        SELECT
            RAW_ID::VARCHAR,
 
            'BTCUSDT',
 
            TRY_CAST(RAW_PRICE AS NUMBER(38,18)),
 
            TRY_CAST(RAW_QTY AS NUMBER(38,18)),
 
            -- Time normalisation: microseconds → milliseconds when value >= 10^13
            CASE
                WHEN TRY_CAST(RAW_TIME AS BIGINT) >= 10000000000000
                THEN FLOOR(TRY_CAST(RAW_TIME AS BIGINT) / 1000)
                ELSE TRY_CAST(RAW_TIME AS BIGINT)
            END,
 
            LOWER(RAW_IS_BUYER_MAKER) = 'true',
 
            -- EVENT_TIME — ms epoch → timestamp
            TO_TIMESTAMP_NTZ(
                CASE
                    WHEN TRY_CAST(RAW_TIME AS BIGINT) >= 10000000000000
                    THEN FLOOR(TRY_CAST(RAW_TIME AS BIGINT) / 1000)
                    ELSE TRY_CAST(RAW_TIME AS BIGINT)
                END, 3
            ),
 
            TO_DATE(TO_TIMESTAMP_NTZ(
                CASE
                    WHEN TRY_CAST(RAW_TIME AS BIGINT) >= 10000000000000
                    THEN FLOOR(TRY_CAST(RAW_TIME AS BIGINT) / 1000)
                    ELSE TRY_CAST(RAW_TIME AS BIGINT)
                END, 3
            )),
 
            'binance_vision_csv',
 
            CURRENT_TIMESTAMP(),
 
            (
                RAW_PRICE IS NOT NULL AND RAW_QTY IS NOT NULL
                AND TRY_CAST(RAW_PRICE AS FLOAT) > 0
                AND TRY_CAST(RAW_QTY   AS FLOAT) > 0
            ),
 
            CURRENT_TIMESTAMP()
 
        FROM {staging_table}
    """)
    
    cursor.execute(f"SELECT COUNT(*) FROM {bronze_table}")
    total = cursor.fetchone()
    print(f"[HDMAS SNOWFLAKE BATCH] Insert into bronze done — total rows in {bronze_table}: {total[0] if total else 'unknown'}")
        
    cursor.close()
    connection.close()
    print("[HDMAS SNOWFLAKE BATCH] === FINISHED ===")
    
if __name__ == "__main__":
    main()
    



