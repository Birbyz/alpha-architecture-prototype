"""
HDMAS — Snowflake Kafka → Bronze
---------------------------------
Continuous Kafka consumer that reads raw trade events from the
crypto-trades-raw topic and inserts them into the Snowflake Bronze table.

Matches the existing HDMAS_BRONZE schema:
    TRADE_ID, SYMBOL, PRICE, QUANTITY, EVENT_TS, IS_MAKER,
    EVENT_TIME, TRADE_DATE, SOURCE, INGESTION_TS,
    BRONZE_IS_VALID, BRONZE_INGESTED_AT
"""

import json
import os
import signal
import sys
import time
import snowflake.connector

from datetime import datetime, timezone
from kafka import KafkaConsumer


# --------------------------------------------------
# Config
# --------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv(
    "SNOWFLAKE_KAFKA_BOOTSTRAP_SERVERS",
    os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
)
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC", "crypto-trades-raw")
KAFKA_GROUP_ID = "hdmas-snowflake-bronze-consumer"

SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "HDMAS")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
BRONZE_TABLE        = os.getenv("SNOWFLAKE_BRONZE_TABLE", "HDMAS_BRONZE")

BATCH_SIZE     = int(os.getenv("SF_KAFKA_BATCH_SIZE", "200"))
FLUSH_INTERVAL = int(os.getenv("SF_KAFKA_FLUSH_INTERVAL", "15"))

KAFKA_MAX_RETRIES = 24   # 24 × 5s = 2 min max wait for broker readiness
KAFKA_RETRY_DELAY = 5


# --------------------------------------------------
# Logging
# --------------------------------------------------
def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] [HDMAS SNOWFLAKE KAFKA] {msg}", flush=True)


# --------------------------------------------------
# Graceful shutdown
# --------------------------------------------------
_running = True

def _handle_signal(signum, frame):
    global _running
    _log(f"Received signal {signum} — shutting down gracefully...")
    _running = False

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# --------------------------------------------------
# Snowflake
# --------------------------------------------------
def _connect_snowflake():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
        network_timeout=30,
        login_timeout=30,
    )


# INSERT matches the existing HDMAS_BRONZE schema exactly.
# Plain VALUES insert — executemany works correctly with simple INSERT.
INSERT_SQL = f"""
    INSERT INTO {BRONZE_TABLE} (
        TRADE_ID, SYMBOL, PRICE, QUANTITY, EVENT_TS, IS_MAKER,
        EVENT_TIME, TRADE_DATE, SOURCE, INGESTION_TS,
        BRONZE_IS_VALID, BRONZE_INGESTED_AT
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def _flush(cursor, batch: list) -> int:
    if not batch:
        return 0
    rows = [
        (
            r["trade_id"],
            r["symbol"],
            r["price"],
            r["quantity"],
            r["event_ts"],
            r["is_maker"],
            r["event_time"],
            r["trade_date"],
            r["source"],
            r["ingestion_ts"],
            r["bronze_is_valid"],
            r["bronze_ingested_at"],
        )
        for r in batch
    ]
    cursor.executemany(INSERT_SQL, rows)
    return len(rows)


# --------------------------------------------------
# Message parsing
# --------------------------------------------------
def _parse(raw_value: bytes) -> dict | None:
    """Parse a Kafka message into a Bronze row matching HDMAS_BRONZE schema.
    Returns None if the message cannot be parsed or is missing required fields.
    """
    try:
        payload = json.loads(raw_value.decode("utf-8"))
    except Exception:
        return None

    event_ts  = payload.get("event_ts")
    symbol    = payload.get("symbol", "BTCUSDT")
    price_str = payload.get("price", "")
    qty_str   = payload.get("quantity", "")
    is_maker  = payload.get("is_maker", False)
    ing_time  = payload.get("ingestion_time", "")

    # trade_id lives inside the nested raw Binance payload under key "t"
    try:
        raw_data = payload.get("raw")
        trade_id = str(json.loads(raw_data).get("t", "")) if raw_data else ""
    except Exception:
        trade_id = ""

    if not event_ts or not symbol or not price_str or not qty_str:
        return None

    try:
        price    = float(price_str)
        quantity = float(qty_str)
    except (ValueError, TypeError):
        return None

    try:
        ingestion_ts = datetime.fromisoformat(ing_time) if ing_time else datetime.now(timezone.utc)
    except ValueError:
        ingestion_ts = datetime.now(timezone.utc)

    # EVENT_TIME: human-readable timestamp from event_ts milliseconds
    event_time = datetime.fromtimestamp(event_ts / 1000, tz=timezone.utc)
    trade_date = event_time.date()

    return {
        "trade_id":           trade_id,
        "symbol":             symbol,
        "price":              price,
        "quantity":           quantity,
        "event_ts":           event_ts,
        "is_maker":           is_maker,
        "event_time":         event_time.isoformat(),
        "trade_date":         trade_date.isoformat(),
        "source":             "kafka-stream",
        "ingestion_ts":       ingestion_ts.isoformat(),
        "bronze_is_valid":    True,
        "bronze_ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# --------------------------------------------------
# Main
# --------------------------------------------------
def main() -> None:
    _log(f"Starting — Kafka: {KAFKA_BOOTSTRAP} | Topic: {KAFKA_TOPIC} | Table: {BRONZE_TABLE}")
    _log(f"Batch size: {BATCH_SIZE} rows | Flush interval: {FLUSH_INTERVAL}s")

    # Step 1 — connect to Snowflake
    try:
        conn   = _connect_snowflake()
        cursor = conn.cursor()
        conn.commit()
        _log(f"Snowflake connection established. Target: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{BRONZE_TABLE}")
    except Exception as e:
        _log(f"[ERR] Failed to connect to Snowflake: {e}")
        sys.exit(1)

    # Step 2 — connect to Kafka with retry
    # Kafka broker takes 10-20s to initialise after Docker starts.
    consumer: KafkaConsumer | None = None
    for attempt in range(1, KAFKA_MAX_RETRIES + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
                value_deserializer=None,
            )
            _log(f"Kafka consumer connected (attempt {attempt}/{KAFKA_MAX_RETRIES}).")
            break
        except Exception as e:
            if attempt == KAFKA_MAX_RETRIES:
                _log(f"[ERR] Kafka not available after {KAFKA_MAX_RETRIES} attempts: {e}")
                conn.close()
                sys.exit(1)
            _log(
                f"Kafka not ready yet (attempt {attempt}/{KAFKA_MAX_RETRIES})"
                f" — retrying in {KAFKA_RETRY_DELAY}s..."
            )
            time.sleep(KAFKA_RETRY_DELAY)

    # consumer is guaranteed non-None here — sys.exit(1) covers the failure path
    assert consumer is not None

    # Step 3 — consume loop
    batch:          list  = []
    last_flush:     float = time.time()
    total_inserted: int   = 0

    try:
        while _running:
            for message in consumer:
                if not _running:
                    break

                row = _parse(message.value)
                if row:
                    batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    n = _flush(cursor, batch)
                    conn.commit()
                    total_inserted += n
                    _log(f"Flushed {n} rows (total: {total_inserted})")
                    batch.clear()
                    last_flush = time.time()

            # Flush on time interval even if batch is not full
            if batch and (time.time() - last_flush) >= FLUSH_INTERVAL:
                n = _flush(cursor, batch)
                conn.commit()
                total_inserted += n
                _log(f"Interval flush: {n} rows (total: {total_inserted})")
                batch.clear()
                last_flush = time.time()

    finally:
        if batch:
            try:
                n = _flush(cursor, batch)
                conn.commit()
                total_inserted += n
                _log(f"Final flush: {n} rows")
            except Exception as e:
                _log(f"Final flush failed: {e}")

        consumer.close()
        cursor.close()
        conn.close()
        _log(f"Shutdown complete. Total rows inserted this session: {total_inserted}")


if __name__ == "__main__":
    main()