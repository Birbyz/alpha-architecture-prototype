import os
import json
import time

from decimal import Decimal
from common.utils import get_env_var
from kafka import KafkaProducer
from websocket import WebSocketApp
from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_producer() -> KafkaProducer:
    KAFKA_BOOTSTRAP_SERVERS = get_env_var("KAFKA_BOOTSTRAP_SERVERS")

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=10,
        linger_ms=5,
    )


def main():
    KAFKA_TOPIC = get_env_var("KAFKA_TOPIC")
    CRYPTO_WS_URL = get_env_var("CRYPTO_WS_URL")

    producer = make_producer()

    def on_message(ws, message: str):
        print(f"[crypto-producer] received trade : {message}", flush=True)

        message_data = json.loads(message)
        payload = {
            "source": "binance",
            "stream": "btcusdt@trade",
            "event_ts": message_data["E"],  # ms
            "event_type": message_data.get("e"),  # trade
            "price": message_data.get("p"),
            "quantity": message_data.get("q"),
            "symbol": message_data.get("s"),
            "ingestion_time": utc_now_iso(),
            "is_maker": message_data.get("m"),  # boolean
            "raw": message,  # JSON
        }
        producer.send(KAFKA_TOPIC, payload)
        print(f"[crypto-producer] produced trade to {KAFKA_TOPIC}", flush=True)

    def on_error(ws, error):
        print(f"[crypto-producer]: websocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print(f"[crypto-producer] websocket closed: {close_status_code} - {close_msg}")

    def on_open(ws):
        print(
            f"[crypto-producer] connected to {CRYPTO_WS_URL}, producing to {KAFKA_TOPIC}"
        )

    while True:
        ws = WebSocketApp(
            CRYPTO_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_close=on_close,
            on_error=on_error,
        )
        ws.run_forever(ping_interval=30, ping_timeout=10)
        print("[crypto-producer] reconnecting in 3...")
        time.sleep(3)


if __name__ == "__main__":
    main()

# docker compose up -d --build crypto-producer
# docker compose logs -f crypto-producer

# print message content from kafka
# docker compose exec kafka bash -lc \
# '/opt/kafka/bin/kafka-console-consumer.sh \
#   --bootstrap-server kafka:19092 \
#   --topic crypto-trades-raw \
#   --from-beginning \
#   --max-messages 1'
