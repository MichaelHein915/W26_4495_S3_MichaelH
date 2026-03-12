import json
import logging
import os
import sys
from pathlib import Path

import websocket
from kafka import KafkaProducer

repo_root = Path(__file__).resolve().parents[2]
sys.path.append(str(repo_root / "src"))
from utils.config import get_config


config = get_config()
producer_log_level = os.getenv("PRODUCER_LOG_LEVEL", config.log_level)
logging.basicConfig(level=getattr(logging, producer_log_level.upper(), logging.INFO))
logger = logging.getLogger("crypto-producer")

# Initialize Kafka Producer [cite: 48, 57]
producer = KafkaProducer(
    bootstrap_servers=[config.kafka_server],
    client_id=config.kafka_client_id,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    key_serializer=lambda x: x.encode("utf-8") if isinstance(x, str) else x,
    acks="all",  # Ensure data reliability [cite: 71]
)


def on_message(ws, message):
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        logger.warning("Skipping malformed message: %s", message)
        return
    # Only process actual trade events (ticker channel) [cite: 92, 93]
    if data.get("type") == "ticker" and "price" in data:
        product_id = data.get("product_id", "unknown")
        logger.info("Streaming %s: $%s", product_id, data.get("price"))
        future = producer.send(config.topic_raw, key=product_id, value=data)
        future.add_errback(_on_send_error)


def on_error(ws, error):
    logger.error("WebSocket error: %s", error)


def on_close(ws, close_status_code, close_msg):
    logger.warning("Closed connection: %s %s", close_status_code, close_msg)
    producer.flush(10)


def on_open(ws):
    logger.info("Connection opened. Subscribing to Coinbase...")
    subscribe_msg = {"type": "subscribe", "product_ids": config.symbols, "channels": ["ticker"]}
    ws.send(json.dumps(subscribe_msg))


def _on_send_error(excp):
    logger.error("Kafka send failed", exc_info=excp)


if __name__ == "__main__":
    # Continuous connection with automatic reconnection [cite: 227]
    ws = websocket.WebSocketApp(
        config.coinbase_ws, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close
    )

    logger.info("Starting Producer for %s...", config.symbols)
    ws.run_forever(reconnect=5)  # Reconnect every 5 seconds if dropped [cite: 227]
