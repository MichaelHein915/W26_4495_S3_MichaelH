"""
Multi-exchange producer — runs Coinbase, Binance, and Kraken producers
concurrently, all publishing to the same Kafka topic.
"""

import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

# Ensure sibling exchange modules are importable
_producer_dir = str(Path(__file__).resolve().parent)
if _producer_dir not in sys.path:
    sys.path.insert(0, _producer_dir)

from utils.config import get_config  # noqa: E402

from exchange_binance import BinanceProducer  # noqa: E402
from exchange_coinbase import CoinbaseProducer  # noqa: E402
from exchange_kraken import KrakenProducer  # noqa: E402
from utils.metrics import start_metrics_server  # noqa: E402

EXCHANGE_CLASSES = {
    "coinbase": CoinbaseProducer,
    "binance": BinanceProducer,
    "kraken": KrakenProducer,
}

DEFAULT_WS_URLS = {
    "coinbase": "wss://ws-feed.exchange.coinbase.com",
    "binance": "wss://stream.binance.com:9443",
    "kraken": "wss://ws.kraken.com/v2",
}


def main() -> None:
    config = get_config()
    start_metrics_server(default=9090)
    log_level = os.getenv("PRODUCER_LOG_LEVEL", config.log_level)
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    )
    logger = logging.getLogger("multi-exchange")

    enabled = [e.strip().lower() for e in os.getenv("EXCHANGES", "coinbase,binance,kraken").split(",") if e.strip()]
    logger.info("Enabled exchanges: %s", enabled)

    kafka_producer = KafkaProducer(
        bootstrap_servers=[config.kafka_server],
        client_id=config.kafka_client_id,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        key_serializer=lambda x: x.encode("utf-8") if isinstance(x, str) else x,
        acks="all",
    )

    threads = []
    for name in enabled:
        cls = EXCHANGE_CLASSES.get(name)
        if cls is None:
            logger.warning("Unknown exchange '%s', skipping.", name)
            continue

        ws_url = os.getenv(f"{name.upper()}_WS_URL", DEFAULT_WS_URLS.get(name, ""))
        producer = cls(
            kafka_producer=kafka_producer,
            topic=config.topic_raw,
            symbols=config.symbols,
            ws_url=ws_url,
        )
        t = producer.run_in_thread(reconnect_sec=5)
        threads.append((name, t))
        logger.info("Started %s producer (thread=%s)", name, t.name)

    shutdown_requested = False

    def _shutdown(sig, frame):
        nonlocal shutdown_requested
        logger.info("Shutting down… (signal %s)", sig)
        shutdown_requested = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("All %d producers running. Press Ctrl+C to stop.", len(threads))
    try:
        while not shutdown_requested:
            for name, t in threads:
                if not t.is_alive():
                    logger.warning("%s producer thread died — it will auto-reconnect via WebSocket.", name)
            time.sleep(5)
    finally:
        logger.info("Flushing Kafka producer…")
        kafka_producer.flush(timeout=10)
        kafka_producer.close(timeout=5)
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
