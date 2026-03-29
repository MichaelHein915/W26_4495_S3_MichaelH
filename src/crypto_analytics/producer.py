from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any, Protocol

import websocket
from kafka import KafkaProducer

from crypto_analytics.contracts import RawTradeEvent
from crypto_analytics.settings import AppSettings


logger = logging.getLogger("crypto_analytics.producer")


class KafkaSendFuture(Protocol):
    def add_errback(self, callback: Callable[..., Any]) -> Any:
        """Register an error callback for async Kafka sends."""


class EventProducer(Protocol):
    def send(self, topic: str, key: str | None = None, value: dict | None = None) -> KafkaSendFuture:
        """Publish a message."""

    def flush(self, timeout: float | None = None) -> Any:
        """Flush buffered messages."""


def build_kafka_producer(settings: AppSettings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[settings.kafka_bootstrap_servers],
        client_id=settings.kafka_client_id,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
        acks="all",
    )


class CoinbaseTradeProducer:
    def __init__(self, settings: AppSettings, producer: EventProducer):
        self.settings = settings
        self.producer = producer

    def _on_send_error(self, exc: BaseException) -> None:
        logger.error("Kafka send failed", exc_info=exc)

    def _send_event(self, event: RawTradeEvent) -> None:
        future = self.producer.send(
            self.settings.kafka_topics.raw,
            key=event.product_id,
            value=event.to_dict(),
        )
        future.add_errback(self._on_send_error)

    def handle_raw_message(self, raw_message: str) -> RawTradeEvent | None:
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.warning("Skipping malformed websocket payload: %s", raw_message)
            return None

        event = RawTradeEvent.from_coinbase_ticker(payload)
        if event is None:
            return None

        self._send_event(event)
        logger.info(
            "Published %s trade at $%.2f to %s",
            event.product_id,
            event.price_usd,
            self.settings.kafka_topics.raw,
        )
        return event

    def on_open(self, ws: websocket.WebSocketApp) -> None:
        subscribe_message = {
            "type": "subscribe",
            "product_ids": self.settings.crypto_symbols,
            "channels": ["ticker"],
        }
        logger.info("Connected to Coinbase websocket. Subscribing to %d symbols.", len(self.settings.crypto_symbols))
        ws.send(json.dumps(subscribe_message))

    def on_error(self, _ws: websocket.WebSocketApp, error: Any) -> None:
        logger.error("Coinbase websocket error: %s", error)

    def on_close(
        self,
        _ws: websocket.WebSocketApp,
        close_status_code: int | None,
        close_msg: str | None,
    ) -> None:
        logger.warning("Coinbase websocket closed: code=%s message=%s", close_status_code, close_msg)
        self.producer.flush(10)

    def create_websocket_app(self) -> websocket.WebSocketApp:
        return websocket.WebSocketApp(
            self.settings.coinbase_ws_url,
            on_open=self.on_open,
            on_message=lambda ws, message: self.handle_raw_message(message),
            on_error=self.on_error,
            on_close=self.on_close,
        )

    def run_forever(self, reconnect_seconds: int = 5) -> None:
        logger.info(
            "Starting live producer for %s",
            ", ".join(self.settings.crypto_symbols),
        )
        app = self.create_websocket_app()
        app.run_forever(reconnect=reconnect_seconds)
