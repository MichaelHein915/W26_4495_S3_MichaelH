"""
Base class for exchange WebSocket producers.

Every exchange adapter normalises its raw WebSocket messages into a common
schema before publishing to Kafka.  The common schema is:

    {
      "exchange": str,      # e.g. "coinbase", "binance", "kraken"
      "product_id": str,    # unified symbol like "BTC-USD"
      "price": str,         # decimal string
      "size": str,          # trade size (quantity)
      "time": str,          # ISO-8601 UTC timestamp
      "raw": dict,          # original message (optional, for debugging)
    }
"""

from __future__ import annotations

import abc
import json
import logging
import threading
from typing import Any

import websocket
from kafka import KafkaProducer

logger = logging.getLogger(__name__)

# Optional Prometheus metrics (avoid import if not installed)
try:
    from utils.metrics import KAFKA_SEND_ERRORS, TRADES_PUBLISHED
    _METRICS_AVAILABLE = True
except ImportError:
    _METRICS_AVAILABLE = False


class BaseExchange(abc.ABC):
    """Abstract WebSocket → Kafka producer for a single exchange."""

    name: str = "unknown"

    def __init__(
        self,
        kafka_producer: KafkaProducer,
        topic: str,
        symbols: list[str],
        ws_url: str,
    ) -> None:
        self._producer = kafka_producer
        self._topic = topic
        self._symbols = symbols
        self._ws_url = ws_url
        self._ws: websocket.WebSocketApp | None = None
        self._logger = logging.getLogger(f"exchange.{self.name}")

    # ── Abstract hooks ───────────────────────────────────────────────

    @abc.abstractmethod
    def _build_subscribe_payload(self) -> str:
        """Return the JSON string to send on connect."""

    @abc.abstractmethod
    def _parse_message(self, raw: dict) -> dict | None:
        """Parse a raw WS message into the common schema.

        Return *None* to silently skip non-trade messages.
        """

    def _map_symbols(self, symbols: list[str]) -> list[str]:
        """Convert unified symbols (e.g. BTC-USD) to exchange-native format.

        Override in subclasses whose native format differs.
        """
        return symbols

    # ── WebSocket callbacks ──────────────────────────────────────────

    def _on_open(self, ws: Any) -> None:
        self._logger.info("Connected to %s. Subscribing to %s symbols…", self.name, len(self._symbols))
        ws.send(self._build_subscribe_payload())

    def _on_message(self, ws: Any, message: str) -> None:
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            self._logger.warning("Malformed message from %s: %s", self.name, message[:200])
            return

        result = self._parse_message(data)
        if result is None:
            return

        # Support both single-dict and list-of-dicts (e.g. Kraken batches)
        items = result if isinstance(result, list) else [result]
        for normalised in items:
            product_id = normalised.get("product_id", "unknown")
            self._logger.info("[%s] %s: $%s", self.name, product_id, normalised.get("price"))
            future = self._producer.send(self._topic, key=product_id, value=normalised)
            if _METRICS_AVAILABLE:
                future.add_callback(lambda *_: TRADES_PUBLISHED.labels(exchange=self.name).inc())
            future.add_errback(self._on_send_error)

    def _on_error(self, ws: Any, error: Exception) -> None:
        self._logger.error("[%s] WebSocket error: %s", self.name, error)

    def _on_close(self, ws: Any, status_code: int | None, msg: str | None) -> None:
        self._logger.warning("[%s] Connection closed: %s %s", self.name, status_code, msg)
        self._producer.flush(10)

    def _on_send_error(self, excp: Exception) -> None:
        if _METRICS_AVAILABLE:
            KAFKA_SEND_ERRORS.labels(exchange=self.name).inc()
        self._logger.error("[%s] Kafka send failed", self.name, exc_info=excp)

    # ── Public API ───────────────────────────────────────────────────

    def run_forever(self, reconnect_sec: int = 5) -> None:
        """Blocking call — connect and auto-reconnect."""
        self._ws = websocket.WebSocketApp(
            self._ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._logger.info("Starting %s producer for %s…", self.name, self._symbols)
        self._ws.run_forever(reconnect=reconnect_sec)

    def run_in_thread(self, reconnect_sec: int = 5) -> threading.Thread:
        """Non-blocking — run in a daemon thread and return the thread."""
        t = threading.Thread(target=self.run_forever, args=(reconnect_sec,), daemon=True)
        t.start()
        return t
