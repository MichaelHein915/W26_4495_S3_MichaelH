"""Binance Exchange WebSocket producer.

Binance uses lowercase symbols with no separator (e.g. "btcusdt") and a
combined-stream URL to multiplex several symbols over one connection.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from base_exchange import BaseExchange

# Binance symbol → unified Coinbase-style symbol lookup
_QUOTE_ASSETS = ("USDT", "USD", "BUSD", "USDC")


def _to_unified(binance_symbol: str) -> str:
    """Convert e.g. 'BTCUSDT' → 'BTC-USDT'."""
    upper = binance_symbol.upper()
    for q in _QUOTE_ASSETS:
        if upper.endswith(q):
            base = upper[: -len(q)]
            return f"{base}-{q}"
    return upper


def _to_binance(unified: str) -> str:
    """Convert e.g. 'BTC-USD' → 'btcusdt' (Binance uses USDT pairs)."""
    base, quote = unified.split("-", 1) if "-" in unified else (unified, "USD")
    if quote == "USD":
        quote = "USDT"
    return f"{base}{quote}".lower()


class BinanceProducer(BaseExchange):
    name = "binance"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        native = [_to_binance(s) for s in self._symbols]
        streams = "/".join(f"{s}@trade" for s in native)
        self._ws_url = f"{self._ws_url}/stream?streams={streams}"
        self._native_to_unified = {
            _to_binance(s): s for s in self._symbols
        }

    def _build_subscribe_payload(self) -> str:
        return ""

    def _on_open(self, ws) -> None:
        self._logger.info("Connected to %s. %s streams via combined URL.", self.name, len(self._symbols))

    def _parse_message(self, raw: dict) -> dict | None:
        data = raw.get("data", raw)
        if data.get("e") != "trade":
            return None

        binance_sym = data.get("s", "").lower()
        unified = self._native_to_unified.get(binance_sym, _to_unified(data.get("s", "")))

        ts_ms = data.get("T") or data.get("E")
        if ts_ms:
            iso_time = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
        else:
            iso_time = datetime.now(timezone.utc).isoformat()

        return {
            "exchange": self.name,
            "product_id": unified,
            "price": str(data.get("p", "0")),
            "size": str(data.get("q", "0")),
            "time": iso_time,
            "raw": data,
        }
