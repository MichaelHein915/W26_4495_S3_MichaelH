"""Kraken Exchange WebSocket producer.

Kraken v2 uses JSON-based subscribe messages and returns trade data in a
specific array format via WebSocket.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from base_exchange import BaseExchange

_QUOTE_MAP = {"USD": "USD", "USDT": "USDT"}


def _to_kraken(unified: str) -> str:
    """Convert e.g. 'BTC-USD' → 'XBT/USD' (Kraken convention)."""
    base, quote = unified.split("-", 1) if "-" in unified else (unified, "USD")
    kraken_base = {"BTC": "XBT", "DOGE": "XDG"}.get(base.upper(), base.upper())
    return f"{kraken_base}/{quote.upper()}"


def _from_kraken(pair: str) -> str:
    """Convert e.g. 'XBT/USD' → 'BTC-USD'."""
    if "/" in pair:
        base, quote = pair.split("/", 1)
    else:
        base, quote = pair, "USD"
    unified_base = {"XBT": "BTC", "XDG": "DOGE"}.get(base.upper(), base.upper())
    return f"{unified_base}-{quote.upper()}"


class KrakenProducer(BaseExchange):
    name = "kraken"

    def _build_subscribe_payload(self) -> str:
        pairs = [_to_kraken(s) for s in self._symbols]
        return json.dumps(
            {
                "method": "subscribe",
                "params": {
                    "channel": "trade",
                    "symbol": pairs,
                },
            }
        )

    def _parse_message(self, raw: dict) -> dict | None:
        # Kraken v2 trade messages have channel="trade" and a data array
        if raw.get("channel") != "trade" or "data" not in raw:
            return None

        results = []
        for trade in raw["data"]:
            symbol = trade.get("symbol", "")
            unified = _from_kraken(symbol)
            ts = trade.get("timestamp", datetime.now(timezone.utc).isoformat())

            results.append(
                {
                    "exchange": self.name,
                    "product_id": unified,
                    "price": str(trade.get("price", "0")),
                    "size": str(trade.get("qty", "0")),
                    "time": ts,
                    "raw": trade,
                }
            )

        return results[0] if len(results) == 1 else results if results else None

    # _on_message is inherited from BaseExchange, which handles both
    # single-dict and list-of-dicts returns from _parse_message.
