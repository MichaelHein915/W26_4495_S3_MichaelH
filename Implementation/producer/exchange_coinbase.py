"""Coinbase Exchange WebSocket producer."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from base_exchange import BaseExchange


class CoinbaseProducer(BaseExchange):
    name = "coinbase"

    def _build_subscribe_payload(self) -> str:
        return json.dumps({
            "type": "subscribe",
            "product_ids": self._symbols,
            "channels": ["ticker"],
        })

    def _parse_message(self, raw: dict) -> dict | None:
        if raw.get("type") != "ticker" or "price" not in raw:
            return None

        return {
            "exchange": self.name,
            "product_id": raw.get("product_id", "unknown"),
            "price": raw["price"],
            "size": raw.get("last_size", raw.get("size", "0")),
            "time": raw.get("time", datetime.now(timezone.utc).isoformat()),
            "raw": raw,
        }
