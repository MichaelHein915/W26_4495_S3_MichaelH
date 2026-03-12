"""
Shared trade message parser for the crypto streaming pipeline.

Accepts both:
  - Raw Coinbase format: {type: "ticker", product_id, price, last_size, time, ...}
  - Normalised multi-exchange format: {exchange, product_id, price, size, time, ...}

Returns a flat trade row suitable for sinks: trade_time, product_id, price,
size_qty, notional_usd, exchange.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def parse_trade_message(raw: dict[str, Any]) -> dict[str, Any] | None:
    """
    Parse a Kafka message into a flat trade row.

    Accepts:
      - Raw Coinbase: type=="ticker", product_id, price, last_size/size, time
      - Normalised: exchange, product_id, price, size, time (from multi-exchange producer)

    Returns dict with: trade_time, product_id, price, size_qty, notional_usd, exchange
    or None if the message is not a valid trade.
    """
    # Format 1: Raw Coinbase ticker
    if raw.get("type") == "ticker" and raw.get("price") is not None:
        try:
            price = float(raw["price"])
            size = float(raw.get("last_size") or raw.get("size") or 0)
            product_id = raw.get("product_id", "UNKNOWN")
            trade_time = raw.get("time", datetime.now(timezone.utc).isoformat())
            return {
                "trade_time": trade_time,
                "product_id": product_id,
                "price": price,
                "size_qty": size,
                "notional_usd": price * size,
                "exchange": "coinbase",
            }
        except (ValueError, TypeError):
            return None

    # Format 2: Normalised multi-exchange (exchange, product_id, price, size, time)
    if raw.get("exchange") and raw.get("product_id") is not None and raw.get("price") is not None:
        try:
            price = float(raw["price"])
            size = float(raw.get("size") or 0)
            product_id = str(raw.get("product_id", "UNKNOWN"))
            trade_time = raw.get("time", datetime.now(timezone.utc).isoformat())
            exchange = str(raw.get("exchange", "unknown"))
            return {
                "trade_time": trade_time,
                "product_id": product_id,
                "price": price,
                "size_qty": size,
                "notional_usd": price * size,
                "exchange": exchange,
            }
        except (ValueError, TypeError):
            return None

    return None
