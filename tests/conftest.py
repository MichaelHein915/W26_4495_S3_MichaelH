import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))


def make_ticker_event(
    product_id="BTC-USD",
    price="65000.50",
    last_size="0.001",
    time_str="2026-03-06T12:00:00.000000Z",
):
    """Factory for a realistic Coinbase ticker message."""
    return {
        "type": "ticker",
        "sequence": 123456,
        "product_id": product_id,
        "price": price,
        "open_24h": "64000.00",
        "volume_24h": "1234.5678",
        "low_24h": "63000.00",
        "high_24h": "66000.00",
        "volume_30d": "56789.1234",
        "best_bid": "64999.99",
        "best_bid_size": "0.5",
        "best_ask": "65001.01",
        "best_ask_size": "0.3",
        "side": "buy",
        "time": time_str,
        "trade_id": 99999,
        "last_size": last_size,
    }


def make_parsed_event(
    product_id="BTC-USD",
    price_usd=65000.50,
    size_qty=0.001,
    time_str="2026-03-06T12:00:00.000000Z",
):
    """Factory for a parsed dashboard event dict."""
    event_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
    return {
        "event_time": event_time,
        "price_usd": price_usd,
        "size_qty": size_qty,
        "notional_usd": price_usd * size_qty,
        "product_id": product_id,
    }


@pytest.fixture()
def sample_ticker():
    return make_ticker_event()


@pytest.fixture()
def sample_events():
    """A batch of parsed events across two symbols for aggregation tests."""
    events = []
    for i in range(10):
        events.append(
            make_parsed_event(
                product_id="BTC-USD",
                price_usd=65000.0 + i * 10,
                size_qty=0.001 + i * 0.0001,
                time_str=f"2026-03-06T12:00:{i:02d}.000000Z",
            )
        )
    for i in range(5):
        events.append(
            make_parsed_event(
                product_id="ETH-USD",
                price_usd=3500.0 + i * 5,
                size_qty=0.01 + i * 0.001,
                time_str=f"2026-03-06T12:00:{i:02d}.000000Z",
            )
        )
    return events
