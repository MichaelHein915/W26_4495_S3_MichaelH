from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class RawTradeEvent:
    product_id: str
    event_time: str
    price_usd: float
    size_qty: float
    trade_id: str | None
    source: str
    received_at: str

    @classmethod
    def from_coinbase_ticker(cls, payload: dict) -> "RawTradeEvent | None":
        if payload.get("type") != "ticker":
            return None
        if "price" not in payload or "product_id" not in payload:
            return None

        try:
            price_usd = float(payload["price"])
            size_qty = float(payload.get("last_size") or payload.get("size") or 0.0)
        except (TypeError, ValueError):
            return None

        return cls(
            product_id=str(payload["product_id"]),
            event_time=str(payload.get("time") or _utc_now_iso()),
            price_usd=price_usd,
            size_qty=size_qty,
            trade_id=str(payload["trade_id"]) if payload.get("trade_id") is not None else None,
            source="coinbase",
            received_at=_utc_now_iso(),
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Candle1m:
    product_id: str
    window_start: str
    window_end: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume_qty: float
    trade_count: int
    vwap_usd: float


@dataclass(frozen=True)
class LiveMetric:
    product_id: str
    window_start: str
    window_end: str
    last_price_usd: float
    avg_price_usd: float
    price_change_pct: float
    volume_qty: float
    trade_count: int
    volatility_usd: float
    vwap_usd: float


@dataclass(frozen=True)
class VolumeAlert:
    product_id: str
    window_start: str
    volume_qty: float
    baseline_volume_qty: float
    spike_ratio: float
    severity: str
