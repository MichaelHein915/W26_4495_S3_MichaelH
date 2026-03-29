from __future__ import annotations

from dataclasses import asdict

import pandas as pd

from crypto_analytics.contracts import Candle1m, LiveMetric, RawTradeEvent, VolumeAlert


def _normalize_events(raw_events: list[dict] | list[RawTradeEvent] | pd.DataFrame) -> pd.DataFrame:
    if isinstance(raw_events, pd.DataFrame):
        frame = raw_events.copy()
    else:
        normalized = [
            asdict(event) if isinstance(event, RawTradeEvent) else dict(event) for event in raw_events
        ]
        frame = pd.DataFrame(normalized)

    if frame.empty:
        return frame

    frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    frame["received_at"] = pd.to_datetime(frame["received_at"], utc=True, errors="coerce")
    frame["size_qty"] = frame["size_qty"].astype(float)
    frame["price_usd"] = frame["price_usd"].astype(float)
    frame["window_start"] = frame["event_time"].dt.floor("min")
    frame["window_end"] = frame["window_start"] + pd.Timedelta(minutes=1)
    frame = frame.sort_values(["product_id", "event_time", "received_at"]).reset_index(drop=True)
    frame["notional_usd"] = frame["price_usd"] * frame["size_qty"]
    return frame


def _format_timestamp(value: pd.Timestamp) -> str:
    return value.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_candles(raw_events: list[dict] | list[RawTradeEvent] | pd.DataFrame) -> list[Candle1m]:
    frame = _normalize_events(raw_events)
    if frame.empty:
        return []

    candles: list[Candle1m] = []
    grouped = frame.groupby(["product_id", "window_start", "window_end"], sort=True)
    for (product_id, window_start, window_end), group in grouped:
        open_price = float(group.iloc[0]["price_usd"])
        close_price = float(group.iloc[-1]["price_usd"])
        total_volume = float(group["size_qty"].sum())
        total_notional = float(group["notional_usd"].sum())
        vwap = close_price if total_volume == 0 else total_notional / total_volume

        candles.append(
            Candle1m(
                product_id=product_id,
                window_start=_format_timestamp(window_start),
                window_end=_format_timestamp(window_end),
                open_price=open_price,
                high_price=float(group["price_usd"].max()),
                low_price=float(group["price_usd"].min()),
                close_price=close_price,
                volume_qty=total_volume,
                trade_count=int(len(group)),
                vwap_usd=float(vwap),
            )
        )

    return candles


def compute_live_metrics(raw_events: list[dict] | list[RawTradeEvent] | pd.DataFrame) -> list[LiveMetric]:
    frame = _normalize_events(raw_events)
    if frame.empty:
        return []

    metrics: list[LiveMetric] = []
    grouped = frame.groupby(["product_id", "window_start", "window_end"], sort=True)
    for (product_id, window_start, window_end), group in grouped:
        open_price = float(group.iloc[0]["price_usd"])
        last_price = float(group.iloc[-1]["price_usd"])
        avg_price = float(group["price_usd"].mean())
        total_volume = float(group["size_qty"].sum())
        total_notional = float(group["notional_usd"].sum())
        vwap = last_price if total_volume == 0 else total_notional / total_volume
        price_change_pct = 0.0 if open_price == 0 else ((last_price - open_price) / open_price) * 100
        volatility = float(group["price_usd"].std(ddof=0)) if len(group) > 1 else 0.0

        metrics.append(
            LiveMetric(
                product_id=product_id,
                window_start=_format_timestamp(window_start),
                window_end=_format_timestamp(window_end),
                last_price_usd=last_price,
                avg_price_usd=avg_price,
                price_change_pct=price_change_pct,
                volume_qty=total_volume,
                trade_count=int(len(group)),
                volatility_usd=volatility,
                vwap_usd=float(vwap),
            )
        )

    return metrics


def compute_volume_alerts(
    candles: list[Candle1m],
    volume_baselines: dict[str, float],
    spike_threshold: float = 2.0,
) -> list[VolumeAlert]:
    alerts: list[VolumeAlert] = []

    for candle in candles:
        baseline = float(volume_baselines.get(candle.product_id, 0.0))
        if baseline <= 0:
            continue

        spike_ratio = candle.volume_qty / baseline
        if spike_ratio < spike_threshold:
            continue

        severity = "high" if spike_ratio >= 3.0 else "medium"
        alerts.append(
            VolumeAlert(
                product_id=candle.product_id,
                window_start=candle.window_start,
                volume_qty=candle.volume_qty,
                baseline_volume_qty=baseline,
                spike_ratio=spike_ratio,
                severity=severity,
            )
        )

    return alerts
