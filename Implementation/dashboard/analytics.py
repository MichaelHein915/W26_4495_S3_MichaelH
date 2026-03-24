"""
Pure analytics functions for the crypto streaming dashboard.

All functions are stateless — they take data in and return results out,
with no side effects, Kafka access, or shared-state mutation.
"""

from collections import deque
from datetime import datetime

import pandas as pd


def parse_event_time(raw_time: str) -> datetime | None:
    if not raw_time:
        return None
    try:
        return datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
    except ValueError:
        return None


def compute_metrics(events: list) -> list[dict]:
    if not events:
        return []
    df = pd.DataFrame(events)
    metrics = (
        df.groupby("product_id", as_index=False)
        .agg(
            trade_count=("price_usd", "count"),
            avg_price_usd=("price_usd", "mean"),
            total_volume_qty=("size_qty", "sum"),
            total_notional_usd=("notional_usd", "sum"),
            volatility_usd=("price_usd", "std"),
            first_price=("price_usd", "first"),
            last_price=("price_usd", "last"),
        )
        .sort_values("product_id")
    )
    metrics["vwap_usd"] = metrics["avg_price_usd"]
    has_volume = metrics["total_volume_qty"] > 0
    metrics.loc[has_volume, "vwap_usd"] = (
        metrics.loc[has_volume, "total_notional_usd"] / metrics.loc[has_volume, "total_volume_qty"]
    )
    metrics["volatility_usd"] = metrics["volatility_usd"].fillna(0.0)
    metrics["price_change_pct"] = (
        (((metrics["last_price"] - metrics["first_price"]) / metrics["first_price"]) * 100).round(2).fillna(0.0)
    )
    for col in ["avg_price_usd", "vwap_usd", "volatility_usd", "total_volume_qty"]:
        metrics[col] = metrics[col].round(4 if col == "total_volume_qty" else 2)
    return metrics[
        [
            "product_id",
            "trade_count",
            "avg_price_usd",
            "vwap_usd",
            "volatility_usd",
            "total_volume_qty",
            "price_change_pct",
        ]
    ].to_dict(orient="records")


def normalize_symbol_base(product_id: str) -> str:
    """Extract base asset for cross-exchange comparison. BTC-USD, BTC-USDT -> BTC."""
    if "-" in product_id:
        return product_id.split("-", 1)[0].upper()
    return product_id.upper()


def compute_arbitrage_opportunities(events: list, threshold_pct: float = 0.3) -> list[dict]:
    """
    Detect cross-exchange price differences. Returns opportunities where
    spread between cheapest and most expensive exchange exceeds threshold.
    """
    if not events or len(events) < 2:
        return []
    df = pd.DataFrame(events)
    if "exchange" not in df.columns:
        df["exchange"] = "coinbase"
    df["exchange"] = df["exchange"].fillna("coinbase")
    df["base"] = df["product_id"].apply(normalize_symbol_base)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)

    by_base_exchange = (
        df.groupby(["base", "exchange"], as_index=False)
        .agg(avg_price=("price_usd", "mean"), last_price=("price_usd", "last"))
        .groupby("base")
    )

    opportunities = []
    for base, grp in by_base_exchange:
        if len(grp) < 2:
            continue
        prices = grp.set_index("exchange")["avg_price"]
        min_price = prices.min()
        max_price = prices.max()
        if min_price <= 0:
            continue
        spread_pct = ((max_price - min_price) / min_price) * 100
        if spread_pct < threshold_pct:
            continue
        cheap_ex = prices.idxmin()
        expensive_ex = prices.idxmax()
        opportunities.append(
            {
                "product_id": f"{base}-USD",
                "base": base,
                "cheap_exchange": cheap_ex,
                "expensive_exchange": expensive_ex,
                "cheap_price": round(float(min_price), 2),
                "expensive_price": round(float(max_price), 2),
                "spread_pct": round(spread_pct, 2),
            }
        )
    return sorted(opportunities, key=lambda x: -x["spread_pct"])


def compute_exchange_metrics(events: list) -> list[dict]:
    """Per (product_id, exchange): avg_price, trade_count, total_volume for exchange comparison chart."""
    if not events:
        return []
    df = pd.DataFrame(events)
    if "exchange" not in df.columns:
        df["exchange"] = "coinbase"
    df["exchange"] = df["exchange"].fillna("coinbase")
    agg = df.groupby(["product_id", "exchange"], as_index=False).agg(
        avg_price_usd=("price_usd", "mean"),
        trade_count=("price_usd", "count"),
        total_volume_qty=("size_qty", "sum"),
    )
    for col in ["avg_price_usd", "total_volume_qty"]:
        agg[col] = agg[col].round(4 if col == "total_volume_qty" else 2)
    return agg.to_dict(orient="records")


def compute_volume_timeseries(events: list) -> list[dict]:
    """Total volume per 30s bucket for volume-over-time chart."""
    if not events:
        return []
    df = pd.DataFrame(events)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    ts = df.set_index("event_time").resample("30s").agg(total_volume_qty=("size_qty", "sum")).reset_index().dropna()
    ts["event_time"] = ts["event_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    ts["total_volume_qty"] = ts["total_volume_qty"].round(4)
    return ts.to_dict(orient="records")


def compute_volume_by_exchange_timeseries(events: list) -> list[dict]:
    """Volume per 30s bucket per exchange for stacked area chart."""
    if not events:
        return []
    df = pd.DataFrame(events)
    if "exchange" not in df.columns:
        df["exchange"] = "coinbase"
    df["exchange"] = df["exchange"].fillna("coinbase")
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    ts = (
        df.set_index("event_time")
        .groupby("exchange")
        .resample("30s", include_groups=False)
        .agg(volume=("size_qty", "sum"))
        .reset_index()
    )
    ts["event_time"] = ts["event_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    ts["volume"] = ts["volume"].round(4)
    return ts.to_dict(orient="records")


def compute_heatmap_data(events: list) -> dict:
    """Price change % by symbol x time bucket for heatmap. Returns {labels, times, matrix}."""
    if not events:
        return {"labels": [], "times": [], "matrix": []}
    df = pd.DataFrame(events)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df["bucket"] = df["event_time"].dt.floor("30s")
    agg = df.groupby(["product_id", "bucket"], as_index=False).agg(avg_price=("price_usd", "mean"))
    if agg.empty:
        return {"labels": [], "times": [], "matrix": []}
    buckets = sorted(agg["bucket"].unique())[-20:]
    labels = sorted(agg["product_id"].unique())
    matrix = []
    for pid in labels:
        row = []
        for b in buckets:
            v = agg[(agg["product_id"] == pid) & (agg["bucket"] == b)]["avg_price"]
            row.append(float(v.iloc[0]) if len(v) > 0 else None)
        first = next((v for v in row if v is not None), None)
        pct_row = [
            round(((v - first) / first * 100), 2) if first and v is not None else None
            for v in row
        ]
        matrix.append(pct_row)
    return {
        "labels": labels,
        "times": [b.strftime("%H:%M") for b in buckets],
        "matrix": matrix,
    }


def get_recent_trades(events: list, limit: int = 20) -> list[dict]:
    """Last N trades for ticker display."""
    if not events:
        return []
    sorted_events = sorted(events, key=lambda e: e["event_time"], reverse=True)
    out = []
    for e in sorted_events[:limit]:
        t = e["event_time"]
        time_str = t.strftime("%H:%M:%S") if hasattr(t, "strftime") else str(t)[:19]
        out.append(
            {
                "product_id": e["product_id"],
                "price_usd": round(float(e["price_usd"]), 2),
                "size_qty": round(float(e.get("size_qty", 0)), 4),
                "exchange": e.get("exchange", "coinbase"),
                "event_time": time_str,
            }
        )
    return out


def compute_exchange_stats(events: list) -> dict:
    """Per-exchange trade count and list of unique exchanges seen."""
    if not events:
        return {"exchanges": [], "exchange_counts": {}, "exchange_symbols": {}}
    df = pd.DataFrame(events)
    if "exchange" not in df.columns:
        df["exchange"] = "coinbase"
    df["exchange"] = df["exchange"].fillna("coinbase")
    by_exchange = df.groupby("exchange", as_index=False).agg(
        trade_count=("price_usd", "count"),
    )
    exchange_counts = dict(zip(by_exchange["exchange"], by_exchange["trade_count"]))
    exchange_symbols = {}
    for exch, grp in df.groupby("exchange"):
        exchange_symbols[exch] = sorted(grp["product_id"].unique().tolist())
    return {
        "exchanges": sorted(df["exchange"].unique().tolist()),
        "exchange_counts": exchange_counts,
        "exchange_symbols": exchange_symbols,
    }


def compute_sparklines(events: list) -> dict[str, list[float]]:
    """Return last ~20 price samples per symbol for inline sparklines."""
    if not events:
        return {}
    df = pd.DataFrame(events)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    sparklines = {}
    for pid, grp in df.groupby("product_id"):
        prices = grp.sort_values("event_time")["price_usd"].tolist()
        step = max(1, len(prices) // 20)
        sparklines[pid] = [round(p, 2) for p in prices[::step][-20:]]
    return sparklines


def compute_candles(events: list, symbol: str | None = None) -> list[dict]:
    """Compute 1-minute OHLCV candles, optionally filtered to a single symbol."""
    if not events:
        return []
    df = pd.DataFrame(events)
    if symbol:
        df = df[df["product_id"] == symbol]
    if df.empty:
        return []
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df = df.set_index("event_time")

    candles = (
        df.groupby("product_id")
        .resample("1min", include_groups=False)
        .agg(
            open=("price_usd", "first"),
            high=("price_usd", "max"),
            low=("price_usd", "min"),
            close=("price_usd", "last"),
            volume=("size_qty", "sum"),
        )
        .reset_index()
    )
    candles = candles.dropna(subset=["open"])
    candles["event_time"] = candles["event_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    for col in ["open", "high", "low", "close"]:
        candles[col] = candles[col].round(2)
    candles["volume"] = candles["volume"].round(6)
    return candles.to_dict(orient="records")


def compute_timeseries(events: list) -> list[dict]:
    if not events:
        return []
    df = pd.DataFrame(events)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df["avg_price_usd"] = df["price_usd"].astype(float)
    ts = (
        df.set_index("event_time")
        .groupby("product_id")
        .resample("30s", include_groups=False)
        .mean(numeric_only=True)
        .reset_index()
        .sort_values(["product_id", "event_time"])
    )
    ts = ts.dropna(subset=["avg_price_usd"])
    ts["event_time"] = ts["event_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    return ts.to_dict(orient="records")


def compute_volume_spikes(
    metrics: list[dict], volume_history: dict, threshold: float = 2.0
) -> tuple[list[dict], dict]:
    """Returns (alerts, updated volume_history)."""
    alerts = []
    for row in metrics:
        symbol = row["product_id"]
        current = float(row["total_volume_qty"])
        history = volume_history.setdefault(symbol, deque(maxlen=30))
        if len(history) >= 5:
            baseline = sum(history) / len(history)
            if baseline > 0 and current >= threshold * baseline:
                alerts.append(
                    {
                        "product_id": symbol,
                        "current_volume": current,
                        "baseline_volume": baseline,
                        "spike_ratio": round(current / baseline, 2),
                    }
                )
        history.append(current)
    return alerts, volume_history
