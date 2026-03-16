"""
Flask API server for the crypto streaming dashboard.
Consumes Kafka messages in a background thread and exposes REST endpoints.
"""

import json
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, request, send_from_directory
from kafka import KafkaConsumer
from prometheus_client import make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware

repo_root = Path(__file__).resolve().parents[2]
sys.path.append(str(repo_root / "src"))
from utils.config import get_config
from utils.alerts import alert_arbitrage, alert_volume_spike, alert_price_threshold, alert_anomaly
from utils.anomaly import AnomalyDetector
from utils.metrics import (
    DASHBOARD_EVENTS,
    DASHBOARD_FRESHNESS_SECONDS,
    DASHBOARD_KAFKA_ERROR,
    DASHBOARD_LAST_POLL_COUNT,
    DASHBOARD_REQUEST_DURATION,
    DASHBOARD_REQUESTS,
)

app = Flask(__name__, static_folder="web", static_url_path="/")

# Mount Prometheus metrics at /metrics
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {"/metrics": make_wsgi_app()})

# Shared state (updated by background thread)
_state = {
    "consumer": None,
    "events": deque(),
    "last_event_time": None,
    "volume_history": {},
    "kafka_error": None,
    "last_poll_count": 0,
    "running": True,
    "anomaly_detector": None,
}
_state_lock = threading.Lock()


@app.before_request
def _before_request():
    request.start_time = time.time()


@app.after_request
def _after_request(response):
    if hasattr(request, "start_time"):
        endpoint = request.endpoint or request.path or "unknown"
        DASHBOARD_REQUESTS.labels(endpoint=str(endpoint), method=request.method).inc()
        DASHBOARD_REQUEST_DURATION.labels(endpoint=str(endpoint)).observe(
            time.time() - request.start_time
        )
    return response


WINDOW_MINUTES = 3
MAX_RETENTION_MINUTES = 10


def _parse_event_time(raw_time: str) -> datetime | None:
    if not raw_time:
        return None
    try:
        return datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
    except ValueError:
        return None


def _init_consumer(topic: str, bootstrap_servers: str) -> KafkaConsumer | None:
    try:
        return KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            group_id="crypto-dashboard-api",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            # Avoid premature disconnects: longer timeouts for local/Docker setups
            session_timeout_ms=30000,
            heartbeat_interval_ms=5000,
            request_timeout_ms=40000,
        )
    except Exception as e:
        return None


def _poll_and_ingest():
    """Background thread: poll Kafka and update shared state."""
    config = get_config()
    consumer = None

    while _state["running"]:
        if consumer is None:
            consumer = _init_consumer(config.topic_raw, config.kafka_server)
            with _state_lock:
                _state["consumer"] = consumer
                if consumer is None:
                    _state["kafka_error"] = "Failed to connect to Kafka"
                else:
                    _state["events"] = deque()
                    _state["volume_history"] = {}
                    _state["kafka_error"] = None
            if consumer is None:
                time.sleep(5)
                continue

        try:
            polled = consumer.poll(timeout_ms=1000, max_records=200)
            messages = []
            for records in polled.values():
                for record in records:
                    messages.append(record.value)

            with _state_lock:
                _state["last_poll_count"] = len(messages)
                _state["kafka_error"] = None
                DASHBOARD_LAST_POLL_COUNT.set(len(messages))
                DASHBOARD_KAFKA_ERROR.set(0)

                for msg in messages:
                    event_time = _parse_event_time(msg.get("time"))
                    price = msg.get("price")
                    product_id = msg.get("product_id")
                    if not event_time or price is None or not product_id:
                        continue
                    try:
                        price_value = float(price)
                    except (TypeError, ValueError):
                        continue
                    raw_size = msg.get("last_size", msg.get("size", 0))
                    try:
                        size_qty = float(raw_size)
                    except (TypeError, ValueError):
                        size_qty = 0.0

                    exchange = msg.get("exchange", "coinbase")

                    _state["events"].append(
                        {
                            "event_time": event_time,
                            "price_usd": price_value,
                            "size_qty": size_qty,
                            "notional_usd": price_value * size_qty,
                            "product_id": product_id,
                            "exchange": exchange,
                        }
                    )
                    if _state["last_event_time"] is None or event_time > _state["last_event_time"]:
                        _state["last_event_time"] = event_time

                cutoff = datetime.now(timezone.utc) - timedelta(minutes=MAX_RETENTION_MINUTES)
                while _state["events"] and _state["events"][0]["event_time"] < cutoff:
                    _state["events"].popleft()

                DASHBOARD_EVENTS.set(len(_state["events"]))
                if _state["last_event_time"]:
                    DASHBOARD_FRESHNESS_SECONDS.set(
                        (datetime.now(timezone.utc) - _state["last_event_time"]).total_seconds()
                    )

        except Exception as e:
            with _state_lock:
                _state["kafka_error"] = str(e)
                DASHBOARD_KAFKA_ERROR.set(1)
            try:
                consumer.close()
            except Exception:
                pass
            consumer = None
            time.sleep(2)


def _get_events():
    with _state_lock:
        return list(_state["events"]), dict(_state)


def _compute_metrics(events: list) -> list[dict]:
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


def _normalize_symbol_base(product_id: str) -> str:
    """Extract base asset for cross-exchange comparison. BTC-USD, BTC-USDT -> BTC."""
    if "-" in product_id:
        return product_id.split("-", 1)[0].upper()
    return product_id.upper()


def _compute_arbitrage_opportunities(events: list, threshold_pct: float = 0.3) -> list[dict]:
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
    df["base"] = df["product_id"].apply(_normalize_symbol_base)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)

    # Avg price per (base, exchange) in the window
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


def _compute_exchange_metrics(events: list) -> list[dict]:
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


def _compute_volume_timeseries(events: list) -> list[dict]:
    """Total volume per 30s bucket for volume-over-time chart."""
    if not events:
        return []
    df = pd.DataFrame(events)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    ts = df.set_index("event_time").resample("30s").agg(total_volume_qty=("size_qty", "sum")).reset_index().dropna()
    ts["event_time"] = ts["event_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    ts["total_volume_qty"] = ts["total_volume_qty"].round(4)
    return ts.to_dict(orient="records")


def _compute_volume_by_exchange_timeseries(events: list) -> list[dict]:
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


def _compute_heatmap_data(events: list) -> dict:
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


def _get_recent_trades(events: list, limit: int = 20) -> list[dict]:
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


def _compute_exchange_stats(events: list) -> dict:
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


def _compute_sparklines(events: list) -> dict[str, list[float]]:
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


def _compute_candles(events: list, symbol: str | None = None) -> list[dict]:
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


def _compute_timeseries(events: list) -> list[dict]:
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


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/favicon.ico")
def favicon():
    """Prevent 404 when browser requests favicon."""
    return "", 204


@app.route("/api/docs")
def api_docs():
    """REST API documentation."""
    return send_from_directory(app.static_folder, "api-docs.html")


@app.route("/health")
def health():
    """Health check: Kafka connectivity and data freshness."""
    events, meta = _get_events()
    kafka_error = meta.get("kafka_error")
    last_event_time = meta.get("last_event_time")
    event_count = len(events)

    now = datetime.now(timezone.utc)
    freshness_seconds = None
    if last_event_time:
        freshness_seconds = (now - last_event_time).total_seconds()

    # Healthy if Kafka connected (no error) and either: no data yet (waiting), or data is fresh (<5min)
    stale_threshold_sec = 300
    is_healthy = kafka_error is None and (
        (event_count == 0 and freshness_seconds is None)
        or (event_count > 0 and freshness_seconds is not None and freshness_seconds < stale_threshold_sec)
    )
    status_code = 200 if is_healthy else 503

    return (
        jsonify(
            {
                "status": "ok" if is_healthy else "degraded",
                "kafka_connected": kafka_error is None,
                "kafka_error": kafka_error,
                "event_count": event_count,
                "freshness_seconds": round(freshness_seconds, 1) if freshness_seconds is not None else None,
                "last_event_time": last_event_time.isoformat() if last_event_time else None,
            }
        ),
        status_code,
    )


@app.route("/api/candles")
def candles_endpoint():
    """OHLCV candles for a specific symbol."""
    symbol = request.args.get("symbol", "")
    window = int(request.args.get("window", WINDOW_MINUTES))
    window = max(1, min(window, 30))
    exchange_filter = request.args.get("exchange", "").strip().lower()
    events, _ = _get_events()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=window)
    events = [e for e in events if e["event_time"] >= cutoff]
    if exchange_filter:
        events = [e for e in events if e.get("exchange", "coinbase").lower() == exchange_filter]
    return jsonify({"candles": _compute_candles(events, symbol or None)})


def _compute_volume_spikes(
    metrics: list[dict], volume_history: dict, threshold: float = 2.0
) -> tuple[list[dict], dict]:
    """Returns (alerts, updated volume_history). Caller should store volume_history back in state."""
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


def _build_dashboard_payload(window_minutes: int, exchange_filter: str) -> dict:
    """Build full dashboard payload for given window and exchange filter."""
    events, meta = _get_events()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    events = [e for e in events if e["event_time"] >= cutoff]
    if exchange_filter:
        events = [e for e in events if e.get("exchange", "coinbase").lower() == exchange_filter]

    config = get_config()
    arbitrage = _compute_arbitrage_opportunities(events, threshold_pct=config.alert_arbitrage_threshold_pct)
    for opp in arbitrage:
        alert_arbitrage(
            config.slack_webhook_url,
            opp["product_id"],
            opp["cheap_exchange"],
            opp["expensive_exchange"],
            opp["cheap_price"],
            opp["expensive_price"],
            opp["spread_pct"],
            email_to=config.alert_email_to,
            smtp_host=config.smtp_host,
            smtp_port=config.smtp_port,
            smtp_user=config.smtp_user,
            smtp_password=config.smtp_password,
            smtp_use_tls=config.smtp_use_tls,
        )

    metrics = _compute_metrics(events)
    anomalies = []
    if config.alert_anomaly_enabled and metrics:
        with _state_lock:
            if _state["anomaly_detector"] is None:
                _state["anomaly_detector"] = AnomalyDetector(
                    contamination=config.anomaly_contamination,
                )
        detector = _state["anomaly_detector"]
        anomalies = detector.detect(metrics)
        for a in anomalies:
            alert_anomaly(
                config.slack_webhook_url,
                a["product_id"],
                a["anomaly_score"],
                a["trade_count"],
                a["volatility_usd"],
                a["total_volume_qty"],
                a["price_change_pct"],
                email_to=config.alert_email_to,
                smtp_host=config.smtp_host,
                smtp_port=config.smtp_port,
                smtp_user=config.smtp_user,
                smtp_password=config.smtp_password,
                smtp_use_tls=config.smtp_use_tls,
            )
    timeseries = _compute_timeseries(events)
    sparklines = _compute_sparklines(events)
    exchange_stats = _compute_exchange_stats(events)
    exchange_metrics = _compute_exchange_metrics(events)
    volume_timeseries = _compute_volume_timeseries(events)
    volume_by_exchange_ts = _compute_volume_by_exchange_timeseries(events)
    heatmap_data = _compute_heatmap_data(events)
    recent_trades = _get_recent_trades(events, limit=25)
    volume_history = {k: deque(v, maxlen=30) for k, v in meta.get("volume_history", {}).items()}
    alerts, volume_history = _compute_volume_spikes(metrics, volume_history, threshold=config.alert_volume_spike_ratio)
    for a in alerts:
        alert_volume_spike(
            config.slack_webhook_url,
            a["product_id"],
            a["current_volume"],
            a["baseline_volume"],
            a["spike_ratio"],
            email_to=config.alert_email_to,
            smtp_host=config.smtp_host,
            smtp_port=config.smtp_port,
            smtp_user=config.smtp_user,
            smtp_password=config.smtp_password,
            smtp_use_tls=config.smtp_use_tls,
        )

    price_alerts = []
    for symbol, direction, threshold_price in config.alert_price_thresholds:
        for m in metrics:
            if m["product_id"].upper() == symbol.upper():
                current = float(m["avg_price_usd"])
                triggered = (direction == "above" and current >= threshold_price) or (
                    direction == "below" and current <= threshold_price
                )
                if triggered:
                    price_alerts.append(
                        {
                            "product_id": m["product_id"],
                            "direction": direction,
                            "threshold_price": threshold_price,
                            "current_price": current,
                        }
                    )
                    alert_price_threshold(
                        config.slack_webhook_url,
                        m["product_id"],
                        direction,
                        threshold_price,
                        current,
                        email_to=config.alert_email_to,
                        smtp_host=config.smtp_host,
                        smtp_port=config.smtp_port,
                        smtp_user=config.smtp_user,
                        smtp_password=config.smtp_password,
                        smtp_use_tls=config.smtp_use_tls,
                    )
                break

    with _state_lock:
        _state["volume_history"] = {k: list(v) for k, v in volume_history.items()}

    now = datetime.now(timezone.utc)
    last_event_time = meta.get("last_event_time")
    freshness_seconds = None
    if last_event_time:
        freshness_seconds = (now - last_event_time).total_seconds()

    latency_seconds = None
    if events:
        recent = events[-50:]
        total = sum((now - e["event_time"]).total_seconds() for e in recent)
        latency_seconds = total / len(recent)

    kafka_error = meta.get("kafka_error")
    last_poll_count = meta.get("last_poll_count", 0)
    event_count = len(events)

    if kafka_error:
        kafka_status = f"error: {kafka_error}"
    elif last_poll_count > 0:
        kafka_status = f"receiving ({last_poll_count} new)"
    elif event_count == 0:
        kafka_status = "waiting for data"
    else:
        kafka_status = "no new messages"

    total_trades = sum(m["trade_count"] for m in metrics)
    total_volume = sum(m["total_volume_qty"] for m in metrics)
    live_symbols = len(metrics)

    return {
        "metrics": metrics,
        "timeseries": timeseries,
        "sparklines": sparklines,
        "alerts": alerts,
        "anomalies": anomalies,
        "price_alerts": price_alerts,
        "arbitrage": arbitrage,
        "exchange_stats": exchange_stats,
        "exchange_metrics": exchange_metrics,
        "volume_timeseries": volume_timeseries,
        "volume_by_exchange_ts": volume_by_exchange_ts,
        "heatmap_data": heatmap_data,
        "recent_trades": recent_trades,
        "status": {
            "kafka_status": kafka_status,
            "kafka_error": kafka_error,
            "freshness_seconds": freshness_seconds,
            "latency_seconds": latency_seconds,
            "total_trades": total_trades,
            "total_volume": total_volume,
            "live_symbols": live_symbols,
            "event_count": event_count,
            "window_minutes": window_minutes,
            "updated_at": now.strftime("%H:%M:%S UTC"),
        },
    }


@app.route("/api/dashboard")
def dashboard():
    """Single endpoint returning all dashboard data."""
    window_minutes = int(request.args.get("window", WINDOW_MINUTES))
    window_minutes = max(1, min(window_minutes, 30))
    exchange_filter = request.args.get("exchange", "").strip().lower()
    return jsonify(_build_dashboard_payload(window_minutes, exchange_filter))


def main():
    get_config()  # validate config on startup
    t = threading.Thread(target=_poll_and_ingest, daemon=True)
    t.start()
    port = int(__import__("os").environ.get("DASHBOARD_PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
