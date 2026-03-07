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
from flask import Flask, jsonify, send_from_directory
from kafka import KafkaConsumer

repo_root = Path(__file__).resolve().parents[2]
sys.path.append(str(repo_root / "src"))
from utils.config import get_config

app = Flask(__name__, static_folder="web", static_url_path="/")

# Shared state (updated by background thread)
_state = {
    "consumer": None,
    "events": deque(),
    "last_event_time": None,
    "volume_history": {},
    "kafka_error": None,
    "last_poll_count": 0,
    "running": True,
}
_state_lock = threading.Lock()

WINDOW_MINUTES = 3


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

                    _state["events"].append(
                        {
                            "event_time": event_time,
                            "price_usd": price_value,
                            "size_qty": size_qty,
                            "notional_usd": price_value * size_qty,
                            "product_id": product_id,
                        }
                    )
                    if (
                        _state["last_event_time"] is None
                        or event_time > _state["last_event_time"]
                    ):
                        _state["last_event_time"] = event_time

                cutoff = datetime.now(timezone.utc) - timedelta(minutes=WINDOW_MINUTES)
                while _state["events"] and _state["events"][0]["event_time"] < cutoff:
                    _state["events"].popleft()

        except Exception as e:
            with _state_lock:
                _state["kafka_error"] = str(e)
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
        )
        .sort_values("product_id")
    )
    metrics["vwap_usd"] = metrics["avg_price_usd"]
    has_volume = metrics["total_volume_qty"] > 0
    metrics.loc[has_volume, "vwap_usd"] = (
        metrics.loc[has_volume, "total_notional_usd"]
        / metrics.loc[has_volume, "total_volume_qty"]
    )
    metrics["volatility_usd"] = metrics["volatility_usd"].fillna(0.0)
    for col in ["avg_price_usd", "vwap_usd", "volatility_usd", "total_volume_qty"]:
        metrics[col] = metrics[col].round(4 if col == "total_volume_qty" else 2)
    return metrics[
        ["product_id", "trade_count", "avg_price_usd", "vwap_usd", "volatility_usd", "total_volume_qty"]
    ].to_dict(orient="records")


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


def _compute_volume_spikes(metrics: list[dict], volume_history: dict) -> tuple[list[dict], dict]:
    """Returns (alerts, updated volume_history). Caller should store volume_history back in state."""
    alerts = []
    for row in metrics:
        symbol = row["product_id"]
        current = float(row["total_volume_qty"])
        history = volume_history.setdefault(symbol, deque(maxlen=30))
        if len(history) >= 5:
            baseline = sum(history) / len(history)
            if baseline > 0 and current >= 2.0 * baseline:
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


@app.route("/api/dashboard")
def dashboard():
    """Single endpoint returning all dashboard data."""
    events, meta = _get_events()
    metrics = _compute_metrics(events)
    timeseries = _compute_timeseries(events)
    volume_history = {
        k: deque(v, maxlen=30) for k, v in meta.get("volume_history", {}).items()
    }
    alerts, volume_history = _compute_volume_spikes(metrics, volume_history)
    with _state_lock:
        _state["volume_history"] = {k: list(v) for k, v in volume_history.items()}

    now = datetime.now(timezone.utc)
    last_event_time = meta.get("last_event_time")
    freshness_seconds = None
    if last_event_time:
        freshness_seconds = (now - last_event_time).total_seconds()

    # Use last 50 events only – reflects pipeline latency, not full-window age
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

    return jsonify(
        {
            "metrics": metrics,
            "timeseries": timeseries,
            "alerts": alerts,
            "status": {
                "kafka_status": kafka_status,
                "kafka_error": kafka_error,
                "freshness_seconds": freshness_seconds,
                "latency_seconds": latency_seconds,
                "total_trades": total_trades,
                "total_volume": total_volume,
                "live_symbols": live_symbols,
                "event_count": event_count,
                "window_minutes": WINDOW_MINUTES,
                "updated_at": now.strftime("%H:%M:%S UTC"),
            },
        }
    )


def main():
    get_config()  # validate config on startup
    t = threading.Thread(target=_poll_and_ingest, daemon=True)
    t.start()
    port = int(__import__("os").environ.get("DASHBOARD_PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
