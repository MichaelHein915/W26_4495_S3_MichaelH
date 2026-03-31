"""
Flask API server for the crypto streaming dashboard.
Consumes Kafka messages in a background thread and exposes REST endpoints.
"""

import json
import logging
import os
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

# Ensure sibling modules (analytics, ai_routes) are importable when loaded as a package
_dashboard_dir = str(Path(__file__).resolve().parent)
if _dashboard_dir not in sys.path:
    sys.path.insert(0, _dashboard_dir)

from utils.config import get_config  # noqa: E402
from utils.alerts import alert_arbitrage, alert_volume_spike, alert_price_threshold, alert_anomaly  # noqa: E402
from utils.anomaly import AnomalyDetector  # noqa: E402
from utils.metrics import (  # noqa: E402
    DASHBOARD_EVENTS,
    DASHBOARD_FRESHNESS_SECONDS,
    DASHBOARD_KAFKA_ERROR,
    DASHBOARD_LAST_POLL_COUNT,
    DASHBOARD_REQUEST_DURATION,
    DASHBOARD_REQUESTS,
)

from analytics import (  # noqa: E402
    parse_event_time as _parse_event_time,
    compute_metrics as _compute_metrics,
    compute_arbitrage_opportunities as _compute_arbitrage_opportunities,
    compute_exchange_metrics as _compute_exchange_metrics,
    compute_volume_timeseries as _compute_volume_timeseries,
    compute_volume_by_exchange_timeseries as _compute_volume_by_exchange_timeseries,
    compute_heatmap_data as _compute_heatmap_data,
    get_recent_trades as _get_recent_trades,
    compute_exchange_stats as _compute_exchange_stats,
    compute_sparklines as _compute_sparklines,
    compute_candles as _compute_candles,
    compute_timeseries as _compute_timeseries,
    compute_volume_spikes as _compute_volume_spikes,
    compute_sentiment_summary as _compute_sentiment_summary,
    compute_sentiment_by_symbol as _compute_sentiment_by_symbol,
    compute_sentiment_timeseries as _compute_sentiment_timeseries,
    compute_news_spike_vs_price as _compute_news_spike_vs_price,
)
from ai_routes import ai_bp, start_insight_loop  # noqa: E402

log = logging.getLogger(__name__)

app = Flask(__name__, static_folder="web", static_url_path="/")
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {"/metrics": make_wsgi_app()})
app.register_blueprint(ai_bp, url_prefix="/api/ai")

# ── Shared state (updated by background ingestion thread) ────────────

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

# ── News pipeline shared state ────────────────────────────────────────
_news_state = {
    "consumer": None,
    "events": deque(),
    "kafka_error": None,
    "running": True,
}
_news_lock = threading.Lock()

NEWS_MAX_ARTICLES = 200
NEWS_RETENTION_MINUTES = 1440

WINDOW_MINUTES = 3
MAX_RETENTION_MINUTES = 10
ALERT_INTERVAL_SEC = 15


# ── Request hooks ────────────────────────────────────────────────────

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


# ── Kafka ingestion ──────────────────────────────────────────────────

def _init_consumer(topic: str, bootstrap_servers: str) -> KafkaConsumer | None:
    try:
        return KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            group_id="crypto-dashboard-api",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            session_timeout_ms=30000,
            heartbeat_interval_ms=5000,
            request_timeout_ms=40000,
        )
    except Exception:
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


# ── News Kafka ingestion ──────────────────────────────────────────────

def _poll_news():
    """Background thread: poll Kafka news topic and update news state."""
    config = get_config()
    if not config.news_enabled:
        return
    consumer = None

    while _news_state["running"]:
        if consumer is None:
            try:
                consumer = KafkaConsumer(
                    config.topic_news,
                    bootstrap_servers=[config.kafka_server],
                    group_id="crypto-dashboard-news",
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=5000,
                    request_timeout_ms=40000,
                )
            except Exception:
                log.exception("Failed to connect news consumer")
                consumer = None
                time.sleep(5)
                continue
            with _news_lock:
                _news_state["consumer"] = consumer
                _news_state["events"] = deque()
                _news_state["kafka_error"] = None

        try:
            polled = consumer.poll(timeout_ms=2000, max_records=100)
            messages = []
            for records in polled.values():
                for record in records:
                    messages.append(record.value)

            with _news_lock:
                _news_state["kafka_error"] = None
                for msg in messages:
                    msg["_ingested_at"] = datetime.now(timezone.utc).isoformat()
                    _news_state["events"].append(msg)

                while len(_news_state["events"]) > NEWS_MAX_ARTICLES:
                    _news_state["events"].popleft()

                cutoff = datetime.now(timezone.utc) - timedelta(minutes=NEWS_RETENTION_MINUTES)
                while _news_state["events"]:
                    oldest = _news_state["events"][0]
                    pub = oldest.get("published_at", oldest.get("fetched_at", ""))
                    try:
                        pub_dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                    except (ValueError, AttributeError):
                        _news_state["events"].popleft()
                        continue
                    if pub_dt < cutoff:
                        _news_state["events"].popleft()
                    else:
                        break

        except Exception as e:
            with _news_lock:
                _news_state["kafka_error"] = str(e)
            try:
                consumer.close()
            except Exception:
                pass
            consumer = None
            time.sleep(2)


# ── State access ─────────────────────────────────────────────────────

def _get_events():
    with _state_lock:
        return list(_state["events"]), dict(_state)


def _get_news_events():
    with _news_lock:
        return list(_news_state["events"])


# ── Dashboard payload builder ────────────────────────────────────────

def _resolve_spike_symbol(requested: str, metrics: list) -> str:
    """Pick product_id for news-vs-price chart: explicit param, else busiest symbol, else BTC-USD."""
    requested_raw = (requested or "").strip().upper()
    ids = [m["product_id"] for m in metrics]
    if requested_raw:
        if requested_raw in ids:
            return requested_raw
        if "-" not in requested_raw:
            candidate = f"{requested_raw}-USD"
            if candidate in ids:
                return candidate
            return candidate
        return requested_raw
    if ids:
        return max(metrics, key=lambda m: m["trade_count"])["product_id"]
    return "BTC-USD"


def _build_dashboard_payload(
    window_minutes: int, exchange_filter: str, spike_symbol: str = ""
) -> dict:
    """Build full dashboard payload. Computation only — no alert notifications."""
    events, meta = _get_events()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    events = [e for e in events if e["event_time"] >= cutoff]
    if exchange_filter:
        events = [e for e in events if e.get("exchange", "coinbase").lower() == exchange_filter]

    config = get_config()
    arbitrage = _compute_arbitrage_opportunities(events, threshold_pct=config.alert_arbitrage_threshold_pct)

    metrics = _compute_metrics(events)
    anomalies = []
    if config.alert_anomaly_enabled and metrics:
        with _state_lock:
            if _state["anomaly_detector"] is None:
                _state["anomaly_detector"] = AnomalyDetector(
                    contamination=config.anomaly_contamination,
                )
        detector = _state["anomaly_detector"]
        try:
            anomalies = detector.detect(metrics)
        except Exception:
            anomalies = []

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

    news_events = _get_news_events()
    cutoff_news = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    news_in_window = []
    for n in news_events:
        t = _parse_event_time(n.get("published_at")) or _parse_event_time(n.get("fetched_at"))
        if t is not None and t >= cutoff_news:
            news_in_window.append(n)

    sentiment_summary = _compute_sentiment_summary(news_events)
    sentiment_by_symbol = _compute_sentiment_by_symbol(news_events)
    sentiment_timeseries = _compute_sentiment_timeseries(news_events)

    spike_sym = _resolve_spike_symbol(spike_symbol, metrics)
    news_spike_series = _compute_news_spike_vs_price(
        events, news_in_window, spike_sym, bucket_minutes=5
    )

    recent_news = sorted(
        news_events,
        key=lambda n: n.get("published_at", ""),
        reverse=True,
    )[:20]

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
        "news": recent_news,
        "sentiment_summary": sentiment_summary,
        "sentiment_by_symbol": sentiment_by_symbol,
        "sentiment_timeseries": sentiment_timeseries,
        "news_spike_vs_price": {
            "symbol": spike_sym,
            "bucket_minutes": 5,
            "series": news_spike_series,
        },
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


# ── Background alert loop ────────────────────────────────────────────

def _alert_loop():
    """Background thread: periodically check for and fire alert notifications."""
    config = get_config()
    time.sleep(ALERT_INTERVAL_SEC)

    while _state["running"]:
        try:
            payload = _build_dashboard_payload(WINDOW_MINUTES, "")

            for opp in payload.get("arbitrage", []):
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

            for a in payload.get("anomalies", []):
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

            for a in payload.get("alerts", []):
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

            for pa in payload.get("price_alerts", []):
                alert_price_threshold(
                    config.slack_webhook_url,
                    pa["product_id"],
                    pa["direction"],
                    pa["threshold_price"],
                    pa["current_price"],
                    email_to=config.alert_email_to,
                    smtp_host=config.smtp_host,
                    smtp_port=config.smtp_port,
                    smtp_user=config.smtp_user,
                    smtp_password=config.smtp_password,
                    smtp_use_tls=config.smtp_use_tls,
                )

        except Exception:
            log.exception("Alert loop error")

        time.sleep(ALERT_INTERVAL_SEC)


# ── Routes ───────────────────────────────────────────────────────────

@app.route("/")
def landing():
    return send_from_directory(app.static_folder, "landing.html")


@app.route("/dashboard")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/news")
def news_page():
    return send_from_directory(app.static_folder, "news.html")


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


@app.route("/api/news")
def news_endpoint():
    """Recent news articles with sentiment, optionally filtered by currency."""
    currency = request.args.get("currency", "").strip().upper()
    limit = int(request.args.get("limit", "50"))
    limit = max(1, min(limit, 200))

    news_events = _get_news_events()
    if currency:
        news_events = [n for n in news_events if currency in n.get("currencies", [])]

    news_events = sorted(
        news_events,
        key=lambda n: n.get("published_at", ""),
        reverse=True,
    )[:limit]

    return jsonify({"articles": news_events, "count": len(news_events)})


@app.route("/api/sentiment")
def sentiment_endpoint():
    """Aggregated sentiment per symbol and overall market."""
    news_events = _get_news_events()
    return jsonify({
        "summary": _compute_sentiment_summary(news_events),
        "by_symbol": _compute_sentiment_by_symbol(news_events),
        "timeseries": _compute_sentiment_timeseries(news_events),
    })


@app.route("/api/dashboard")
def dashboard():
    """Single endpoint returning all dashboard data."""
    window_minutes = int(request.args.get("window", WINDOW_MINUTES))
    window_minutes = max(1, min(window_minutes, 30))
    exchange_filter = request.args.get("exchange", "").strip().lower()
    spike_symbol = request.args.get("spike_symbol", "").strip()
    return jsonify(
        _build_dashboard_payload(window_minutes, exchange_filter, spike_symbol=spike_symbol)
    )


# ── Entrypoint ───────────────────────────────────────────────────────

def main():
    get_config()

    # Store payload builder on app for AI routes
    app.config["get_dashboard_payload"] = _build_dashboard_payload
    app.config["WINDOW_MINUTES"] = WINDOW_MINUTES

    # Kafka ingestion thread
    threading.Thread(target=_poll_and_ingest, daemon=True).start()

    # News pipeline ingestion thread
    config_obj = get_config()
    if config_obj.news_enabled:
        threading.Thread(target=_poll_news, daemon=True).start()

    # Alert notification thread (decoupled from API request cycle)
    threading.Thread(target=_alert_loop, daemon=True).start()

    # AI insight thread
    config = get_config()
    if config.ai_enabled:
        start_insight_loop(_build_dashboard_payload, lambda: _state["running"], WINDOW_MINUTES)

    port = int(os.environ.get("DASHBOARD_PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
