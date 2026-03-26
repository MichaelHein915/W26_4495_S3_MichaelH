"""
Prometheus metrics utilities for the crypto streaming pipeline.

Provides a shared metrics server and common metric definitions.
"""

from __future__ import annotations

import os
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ---------------------------------------------------------------------------
# Producer metrics (used by base_exchange)
# ---------------------------------------------------------------------------
TRADES_PUBLISHED = Counter(
    "crypto_trades_published_total",
    "Total trades published to Kafka",
    ["exchange"],
)
KAFKA_SEND_ERRORS = Counter(
    "crypto_kafka_send_errors_total",
    "Kafka send failures",
    ["exchange"],
)

# ---------------------------------------------------------------------------
# Dashboard API metrics
# ---------------------------------------------------------------------------
DASHBOARD_REQUESTS = Counter(
    "crypto_dashboard_requests_total",
    "Total dashboard API requests",
    ["endpoint", "method"],
)
DASHBOARD_REQUEST_DURATION = Histogram(
    "crypto_dashboard_request_duration_seconds",
    "Dashboard API request duration",
    ["endpoint"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
DASHBOARD_EVENTS = Gauge(
    "crypto_dashboard_events_total",
    "Number of events in dashboard buffer",
)
DASHBOARD_LAST_POLL_COUNT = Gauge(
    "crypto_dashboard_kafka_poll_count",
    "Messages received in last Kafka poll",
)
DASHBOARD_FRESHNESS_SECONDS = Gauge(
    "crypto_dashboard_data_freshness_seconds",
    "Seconds since last event received",
)
DASHBOARD_KAFKA_ERROR = Gauge(
    "crypto_dashboard_kafka_error",
    "1 if Kafka has an error, 0 otherwise",
)

# ---------------------------------------------------------------------------
# Consumer metrics (Spark)
# ---------------------------------------------------------------------------
CONSUMER_RUNNING = Gauge(
    "crypto_consumer_running",
    "1 if Spark consumer is running",
)

# ---------------------------------------------------------------------------
# Sink metrics (S3, Redshift)
# ---------------------------------------------------------------------------
SINK_RECORDS_FLUSHED = Counter(
    "crypto_sink_records_flushed_total",
    "Records flushed to storage",
    ["sink_type"],
)
SINK_FLUSH_ERRORS = Counter(
    "crypto_sink_flush_errors_total",
    "Sink flush failures",
    ["sink_type"],
)


# ---------------------------------------------------------------------------
# News pipeline metrics
# ---------------------------------------------------------------------------
NEWS_ARTICLES_FETCHED = Counter(
    "crypto_news_articles_fetched_total",
    "Articles fetched from news sources",
    ["source"],
)
NEWS_ARTICLES_PUBLISHED = Counter(
    "crypto_news_articles_published_total",
    "Articles published to Kafka news topic",
)
NEWS_FETCH_ERRORS = Counter(
    "crypto_news_fetch_errors_total",
    "News source fetch failures",
    ["source"],
)
NEWS_SENTIMENT_DISTRIBUTION = Counter(
    "crypto_news_sentiment_total",
    "Sentiment label distribution of published articles",
    ["label"],
)


def start_metrics_server(port: int | None = None, default: int = 9090) -> None:
    """Start Prometheus metrics HTTP server in a background daemon thread."""
    p = port if port is not None else int(os.environ.get("METRICS_PORT", str(default)))
    start_http_server(p)
    import logging
    logging.getLogger(__name__).info("Prometheus metrics server listening on :%d", p)
