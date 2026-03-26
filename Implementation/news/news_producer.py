"""
News pipeline producer — polls pluggable news sources, applies VADER
sentiment analysis, and publishes enriched articles to Kafka.
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

_news_dir = str(Path(__file__).resolve().parent)
if _news_dir not in sys.path:
    sys.path.insert(0, _news_dir)

from utils.config import get_config  # noqa: E402
from utils.metrics import start_metrics_server  # noqa: E402

try:
    from utils.metrics import (
        NEWS_ARTICLES_FETCHED,
        NEWS_ARTICLES_PUBLISHED,
        NEWS_FETCH_ERRORS,
        NEWS_SENTIMENT_DISTRIBUTION,
    )
    _METRICS = True
except ImportError:
    _METRICS = False

from sentiment import analyze as analyze_sentiment  # noqa: E402
from sources.cryptopanic import CryptoPanicSource  # noqa: E402
from sources.rss_source import RSSNewsSource  # noqa: E402


def _build_sources(config):
    """Instantiate enabled news source adapters."""
    sources = []
    for name in config.news_sources:
        if name == "cryptopanic":
            currencies = [s.split("-")[0] for s in config.symbols[:10]]
            sources.append(
                CryptoPanicSource(
                    api_key=config.cryptopanic_api_key,
                    currencies=currencies,
                )
            )
        elif name == "rss":
            sources.append(RSSNewsSource())
        else:
            logging.getLogger("news-producer").warning(
                "Unknown news source '%s', skipping.", name
            )
    return sources


def main() -> None:
    config = get_config()
    start_metrics_server(default=9094)

    log_level = os.getenv("NEWS_LOG_LEVEL", config.log_level)
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    )
    logger = logging.getLogger("news-producer")

    if not config.news_enabled:
        logger.warning("NEWS_ENABLED is false — exiting.")
        return

    poll_interval = config.news_poll_interval_sec
    logger.info("Poll interval: %ds", poll_interval)

    kafka_producer = KafkaProducer(
        bootstrap_servers=[config.kafka_server],
        client_id="crypto-news-producer",
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        key_serializer=lambda x: x.encode("utf-8") if isinstance(x, str) else x,
        acks="all",
    )

    sources = _build_sources(config)
    if not sources:
        logger.error("No news sources configured — exiting.")
        return

    logger.info("Enabled sources: %s", [s.name for s in sources])

    shutdown_requested = False

    def _shutdown(sig, frame):
        nonlocal shutdown_requested
        logger.info("Shutting down… (signal %s)", sig)
        shutdown_requested = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    global_seen: set[str] = set()
    MAX_SEEN = 5000

    while not shutdown_requested:
        for source in sources:
            try:
                articles = source.fetch_articles()
                if _METRICS:
                    NEWS_ARTICLES_FETCHED.labels(source=source.name).inc(len(articles))
            except Exception:
                logger.exception("Error fetching from %s", source.name)
                if _METRICS:
                    NEWS_FETCH_ERRORS.labels(source=source.name).inc()
                continue

            for article in articles:
                url = article.get("url", "")
                if url in global_seen:
                    continue
                global_seen.add(url)

                sentiment = analyze_sentiment(article.get("title", ""))
                article["sentiment"] = sentiment
                article["fetched_at"] = datetime.now(timezone.utc).isoformat()

                key = article.get("currencies", ["CRYPTO"])[0] if article.get("currencies") else "CRYPTO"
                kafka_producer.send(config.topic_news, key=key, value=article)

                if _METRICS:
                    NEWS_ARTICLES_PUBLISHED.inc()
                    NEWS_SENTIMENT_DISTRIBUTION.labels(label=sentiment["label"]).inc()

                logger.info(
                    "[%s] %s | %s | %s",
                    source.name,
                    sentiment["label"],
                    article.get("title", "")[:80],
                    article.get("currencies", []),
                )

        kafka_producer.flush(timeout=10)

        if len(global_seen) > MAX_SEEN:
            global_seen.clear()

        for _ in range(poll_interval):
            if shutdown_requested:
                break
            time.sleep(1)

    logger.info("Flushing Kafka producer…")
    kafka_producer.flush(timeout=10)
    kafka_producer.close(timeout=5)
    logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
