"""Cloud streaming entry point — Kafka -> PySpark -> ClickHouse.

Usage (Docker CMD):
    python -m crypto_analytics.cli.streaming_cloud

Environment variables (from .env.cloud):
    APP_ENV=cloud
    KAFKA_BOOTSTRAP_SERVERS=kafka:29092
    CLICKHOUSE_HOST=10.0.1.20
    CLICKHOUSE_PORT=8123
    CLICKHOUSE_DB=crypto
    CLICKHOUSE_USER=crypto_writer
    CLICKHOUSE_PASSWORD=<secret>
    CHECKPOINT_DIR=/data/checkpoints
"""
from __future__ import annotations

import logging
import os
import shutil

from crypto_analytics.clickhouse_store import ClickHouseAnalyticsStore
from crypto_analytics.logging import configure_logging
from crypto_analytics.settings import AppSettings
from crypto_analytics.streaming import CryptoAnalyticsStreamingJob


logger = logging.getLogger("crypto_analytics.streaming_cloud")


def _should_reset_checkpoint() -> bool:
    return os.getenv("RESET_STREAMING_CHECKPOINT", "").strip().lower() in {
        "1", "true", "yes", "on",
    }


def main() -> None:
    settings = AppSettings.from_env()
    configure_logging(settings.log_level)

    # Checkpoints live on the attached volume mounted into the Spark container.
    checkpoint_dirs = [
        settings.checkpoint_dir / "raw_trades",
        settings.checkpoint_dir / "windowed_agg",
    ]
    settings.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    if _should_reset_checkpoint():
        for path in checkpoint_dirs:
            if path.exists():
                logger.warning("Resetting checkpoint at %s", path)
                shutil.rmtree(path)

    store = ClickHouseAnalyticsStore.from_settings(settings)

    logger.info("Cloud streaming job starting.")
    logger.info("Kafka:      %s", settings.kafka_bootstrap_servers)
    logger.info("ClickHouse: %s:%s / %s", settings.clickhouse_host, settings.clickhouse_port, settings.clickhouse_db)
    logger.info("Checkpoint: %s", settings.checkpoint_dir)
    logger.info("Topic (raw): %s", settings.kafka_topics.raw)

    job = CryptoAnalyticsStreamingJob(settings=settings, store=store)
    query = job.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
