from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count as spark_count,
    expr,
    from_json,
    max as spark_max,
    mean as spark_mean,
    min as spark_min,
    stddev_pop,
    sum as spark_sum,
    to_timestamp,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from crypto_analytics.clickhouse_store import ClickHouseAnalyticsStore
from crypto_analytics.contracts import Candle1m, LiveMetric, RawTradeEvent
from crypto_analytics.settings import AppSettings
from crypto_analytics.transformations import compute_volume_alerts


logger = logging.getLogger("crypto_analytics.streaming")
KNOWN_UNSTABLE_KAFKA_STREAMING_VERSIONS = {"4.1.1"}

RAW_TRADE_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("price_usd", DoubleType(), False),
        StructField("size_qty", DoubleType(), False),
        StructField("trade_id", StringType(), True),
        StructField("source", StringType(), False),
        StructField("received_at", StringType(), False),
    ]
)


def pin_spark_python_interpreters(python_executable: str | None = None) -> tuple[str, str]:
    python_executable = python_executable or sys.executable

    shared_python = (
        os.getenv("PYSPARK_PYTHON")
        or os.getenv("PYSPARK_DRIVER_PYTHON")
        or python_executable
    )
    worker_python = os.getenv("PYSPARK_PYTHON", shared_python)
    driver_python = os.getenv("PYSPARK_DRIVER_PYTHON", shared_python)

    os.environ["PYSPARK_PYTHON"] = worker_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = driver_python
    return worker_python, driver_python


def spark_scala_binary_version(spark_version: str) -> str:
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", spark_version)
    if not match:
        raise RuntimeError(f"Unsupported Spark version format: {spark_version}")

    major_version = int(match.group(1))
    if major_version == 3:
        return "2.12"
    if major_version == 4:
        return "2.13"
    raise RuntimeError(
        f"Unsupported Spark major version {major_version}. Set SPARK_KAFKA_PACKAGE explicitly."
    )


def default_kafka_package(spark_version: str | None = None) -> str:
    resolved_version = spark_version or pyspark.__version__
    scala_binary_version = spark_scala_binary_version(resolved_version)
    return (
        "org.apache.spark:"
        f"spark-sql-kafka-0-10_{scala_binary_version}:{resolved_version}"
    )


def validate_spark_runtime(spark_version: str) -> None:
    if spark_version not in KNOWN_UNSTABLE_KAFKA_STREAMING_VERSIONS:
        return

    if os.getenv("ALLOW_UNSTABLE_SPARK", "").lower() in {"1", "true", "yes"}:
        logger.warning(
            "Running with unstable Spark %s because ALLOW_UNSTABLE_SPARK is enabled.",
            spark_version,
        )
        return

    raise RuntimeError(
        "Installed pyspark 4.1.1 is known to crash with Kafka micro-batch streaming "
        "after a successful batch completes. Run `uv sync` to install the pinned "
        "stable Spark 3.5.5 runtime, or set ALLOW_UNSTABLE_SPARK=1 only if you are "
        "intentionally testing the upstream issue."
    )


def build_spark_session() -> SparkSession:
    worker_python, driver_python = pin_spark_python_interpreters()
    spark_version = pyspark.__version__
    validate_spark_runtime(spark_version)

    kafka_package = os.getenv("SPARK_KAFKA_PACKAGE", default_kafka_package(spark_version))
    logger.info("Starting Spark %s with Kafka package %s", spark_version, kafka_package)
    logger.info(
        "Spark Python interpreters pinned to driver=%s worker=%s",
        driver_python,
        worker_python,
    )
    spark = (
        SparkSession.builder.appName("enterprise-crypto-analytics-streaming")
        .config("spark.jars.packages", kafka_package)
        .getOrCreate()
    )
    # Suppress verbose Spark INFO logs so container logs stay readable.
    spark.sparkContext.setLogLevel("WARN")
    return spark


class CryptoAnalyticsStreamingJob:
    def __init__(self, settings: AppSettings, store: ClickHouseAnalyticsStore):
        self.settings = settings
        self.store = store
        # Spark invokes each foreachBatch callback independently. Keep ClickHouse
        # writes serialized through the shared client for simpler lifecycle control.
        self._store_lock = threading.Lock()

    def build_raw_stream(self, spark: SparkSession) -> DataFrame:
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.settings.kafka_bootstrap_servers)
            .option("subscribe", self.settings.kafka_topics.raw)
            .option("startingOffsets", "latest")
            .option(
                "failOnDataLoss",
                str(self.settings.kafka_fail_on_data_loss).lower(),
            )
            .load()
        )

    def build_parsed_stream(self, raw_stream: DataFrame) -> DataFrame:
        return (
            raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
            .select(from_json(col("json_str"), RAW_TRADE_SCHEMA).alias("event"))
            .select("event.*")
            .where(col("product_id").isNotNull())
            # Keep the original event_time string for ClickHouse insertion while also
            # providing Spark with a true timestamp column for watermark tracking.
            .withColumn("event_time_ts", to_timestamp(col("event_time")))
            .where(col("event_time_ts").isNotNull())
        )

    def build_windowed_agg_stream(self, parsed_stream: DataFrame) -> DataFrame:
        """Build a Spark-native 1-minute tumbling-window aggregation stream.

        withWatermark tells Spark to track event-time progress and drop events that
        arrive more than 5 minutes late — satisfying the out-of-order handling
        requirement.  All VWAP, volume, and volatility calculations run inside
        the Spark engine; no toPandas() is used for aggregation logic.
        """
        return (
            parsed_stream
            .withWatermark("event_time_ts", "5 minutes")
            .withColumn("notional_usd", col("price_usd") * col("size_qty"))
            .groupBy(
                window(col("event_time_ts"), "1 minute"),
                col("product_id"),
            )
            .agg(
                # min_by / max_by give the price at the earliest / latest event
                # inside each window — correct open and close semantics.
                expr("min_by(price_usd, event_time_ts)").alias("open_price"),
                expr("max_by(price_usd, event_time_ts)").alias("close_price"),
                spark_max("price_usd").alias("high_price"),
                spark_min("price_usd").alias("low_price"),
                spark_sum("size_qty").alias("volume_qty"),
                spark_count("*").alias("trade_count"),
                # VWAP = total notional / total volume (null-safe: Spark returns null
                # on division by zero, handled in process_agg_batch)
                (spark_sum("notional_usd") / spark_sum("size_qty")).alias("vwap_usd"),
                spark_mean("price_usd").alias("avg_price_usd"),
                # Population std-dev: returns null for single-row windows, coalesced
                # to 0.0 in process_agg_batch.
                stddev_pop("price_usd").alias("volatility_usd"),
            )
        )

    # ------------------------------------------------------------------
    # Query 1 callback — raw trades audit log
    # ------------------------------------------------------------------

    def process_raw_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Write individual trade events to raw_trades for audit and replay."""
        if batch_df.rdd.isEmpty():
            return

        batch_pdf = batch_df.select(
            "product_id", "event_time", "price_usd",
            "size_qty", "trade_id", "source", "received_at",
        ).toPandas()

        raw_events = [RawTradeEvent(**row) for row in batch_pdf.to_dict(orient="records")]
        if not raw_events:
            return

        try:
            with self._store_lock:
                inserted = self.store.insert_raw_trades(raw_events)
            logger.info("Raw batch %s: inserted %d trades.", batch_id, inserted)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Raw batch %s: insert failed (non-fatal): %s", batch_id, exc)

    # ------------------------------------------------------------------
    # Query 2 callback — Spark-aggregated candles / metrics / alerts
    # ------------------------------------------------------------------

    def process_agg_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Persist Spark-native windowed aggregations to ClickHouse.

        batch_df rows contain one row per (window, product_id) produced by the
        Spark streaming engine. The groupBy/window aggregation already happened
        inside Spark before this callback runs.
        """
        if batch_df.rdd.isEmpty():
            logger.info("Agg batch %s is empty. Skipping.", batch_id)
            return

        candles: list[Candle1m] = []
        live_metrics: list[LiveMetric] = []

        for row in batch_df.collect():
            win = row["window"]
            win_start = win["start"].strftime("%Y-%m-%dT%H:%M:%SZ")
            win_end = win["end"].strftime("%Y-%m-%dT%H:%M:%SZ")
            pid = str(row["product_id"])

            open_p = float(row["open_price"] or 0.0)
            close_p = float(row["close_price"] or 0.0)
            high_p = float(row["high_price"] or 0.0)
            low_p = float(row["low_price"] or 0.0)
            volume = float(row["volume_qty"] or 0.0)
            trade_ct = int(row["trade_count"])
            # vwap_usd is null when volume == 0 (Spark div-by-zero); fall back to close.
            vwap = float(row["vwap_usd"]) if row["vwap_usd"] is not None else close_p
            avg_price = float(row["avg_price_usd"] or close_p)
            # stddev_pop is null for single-trade windows — treat as 0.
            volatility = float(row["volatility_usd"] or 0.0)
            price_change_pct = (
                0.0 if open_p == 0 else (close_p - open_p) / open_p * 100.0
            )

            candles.append(
                Candle1m(
                    product_id=pid,
                    window_start=win_start,
                    window_end=win_end,
                    open_price=open_p,
                    high_price=high_p,
                    low_price=low_p,
                    close_price=close_p,
                    volume_qty=volume,
                    trade_count=trade_ct,
                    vwap_usd=vwap,
                )
            )
            live_metrics.append(
                LiveMetric(
                    product_id=pid,
                    window_start=win_start,
                    window_end=win_end,
                    last_price_usd=close_p,
                    avg_price_usd=avg_price,
                    price_change_pct=price_change_pct,
                    volume_qty=volume,
                    trade_count=trade_ct,
                    volatility_usd=volatility,
                    vwap_usd=vwap,
                )
            )

        with self._store_lock:
            volume_baselines = self.store.get_average_volume_baselines()
            alerts = compute_volume_alerts(candles, volume_baselines)

            raw_count, candle_count, metric_count, alert_count = self._persist_with_retries(
                raw_events=[],
                candles=candles,
                live_metrics=live_metrics,
                alerts=alerts,
                batch_id=batch_id,
            )

        logger.info(
            "Agg batch %s: %d candles, %d live metrics, %d alerts.",
            batch_id,
            candle_count,
            metric_count,
            alert_count,
        )

    def _persist_with_retries(
        self,
        *,
        raw_events: list[RawTradeEvent],
        candles,
        live_metrics,
        alerts,
        batch_id: int,
        max_attempts: int = 5,
        base_sleep_seconds: float = 0.25,
    ) -> tuple[int, int, int, int]:
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                return self.store.persist_batch(
                    raw_events=raw_events,
                    candles=candles,
                    live_metrics=live_metrics,
                    alerts=alerts,
                )
            except Exception as exc:
                last_error = exc
                if attempt == max_attempts or not self._is_retryable_storage_error(exc):
                    raise

                sleep_seconds = base_sleep_seconds * attempt
                logger.warning(
                    "Batch %s hit a temporary storage error on attempt %s/%s: %s. "
                    "Retrying in %.2fs.",
                    batch_id,
                    attempt,
                    max_attempts,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

        if last_error is not None:
            raise last_error

        raise RuntimeError("ClickHouse persistence retries exhausted unexpectedly.")

    def _is_retryable_storage_error(self, error: Exception) -> bool:
        message = str(error).lower()
        return any(
            token in message
            for token in {
                "lock",
                "locked",
                "io error",
                "busy",
                "timeout",
                "temporarily unavailable",
                "connection reset",
            }
        )

    def start(self):
        self.store.initialize()
        spark = build_spark_session()
        spark.sparkContext.setLogLevel(self.settings.log_level.upper())

        # Query 1: raw trades audit log.
        raw_stream_1 = self.build_raw_stream(spark)
        parsed_stream_1 = self.build_parsed_stream(raw_stream_1)
        raw_query = (
            parsed_stream_1.writeStream
            .foreachBatch(self.process_raw_batch)
            .outputMode("append")
            .option(
                "checkpointLocation",
                str(self.settings.checkpoint_dir / "raw_trades"),
            )
            .start()
        )

        # Query 2: Spark-native windowed aggregation with watermarking.
        raw_stream_2 = self.build_raw_stream(spark)
        parsed_stream_2 = self.build_parsed_stream(raw_stream_2)
        agg_stream = self.build_windowed_agg_stream(parsed_stream_2)
        agg_query = (
            agg_stream.writeStream
            .foreachBatch(self.process_agg_batch)
            .outputMode("update")
            .option(
                "checkpointLocation",
                str(self.settings.checkpoint_dir / "windowed_agg"),
            )
            .start()
        )

        destination = (
            f"ClickHouse {self.settings.clickhouse_host}:"
            f"{self.settings.clickhouse_port}/{self.settings.clickhouse_db}"
        )
        logger.info(
            "Started 2 streaming queries (raw_trades + windowed_agg). "
            "Writing analytics into %s",
            destination,
        )
        self._raw_query = raw_query
        return agg_query
