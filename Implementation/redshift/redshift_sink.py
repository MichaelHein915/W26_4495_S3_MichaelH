"""
Kafka → S3 → Redshift micro-batch sink.

Consumes raw trade events from the crypto.trades.raw topic, buffers them
in memory, and periodically flushes to Redshift via S3 COPY.  Each flush
writes both raw trades and pre-aggregated 1-minute OHLCV candles.

Usage:
    python redshift_sink.py
"""

import io
import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import redshift_connector

from utils.config import get_config
from utils.metrics import SINK_RECORDS_FLUSHED, SINK_FLUSH_ERRORS
from utils.parse_trade import parse_trade_message

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
)
logger = logging.getLogger("redshift-sink")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
config = get_config()
logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))

REQUIRED = {
    "REDSHIFT_HOST": config.redshift_host,
    "REDSHIFT_PASSWORD": config.redshift_password,
    "S3_BUCKET": config.s3_bucket,
    "REDSHIFT_IAM_ROLE": config.redshift_iam_role,
}
missing = [k for k, v in REQUIRED.items() if not v]
if missing:
    logger.error("Missing required env vars: %s", ", ".join(missing))
    sys.exit(1)

s3 = boto3.client("s3", region_name=config.aws_region)

# ---------------------------------------------------------------------------
# Redshift helpers
# ---------------------------------------------------------------------------


def _get_conn():
    return redshift_connector.connect(
        host=config.redshift_host,
        port=config.redshift_port,
        database=config.redshift_db,
        user=config.redshift_user,
        password=config.redshift_password,
    )


def _copy_from_s3(cursor, table: str, s3_key: str):
    sql = (
        f"COPY {table} FROM 's3://{config.s3_bucket}/{s3_key}' IAM_ROLE '{config.redshift_iam_role}' FORMAT AS PARQUET;"
    )
    cursor.execute(sql)


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def _upload_parquet(df: pd.DataFrame, s3_key: str):
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=config.s3_bucket, Key=s3_key, Body=buf.getvalue())
    logger.info("Uploaded s3://%s/%s (%d rows)", config.s3_bucket, s3_key, len(df))


# ---------------------------------------------------------------------------
# OHLCV aggregation
# ---------------------------------------------------------------------------


def _compute_candles(df: pd.DataFrame) -> pd.DataFrame:
    """Resample raw trades into 1-minute OHLCV candles per product and exchange."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    if "exchange" not in df.columns:
        df["exchange"] = "coinbase"
    df["exchange"] = df["exchange"].fillna("coinbase")
    df["trade_time"] = pd.to_datetime(df["trade_time"], utc=True)
    df = df.set_index("trade_time")

    candles = (
        df.groupby(["product_id", "exchange"])
        .resample("1min", include_groups=False)
        .agg(
            open_price=("price", "first"),
            high_price=("price", "max"),
            low_price=("price", "min"),
            close_price=("price", "last"),
            volume=("size_qty", "sum"),
            trade_count=("price", "count"),
            vwap=("notional_usd", "sum"),
        )
    )

    candles = candles.reset_index()
    # VWAP = total notional / total volume
    candles["vwap"] = candles.apply(
        lambda r: r["vwap"] / r["volume"] if r["volume"] > 0 else r["close_price"],
        axis=1,
    )
    candles = candles.rename(columns={"trade_time": "window_start"})
    candles["window_end"] = candles["window_start"] + pd.Timedelta(minutes=1)

    # Strip timezone for Redshift TIMESTAMP columns
    candles["window_start"] = candles["window_start"].dt.tz_localize(None)
    candles["window_end"] = candles["window_end"].dt.tz_localize(None)

    candles = candles[candles["trade_count"] > 0]
    return candles[
        [
            "window_start",
            "window_end",
            "product_id",
            "exchange",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "trade_count",
            "vwap",
        ]
    ]


# ---------------------------------------------------------------------------
# Message parsing — uses shared parser for raw Coinbase + normalised multi-exchange
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Flush logic
# ---------------------------------------------------------------------------


def flush_buffer(buffer: list[dict]):
    """Write raw trades + candles to S3 then COPY into Redshift."""
    if not buffer:
        return

    batch_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"
    prefix = config.s3_staging_prefix.rstrip("/")
    ts_partition = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")

    # ---- raw trades ----
    raw_df = pd.DataFrame(buffer)
    raw_df["batch_id"] = batch_id
    # Strip timezone for Redshift TIMESTAMP columns
    raw_df["trade_time"] = pd.to_datetime(raw_df["trade_time"], utc=True).dt.tz_localize(None)

    raw_key = f"{prefix}/raw_trades/{ts_partition}/{batch_id}.parquet"
    _upload_parquet(raw_df, raw_key)

    # ---- candles ----
    candles_df = _compute_candles(pd.DataFrame(buffer))
    candles_key = None
    if not candles_df.empty:
        candles_df["batch_id"] = batch_id
        candles_key = f"{prefix}/candles_1m/{ts_partition}/{batch_id}.parquet"
        _upload_parquet(candles_df, candles_key)

    # ---- Redshift COPY ----
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            _copy_from_s3(cur, "crypto.raw_trades", raw_key)
            logger.info("COPY raw_trades complete — %d rows", len(raw_df))

            if candles_key:
                _copy_from_s3(cur, "crypto.candles_1m", candles_key)
                logger.info("COPY candles_1m complete — %d rows", len(candles_df))

            conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Redshift COPY failed for batch %s", batch_id)
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------


def main():
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        config.topic_raw,
        bootstrap_servers=[config.kafka_server],
        group_id="redshift-sink-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    logger.info(
        "Redshift sink started — topic=%s, flush every %ds or %d records",
        config.topic_raw,
        config.sink_flush_interval_sec,
        config.sink_flush_max_records,
    )

    buffer: list[dict] = []
    last_flush = time.monotonic()
    shutdown = False

    def _handle_signal(signum, _frame):
        nonlocal shutdown
        logger.info("Received signal %s — flushing and shutting down", signum)
        shutdown = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        while not shutdown:
            batch = consumer.poll(timeout_ms=1000, max_records=500)

            for _tp, messages in batch.items():
                for msg in messages:
                    row = parse_trade_message(msg.value)
                    if row:
                        buffer.append(row)

            elapsed = time.monotonic() - last_flush
            should_flush = (len(buffer) >= config.sink_flush_max_records) or (
                elapsed >= config.sink_flush_interval_sec and buffer
            )

            if should_flush:
                logger.info("Flushing %d records (%.1fs since last flush)", len(buffer), elapsed)
                try:
                    flush_buffer(buffer)
                    SINK_RECORDS_FLUSHED.labels(sink_type="redshift").inc(len(buffer))
                except Exception:
                    SINK_FLUSH_ERRORS.labels(sink_type="redshift").inc()
                    logger.exception("Flush failed — records kept in buffer for retry")
                    continue
                buffer.clear()
                last_flush = time.monotonic()

    finally:
        if buffer:
            logger.info("Final flush — %d records remaining", len(buffer))
            try:
                flush_buffer(buffer)
            except Exception:
                logger.exception("Final flush failed — %d records lost", len(buffer))
        consumer.close()
        logger.info("Redshift sink stopped.")


if __name__ == "__main__":
    main()
