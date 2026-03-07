"""
Kafka → S3 Parquet sink for Athena.

Consumes raw trade events from the crypto.trades.raw topic, buffers them
in memory, and periodically flushes Parquet files to S3 using Hive-style
partitioning (year=/month=/day=/hour=).  Each flush writes both raw trades
and pre-aggregated 1-minute OHLCV candles.  Athena queries the data directly.

Usage:
    python s3_sink.py
"""

import io
import json
import logging
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))
from utils.config import get_config

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
)
logger = logging.getLogger("s3-sink")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
config = get_config()
logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))

if not config.s3_bucket:
    logger.error("S3_BUCKET must be set in .env")
    sys.exit(1)

s3 = boto3.client("s3", region_name=config.aws_region)


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
    """Resample raw trades into 1-minute OHLCV candles per product."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["trade_time"] = pd.to_datetime(df["trade_time"], utc=True)
    df = df.set_index("trade_time")

    candles = (
        df.groupby("product_id")
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
    candles["vwap"] = candles.apply(
        lambda r: r["vwap"] / r["volume"] if r["volume"] > 0 else r["close_price"],
        axis=1,
    )
    candles = candles.rename(columns={"trade_time": "window_start"})
    candles["window_end"] = candles["window_start"] + pd.Timedelta(minutes=1)
    candles = candles[candles["trade_count"] > 0]

    return candles[
        [
            "window_start", "window_end", "product_id",
            "open_price", "high_price", "low_price", "close_price",
            "volume", "trade_count", "vwap",
        ]
    ]


# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------

def _parse_message(raw: dict) -> dict | None:
    """Convert a Coinbase ticker message into a flat trade row."""
    if raw.get("type") != "ticker" or "price" not in raw:
        return None
    try:
        price = float(raw["price"])
        size = float(raw.get("last_size") or raw.get("size") or 0)
        return {
            "trade_time": raw.get("time", datetime.now(timezone.utc).isoformat()),
            "product_id": raw.get("product_id", "UNKNOWN"),
            "price": price,
            "size_qty": size,
            "notional_usd": price * size,
        }
    except (ValueError, TypeError) as exc:
        logger.warning("Skipping malformed message: %s — %s", raw, exc)
        return None


# ---------------------------------------------------------------------------
# Flush logic — Hive-style partitioned writes to S3
# ---------------------------------------------------------------------------

def flush_buffer(buffer: list[dict]):
    """Write raw trades + candles as Parquet to S3 with Hive partitioning."""
    if not buffer:
        return

    now = datetime.now(timezone.utc)
    batch_id = f"{now.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"
    prefix = config.s3_staging_prefix.rstrip("/")
    partition = now.strftime("year=%Y/month=%m/day=%d/hour=%H")

    # ---- raw trades ----
    raw_df = pd.DataFrame(buffer)
    raw_df["batch_id"] = batch_id
    raw_df["trade_time"] = pd.to_datetime(raw_df["trade_time"], utc=True)
    # Store as timezone-naive strings for Athena TIMESTAMP compatibility
    raw_df["trade_time"] = raw_df["trade_time"].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    raw_key = f"{prefix}/raw_trades/{partition}/{batch_id}.parquet"
    _upload_parquet(raw_df, raw_key)

    # ---- candles ----
    candles_df = _compute_candles(pd.DataFrame(buffer))
    if not candles_df.empty:
        candles_df["batch_id"] = batch_id
        candles_df["window_start"] = candles_df["window_start"].dt.strftime("%Y-%m-%d %H:%M:%S")
        candles_df["window_end"] = candles_df["window_end"].dt.strftime("%Y-%m-%d %H:%M:%S")
        candles_key = f"{prefix}/candles_1m/{partition}/{batch_id}.parquet"
        _upload_parquet(candles_df, candles_key)

    logger.info(
        "Flush complete — %d raw trades, %d candles",
        len(raw_df),
        len(candles_df) if not candles_df.empty else 0,
    )


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def main():
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        config.topic_raw,
        bootstrap_servers=[config.kafka_server],
        group_id="s3-sink-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    logger.info(
        "S3 sink started — topic=%s, flush every %ds or %d records, bucket=%s",
        config.topic_raw,
        config.sink_flush_interval_sec,
        config.sink_flush_max_records,
        config.s3_bucket,
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
                    row = _parse_message(msg.value)
                    if row:
                        buffer.append(row)

            elapsed = time.monotonic() - last_flush
            should_flush = (
                (len(buffer) >= config.sink_flush_max_records)
                or (elapsed >= config.sink_flush_interval_sec and buffer)
            )

            if should_flush:
                logger.info("Flushing %d records (%.1fs since last flush)", len(buffer), elapsed)
                try:
                    flush_buffer(buffer)
                except Exception:
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
        logger.info("S3 sink stopped.")


if __name__ == "__main__":
    main()
