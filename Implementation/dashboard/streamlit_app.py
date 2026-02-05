import json
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

repo_root = Path(__file__).resolve().parents[2]
sys.path.append(str(repo_root / "src"))
from utils.config import get_config


def _parse_event_time(raw_time: str) -> datetime | None:
    if not raw_time:
        return None
    try:
        return datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
    except ValueError:
        return None


def _init_consumer(topic: str, bootstrap_servers: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_servers],
        group_id="crypto-streamlit-dashboard",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def _poll_messages(consumer: KafkaConsumer, max_records: int = 200) -> list[dict]:
    polled = consumer.poll(timeout_ms=1000, max_records=max_records)
    messages = []
    for records in polled.values():
        for record in records:
            messages.append(record.value)
    return messages


def _prune_events(events: deque, cutoff: datetime) -> None:
    while events and events[0]["event_time"] < cutoff:
        events.popleft()


def _compute_metrics(events: deque) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["product_id", "trade_count", "avg_price_usd"])
    df = pd.DataFrame(list(events))
    metrics = (
        df.groupby("product_id", as_index=False)
        .agg(trade_count=("price_usd", "count"), avg_price_usd=("price_usd", "mean"))
        .sort_values("product_id")
    )
    metrics["avg_price_usd"] = metrics["avg_price_usd"].round(2)
    return metrics


def _compute_timeseries(events: deque) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["event_time", "product_id", "avg_price_usd"])
    df = pd.DataFrame(list(events))
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df["avg_price_usd"] = df["price_usd"].astype(float)
    timeseries = (
        df.set_index("event_time")
        .groupby("product_id")
        .resample("30s")
        .mean(numeric_only=True)
        .reset_index()
        .sort_values(["product_id", "event_time"])
    )
    return timeseries


def _compute_latency_seconds(events: deque) -> float | None:
    if not events:
        return None
    now = datetime.now(timezone.utc)
    total = 0.0
    for event in events:
        total += (now - event["event_time"]).total_seconds()
    return total / len(events)


def main() -> None:
    st.set_page_config(page_title="Crypto 3-Minute Metrics", layout="wide")
    st.title("Crypto 3-Minute Metrics (Live)")

    config = get_config()
    refresh_seconds = st.sidebar.slider("Refresh seconds", 1, 5, 2)
    running = st.sidebar.toggle("Live update", value=True)
    window_minutes = 3

    if "consumer" not in st.session_state:
        st.session_state.consumer = _init_consumer(config.topic_raw, config.kafka_server)
    if "events" not in st.session_state:
        st.session_state.events = deque()
    if "last_event_time" not in st.session_state:
        st.session_state.last_event_time = None

    consumer = st.session_state.consumer
    events = st.session_state.events
    last_event_time = st.session_state.last_event_time

    poll_error = None
    polled_count = 0
    if running:
        try:
            messages = _poll_messages(consumer)
            polled_count = len(messages)
        except Exception as exc:
            messages = []
            poll_error = str(exc)

        for message in messages:
            event_time = _parse_event_time(message.get("time"))
            price = message.get("price")
            product_id = message.get("product_id")
            if not event_time or price is None or not product_id:
                continue
            try:
                price_value = float(price)
            except (TypeError, ValueError):
                continue
            events.append(
                {
                    "event_time": event_time,
                    "price_usd": price_value,
                    "product_id": product_id,
                }
            )
            if last_event_time is None or event_time > last_event_time:
                last_event_time = event_time

        cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        _prune_events(events, cutoff)

    st.session_state.last_event_time = last_event_time

    metrics = _compute_metrics(events)
    timeseries = _compute_timeseries(events)
    latency_seconds = _compute_latency_seconds(events)
    st.subheader("Rolling 3-Minute Summary")
    if poll_error:
        st.error(f"Kafka status: error polling ({poll_error})")
    elif not running:
        st.info("Kafka status: live update paused")
    elif polled_count > 0:
        st.success(f"Kafka status: receiving data ({polled_count} new messages)")
    elif len(events) == 0:
        st.warning("Kafka status: waiting for data...")
    else:
        st.info("Kafka status: no new messages in last poll")
    if last_event_time:
        age_seconds = (datetime.now(timezone.utc) - last_event_time).total_seconds()
        if age_seconds <= 10:
            st.success(f"Data freshness: {age_seconds:.1f}s since last trade")
        elif age_seconds <= 30:
            st.warning(f"Data freshness: {age_seconds:.1f}s since last trade")
        else:
            st.error(f"Data freshness: {age_seconds:.1f}s since last trade")
    if latency_seconds is not None:
        if latency_seconds <= 2:
            st.success(f"Avg event latency: {latency_seconds:.2f}s")
        elif latency_seconds <= 5:
            st.warning(f"Avg event latency: {latency_seconds:.2f}s")
        else:
            st.error(f"Avg event latency: {latency_seconds:.2f}s")
    st.dataframe(metrics, use_container_width=True)
    st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")

    if not metrics.empty:
        chart_df = metrics.set_index("product_id")[["avg_price_usd", "trade_count"]]
        st.bar_chart(chart_df, height=240)

    if not timeseries.empty:
        st.subheader("Avg Price Trend (30s buckets)")
        pivot = timeseries.pivot(
            index="event_time", columns="product_id", values="avg_price_usd"
        )
        st.line_chart(pivot, height=260)

    st.caption(
        f"Window: last {window_minutes} minutes. "
        f"Events tracked: {len(events)}."
    )

    if running:
        time.sleep(refresh_seconds)
        st.rerun()


if __name__ == "__main__":
    main()
