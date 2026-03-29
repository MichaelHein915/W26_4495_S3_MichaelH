"""ClickHouse analytics store for the cloud deployment."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from crypto_analytics.contracts import Candle1m, LiveMetric, RawTradeEvent, VolumeAlert
from crypto_analytics.settings import AppSettings

logger = logging.getLogger(__name__)


def _to_clickhouse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _make_client(settings: AppSettings) -> Client:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_db,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        connect_timeout=10,
        send_receive_timeout=30,
    )


class ClickHouseAnalyticsStore:
    """Cloud analytics store — writes processed data to ClickHouse."""

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self._client: Client | None = None

    @classmethod
    def from_settings(cls, settings: AppSettings) -> "ClickHouseAnalyticsStore":
        return cls(settings)

    def _get_client(self) -> Client:
        """Lazy-initialise the ClickHouse client (one per process)."""
        if self._client is None:
            self._client = _make_client(self.settings)
        return self._client

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def initialize(self) -> None:
        """No-op: schema is applied via Docker entrypoint (clickhouse/schema.sql)."""
        logger.info("ClickHouseAnalyticsStore.initialize() — schema managed by Docker entrypoint.")

    def persist_batch(
        self,
        *,
        raw_events: Iterable[RawTradeEvent],
        candles: Iterable[Candle1m],
        live_metrics: Iterable[LiveMetric],
        alerts: Iterable[VolumeAlert],
    ) -> tuple[int, int, int, int]:
        raw_rows    = list(raw_events)
        candle_rows = list(candles)
        metric_rows = list(live_metrics)
        alert_rows  = list(alerts)

        client = self._get_client()

        if raw_rows:
            self._insert_raw_trades(client, raw_rows)
        if candle_rows:
            self._insert_candles(client, candle_rows)
        if metric_rows:
            self._insert_live_metrics(client, metric_rows)
        if alert_rows:
            self._insert_alerts(client, alert_rows)

        return len(raw_rows), len(candle_rows), len(metric_rows), len(alert_rows)

    def insert_raw_trades(self, events: Iterable[RawTradeEvent]) -> int:
        rows = list(events)
        if rows:
            self._insert_raw_trades(self._get_client(), rows)
        return len(rows)

    def get_average_volume_baselines(self, lookback_windows: int = 20) -> dict[str, float]:
        """Return rolling average volume per symbol over the last N candles."""
        client = self._get_client()
        result = client.query(
            """
            SELECT product_id, avg(volume_qty) AS avg_volume_qty
            FROM (
                SELECT product_id, volume_qty,
                       row_number() OVER (
                           PARTITION BY product_id ORDER BY window_start DESC
                       ) AS rn
                FROM crypto.candles_1m
            )
            WHERE rn <= {lookback:UInt32}
            GROUP BY product_id
            """,
            parameters={"lookback": lookback_windows},
        )
        return {
            row[0]: float(row[1])
            for row in result.result_rows
        }

    # ── Private insert helpers ────────────────────────────────────────────────

    def _insert_raw_trades(self, client: Client, rows: list[RawTradeEvent]) -> None:
        data = [
            [
                r.product_id,
                _to_clickhouse_datetime(r.event_time),
                r.price_usd,
                r.size_qty,
                r.trade_id or "",
                r.source,
                _to_clickhouse_datetime(r.received_at),
            ]
            for r in rows
        ]
        client.insert(
            "raw_trades",
            data=data,
            column_names=["product_id", "event_time", "price_usd", "size_qty",
                          "trade_id", "source", "received_at"],
        )

    def _insert_candles(self, client: Client, rows: list[Candle1m]) -> None:
        data = [
            [
                r.product_id,
                _to_clickhouse_datetime(r.window_start),
                _to_clickhouse_datetime(r.window_end),
                r.open_price,
                r.high_price,
                r.low_price,
                r.close_price,
                r.volume_qty,
                r.trade_count,
                r.vwap_usd,
            ]
            for r in rows
        ]
        client.insert(
            "candles_1m",
            data=data,
            column_names=["product_id", "window_start", "window_end",
                          "open_price", "high_price", "low_price", "close_price",
                          "volume_qty", "trade_count", "vwap_usd"],
        )
        logger.debug("Inserted %d candles into ClickHouse.", len(rows))

    def _insert_live_metrics(self, client: Client, rows: list[LiveMetric]) -> None:
        data = [
            [
                r.product_id,
                _to_clickhouse_datetime(r.window_start),
                _to_clickhouse_datetime(r.window_end),
                r.last_price_usd,
                r.avg_price_usd,
                r.price_change_pct,
                r.volume_qty,
                r.trade_count,
                r.volatility_usd,
                r.vwap_usd,
            ]
            for r in rows
        ]
        client.insert(
            "live_metrics",
            data=data,
            column_names=["product_id", "window_start", "window_end",
                          "last_price_usd", "avg_price_usd", "price_change_pct",
                          "volume_qty", "trade_count", "volatility_usd", "vwap_usd"],
        )

    def _insert_alerts(self, client: Client, rows: list[VolumeAlert]) -> None:
        data = [
            [
                r.product_id,
                _to_clickhouse_datetime(r.window_start),
                r.spike_ratio,
                r.volume_qty,
                r.baseline_volume_qty,
                r.severity,
            ]
            for r in rows
        ]
        client.insert(
            "alerts",
            data=data,
            column_names=["product_id", "window_start", "spike_ratio",
                          "volume_qty", "baseline_volume_qty", "severity"],
        )
