"""ClickHouse-backed dashboard query service for the cloud deployment."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from crypto_analytics.anomaly import compute_signals as _compute_anomaly_signals
from crypto_analytics.settings import AppSettings

logger = logging.getLogger(__name__)


def _format_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value


def _parse_utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


class ClickHouseDashboardQueryService:
    """Reads dashboard data from ClickHouse for the cloud deployment."""

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self._client: Client | None = None

    def _get_client(self) -> Client:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.settings.clickhouse_host,
                port=self.settings.clickhouse_port,
                database=self.settings.clickhouse_db,
                username=self.settings.clickhouse_user,
                password=self.settings.clickhouse_password,
                connect_timeout=10,
                send_receive_timeout=30,
            )
        return self._client

    def _fetch_all(self, sql: str, parameters: dict | None = None) -> list[dict[str, Any]]:
        client = self._get_client()
        result = client.query(sql, parameters=parameters or {})
        columns = result.column_names
        return [
            {col: _format_value(val) for col, val in zip(columns, row)}
            for row in result.result_rows
        ]

    # ── Public query methods (same signature as DashboardQueryService) ────────

    def list_symbols(self) -> list[str]:
        rows = self._fetch_all(
            """
            SELECT DISTINCT product_id FROM (
                SELECT product_id FROM live_metrics
                UNION DISTINCT
                SELECT product_id FROM candles_1m
                UNION DISTINCT
                SELECT product_id FROM alerts
            ) ORDER BY product_id
            """
        )
        return [row["product_id"] for row in rows]

    def get_market_overview(self) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT
                product_id,
                window_start,
                window_end,
                last_price_usd,
                avg_price_usd,
                price_change_pct,
                volume_qty,
                volume_qty * last_price_usd AS notional_volume_usd,
                trade_count,
                volatility_usd,
                vwap_usd
            FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY product_id ORDER BY window_start DESC
                    ) AS rn
                FROM live_metrics
            )
            WHERE rn = 1
            ORDER BY notional_volume_usd DESC, abs(price_change_pct) DESC, product_id ASC
            """
        )

    def get_market_sparklines(self, n_candles: int = 8) -> dict[str, list[float]]:
        rows = self._fetch_all(
            """
            SELECT product_id, close_price
            FROM (
                SELECT product_id, close_price,
                       row_number() OVER (
                           PARTITION BY product_id ORDER BY window_start DESC
                       ) AS rn
                FROM candles_1m
            )
            WHERE rn <= {n:UInt32}
            ORDER BY product_id ASC, rn DESC
            """,
            parameters={"n": n_candles},
        )
        result: dict[str, list[float]] = {}
        for row in rows:
            pid = row["product_id"]
            price = row["close_price"]
            if price is not None:
                result.setdefault(pid, []).append(float(price))
        return result

    def get_candles(self, symbol: str, limit: int = 60) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT product_id, window_start, window_end,
                   open_price, high_price, low_price, close_price,
                   volume_qty, trade_count, vwap_usd
            FROM (
                SELECT *
                FROM candles_1m
                WHERE product_id = {symbol:String}
                ORDER BY window_start DESC
                LIMIT {limit:UInt32}
            )
            ORDER BY window_start ASC
            """,
            parameters={"symbol": symbol, "limit": limit},
        )

    def get_alerts(self, symbol: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        if symbol:
            return self._fetch_all(
                """
                SELECT product_id, window_start,
                       max(volume_qty) AS volume_qty,
                       max(baseline_volume_qty) AS baseline_volume_qty,
                       max(spike_ratio) AS spike_ratio,
                       max(severity) AS severity
                FROM alerts
                WHERE product_id = {symbol:String}
                GROUP BY product_id, window_start
                ORDER BY window_start DESC, product_id ASC
                LIMIT {limit:UInt32}
                """,
                parameters={"symbol": symbol, "limit": limit},
            )
        return self._fetch_all(
            """
            SELECT product_id, window_start,
                   max(volume_qty) AS volume_qty,
                   max(baseline_volume_qty) AS baseline_volume_qty,
                   max(spike_ratio) AS spike_ratio,
                   max(severity) AS severity
            FROM alerts
            GROUP BY product_id, window_start
            ORDER BY window_start DESC, product_id ASC
            LIMIT {limit:UInt32}
            """,
            parameters={"limit": limit},
        )

    def get_pipeline_health(self) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT 'candles_1m' AS table_name,
                   count() AS row_count,
                   max(window_end) AS latest_timestamp
            FROM candles_1m
            UNION ALL
            SELECT 'live_metrics' AS table_name,
                   count() AS row_count,
                   max(window_end) AS latest_timestamp
            FROM live_metrics
            UNION ALL
            SELECT 'alerts' AS table_name,
                   count() AS row_count,
                   max(window_start) AS latest_timestamp
            FROM alerts
            """
        )

    # ── Main snapshot (identical return shape to DashboardQueryService) ────────

    def get_dashboard_snapshot(
        self,
        symbol: str | None = None,
        candle_limit: int = 60,
        alert_limit: int = 20,
    ) -> dict[str, Any]:
        symbols        = self.list_symbols()
        selected       = symbol if symbol in symbols else (symbols[0] if symbols else None)
        market_overview = self.get_market_overview()
        pipeline_health = self.get_pipeline_health()
        overview       = next(
            (m for m in market_overview if m["product_id"] == selected), None
        )
        candles        = self.get_candles(selected, limit=candle_limit) if selected else []
        alerts         = self.get_alerts(selected, limit=alert_limit) if selected else []
        recent_alerts  = self.get_alerts(limit=alert_limit)
        sparklines     = self.get_market_sparklines()

        last_updated_at = max(
            (m["window_end"] for m in market_overview if m.get("window_end")),
            default=None,
        )
        freshness_seconds = None
        if last_updated_at:
            ts = _parse_utc_timestamp(last_updated_at)
            if ts:
                freshness_seconds = max(
                    int((datetime.now(timezone.utc) - ts).total_seconds()), 0
                )

        def _safe(v: Any, default: float = 0.0) -> float:
            try:
                r = float(v)
                return r if r == r else default
            except (TypeError, ValueError):
                return default

        top_movers = sorted(
            market_overview,
            key=lambda m: abs(_safe(m.get("price_change_pct"))),
            reverse=True,
        )[:3]
        top_volume = sorted(
            market_overview,
            key=lambda m: _safe(m.get("notional_volume_usd")),
            reverse=True,
        )[:3]

        anomaly_signals = _compute_anomaly_signals(overview, candles)

        return {
            "symbols":          symbols,
            "selected_symbol":  selected,
            "summary": {
                "tracked_symbols":           len(symbols),
                "symbols_with_live_metrics": len(market_overview),
                "selected_symbol_alerts":    len(alerts),
                "recent_alert_count":        len(recent_alerts),
                "last_updated_at":           last_updated_at,
                "freshness_seconds":         freshness_seconds,
            },
            "overview":         overview,
            "market_overview":  market_overview,
            "market_leaders": {
                "top_movers": top_movers,
                "top_volume": top_volume,
            },
            "pipeline_health":  pipeline_health,
            "candles":          candles,
            "alerts":           alerts,
            "recent_alerts":    recent_alerts,
            "anomaly_signals":  anomaly_signals,
            "sparklines":       sparklines,
        }
