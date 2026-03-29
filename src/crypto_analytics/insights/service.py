from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _format_price(value: float | None) -> str:
    if value is None:
        return "unknown price"
    if abs(value) >= 1000:
        return f"${value:,.2f}"
    return f"${value:,.4f}"


def _format_percent(value: float | None) -> str:
    if value is None:
        return "0.00%"
    return f"{value:+.2f}%"


def _format_volume(value: float | None) -> str:
    if value is None:
        return "0"
    return f"{value:,.4f}".rstrip("0").rstrip(".")


def _sf(value: Any, default: float = 0.0) -> float:
    """Safe float: returns default instead of raising on None / bad values."""
    try:
        result = float(value)
        return result if result == result else default  # reject NaN
    except (TypeError, ValueError):
        return default


def _si(value: Any, default: int = 0) -> int:
    """Safe int: returns default instead of raising on None / bad values."""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


class DeterministicInsightsService:
    def __init__(self, model_backend: str, model_name: str):
        self.model_backend = model_backend
        self.model_name = model_name

    def build_from_snapshot(
        self,
        snapshot: dict[str, Any],
        selected_symbol: str | None = None,
    ) -> dict[str, Any]:
        summary = snapshot.get("summary", {})
        overview = snapshot.get("overview")
        market_leaders = snapshot.get("market_leaders", {})
        pipeline_health = snapshot.get("pipeline_health", [])
        recent_alerts = snapshot.get("recent_alerts", [])

        headline = self._build_headline(overview, recent_alerts, selected_symbol)
        bullets = self._build_bullets(
            overview=overview,
            market_leaders=market_leaders,
            summary=summary,
            pipeline_health=pipeline_health,
            recent_alerts=recent_alerts,
            selected_symbol=selected_symbol,
        )

        return {
            "type": "deterministic",
            "backend": self.model_backend,
            "model_name": self.model_name,
            "headline": headline,
            "bullets": bullets,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

    def _build_headline(
        self,
        overview: dict[str, Any] | None,
        recent_alerts: list[dict[str, Any]],
        selected_symbol: str | None,
    ) -> str:
        if overview:
            if recent_alerts:
                return (
                    f"{selected_symbol} is the current focus, and the market has "
                    f"{len(recent_alerts)} recent alert(s) to review."
                )
            return (
                f"{selected_symbol} is the current focus, with the latest processed price at "
                f"{_format_price(_sf(overview.get('last_price_usd')))}."
            )

        if recent_alerts:
            return f"The market has {len(recent_alerts)} recent alert(s), but no symbol is currently selected."

        return "The insights panel is waiting for processed market data."

    def _build_bullets(
        self,
        overview: dict[str, Any] | None,
        market_leaders: dict[str, Any],
        summary: dict[str, Any],
        pipeline_health: list[dict[str, Any]],
        recent_alerts: list[dict[str, Any]],
        selected_symbol: str | None,
    ) -> list[str]:
        bullets: list[str] = []

        if overview and selected_symbol:
            bullets.append(
                f"{selected_symbol} last traded at {_format_price(_sf(overview.get('last_price_usd')))}, "
                f"with a 1-minute move of {_format_percent(_sf(overview.get('price_change_pct')))} "
                f"across {_si(overview.get('trade_count'))} trades."
            )

        top_movers = market_leaders.get("top_movers", [])
        if top_movers:
            mover = top_movers[0]
            bullets.append(
                f"{mover.get('product_id', 'Unknown')} is the strongest short-term mover at "
                f"{_format_percent(_sf(mover.get('price_change_pct')))} in the latest processed window."
            )

        top_volume = market_leaders.get("top_volume", [])
        if top_volume:
            volume_leader = top_volume[0]
            bullets.append(
                f"{volume_leader.get('product_id', 'Unknown')} is leading volume with "
                f"{_format_volume(_sf(volume_leader.get('volume_qty')))} units traded in the latest minute."
            )

        if recent_alerts:
            alert = recent_alerts[0]
            bullets.append(
                f"The most recent alert is {alert.get('severity', 'unknown')} severity "
                f"for {alert.get('product_id', 'Unknown')}, "
                f"with a spike ratio of {_sf(alert.get('spike_ratio')):.2f}x."
            )
        else:
            bullets.append("No recent volume alerts were detected in the processed alert table.")

        freshness_seconds = summary.get("freshness_seconds")
        live_metrics_health = next(
            (row for row in pipeline_health if row.get("table_name") == "live_metrics"),
            None,
        )
        if freshness_seconds is None:
            bullets.append("The pipeline has not produced a fresh processed batch yet.")
        else:
            bullets.append(
                f"The latest processed batch is {int(freshness_seconds)} seconds old, and "
                f"the live metrics table currently holds {int(live_metrics_health['row_count']) if live_metrics_health else 0} rows."
            )

        return bullets[:4]
