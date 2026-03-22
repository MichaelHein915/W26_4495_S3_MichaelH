"""
Build structured market-data context strings for LLM prompts.
Converts dashboard payload dicts into concise text the model can reason over.
"""

from datetime import datetime, timezone


def build_market_context(payload: dict) -> str:
    """
    Convert the output of _build_dashboard_payload() into a text summary
    suitable for injection into an LLM system prompt.
    """
    parts: list[str] = []

    status = payload.get("status", {})
    window = status.get("window_minutes", "?")
    parts.append(
        f"Snapshot: {status.get('updated_at', 'N/A')} | "
        f"Window: last {window} min | "
        f"Events: {status.get('event_count', 0)} | "
        f"Symbols: {status.get('live_symbols', 0)} | "
        f"Exchanges: {len(payload.get('exchange_stats', {}).get('exchanges', []))}"
    )

    metrics = payload.get("metrics", [])
    if metrics:
        header = "| Symbol | Avg Price | Change% | Trades | VWAP | Volatility | Volume |"
        sep = "|--------|-----------|---------|--------|------|------------|--------|"
        rows = []
        for m in metrics:
            rows.append(
                f"| {m['product_id']} "
                f"| ${m['avg_price_usd']:,.2f} "
                f"| {m['price_change_pct']:+.2f}% "
                f"| {m['trade_count']} "
                f"| ${m['vwap_usd']:,.2f} "
                f"| ${m['volatility_usd']:,.2f} "
                f"| {m['total_volume_qty']:.4f} |"
            )
        parts.append("\nMETRICS:\n" + header + "\n" + sep + "\n" + "\n".join(rows))

    alerts = payload.get("alerts", [])
    if alerts:
        lines = []
        for a in alerts:
            lines.append(
                f"- {a['product_id']}: volume {a['current_volume']:.4f} "
                f"({a['spike_ratio']}x above baseline {a['baseline_volume']:.4f})"
            )
        parts.append("\nVOLUME SPIKE ALERTS:\n" + "\n".join(lines))

    anomalies = payload.get("anomalies", [])
    if anomalies:
        lines = []
        for a in anomalies:
            lines.append(
                f"- {a['product_id']}: anomaly_score={a['anomaly_score']}, "
                f"trades={a['trade_count']}, volatility=${a.get('volatility_usd', 0):.2f}, "
                f"change={a.get('price_change_pct', 0):+.2f}%"
            )
        parts.append("\nML ANOMALIES (Isolation Forest):\n" + "\n".join(lines))

    price_alerts = payload.get("price_alerts", [])
    if price_alerts:
        lines = []
        for a in price_alerts:
            lines.append(
                f"- {a['product_id']}: ${a['current_price']:,.2f} "
                f"{a['direction']} threshold ${a['threshold_price']:,.2f}"
            )
        parts.append("\nPRICE THRESHOLD ALERTS:\n" + "\n".join(lines))

    arbitrage = payload.get("arbitrage", [])
    if arbitrage:
        lines = []
        for a in arbitrage:
            lines.append(
                f"- {a['product_id']}: Buy {a['cheap_exchange']} "
                f"(${a['cheap_price']:,.2f}) -> Sell {a['expensive_exchange']} "
                f"(${a['expensive_price']:,.2f}) = {a['spread_pct']}% spread"
            )
        parts.append("\nARBITRAGE OPPORTUNITIES:\n" + "\n".join(lines))

    exchange_stats = payload.get("exchange_stats", {})
    ex_counts = exchange_stats.get("exchange_counts", {})
    if ex_counts:
        lines = [f"- {ex}: {count} trades" for ex, count in sorted(ex_counts.items())]
        parts.append("\nEXCHANGE ACTIVITY:\n" + "\n".join(lines))

    recent = payload.get("recent_trades", [])
    if recent:
        top5 = recent[:5]
        lines = []
        for t in top5:
            lines.append(
                f"- {t['event_time']} {t['product_id']} "
                f"${t['price_usd']:,.2f} x{t['size_qty']:.4f} [{t['exchange']}]"
            )
        parts.append(f"\nRECENT TRADES (last {len(top5)}):\n" + "\n".join(lines))

    return "\n".join(parts)


def build_insight_prompt(payload: dict) -> str:
    """
    Build a prompt asking the LLM to generate a concise market summary
    from the current dashboard data.
    """
    context = build_market_context(payload)
    return (
        f"{context}\n\n"
        "Based on the live market data above, write a brief market insight report "
        "(3-5 bullet points). Cover:\n"
        "1. Overall market direction and sentiment\n"
        "2. Notable movers (biggest gainers/losers)\n"
        "3. Any active alerts, anomalies, or arbitrage opportunities\n"
        "4. Key volume or volatility observations\n"
        "Be concise and specific — cite exact numbers."
    )


def build_query_prompt(user_query: str, payload: dict) -> str:
    """
    Build a prompt for natural-language data queries.
    The LLM extracts filters and returns structured JSON.
    """
    context = build_market_context(payload)
    return (
        f"{context}\n\n"
        f"User query: \"{user_query}\"\n\n"
        "Parse the user's natural-language query against the live market data above. "
        "Return a JSON object with:\n"
        "- \"answer\": a short human-readable answer to the query\n"
        "- \"symbols\": list of relevant symbol IDs (e.g. [\"BTC-USD\"]), or [] for all\n"
        "- \"exchanges\": list of relevant exchanges, or [] for all\n"
        "- \"data\": relevant data rows extracted from the metrics table\n\n"
        "Return ONLY valid JSON, no markdown fences."
    )
