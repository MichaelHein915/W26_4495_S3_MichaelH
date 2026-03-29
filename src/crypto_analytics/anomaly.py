"""
Deterministic anomaly signal detection.

All signals are computed from data already fetched for the dashboard snapshot —
no ML model and no extra database queries. Pure Python math over the
live_metrics overview and recent candles_1m rows.

Each signal returned:
  id          – stable machine identifier
  label       – short human label  (shown in badge)
  value_str   – formatted measurement (shown next to label)
  description – plain-English explanation of what the signal means
  severity    – "warning" | "info" | "neutral"
  direction   – "positive" | "negative" | "neutral"
"""

from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any


# ── helpers ──────────────────────────────────────────────────────────────────

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def _pct(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"


def _stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance) if variance > 0 else 0.0


def _parse_utc_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _completed_candles(
    candles: list[dict[str, Any]],
    current_time: datetime,
) -> list[dict[str, Any]]:
    completed: list[dict[str, Any]] = []
    for candle in candles:
        window_end = _parse_utc_timestamp(candle.get("window_end"))
        if window_end is None:
            continue
        if window_end <= current_time:
            completed.append(candle)
    return completed


def _overview_from_candle(candle: dict[str, Any]) -> dict[str, Any]:
    return {
        "last_price_usd": candle.get("close_price"),
        "vwap_usd": candle.get("vwap_usd"),
        "trade_count": candle.get("trade_count"),
    }


# ── individual signal detectors ───────────────────────────────────────────────

def _vwap_deviation(overview: dict[str, Any]) -> dict[str, Any] | None:
    """
    How far the current price is from VWAP.

    Above VWAP  → buyers drove price up relative to where big trades settled.
    Below VWAP  → sellers have pushed price below the weighted average.
    """
    price = _safe_float(overview.get("last_price_usd"))
    vwap  = _safe_float(overview.get("vwap_usd"))
    if price == 0 or vwap == 0:
        return None

    dev_pct = (price - vwap) / vwap * 100.0

    if abs(dev_pct) < 0.3:
        return {
            "id": "vwap_deviation",
            "label": "At VWAP",
            "value_str": _pct(dev_pct),
            "description": (
                "Price is right in line with the volume-weighted average. "
                "Buying and selling pressure are roughly balanced this window."
            ),
            "severity": "neutral",
            "direction": "neutral",
        }

    if dev_pct >= 1.5:
        severity, direction = "warning", "positive"
        desc = (
            f"Price is {_pct(dev_pct)} above VWAP — buyers are paying a premium. "
            "Large trades earlier in this window settled at lower prices. "
            "Watch for a pullback toward VWAP."
        )
    elif dev_pct >= 0.3:
        severity, direction = "info", "positive"
        desc = (
            f"Price is {_pct(dev_pct)} above VWAP — mild upward bias. "
            "Buyers are slightly in control this window."
        )
    elif dev_pct <= -1.5:
        severity, direction = "warning", "negative"
        desc = (
            f"Price is {_pct(dev_pct)} below VWAP — sellers are pushing price down. "
            "Big trades settled at higher prices. "
            "Watch for a bounce or continued selling."
        )
    else:
        severity, direction = "info", "negative"
        desc = (
            f"Price is {_pct(dev_pct)} below VWAP — mild downward bias. "
            "Sellers are slightly in control this window."
        )

    label = "Above VWAP" if dev_pct > 0 else "Below VWAP"
    return {
        "id": "vwap_deviation",
        "label": label,
        "value_str": _pct(dev_pct),
        "description": desc,
        "severity": severity,
        "direction": direction,
    }


def _price_volume_divergence(
    prev: dict[str, Any],
    curr: dict[str, Any],
) -> dict[str, Any] | None:
    """
    Compare price direction vs volume direction across the last two candles.

    Price up + volume up   → bullish confirmation (strong move)
    Price up + volume down → bearish divergence   (weak move, likely to stall)
    Price down + volume up → bearish confirmation (real selling pressure)
    Price down + volume down → dead cat, weak selling (may not sustain)
    """
    curr_open  = _safe_float(curr.get("open_price"))
    curr_close = _safe_float(curr.get("close_price"))
    curr_vol   = _safe_float(curr.get("volume_qty"))
    prev_vol   = _safe_float(prev.get("volume_qty"))

    if curr_open == 0 or prev_vol == 0:
        return None

    price_up  = curr_close > curr_open
    vol_up    = curr_vol   > prev_vol

    price_chg_pct = (curr_close - curr_open) / curr_open * 100.0
    vol_chg_pct   = (curr_vol - prev_vol) / prev_vol * 100.0 if prev_vol else 0.0
    flat_price_threshold_pct = 0.1
    meaningful_volume_change_pct = 25.0

    if abs(price_chg_pct) < flat_price_threshold_pct:
        if vol_up and vol_chg_pct >= 100.0:
            return {
                "id": "price_volume_divergence",
                "label": "Volume Surge, Price Flat",
                "value_str": f"Price {_pct(price_chg_pct)}, Vol +{vol_chg_pct:.0f}%",
                "description": (
                    "Volume jumped sharply but price barely moved. That usually means the market "
                    "is absorbing heavy flow rather than cleanly breaking up or down. Wait for "
                    "directional follow-through before treating this as bullish or bearish."
                ),
                "severity": "info",
                "direction": "neutral",
            }
        return None

    if abs(vol_chg_pct) < meaningful_volume_change_pct:
        return None

    if price_up and vol_up:
        return {
            "id": "price_volume_divergence",
            "label": "Bullish Confirmation",
            "value_str": f"Price {_pct(price_chg_pct)}, Vol +{vol_chg_pct:.0f}%",
            "description": (
                "Price is rising and volume is increasing — the move has conviction. "
                "This is the textbook bullish signal: buyers are stepping in with size."
            ),
            "severity": "info",
            "direction": "positive",
        }

    if price_up and not vol_up:
        return {
            "id": "price_volume_divergence",
            "label": "Bearish Divergence",
            "value_str": f"Price {_pct(price_chg_pct)}, Vol {vol_chg_pct:.0f}%",
            "description": (
                "Price went up but volume is shrinking — a weak move. "
                "When price rises on falling volume, there are fewer buyers behind it. "
                "These moves often stall or reverse. Treat with caution."
            ),
            "severity": "warning",
            "direction": "negative",
        }

    if not price_up and vol_up:
        return {
            "id": "price_volume_divergence",
            "label": "Bearish Confirmation",
            "value_str": f"Price {_pct(price_chg_pct)}, Vol +{vol_chg_pct:.0f}%",
            "description": (
                "Price is falling and volume is rising — sellers have real conviction. "
                "High-volume drops are harder to reverse quickly. "
                "This is the most reliable bearish signal."
            ),
            "severity": "warning",
            "direction": "negative",
        }

    # price down + vol down
    return {
        "id": "price_volume_divergence",
        "label": "Weak Selling",
        "value_str": f"Price {_pct(price_chg_pct)}, Vol {vol_chg_pct:.0f}%",
        "description": (
            "Price dipped but volume is falling too — not panic selling. "
            "Low-volume dips often recover. The market is losing interest rather "
            "than actively selling off."
        ),
        "severity": "info",
        "direction": "negative",
    }


def _volume_zscore(candles: list[dict[str, Any]]) -> dict[str, Any] | None:
    """
    How many standard deviations the latest candle's volume is from the rolling mean.

    Z > 2.5  → extreme volume spike
    Z > 1.5  → elevated volume
    Z < -1.0 → thin market (below average)
    """
    vols = [_safe_float(c.get("volume_qty")) for c in candles]
    if len(vols) < 5 or vols[-1] == 0:
        return None

    history = vols[:-1]
    current = vols[-1]
    mean  = sum(history) / len(history)
    std   = _stddev(history)

    if std == 0:
        return None

    z = (current - mean) / std

    if z >= 2.5:
        return {
            "id": "volume_zscore",
            "label": "Extreme Volume",
            "value_str": f"Z = +{z:.1f}",
            "description": (
                f"Volume is {z:.1f} standard deviations above the recent average — "
                "well outside normal range. Something significant is driving heavy "
                "participation right now. Check for news, large orders, or a breakout."
            ),
            "severity": "warning",
            "direction": "neutral",
        }

    if z >= 1.5:
        return {
            "id": "volume_zscore",
            "label": "Elevated Volume",
            "value_str": f"Z = +{z:.1f}",
            "description": (
                f"Volume is {z:.1f} standard deviations above average — "
                "noticeably higher than normal but not extreme. "
                "Worth monitoring."
            ),
            "severity": "info",
            "direction": "neutral",
        }

    if z <= -1.0:
        return {
            "id": "volume_zscore",
            "label": "Thin Market",
            "value_str": f"Z = {z:.1f}",
            "description": (
                "Volume is below normal — fewer participants are trading right now. "
                "Price moves on thin volume can be misleading and are easier to reverse. "
                "Don't over-read small price changes."
            ),
            "severity": "info",
            "direction": "neutral",
        }

    # Normal range — only include if marginally interesting
    return None


def _momentum(candles: list[dict[str, Any]]) -> dict[str, Any] | None:
    """
    Is the price change per window accelerating or decelerating?

    Looks at the last 3 candles' per-candle price change (open → close).
    Acceleration = each window's change is bigger than the last.
    Deceleration = change is shrinking window-over-window.
    """
    if len(candles) < 3:
        return None

    recent = candles[-3:]
    changes = []
    for c in recent:
        o = _safe_float(c.get("open_price"))
        cl = _safe_float(c.get("close_price"))
        if o == 0:
            return None
        changes.append((cl - o) / o * 100.0)

    c0, c1, c2 = changes
    all_positive = c0 > 0 and c1 > 0 and c2 > 0
    all_negative = c0 < 0 and c1 < 0 and c2 < 0

    accelerating_up   = all_positive and abs(c2) > abs(c1) > abs(c0)
    decelerating_up   = all_positive and abs(c2) < abs(c1)
    accelerating_down = all_negative and abs(c2) > abs(c1) > abs(c0)
    decelerating_down = all_negative and abs(c2) < abs(c1)

    if accelerating_up:
        return {
            "id": "momentum",
            "label": "Accelerating Up",
            "value_str": f"{_pct(c0)} → {_pct(c1)} → {_pct(c2)}",
            "description": (
                "Each of the last 3 windows has moved up by a larger amount than the previous. "
                "Upward momentum is building. Strong moves like this can continue — "
                "or exhaust suddenly. Watch volume for confirmation."
            ),
            "severity": "info",
            "direction": "positive",
        }

    if decelerating_up:
        return {
            "id": "momentum",
            "label": "Momentum Fading",
            "value_str": f"{_pct(c0)} → {_pct(c1)} → {_pct(c2)}",
            "description": (
                "Price is still rising but each window's gain is smaller than the last. "
                "Buying pressure is weakening. A pause or reversal is more likely than continuation."
            ),
            "severity": "warning",
            "direction": "negative",
        }

    if accelerating_down:
        return {
            "id": "momentum",
            "label": "Accelerating Down",
            "value_str": f"{_pct(c0)} → {_pct(c1)} → {_pct(c2)}",
            "description": (
                "Each of the last 3 windows has dropped by a larger amount than the previous. "
                "Downward momentum is building. Selling pressure is increasing."
            ),
            "severity": "warning",
            "direction": "negative",
        }

    if decelerating_down:
        return {
            "id": "momentum",
            "label": "Selling Slowing",
            "value_str": f"{_pct(c0)} → {_pct(c1)} → {_pct(c2)}",
            "description": (
                "Price is still dropping but each window's loss is smaller than the last. "
                "Selling pressure is fading. A stabilisation or bounce becomes more likely."
            ),
            "severity": "info",
            "direction": "positive",
        }

    return None


def _thin_market(
    overview: dict[str, Any],
    candles: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """
    Flag windows where the trade count is far below the rolling average.
    Thin books = unreliable price signals.
    """
    trade_counts = [_safe_float(c.get("trade_count")) for c in candles[:-1]]
    if len(trade_counts) < 4:
        return None

    avg_count = sum(trade_counts) / len(trade_counts)
    current_count = _safe_float(overview.get("trade_count"))

    if avg_count == 0 or current_count == 0:
        return None

    ratio = current_count / avg_count
    if ratio < 0.3:
        return {
            "id": "thin_market",
            "label": "Very Thin Book",
            "value_str": f"{int(current_count)} trades (avg {int(avg_count)})",
            "description": (
                f"Only {int(current_count)} trades this window vs. an average of {int(avg_count)}. "
                "The market is unusually quiet. Price moves in thin markets are easier to "
                "manufacture and harder to trust. Wait for volume to return before acting."
            ),
            "severity": "warning",
            "direction": "neutral",
        }

    return None


# ── public API ────────────────────────────────────────────────────────────────

def compute_signals(
    overview: dict[str, Any] | None,
    candles: list[dict[str, Any]],
    current_time: datetime | None = None,
) -> list[dict[str, Any]]:
    """
    Compute all anomaly signals for a single symbol.

    Args:
        overview: Latest live_metrics row for the symbol (may be None).
        candles:  Candles list ordered oldest → newest (from get_candles).

    Returns:
        List of signal dicts, sorted warning-first then info then neutral.
        Empty list if insufficient data.
    """
    signals: list[dict[str, Any]] = []
    current_time = current_time or datetime.now(timezone.utc)

    if not candles:
        return signals

    completed_candles = _completed_candles(candles, current_time)
    if not completed_candles:
        return signals

    signal_overview = _overview_from_candle(completed_candles[-1])

    if signal_overview:
        sig = _vwap_deviation(signal_overview)
        if sig:
            signals.append(sig)

    if len(completed_candles) >= 2:
        sig = _price_volume_divergence(completed_candles[-2], completed_candles[-1])
        if sig:
            signals.append(sig)

    if len(completed_candles) >= 5:
        sig = _volume_zscore(completed_candles)
        if sig:
            signals.append(sig)

    if len(completed_candles) >= 3:
        sig = _momentum(completed_candles[-3:])
        if sig:
            signals.append(sig)

    if signal_overview and len(completed_candles) >= 5:
        sig = _thin_market(signal_overview, completed_candles)
        if sig:
            signals.append(sig)

    _PRIORITY = {"warning": 0, "info": 1, "neutral": 2}
    signals.sort(key=lambda s: _PRIORITY.get(s["severity"], 3))

    return signals
