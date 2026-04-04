"""
Persistent operator-tunable alert thresholds (JSON) with optional delivery history.

Secrets (Slack webhook, SMTP) stay in environment — only numeric/boolean rules are stored here.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_lock = threading.Lock()
SETTINGS_VERSION = 1
MAX_HISTORY = 200

# Populated at runtime from env + disk
_history: list[dict[str, Any]] = []

# Avoid re-reading JSON on every /api/dashboard poll (key = path + mtime)
_cache_key: tuple[str, float] | None = None
_cached_tuning: AlertTuning | None = None


def _invalidate_tuning_cache() -> None:
    global _cache_key, _cached_tuning
    _cache_key = None
    _cached_tuning = None


def _repo_data_dir() -> Path:
    root = Path(__file__).resolve().parents[2]
    return root / "data"


def settings_path() -> Path:
    raw = os.environ.get("ALERT_SETTINGS_PATH", "").strip()
    if raw:
        return Path(raw).expanduser()
    return _repo_data_dir() / "alert_settings.json"


def saved_alert_settings_exist() -> bool:
    return settings_path().is_file()


def history_path() -> Path:
    raw = os.environ.get("ALERT_HISTORY_PATH", "").strip()
    if raw:
        return Path(raw).expanduser()
    return _repo_data_dir() / "alert_history.json"


@dataclass
class AlertTuning:
    arbitrage_threshold_pct: float
    volume_spike_ratio: float
    price_thresholds: list[tuple[str, str, float]]
    anomaly_enabled: bool
    anomaly_contamination: float
    alert_cooldown_sec: int

    def to_api_dict(self) -> dict[str, Any]:
        return {
            "arbitrage_threshold_pct": self.arbitrage_threshold_pct,
            "volume_spike_ratio": self.volume_spike_ratio,
            "price_thresholds": [
                {"symbol": s, "direction": d, "price": p} for s, d, p in self.price_thresholds
            ],
            "anomaly_enabled": self.anomaly_enabled,
            "anomaly_contamination": self.anomaly_contamination,
            "alert_cooldown_sec": self.alert_cooldown_sec,
        }


def _defaults_from_config(cfg: Any) -> AlertTuning:
    return AlertTuning(
        arbitrage_threshold_pct=float(cfg.alert_arbitrage_threshold_pct),
        volume_spike_ratio=float(cfg.alert_volume_spike_ratio),
        price_thresholds=list(cfg.alert_price_thresholds),
        anomaly_enabled=bool(cfg.alert_anomaly_enabled),
        anomaly_contamination=float(cfg.anomaly_contamination),
        alert_cooldown_sec=60,
    )


def _normalize_price_thresholds(raw: Any) -> list[tuple[str, str, float]]:
    if not isinstance(raw, list):
        return []
    out: list[tuple[str, str, float]] = []
    for item in raw[:50]:
        if not isinstance(item, dict):
            continue
        sym = str(item.get("symbol", "")).strip().upper()
        direction = str(item.get("direction", "")).strip().lower()
        try:
            price = float(item.get("price"))
        except (TypeError, ValueError):
            continue
        if not sym or direction not in ("above", "below") or price <= 0:
            continue
        out.append((sym, direction, price))
    return out


def _tuning_from_dict(data: dict[str, Any], fallback: AlertTuning) -> AlertTuning:
    def f(key: str, default: float) -> float:
        try:
            return float(data[key])
        except (KeyError, TypeError, ValueError):
            return default

    def b(key: str, default: bool) -> bool:
        v = data.get(key, default)
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.lower() in ("1", "true", "yes")
        return bool(v)

    cd = int(f("alert_cooldown_sec", fallback.alert_cooldown_sec))
    return AlertTuning(
        arbitrage_threshold_pct=f("arbitrage_threshold_pct", fallback.arbitrage_threshold_pct),
        volume_spike_ratio=f("volume_spike_ratio", fallback.volume_spike_ratio),
        price_thresholds=_normalize_price_thresholds(
            data.get("price_thresholds", [dict(symbol=s, direction=d, price=p) for s, d, p in fallback.price_thresholds])
        ),
        anomaly_enabled=b("anomaly_enabled", fallback.anomaly_enabled),
        anomaly_contamination=f("anomaly_contamination", fallback.anomaly_contamination),
        alert_cooldown_sec=cd,
    )


def validate_tuning_payload(data: dict[str, Any], cfg: Any) -> tuple[AlertTuning | None, str | None]:
    """Return (tuning, error_message)."""
    base = _defaults_from_config(cfg)
    try:
        arb = float(data.get("arbitrage_threshold_pct", base.arbitrage_threshold_pct))
        vol = float(data.get("volume_spike_ratio", base.volume_spike_ratio))
        anom_c = float(data.get("anomaly_contamination", base.anomaly_contamination))
        cd = int(data.get("alert_cooldown_sec", base.alert_cooldown_sec))
    except (TypeError, ValueError):
        return None, "Invalid numeric field"

    if not (0.01 <= arb <= 50.0):
        return None, "arbitrage_threshold_pct must be between 0.01 and 50"
    if not (1.01 <= vol <= 100.0):
        return None, "volume_spike_ratio must be between 1.01 and 100"
    if not (0.01 <= anom_c <= 0.5):
        return None, "anomaly_contamination must be between 0.01 and 0.5"
    if not (10 <= cd <= 3600):
        return None, "alert_cooldown_sec must be between 10 and 3600"

    ae = data.get("anomaly_enabled", base.anomaly_enabled)
    if isinstance(ae, str):
        ae = ae.lower() in ("1", "true", "yes")
    ae = bool(ae)

    pts = _normalize_price_thresholds(data.get("price_thresholds", []))
    tuning = AlertTuning(
        arbitrage_threshold_pct=arb,
        volume_spike_ratio=vol,
        price_thresholds=pts,
        anomaly_enabled=ae,
        anomaly_contamination=anom_c,
        alert_cooldown_sec=cd,
    )
    return tuning, None


def _read_settings_file(fallback: AlertTuning) -> AlertTuning:
    path = settings_path()
    if not path.is_file():
        return fallback
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return fallback
        if data.get("version", SETTINGS_VERSION) != SETTINGS_VERSION:
            logger.warning("alert_settings.json version mismatch, merging with config defaults")
        return _tuning_from_dict(data, fallback)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Could not read %s: %s", path, e)
        return fallback


def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    text = json.dumps(obj, indent=2, sort_keys=isinstance(obj, dict))
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def get_alert_tuning(cfg: Any) -> AlertTuning:
    """Effective tuning: JSON file overrides env-backed config defaults."""
    path = settings_path()
    pstr = str(path.resolve())
    try:
        mkey = path.stat().st_mtime if path.is_file() else 0.0
    except OSError:
        mkey = 0.0
    cache_tuple = (pstr, mkey)
    with _lock:
        global _cache_key, _cached_tuning
        if _cached_tuning is not None and _cache_key == cache_tuple:
            return _cached_tuning
        tuning = _read_settings_file(_defaults_from_config(cfg))
        _cache_key = cache_tuple
        _cached_tuning = tuning
        return tuning


def save_alert_tuning(tuning: AlertTuning) -> None:
    payload = {
        "version": SETTINGS_VERSION,
        "arbitrage_threshold_pct": tuning.arbitrage_threshold_pct,
        "volume_spike_ratio": tuning.volume_spike_ratio,
        "price_thresholds": [
            {"symbol": s, "direction": d, "price": p} for s, d, p in tuning.price_thresholds
        ],
        "anomaly_enabled": tuning.anomaly_enabled,
        "anomaly_contamination": tuning.anomaly_contamination,
        "alert_cooldown_sec": tuning.alert_cooldown_sec,
    }
    with _lock:
        _invalidate_tuning_cache()
        _atomic_write_json(settings_path(), payload)
        path = settings_path()
        try:
            global _cache_key, _cached_tuning
            mkey = path.stat().st_mtime if path.is_file() else 0.0
            _cache_key = (str(path.resolve()), mkey)
            _cached_tuning = tuning
        except OSError:
            pass


def reset_alert_settings_file(cfg: Any) -> AlertTuning:
    """Remove settings file and return env defaults."""
    with _lock:
        _invalidate_tuning_cache()
        path = settings_path()
        try:
            if path.is_file():
                path.unlink()
        except OSError as e:
            logger.warning("Could not remove %s: %s", path, e)
        tuning = _defaults_from_config(cfg)
        p = settings_path()
        _cache_key = (str(p.resolve()), 0.0)
        _cached_tuning = tuning
        return tuning


def load_history_from_disk() -> None:
    global _history
    path = history_path()
    if not path.is_file():
        _history = []
        return
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            _history = data[-MAX_HISTORY:]
        elif isinstance(data, dict) and isinstance(data.get("events"), list):
            _history = data["events"][-MAX_HISTORY:]
        else:
            _history = []
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Could not read alert history %s: %s", path, e)
        _history = []


def _persist_history() -> None:
    path = history_path()
    _atomic_write_json(path, _history[-MAX_HISTORY:])


def record_alert_event(
    alert_type: str,
    summary: str,
    detail: dict[str, Any] | None = None,
) -> None:
    """Append a fired notification to in-memory history and persist."""
    entry = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "type": alert_type,
        "summary": summary[:500],
        "detail": detail or {},
    }
    with _lock:
        global _history
        _history.append(entry)
        if len(_history) > MAX_HISTORY:
            _history = _history[-MAX_HISTORY:]
        try:
            _persist_history()
        except OSError as e:
            logger.warning("Could not persist alert history: %s", e)


def get_alert_history(limit: int = 50) -> list[dict[str, Any]]:
    lim = max(1, min(limit, MAX_HISTORY))
    with _lock:
        return list(_history[-lim:][::-1])
