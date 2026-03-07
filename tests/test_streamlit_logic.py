"""Tests for pure logic functions in streamlit_app.py.

These tests don't launch the Streamlit UI — they exercise the computation
helpers that can be called directly.
"""

from collections import deque
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from conftest import make_parsed_event


def _import_streamlit_app():
    mock_cfg = MagicMock()
    mock_cfg.kafka_server = "localhost:9092"
    mock_cfg.topic_raw = "crypto.trades.raw"
    mock_cfg.log_level = "WARNING"

    with patch("utils.config.get_config", return_value=mock_cfg):
        import importlib
        import Implementation.dashboard.streamlit_app as mod

        importlib.reload(mod)
    return mod


st_app = _import_streamlit_app()


class TestParseEventTime:
    def test_iso_z_suffix(self):
        result = st_app._parse_event_time("2026-03-06T12:00:00.000000Z")
        assert result is not None
        assert result.tzinfo is not None

    def test_empty_returns_none(self):
        assert st_app._parse_event_time("") is None

    def test_none_returns_none(self):
        assert st_app._parse_event_time(None) is None

    def test_bad_format_returns_none(self):
        assert st_app._parse_event_time("yesterday") is None


class TestPruneEvents:
    def test_removes_old_events(self):
        now = datetime.now(timezone.utc)
        events = deque(
            [
                {"event_time": now - timedelta(minutes=10)},
                {"event_time": now - timedelta(minutes=5)},
                {"event_time": now - timedelta(seconds=30)},
            ]
        )
        cutoff = now - timedelta(minutes=3)
        st_app._prune_events(events, cutoff)
        assert len(events) == 1

    def test_empty_deque(self):
        events = deque()
        st_app._prune_events(events, datetime.now(timezone.utc))
        assert len(events) == 0

    def test_keeps_all_when_recent(self):
        now = datetime.now(timezone.utc)
        events = deque(
            [
                {"event_time": now - timedelta(seconds=10)},
                {"event_time": now - timedelta(seconds=5)},
            ]
        )
        cutoff = now - timedelta(minutes=3)
        st_app._prune_events(events, cutoff)
        assert len(events) == 2


class TestComputeMetrics:
    def test_empty_deque(self):
        result = st_app._compute_metrics(deque())
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_single_product(self):
        events = deque(
            [make_parsed_event(price_usd=100.0 + i) for i in range(5)]
        )
        result = st_app._compute_metrics(events)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["product_id"] == "BTC-USD"
        assert row["trade_count"] == 5

    def test_vwap_with_volume(self):
        events = deque(
            [
                make_parsed_event(price_usd=100.0, size_qty=2.0),
                make_parsed_event(price_usd=200.0, size_qty=1.0),
            ]
        )
        result = st_app._compute_metrics(events)
        row = result.iloc[0]
        expected = (100.0 * 2.0 + 200.0 * 1.0) / (2.0 + 1.0)
        assert row["vwap_usd"] == pytest.approx(expected, abs=0.01)

    def test_volatility_single_event_is_zero(self):
        events = deque([make_parsed_event()])
        result = st_app._compute_metrics(events)
        assert result.iloc[0]["volatility_usd"] == 0.0


class TestComputeTimeseries:
    def test_empty_deque(self):
        result = st_app._compute_timeseries(deque())
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_produces_rows(self):
        events = deque(
            [
                make_parsed_event(time_str=f"2026-03-06T12:00:{i:02d}.000000Z")
                for i in range(5)
            ]
        )
        result = st_app._compute_timeseries(events)
        assert not result.empty
        assert "product_id" in result.columns


class TestComputeLatencySeconds:
    def test_empty_events(self):
        assert st_app._compute_latency_seconds(deque()) is None

    def test_positive_latency(self):
        now = datetime.now(timezone.utc)
        events = deque(
            [{"event_time": now - timedelta(seconds=2)}]
        )
        latency = st_app._compute_latency_seconds(events)
        assert latency is not None
        assert latency >= 2.0


class TestComputeVolumeSpikes:
    def test_no_alert_without_history(self):
        df = pd.DataFrame(
            [{"product_id": "BTC-USD", "total_volume_qty": 10.0}]
        )
        alerts = st_app._compute_volume_spikes(df, {})
        assert alerts == []

    def test_spike_detected(self):
        history = {"BTC-USD": deque([1.0] * 5, maxlen=30)}
        df = pd.DataFrame(
            [{"product_id": "BTC-USD", "total_volume_qty": 5.0}]
        )
        alerts = st_app._compute_volume_spikes(df, history)
        assert len(alerts) == 1
        assert alerts[0]["spike_ratio"] == 5.0

    def test_no_spike_below_threshold(self):
        history = {"BTC-USD": deque([1.0] * 5, maxlen=30)}
        df = pd.DataFrame(
            [{"product_id": "BTC-USD", "total_volume_qty": 1.5}]
        )
        alerts = st_app._compute_volume_spikes(df, history)
        assert alerts == []
