import sys
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from conftest import make_parsed_event


@pytest.fixture(scope="module")
def api():
    """Import api_server with Kafka and config patched out."""
    mock_cfg = MagicMock()
    mock_cfg.kafka_server = "localhost:9092"
    mock_cfg.topic_raw = "crypto.trades.raw"
    mock_cfg.log_level = "WARNING"

    with patch("utils.config.get_config", return_value=mock_cfg):
        import importlib
        import Implementation.dashboard.api_server as mod

        importlib.reload(mod)
    return mod


class TestParseEventTime:
    def test_iso_with_z(self, api):
        result = api._parse_event_time("2026-03-06T12:00:00.000000Z")
        assert result is not None
        assert result.tzinfo is not None
        assert result.year == 2026

    def test_iso_with_offset(self, api):
        result = api._parse_event_time("2026-03-06T12:00:00+00:00")
        assert result is not None

    def test_empty_string(self, api):
        assert api._parse_event_time("") is None

    def test_none(self, api):
        assert api._parse_event_time(None) is None

    def test_invalid_format(self, api):
        assert api._parse_event_time("not-a-date") is None


class TestComputeMetrics:
    def test_empty_events(self, api):
        result = api._compute_metrics([])
        assert result == []

    def test_single_symbol(self, api):
        events = [make_parsed_event(price_usd=100.0 + i) for i in range(5)]
        result = api._compute_metrics(events)
        assert len(result) == 1
        row = result[0]
        assert row["product_id"] == "BTC-USD"
        assert row["trade_count"] == 5
        assert row["avg_price_usd"] == pytest.approx(102.0, abs=0.01)

    def test_multiple_symbols(self, api, sample_events):
        result = api._compute_metrics(sample_events)
        symbols = {r["product_id"] for r in result}
        assert symbols == {"BTC-USD", "ETH-USD"}

    def test_vwap_calculation(self, api):
        events = [
            make_parsed_event(price_usd=100.0, size_qty=2.0),
            make_parsed_event(price_usd=200.0, size_qty=1.0),
        ]
        result = api._compute_metrics(events)
        row = result[0]
        expected_vwap = (100.0 * 2.0 + 200.0 * 1.0) / (2.0 + 1.0)
        assert row["vwap_usd"] == pytest.approx(expected_vwap, abs=0.01)

    def test_zero_volume_vwap_falls_back_to_avg(self, api):
        events = [make_parsed_event(price_usd=100.0, size_qty=0.0)]
        result = api._compute_metrics(events)
        row = result[0]
        assert row["vwap_usd"] == pytest.approx(row["avg_price_usd"], abs=0.01)


class TestComputeTimeseries:
    def test_empty_events(self, api):
        result = api._compute_timeseries([])
        assert result == []

    def test_produces_30s_buckets(self, api, sample_events):
        result = api._compute_timeseries(sample_events)
        assert len(result) > 0
        assert "event_time" in result[0]
        assert "product_id" in result[0]


class TestComputeVolumeSpikes:
    def test_no_history_no_alert(self, api):
        metrics = [{"product_id": "BTC-USD", "total_volume_qty": 10.0}]
        alerts, history = api._compute_volume_spikes(metrics, {})
        assert alerts == []
        assert "BTC-USD" in history

    def test_spike_detected(self, api):
        history = {"BTC-USD": deque([1.0, 1.0, 1.0, 1.0, 1.0], maxlen=30)}
        metrics = [{"product_id": "BTC-USD", "total_volume_qty": 5.0}]
        alerts, _ = api._compute_volume_spikes(metrics, history)
        assert len(alerts) == 1
        assert alerts[0]["product_id"] == "BTC-USD"
        assert alerts[0]["spike_ratio"] == 5.0

    def test_no_spike_within_threshold(self, api):
        history = {"BTC-USD": deque([1.0, 1.0, 1.0, 1.0, 1.0], maxlen=30)}
        metrics = [{"product_id": "BTC-USD", "total_volume_qty": 1.5}]
        alerts, _ = api._compute_volume_spikes(metrics, history)
        assert alerts == []


class TestDashboardEndpoint:
    @pytest.fixture()
    def client(self, api):
        api.app.config["TESTING"] = True
        with api.app.test_client() as c:
            yield c

    def test_dashboard_returns_json(self, api, client):
        now = datetime.now(timezone.utc)
        events = [make_parsed_event(time_str=now.isoformat().replace("+00:00", "Z"))]
        with api._state_lock:
            api._state["events"] = deque(events)
            api._state["last_event_time"] = events[0]["event_time"]
            api._state["volume_history"] = {}
            api._state["kafka_error"] = None
            api._state["last_poll_count"] = 1

        resp = client.get("/api/dashboard")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "metrics" in data
        assert "timeseries" in data
        assert "alerts" in data
        assert "status" in data

    def test_dashboard_empty_state(self, api, client):
        with api._state_lock:
            api._state["events"] = deque()
            api._state["last_event_time"] = None
            api._state["volume_history"] = {}
            api._state["kafka_error"] = None
            api._state["last_poll_count"] = 0

        resp = client.get("/api/dashboard")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["metrics"] == []
        assert data["status"]["kafka_status"] == "waiting for data"

    def test_dashboard_reports_kafka_error(self, api, client):
        with api._state_lock:
            api._state["events"] = deque()
            api._state["last_event_time"] = None
            api._state["volume_history"] = {}
            api._state["kafka_error"] = "Connection refused"
            api._state["last_poll_count"] = 0

        resp = client.get("/api/dashboard")
        data = resp.get_json()
        assert "error" in data["status"]["kafka_status"]
        assert data["status"]["kafka_error"] == "Connection refused"
