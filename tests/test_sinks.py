"""Tests for the _parse_message and _compute_candles logic shared by
redshift_sink.py and s3_sink.py."""

import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from conftest import make_ticker_event


@pytest.fixture(scope="module")
def s3_sink():
    mock_cfg = MagicMock()
    mock_cfg.kafka_server = "localhost:9092"
    mock_cfg.topic_raw = "crypto.trades.raw"
    mock_cfg.log_level = "WARNING"
    mock_cfg.s3_bucket = "test-bucket"
    mock_cfg.s3_staging_prefix = "crypto-data/"
    mock_cfg.aws_region = "us-west-2"
    mock_cfg.sink_flush_interval_sec = 60
    mock_cfg.sink_flush_max_records = 5000

    with (
        patch("utils.config.get_config", return_value=mock_cfg),
        patch("boto3.client"),
    ):
        import importlib
        import Implementation.athena.s3_sink as mod

        importlib.reload(mod)
    return mod


@pytest.fixture(scope="module")
def redshift_sink():
    mock_cfg = MagicMock()
    mock_cfg.kafka_server = "localhost:9092"
    mock_cfg.topic_raw = "crypto.trades.raw"
    mock_cfg.log_level = "WARNING"
    mock_cfg.redshift_host = "host"
    mock_cfg.redshift_port = 5439
    mock_cfg.redshift_db = "crypto"
    mock_cfg.redshift_user = "admin"
    mock_cfg.redshift_password = "secret"
    mock_cfg.redshift_iam_role = "arn:aws:iam::role/test"
    mock_cfg.s3_bucket = "test-bucket"
    mock_cfg.s3_staging_prefix = "crypto-data/"
    mock_cfg.aws_region = "us-west-2"
    mock_cfg.sink_flush_interval_sec = 60
    mock_cfg.sink_flush_max_records = 5000

    if "redshift_connector" not in sys.modules:
        sys.modules["redshift_connector"] = MagicMock()

    with (
        patch("utils.config.get_config", return_value=mock_cfg),
        patch("boto3.client"),
    ):
        import importlib
        import Implementation.redshift.redshift_sink as mod

        importlib.reload(mod)
    return mod


class TestParseMessageS3Sink:
    def test_valid_ticker(self, s3_sink):
        msg = make_ticker_event(price="65000.50", last_size="0.001")
        result = s3_sink._parse_message(msg)
        assert result is not None
        assert result["product_id"] == "BTC-USD"
        assert result["price"] == 65000.50
        assert result["size_qty"] == 0.001
        assert result["notional_usd"] == pytest.approx(65000.50 * 0.001)

    def test_non_ticker_ignored(self, s3_sink):
        assert s3_sink._parse_message({"type": "subscriptions"}) is None

    def test_missing_price_ignored(self, s3_sink):
        msg = {"type": "ticker", "product_id": "BTC-USD"}
        assert s3_sink._parse_message(msg) is None

    def test_invalid_price_ignored(self, s3_sink):
        msg = make_ticker_event(price="not_a_number")
        assert s3_sink._parse_message(msg) is None

    def test_missing_size_defaults_to_zero(self, s3_sink):
        msg = make_ticker_event()
        del msg["last_size"]
        result = s3_sink._parse_message(msg)
        assert result is not None
        assert result["size_qty"] == 0.0

    def test_product_id_defaults_to_unknown(self, s3_sink):
        msg = make_ticker_event()
        del msg["product_id"]
        result = s3_sink._parse_message(msg)
        assert result["product_id"] == "UNKNOWN"


class TestParseMessageRedshiftSink:
    def test_valid_ticker(self, redshift_sink):
        msg = make_ticker_event(price="42000.00", last_size="0.5")
        result = redshift_sink._parse_message(msg)
        assert result is not None
        assert result["price"] == 42000.0
        assert result["size_qty"] == 0.5

    def test_non_ticker_ignored(self, redshift_sink):
        assert redshift_sink._parse_message({"type": "heartbeat"}) is None


class TestComputeCandles:
    def _make_raw_df(self, count=60):
        rows = []
        for i in range(count):
            rows.append(
                {
                    "trade_time": f"2026-03-06T12:00:{i % 60:02d}.000Z",
                    "product_id": "BTC-USD",
                    "price": 65000.0 + i,
                    "size_qty": 0.001,
                    "notional_usd": (65000.0 + i) * 0.001,
                }
            )
        return pd.DataFrame(rows)

    def test_candle_aggregation(self, s3_sink):
        df = self._make_raw_df(60)
        candles = s3_sink._compute_candles(df)
        assert not candles.empty
        assert "open_price" in candles.columns
        assert "high_price" in candles.columns
        assert "low_price" in candles.columns
        assert "close_price" in candles.columns
        assert "volume" in candles.columns
        assert "trade_count" in candles.columns
        assert "vwap" in candles.columns

    def test_empty_df(self, s3_sink):
        candles = s3_sink._compute_candles(pd.DataFrame())
        assert candles.empty

    def test_ohlcv_values(self, s3_sink):
        rows = [
            {
                "trade_time": "2026-03-06T12:00:00.000Z",
                "product_id": "BTC-USD",
                "price": 100.0,
                "size_qty": 1.0,
                "notional_usd": 100.0,
            },
            {
                "trade_time": "2026-03-06T12:00:30.000Z",
                "product_id": "BTC-USD",
                "price": 120.0,
                "size_qty": 2.0,
                "notional_usd": 240.0,
            },
            {
                "trade_time": "2026-03-06T12:00:45.000Z",
                "product_id": "BTC-USD",
                "price": 90.0,
                "size_qty": 1.0,
                "notional_usd": 90.0,
            },
        ]
        df = pd.DataFrame(rows)
        candles = s3_sink._compute_candles(df)
        assert len(candles) == 1
        c = candles.iloc[0]
        assert c["open_price"] == 100.0
        assert c["high_price"] == 120.0
        assert c["low_price"] == 90.0
        assert c["close_price"] == 90.0
        assert c["volume"] == pytest.approx(4.0)
        assert c["trade_count"] == 3
        expected_vwap = (100.0 + 240.0 + 90.0) / 4.0
        assert c["vwap"] == pytest.approx(expected_vwap, abs=0.01)

    def test_multiple_products(self, s3_sink):
        rows = [
            {
                "trade_time": "2026-03-06T12:00:00.000Z",
                "product_id": "BTC-USD",
                "price": 65000.0,
                "size_qty": 0.1,
                "notional_usd": 6500.0,
            },
            {
                "trade_time": "2026-03-06T12:00:00.000Z",
                "product_id": "ETH-USD",
                "price": 3500.0,
                "size_qty": 1.0,
                "notional_usd": 3500.0,
            },
        ]
        df = pd.DataFrame(rows)
        candles = s3_sink._compute_candles(df)
        products = set(candles["product_id"])
        assert products == {"BTC-USD", "ETH-USD"}
