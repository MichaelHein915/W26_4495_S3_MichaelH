import os
from unittest.mock import patch

import pytest

from utils.config import AppConfig, _parse_price_thresholds, _parse_symbols, get_config


class TestParseSymbols:
    def test_comma_separated(self):
        assert _parse_symbols("BTC-USD,ETH-USD,SOL-USD") == [
            "BTC-USD",
            "ETH-USD",
            "SOL-USD",
        ]

    def test_strips_whitespace(self):
        assert _parse_symbols("  BTC-USD , ETH-USD  ") == ["BTC-USD", "ETH-USD"]

    def test_empty_string(self):
        assert _parse_symbols("") == []

    def test_single_symbol(self):
        assert _parse_symbols("BTC-USD") == ["BTC-USD"]

    def test_trailing_comma(self):
        assert _parse_symbols("BTC-USD,ETH-USD,") == ["BTC-USD", "ETH-USD"]


class TestParsePriceThresholds:
    def test_valid_above(self):
        assert _parse_price_thresholds("BTC-USD:above:100000") == [
            ("BTC-USD", "above", 100000.0)
        ]

    def test_valid_below(self):
        assert _parse_price_thresholds("ETH-USD:below:3000") == [
            ("ETH-USD", "below", 3000.0)
        ]

    def test_multiple(self):
        result = _parse_price_thresholds("BTC-USD:above:100000,ETH-USD:below:3000,SOL-USD:above:200")
        assert result == [
            ("BTC-USD", "above", 100000.0),
            ("ETH-USD", "below", 3000.0),
            ("SOL-USD", "above", 200.0),
        ]

    def test_empty_string(self):
        assert _parse_price_thresholds("") == []

    def test_invalid_skipped(self):
        result = _parse_price_thresholds("BTC-USD:above:100000,invalid,SOL-USD:below:50")
        assert result == [
            ("BTC-USD", "above", 100000.0),
            ("SOL-USD", "below", 50.0),
        ]

    def test_invalid_direction_skipped(self):
        result = _parse_price_thresholds("BTC-USD:equals:100000")
        assert result == []


class TestGetConfig:
    def test_default_values(self):
        keys_to_remove = [
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_TOPIC_RAW",
            "COINBASE_WS_URL",
            "CRYPTO_SYMBOLS",
            "LOG_LEVEL",
            "KAFKA_CLIENT_ID",
        ]
        cleaned_env = {k: v for k, v in os.environ.items() if k not in keys_to_remove}
        with patch.dict(os.environ, cleaned_env, clear=True):
            cfg = get_config()

        assert cfg.kafka_server == "localhost:9092"
        assert cfg.topic_raw == "crypto.trades.raw"
        assert cfg.coinbase_ws == "wss://ws-feed.exchange.coinbase.com"
        assert cfg.log_level == "INFO"
        assert cfg.kafka_client_id == "crypto-producer"
        assert len(cfg.symbols) == 10
        assert "BTC-USD" in cfg.symbols

    def test_custom_values(self):
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
            "KAFKA_TOPIC_RAW": "custom.topic",
            "COINBASE_WS_URL": "wss://custom.ws",
            "CRYPTO_SYMBOLS": "BTC-USD,ETH-USD",
            "LOG_LEVEL": "DEBUG",
            "KAFKA_CLIENT_ID": "test-client",
            "AWS_REGION": "eu-west-1",
            "SINK_FLUSH_INTERVAL_SEC": "30",
            "SINK_FLUSH_MAX_RECORDS": "1000",
        }
        with patch.dict(os.environ, env, clear=False):
            cfg = get_config()

        assert cfg.kafka_server == "broker:29092"
        assert cfg.topic_raw == "custom.topic"
        assert cfg.symbols == ["BTC-USD", "ETH-USD"]
        assert cfg.log_level == "DEBUG"
        assert cfg.aws_region == "eu-west-1"
        assert cfg.sink_flush_interval_sec == 30
        assert cfg.sink_flush_max_records == 1000

    def test_alert_price_thresholds(self):
        env = {
            "ALERT_PRICE_THRESHOLDS": "BTC-USD:above:100000,ETH-USD:below:3000",
        }
        with patch.dict(os.environ, env, clear=False):
            cfg = get_config()
        assert cfg.alert_price_thresholds == [
            ("BTC-USD", "above", 100000.0),
            ("ETH-USD", "below", 3000.0),
        ]

    def test_config_is_frozen(self):
        cfg = get_config()
        with pytest.raises(AttributeError):
            cfg.kafka_server = "new-server"
