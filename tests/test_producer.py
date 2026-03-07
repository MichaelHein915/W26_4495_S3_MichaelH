import json
import sys
from unittest.mock import MagicMock, patch

import pytest

from conftest import make_ticker_event


class TestOnMessage:
    """Tests for coinbase_producer.on_message callback."""

    @pytest.fixture(autouse=True)
    def _patch_producer(self):
        mock_cfg = MagicMock()
        mock_cfg.kafka_server = "localhost:9092"
        mock_cfg.kafka_client_id = "test"
        mock_cfg.topic_raw = "crypto.trades.raw"
        mock_cfg.symbols = ["BTC-USD"]
        mock_cfg.log_level = "WARNING"
        mock_cfg.coinbase_ws = "wss://ws-feed.exchange.coinbase.com"

        self.mock_producer_instance = MagicMock()
        mock_future = MagicMock()
        self.mock_producer_instance.send.return_value = mock_future

        with (
            patch("utils.config.get_config", return_value=mock_cfg),
            patch(
                "kafka.KafkaProducer",
                return_value=self.mock_producer_instance,
            ),
        ):
            import importlib
            import Implementation.producer.coinbase_producer as mod

            importlib.reload(mod)
            self.module = mod
            self.mock_producer_instance.reset_mock()
            mock_future.reset_mock()
            yield

    def test_valid_ticker_sends_to_kafka(self):
        msg = json.dumps(make_ticker_event())
        self.module.on_message(None, msg)

        self.mock_producer_instance.send.assert_called_once()
        call_args = self.mock_producer_instance.send.call_args
        assert call_args[0][0] == "crypto.trades.raw"
        assert call_args[1]["key"] == "BTC-USD"
        assert call_args[1]["value"]["price"] == "65000.50"

    def test_non_ticker_ignored(self):
        msg = json.dumps({"type": "subscriptions", "channels": []})
        self.module.on_message(None, msg)
        self.mock_producer_instance.send.assert_not_called()

    def test_ticker_without_price_ignored(self):
        event = make_ticker_event()
        del event["price"]
        self.module.on_message(None, json.dumps(event))
        self.mock_producer_instance.send.assert_not_called()

    def test_malformed_json_ignored(self):
        self.module.on_message(None, "not valid json{{{")
        self.mock_producer_instance.send.assert_not_called()

    def test_uses_product_id_as_key(self):
        msg = json.dumps(make_ticker_event(product_id="ETH-USD"))
        self.module.on_message(None, msg)
        call_args = self.mock_producer_instance.send.call_args
        assert call_args[1]["key"] == "ETH-USD"

    def test_missing_product_id_uses_unknown(self):
        event = make_ticker_event()
        del event["product_id"]
        self.module.on_message(None, json.dumps(event))
        call_args = self.mock_producer_instance.send.call_args
        assert call_args[1]["key"] == "unknown"


class TestOnOpen:
    @pytest.fixture(autouse=True)
    def _patch_producer(self):
        mock_cfg = MagicMock()
        mock_cfg.kafka_server = "localhost:9092"
        mock_cfg.kafka_client_id = "test"
        mock_cfg.topic_raw = "crypto.trades.raw"
        mock_cfg.symbols = ["BTC-USD", "ETH-USD"]
        mock_cfg.log_level = "WARNING"
        mock_cfg.coinbase_ws = "wss://ws-feed.exchange.coinbase.com"

        with (
            patch("utils.config.get_config", return_value=mock_cfg),
            patch("kafka.KafkaProducer", return_value=MagicMock()),
        ):
            import importlib
            import Implementation.producer.coinbase_producer as mod

            importlib.reload(mod)
            self.module = mod
            yield

    def test_on_open_sends_subscribe(self):
        ws = MagicMock()
        self.module.on_open(ws)
        ws.send.assert_called_once()

        payload = json.loads(ws.send.call_args[0][0])
        assert payload["type"] == "subscribe"
        assert "ticker" in payload["channels"]
        assert "BTC-USD" in payload["product_ids"]
        assert "ETH-USD" in payload["product_ids"]


class TestOnClose:
    @pytest.fixture(autouse=True)
    def _patch_producer(self):
        mock_cfg = MagicMock()
        mock_cfg.kafka_server = "localhost:9092"
        mock_cfg.kafka_client_id = "test"
        mock_cfg.topic_raw = "crypto.trades.raw"
        mock_cfg.symbols = ["BTC-USD"]
        mock_cfg.log_level = "WARNING"
        mock_cfg.coinbase_ws = "wss://ws-feed.exchange.coinbase.com"

        self.mock_producer_instance = MagicMock()
        with (
            patch("utils.config.get_config", return_value=mock_cfg),
            patch(
                "kafka.KafkaProducer",
                return_value=self.mock_producer_instance,
            ),
        ):
            import importlib
            import Implementation.producer.coinbase_producer as mod

            importlib.reload(mod)
            self.module = mod
            self.mock_producer_instance.reset_mock()
            yield

    def test_on_close_flushes_producer(self):
        self.module.on_close(None, 1000, "Normal closure")
        self.mock_producer_instance.flush.assert_called_once_with(10)
