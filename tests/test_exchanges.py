"""Tests for the multi-exchange producer classes."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, call

import pytest

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "Implementation" / "producer"))


# ── Coinbase ─────────────────────────────────────────────────────────


class TestCoinbaseProducer:
    @pytest.fixture(autouse=True)
    def _setup(self):
        from exchange_coinbase import CoinbaseProducer

        self.producer = CoinbaseProducer(
            kafka_producer=MagicMock(),
            topic="test-topic",
            symbols=["BTC-USD", "ETH-USD"],
            ws_url="wss://fake",
        )

    def test_subscribe_payload(self):
        payload = json.loads(self.producer._build_subscribe_payload())
        assert payload["type"] == "subscribe"
        assert payload["product_ids"] == ["BTC-USD", "ETH-USD"]
        assert payload["channels"] == ["ticker"]

    def test_parse_ticker_message(self):
        raw = {
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": "65000.50",
            "last_size": "0.001",
            "time": "2026-03-06T12:00:00Z",
        }
        result = self.producer._parse_message(raw)
        assert result is not None
        assert result["exchange"] == "coinbase"
        assert result["product_id"] == "BTC-USD"
        assert result["price"] == "65000.50"
        assert result["size"] == "0.001"

    def test_ignores_non_ticker(self):
        raw = {"type": "subscriptions", "channels": []}
        assert self.producer._parse_message(raw) is None

    def test_ignores_no_price(self):
        raw = {"type": "ticker", "product_id": "BTC-USD"}
        assert self.producer._parse_message(raw) is None

    def test_missing_product_id_defaults(self):
        raw = {"type": "ticker", "price": "100"}
        result = self.producer._parse_message(raw)
        assert result["product_id"] == "unknown"


# ── Binance ──────────────────────────────────────────────────────────


class TestBinanceSymbolConversion:
    def test_to_binance(self):
        from exchange_binance import _to_binance

        assert _to_binance("BTC-USD") == "btcusdt"
        assert _to_binance("ETH-USDT") == "ethusdt"

    def test_to_unified(self):
        from exchange_binance import _to_unified

        assert _to_unified("BTCUSDT") == "BTC-USDT"
        assert _to_unified("ETHUSDT") == "ETH-USDT"
        assert _to_unified("SOLUSD") == "SOL-USD"


class TestBinanceProducer:
    @pytest.fixture(autouse=True)
    def _setup(self):
        from exchange_binance import BinanceProducer

        self.producer = BinanceProducer(
            kafka_producer=MagicMock(),
            topic="test-topic",
            symbols=["BTC-USD", "ETH-USD"],
            ws_url="wss://stream.binance.com:9443",
        )

    def test_ws_url_has_streams(self):
        assert "/stream?streams=" in self.producer._ws_url
        assert "btcusdt@trade" in self.producer._ws_url
        assert "ethusdt@trade" in self.producer._ws_url

    def test_subscribe_payload_empty(self):
        assert self.producer._build_subscribe_payload() == ""

    def test_parse_trade_message(self):
        raw = {
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "s": "BTCUSDT",
                "p": "65000.50",
                "q": "0.001",
                "T": 1709726400000,
            },
        }
        result = self.producer._parse_message(raw)
        assert result is not None
        assert result["exchange"] == "binance"
        assert result["product_id"] == "BTC-USD"
        assert result["price"] == "65000.50"
        assert result["size"] == "0.001"

    def test_ignores_non_trade(self):
        raw = {"data": {"e": "kline", "s": "BTCUSDT"}}
        assert self.producer._parse_message(raw) is None

    def test_parse_without_stream_wrapper(self):
        raw = {
            "e": "trade",
            "s": "ETHUSDT",
            "p": "3500",
            "q": "0.1",
            "T": 1709726400000,
        }
        result = self.producer._parse_message(raw)
        assert result is not None
        assert result["product_id"] == "ETH-USD"


# ── Kraken ───────────────────────────────────────────────────────────


class TestKrakenSymbolConversion:
    def test_to_kraken(self):
        from exchange_kraken import _to_kraken

        assert _to_kraken("BTC-USD") == "XBT/USD"
        assert _to_kraken("ETH-USD") == "ETH/USD"
        assert _to_kraken("DOGE-USD") == "XDG/USD"

    def test_from_kraken(self):
        from exchange_kraken import _from_kraken

        assert _from_kraken("XBT/USD") == "BTC-USD"
        assert _from_kraken("ETH/USD") == "ETH-USD"
        assert _from_kraken("XDG/USD") == "DOGE-USD"


class TestKrakenProducer:
    @pytest.fixture(autouse=True)
    def _setup(self):
        from exchange_kraken import KrakenProducer

        self.producer = KrakenProducer(
            kafka_producer=MagicMock(),
            topic="test-topic",
            symbols=["BTC-USD", "ETH-USD"],
            ws_url="wss://ws.kraken.com/v2",
        )

    def test_subscribe_payload(self):
        payload = json.loads(self.producer._build_subscribe_payload())
        assert payload["method"] == "subscribe"
        assert payload["params"]["channel"] == "trade"
        assert "XBT/USD" in payload["params"]["symbol"]
        assert "ETH/USD" in payload["params"]["symbol"]

    def test_parse_trade_message(self):
        raw = {
            "channel": "trade",
            "data": [
                {
                    "symbol": "XBT/USD",
                    "price": 65000.5,
                    "qty": 0.001,
                    "timestamp": "2026-03-06T12:00:00.000Z",
                }
            ],
        }
        result = self.producer._parse_message(raw)
        assert result is not None
        assert result["exchange"] == "kraken"
        assert result["product_id"] == "BTC-USD"
        assert result["price"] == "65000.5"
        assert result["size"] == "0.001"

    def test_parse_multiple_trades(self):
        raw = {
            "channel": "trade",
            "data": [
                {"symbol": "XBT/USD", "price": 65000, "qty": 0.001, "timestamp": "2026-03-06T12:00:00Z"},
                {"symbol": "ETH/USD", "price": 3500, "qty": 0.1, "timestamp": "2026-03-06T12:00:01Z"},
            ],
        }
        result = self.producer._parse_message(raw)
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["product_id"] == "BTC-USD"
        assert result[1]["product_id"] == "ETH-USD"

    def test_ignores_non_trade_channel(self):
        raw = {"channel": "heartbeat", "data": []}
        assert self.producer._parse_message(raw) is None

    def test_ignores_no_data(self):
        raw = {"channel": "trade"}
        assert self.producer._parse_message(raw) is None


# ── BaseExchange ─────────────────────────────────────────────────────


class TestBaseExchangeOnMessage:
    """Test that _on_message publishes normalised data to Kafka."""

    def test_publishes_to_kafka(self):
        from exchange_coinbase import CoinbaseProducer

        mock_kafka = MagicMock()
        producer = CoinbaseProducer(
            kafka_producer=mock_kafka,
            topic="test-topic",
            symbols=["BTC-USD"],
            ws_url="wss://fake",
        )
        msg = json.dumps(
            {
                "type": "ticker",
                "product_id": "BTC-USD",
                "price": "65000",
                "last_size": "0.001",
                "time": "2026-03-06T12:00:00Z",
            }
        )
        producer._on_message(None, msg)
        mock_kafka.send.assert_called_once()
        args, kwargs = mock_kafka.send.call_args
        assert args[0] == "test-topic"
        assert kwargs["key"] == "BTC-USD"
        assert kwargs["value"]["exchange"] == "coinbase"

    def test_skips_malformed_json(self):
        from exchange_coinbase import CoinbaseProducer

        mock_kafka = MagicMock()
        producer = CoinbaseProducer(
            kafka_producer=mock_kafka,
            topic="test-topic",
            symbols=["BTC-USD"],
            ws_url="wss://fake",
        )
        producer._on_message(None, "not valid json {{{")
        mock_kafka.send.assert_not_called()


# ── Exchange stats in API ────────────────────────────────────────────


class TestComputeExchangeStats:
    @pytest.fixture(scope="class")
    def api(self):
        from unittest.mock import patch

        mock_cfg = MagicMock()
        mock_cfg.kafka_server = "localhost:9092"
        mock_cfg.topic_raw = "test"
        mock_cfg.log_level = "WARNING"
        with patch("utils.config.get_config", return_value=mock_cfg):
            import importlib
            import Implementation.dashboard.api_server as mod

            importlib.reload(mod)
        return mod

    def test_empty_events(self, api):
        result = api._compute_exchange_stats([])
        assert result["exchanges"] == []
        assert result["exchange_counts"] == {}

    def test_single_exchange(self, api):
        events = [
            {
                "exchange": "coinbase",
                "product_id": "BTC-USD",
                "price_usd": 65000,
                "size_qty": 0.1,
                "notional_usd": 6500,
            },
            {"exchange": "coinbase", "product_id": "ETH-USD", "price_usd": 3500, "size_qty": 1.0, "notional_usd": 3500},
        ]
        result = api._compute_exchange_stats(events)
        assert result["exchanges"] == ["coinbase"]
        assert result["exchange_counts"]["coinbase"] == 2
        assert sorted(result["exchange_symbols"]["coinbase"]) == ["BTC-USD", "ETH-USD"]

    def test_multiple_exchanges(self, api):
        events = [
            {
                "exchange": "coinbase",
                "product_id": "BTC-USD",
                "price_usd": 65000,
                "size_qty": 0.1,
                "notional_usd": 6500,
            },
            {
                "exchange": "binance",
                "product_id": "BTC-USDT",
                "price_usd": 65010,
                "size_qty": 0.2,
                "notional_usd": 13002,
            },
            {"exchange": "binance", "product_id": "ETH-USDT", "price_usd": 3500, "size_qty": 1.0, "notional_usd": 3500},
            {
                "exchange": "kraken",
                "product_id": "BTC-USD",
                "price_usd": 64990,
                "size_qty": 0.05,
                "notional_usd": 3249.5,
            },
        ]
        result = api._compute_exchange_stats(events)
        assert sorted(result["exchanges"]) == ["binance", "coinbase", "kraken"]
        assert result["exchange_counts"]["binance"] == 2
        assert result["exchange_counts"]["coinbase"] == 1
        assert result["exchange_counts"]["kraken"] == 1
