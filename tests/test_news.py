"""Tests for the news & sentiment pipeline."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure Implementation modules are importable
_news_dir = str(Path(__file__).resolve().parents[1] / "Implementation" / "news")
if _news_dir not in sys.path:
    sys.path.insert(0, _news_dir)

_dashboard_dir = str(Path(__file__).resolve().parents[1] / "Implementation" / "dashboard")
if _dashboard_dir not in sys.path:
    sys.path.insert(0, _dashboard_dir)


# ── Sentiment analysis tests ─────────────────────────────────────────

class TestSentimentAnalysis:
    def test_positive_headline(self):
        from sentiment import analyze

        result = analyze("Bitcoin surges to new all-time high as ETF approved")
        assert result["label"] == "positive"
        assert result["compound"] > 0.05
        assert "pos" in result
        assert "neg" in result
        assert "neu" in result

    def test_negative_headline(self):
        from sentiment import analyze

        result = analyze("Crypto exchange hacked, millions lost in rug pull scam")
        assert result["label"] == "negative"
        assert result["compound"] < -0.05

    def test_neutral_headline(self):
        from sentiment import analyze

        result = analyze("Bitcoin trading volume stable today")
        assert result["label"] in ("neutral", "positive")
        assert -0.3 <= result["compound"] <= 0.3

    def test_crypto_specific_terms(self):
        from sentiment import analyze

        bullish = analyze("BTC is mooning, bulls take over the market")
        assert bullish["compound"] > 0.2

        bearish = analyze("massive dump and liquidation rekt traders")
        assert bearish["compound"] < -0.3

    def test_empty_text(self):
        from sentiment import analyze

        result = analyze("")
        assert result["label"] == "neutral"
        assert result["compound"] == 0.0


# ── CryptoPanic source parsing tests ─────────────────────────────────

class TestCryptoPanicSource:
    SAMPLE_API_RESPONSE = {
        "results": [
            {
                "title": "Bitcoin Hits $100K",
                "url": "https://example.com/btc-100k",
                "published_at": "2026-03-26T10:00:00Z",
                "currencies": [{"code": "BTC"}, {"code": "ETH"}],
                "domain": "example.com",
                "kind": "news",
            },
            {
                "title": "Ethereum Upgrade Announced",
                "url": "https://example.com/eth-upgrade",
                "published_at": "2026-03-26T09:00:00Z",
                "currencies": [{"code": "ETH"}],
                "domain": "example.com",
                "kind": "news",
            },
        ]
    }

    def test_parse_articles(self):
        from sources.cryptopanic import CryptoPanicSource

        source = CryptoPanicSource(api_key="test-key")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = self.SAMPLE_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch("sources.cryptopanic.requests.get", return_value=mock_resp):
            articles = source.fetch_articles()

        assert len(articles) == 2
        assert articles[0]["title"] == "Bitcoin Hits $100K"
        assert articles[0]["source"] == "cryptopanic"
        assert articles[0]["currencies"] == ["BTC", "ETH"]
        assert articles[1]["currencies"] == ["ETH"]

    def test_deduplication(self):
        from sources.cryptopanic import CryptoPanicSource

        source = CryptoPanicSource(api_key="test-key")
        mock_resp = MagicMock()
        mock_resp.json.return_value = self.SAMPLE_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch("sources.cryptopanic.requests.get", return_value=mock_resp):
            first = source.fetch_articles()
            second = source.fetch_articles()

        assert len(first) == 2
        assert len(second) == 0

    def test_missing_api_key(self):
        from sources.cryptopanic import CryptoPanicSource

        source = CryptoPanicSource(api_key="")
        articles = source.fetch_articles()
        assert articles == []

    def test_api_error_returns_empty(self):
        from sources.cryptopanic import CryptoPanicSource
        import requests

        source = CryptoPanicSource(api_key="test-key")

        with patch("sources.cryptopanic.requests.get", side_effect=requests.ConnectionError("fail")):
            articles = source.fetch_articles()

        assert articles == []


# ── Sentiment analytics function tests ────────────────────────────────

class TestSentimentAnalytics:
    @pytest.fixture()
    def news_events(self):
        return [
            {
                "title": "BTC surges",
                "currencies": ["BTC"],
                "published_at": "2026-03-26T10:00:00Z",
                "sentiment": {"compound": 0.8, "pos": 0.5, "neg": 0.0, "neu": 0.5, "label": "positive"},
            },
            {
                "title": "ETH crashed",
                "currencies": ["ETH"],
                "published_at": "2026-03-26T10:05:00Z",
                "sentiment": {"compound": -0.7, "pos": 0.0, "neg": 0.6, "neu": 0.4, "label": "negative"},
            },
            {
                "title": "Market stable",
                "currencies": ["BTC", "ETH"],
                "published_at": "2026-03-26T10:10:00Z",
                "sentiment": {"compound": 0.0, "pos": 0.1, "neg": 0.1, "neu": 0.8, "label": "neutral"},
            },
        ]

    def test_compute_sentiment_summary(self, news_events):
        from analytics import compute_sentiment_summary

        result = compute_sentiment_summary(news_events)
        assert result["total"] == 3
        assert result["positive"] == 1
        assert result["negative"] == 1
        assert result["neutral"] == 1
        assert -1.0 <= result["avg_compound"] <= 1.0
        assert result["label"] in ("positive", "negative", "neutral")

    def test_compute_sentiment_summary_empty(self):
        from analytics import compute_sentiment_summary

        result = compute_sentiment_summary([])
        assert result["total"] == 0
        assert result["label"] == "neutral"

    def test_compute_sentiment_by_symbol(self, news_events):
        from analytics import compute_sentiment_by_symbol

        result = compute_sentiment_by_symbol(news_events)
        symbols = {r["currency"] for r in result}
        assert "BTC" in symbols
        assert "ETH" in symbols
        btc = next(r for r in result if r["currency"] == "BTC")
        assert btc["article_count"] == 2
        assert btc["label"] in ("positive", "negative", "neutral")

    def test_compute_sentiment_timeseries(self, news_events):
        from analytics import compute_sentiment_timeseries

        result = compute_sentiment_timeseries(news_events)
        assert isinstance(result, list)
        for point in result:
            assert "published_at" in point
            assert "avg_compound" in point
            assert "article_count" in point


# ── Dashboard API endpoint tests ──────────────────────────────────────

class TestNewsAPIEndpoints:
    @pytest.fixture()
    def client(self):
        sys.path.insert(0, _dashboard_dir)
        from api_server import app

        app.config["TESTING"] = True
        with app.test_client() as c:
            yield c

    def test_news_endpoint(self, client):
        resp = client.get("/api/news")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "articles" in data
        assert "count" in data

    def test_news_endpoint_currency_filter(self, client):
        resp = client.get("/api/news?currency=BTC")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "articles" in data

    def test_sentiment_endpoint(self, client):
        resp = client.get("/api/sentiment")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "summary" in data
        assert "by_symbol" in data
        assert "timeseries" in data
