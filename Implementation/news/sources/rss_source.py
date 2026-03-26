"""
RSS feed news source adapter.

Fetches crypto news from free RSS feeds (no API key required).
Supports multiple feeds: CoinTelegraph, CoinDesk, Bitcoin Magazine.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

from sources.base_source import BaseNewsSource

logger = logging.getLogger(__name__)

DEFAULT_FEEDS = [
    ("cointelegraph", "https://cointelegraph.com/rss"),
    ("coindesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
]

SYMBOL_KEYWORDS: dict[str, list[str]] = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth", "ether"],
    "SOL": ["solana", "sol"],
    "XRP": ["xrp", "ripple"],
    "ADA": ["cardano", "ada"],
    "DOGE": ["dogecoin", "doge"],
    "AVAX": ["avalanche", "avax"],
    "LINK": ["chainlink", "link"],
    "LTC": ["litecoin", "ltc"],
    "BCH": ["bitcoin cash", "bch"],
    "BNB": ["binance", "bnb"],
    "DOT": ["polkadot", "dot"],
    "MATIC": ["polygon", "matic"],
}


def _extract_currencies(title: str) -> list[str]:
    """Match coin symbols/names mentioned in a headline."""
    lower = title.lower()
    found = []
    for symbol, keywords in SYMBOL_KEYWORDS.items():
        for kw in keywords:
            if re.search(rf"\b{re.escape(kw)}\b", lower):
                found.append(symbol)
                break
    return found


def _extract_domain(url: str) -> str:
    """Pull domain from a URL."""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return ""


class RSSNewsSource(BaseNewsSource):
    """Aggregate crypto news from multiple RSS feeds."""

    def __init__(
        self,
        feeds: list[tuple[str, str]] | None = None,
        timeout: int = 15,
    ) -> None:
        self._feeds = feeds or DEFAULT_FEEDS
        self._timeout = timeout
        self._seen_urls: set[str] = set()

    @property
    def name(self) -> str:
        return "rss"

    def fetch_articles(self) -> list[dict]:
        all_articles: list[dict] = []

        for feed_name, feed_url in self._feeds:
            try:
                resp = requests.get(feed_url, timeout=self._timeout, headers={
                    "User-Agent": "CryptoStreamPipeline/1.0",
                })
                resp.raise_for_status()
            except requests.RequestException:
                logger.warning("RSS fetch failed for %s", feed_name)
                continue

            try:
                root = ET.fromstring(resp.content)
            except ET.ParseError:
                logger.warning("RSS parse failed for %s", feed_name)
                continue

            items = root.findall(".//item")
            for item in items:
                url = item.findtext("link", "").strip()
                if not url or url in self._seen_urls:
                    continue
                self._seen_urls.add(url)

                title = item.findtext("title", "").strip()
                if not title:
                    continue

                pub_date = item.findtext("pubDate", "")
                published_at = ""
                if pub_date:
                    try:
                        dt = parsedate_to_datetime(pub_date)
                        published_at = dt.astimezone(timezone.utc).isoformat()
                    except Exception:
                        published_at = pub_date

                currencies = _extract_currencies(title)
                domain = _extract_domain(url)

                all_articles.append({
                    "source": feed_name,
                    "title": title,
                    "url": url,
                    "published_at": published_at,
                    "currencies": currencies,
                    "domain": domain,
                    "kind": "news",
                })

        logger.info("RSS: fetched %d new articles from %d feeds", len(all_articles), len(self._feeds))

        if len(self._seen_urls) > 5000:
            self._seen_urls.clear()

        return all_articles
