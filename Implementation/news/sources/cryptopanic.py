"""
CryptoPanic news-feed adapter.

Polls the free CryptoPanic API (https://cryptopanic.com/api/v1/posts/)
for hot crypto news and normalises the response into the common article schema.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests

from sources.base_source import BaseNewsSource

logger = logging.getLogger(__name__)


class CryptoPanicSource(BaseNewsSource):
    """Fetch crypto news from the CryptoPanic aggregator API."""

    BASE_URL = "https://cryptopanic.com/api/v1/posts/"

    def __init__(
        self,
        api_key: str,
        currencies: list[str] | None = None,
        filter_kind: str = "",
        page_size: int = 40,
        timeout: int = 15,
    ) -> None:
        self._api_key = api_key
        self._currencies = currencies
        self._filter_kind = filter_kind
        self._page_size = page_size
        self._timeout = timeout
        self._seen_urls: set[str] = set()

    @property
    def name(self) -> str:
        return "cryptopanic"

    def fetch_articles(self) -> list[dict]:
        if not self._api_key:
            logger.warning("CryptoPanic API key not configured — skipping fetch")
            return []

        params: dict[str, str] = {
            "auth_token": self._api_key,
            "kind": "news",
        }
        if self._currencies:
            params["currencies"] = ",".join(self._currencies)
        if self._filter_kind:
            params["filter"] = self._filter_kind

        url = f"{self.BASE_URL}?{urlencode(params)}"

        try:
            resp = requests.get(url, timeout=self._timeout)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException:
            logger.exception("CryptoPanic API request failed")
            return []
        except ValueError:
            logger.exception("CryptoPanic returned invalid JSON")
            return []

        results = data.get("results", [])
        articles: list[dict] = []

        for item in results[: self._page_size]:
            article_url = item.get("url", "")
            if not article_url or article_url in self._seen_urls:
                continue
            self._seen_urls.add(article_url)

            currencies = [
                c.get("code", "").upper()
                for c in (item.get("currencies") or [])
                if c.get("code")
            ]

            domain = item.get("domain", "")
            published = item.get("published_at", "")

            articles.append(
                {
                    "source": self.name,
                    "title": item.get("title", ""),
                    "url": article_url,
                    "published_at": published,
                    "currencies": currencies,
                    "domain": domain,
                    "kind": item.get("kind", "news"),
                }
            )

        logger.info("CryptoPanic: fetched %d new articles", len(articles))
        return articles

    def reset_seen(self) -> None:
        """Clear dedup cache (useful for long-running processes)."""
        self._seen_urls.clear()
