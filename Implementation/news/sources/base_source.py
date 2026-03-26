"""
Abstract base class for pluggable news sources.

Each adapter normalises its API response into a common article schema:

    {
      "source": str,          # e.g. "cryptopanic"
      "title": str,
      "url": str,
      "published_at": str,    # ISO-8601 UTC
      "currencies": list[str],# e.g. ["BTC", "ETH"]
      "domain": str,          # origin site, e.g. "coindesk.com"
      "kind": str,            # "news" | "media" | "analysis"
    }
"""

from __future__ import annotations

import abc


class BaseNewsSource(abc.ABC):
    """Pluggable news-feed adapter."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Short identifier used in logs and metrics labels."""

    @abc.abstractmethod
    def fetch_articles(self) -> list[dict]:
        """Return a list of normalised article dicts.

        Implementations should handle transient errors gracefully
        (log + return an empty list rather than raising).
        """
