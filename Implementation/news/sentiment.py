"""
Crypto-aware sentiment analysis using VADER.

VADER is a lightweight rule-based model tuned for social-media text.
We extend its lexicon with crypto-specific terms so headlines like
"BTC moons after whale accumulation" score correctly.
"""

from __future__ import annotations

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_CRYPTO_LEXICON_UPDATES: dict[str, float] = {
    "moon": 2.5,
    "mooning": 3.0,
    "moonshot": 3.0,
    "bullish": 2.5,
    "bearish": -2.5,
    "pump": 1.8,
    "pumping": 2.0,
    "dump": -2.5,
    "dumping": -2.8,
    "rug": -3.5,
    "rugpull": -3.5,
    "scam": -3.0,
    "hack": -3.0,
    "hacked": -3.2,
    "exploit": -2.8,
    "whale": 1.0,
    "accumulation": 1.5,
    "adoption": 2.0,
    "rally": 2.5,
    "crash": -3.0,
    "crashed": -3.2,
    "plunge": -2.8,
    "plunges": -2.8,
    "surge": 2.5,
    "surges": 2.5,
    "soar": 2.5,
    "soars": 2.5,
    "breakout": 2.0,
    "breakdown": -2.0,
    "ath": 2.5,
    "fud": -2.0,
    "hodl": 1.5,
    "rekt": -3.0,
    "liquidation": -2.0,
    "liquidated": -2.5,
    "ban": -2.5,
    "banned": -2.8,
    "regulation": -0.8,
    "approval": 2.5,
    "approved": 2.8,
    "etf": 1.5,
    "halving": 1.5,
    "airdrop": 1.2,
    "delist": -2.0,
    "delisted": -2.5,
    "partnership": 2.0,
    "integration": 1.5,
    "upgrade": 1.5,
    "vulnerability": -2.5,
}

_analyzer: SentimentIntensityAnalyzer | None = None


def _get_analyzer() -> SentimentIntensityAnalyzer:
    global _analyzer
    if _analyzer is None:
        _analyzer = SentimentIntensityAnalyzer()
        _analyzer.lexicon.update(_CRYPTO_LEXICON_UPDATES)
    return _analyzer


def analyze(text: str) -> dict:
    """Run sentiment analysis on *text* and return a result dict.

    Returns::

        {
            "compound": float,   # -1.0 … +1.0
            "pos": float,
            "neg": float,
            "neu": float,
            "label": "positive" | "negative" | "neutral",
        }
    """
    scores = _get_analyzer().polarity_scores(text)
    compound = scores["compound"]

    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"

    return {
        "compound": round(compound, 4),
        "pos": round(scores["pos"], 4),
        "neg": round(scores["neg"], 4),
        "neu": round(scores["neu"], 4),
        "label": label,
    }
