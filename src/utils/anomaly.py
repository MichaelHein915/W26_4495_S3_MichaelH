"""
ML-based anomaly detection for crypto metrics using Isolation Forest.

Detects unusual patterns in trade_count, volatility, volume, and price change
per symbol. Uses a rolling window of recent metrics to train and adapt.
"""

from __future__ import annotations

import logging
from collections import deque

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

# Features used for anomaly detection (must exist in metrics)
FEATURE_COLS = ["trade_count", "volatility_usd", "total_volume_qty", "price_change_pct"]

# Log-transform volume to reduce skew; add small epsilon to avoid log(0)
VOLUME_EPS = 1e-8


def _build_feature_matrix(metrics: list[dict]) -> tuple[pd.DataFrame, np.ndarray]:
    """Build feature matrix from metrics. Returns (df with product_id, X array)."""
    if not metrics:
        return pd.DataFrame(), np.array([]).reshape(0, len(FEATURE_COLS))

    df = pd.DataFrame(metrics)

    # Build features: log(volume+eps) to handle skew
    X = np.column_stack(
        [
            df["trade_count"].fillna(0).values,
            df["volatility_usd"].fillna(0).values,
            np.log1p(df["total_volume_qty"].fillna(0).values + VOLUME_EPS),
            df["price_change_pct"].fillna(0).values,
        ]
    )
    return df.assign(_row_idx=range(len(df))), X


class AnomalyDetector:
    """
    Isolation Forest-based anomaly detector for per-symbol crypto metrics.
    Trains on a rolling window of recent metrics and flags anomalous symbols.
    """

    def __init__(
        self,
        history_size: int = 100,
        contamination: float = 0.05,
        min_samples_to_fit: int = 30,
    ):
        self.history_size = history_size
        self.contamination = contamination
        self.min_samples_to_fit = min_samples_to_fit
        self._history: deque = deque(maxlen=history_size)
        self._model: IsolationForest | None = None
        self._scaler: StandardScaler | None = None

    def _add_to_history(self, metrics: list[dict]) -> None:
        """Append current metrics to rolling history."""
        for row in metrics:
            if all(k in row for k in FEATURE_COLS):
                self._history.append(row.copy())

    def _fit_if_ready(self) -> bool:
        """Fit model when enough history. Returns True if model is ready."""
        if len(self._history) < self.min_samples_to_fit:
            return False

        _, X = _build_feature_matrix(list(self._history))
        if X.shape[0] < self.min_samples_to_fit:
            return False

        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)
        self._model = IsolationForest(
            contamination=self.contamination,
            random_state=42,
            n_estimators=100,
        )
        self._model.fit(X_scaled)
        logger.debug("Anomaly model fitted on %d samples", X.shape[0])
        return True

    def detect(self, metrics: list[dict]) -> list[dict]:
        """
        Detect anomalies in current metrics. Returns list of anomaly dicts:
        {product_id, anomaly_score, trade_count, volatility_usd, ...}
        """
        if not metrics:
            return []

        self._add_to_history(metrics)

        if not self._fit_if_ready() or self._model is None or self._scaler is None:
            return []

        df, X = _build_feature_matrix(metrics)
        if X.shape[0] == 0:
            return []

        X_scaled = self._scaler.transform(X)
        preds = self._model.predict(X_scaled)  # -1 = anomaly, 1 = normal
        scores = self._model.decision_function(X_scaled)  # lower = more anomalous

        anomalies = []
        for i, (pid, pred, score) in enumerate(zip(df["product_id"], preds, scores)):
            if pred == -1:
                row = metrics[i]
                anomalies.append(
                    {
                        "product_id": pid,
                        "anomaly_score": round(float(score), 4),
                        "trade_count": row.get("trade_count", 0),
                        "volatility_usd": row.get("volatility_usd", 0),
                        "total_volume_qty": row.get("total_volume_qty", 0),
                        "price_change_pct": row.get("price_change_pct", 0),
                    }
                )

        return anomalies
