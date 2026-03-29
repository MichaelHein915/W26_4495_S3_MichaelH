from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


def _split_csv(raw_value: str) -> list[str]:
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _parse_bool(raw_value: str | None, default: bool) -> bool:
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class KafkaTopics:
    raw: str
    candles: str
    metrics: str
    alerts: str


@dataclass(frozen=True)
class AppSettings:
    app_env: str
    log_level: str
    coinbase_ws_url: str
    crypto_symbols: list[str]
    kafka_bootstrap_servers: str
    kafka_client_id: str
    kafka_topics: KafkaTopics
    kafka_fail_on_data_loss: bool
    checkpoint_dir: Path
    insight_model_backend: str
    insight_model_name: str
    chat_model_backend: str
    chat_model_name: str
    chat_model_source: str
    chat_max_tokens: int
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_db: str = "crypto"
    clickhouse_user: str = "crypto_writer"
    clickhouse_password: str = ""

    @classmethod
    def from_env(cls) -> "AppSettings":
        project_root = Path(__file__).resolve().parents[2]
        default_symbols = (
            "BTC-USD,ETH-USD,SOL-USD,XRP-USD,ADA-USD,"
            "DOGE-USD,AVAX-USD,LINK-USD,LTC-USD,BCH-USD"
        )
        checkpoint_dir = project_root / os.getenv(
            "CHECKPOINT_DIR", "data/checkpoints/streaming"
        )

        return cls(
            app_env=os.getenv("APP_ENV", "cloud"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            coinbase_ws_url=os.getenv(
                "COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com"
            ),
            crypto_symbols=_split_csv(os.getenv("CRYPTO_SYMBOLS", default_symbols)),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "crypto-producer-cloud"),
            kafka_topics=KafkaTopics(
                raw=os.getenv("KAFKA_TOPIC_RAW", "crypto.trades.raw.v1"),
                candles=os.getenv("KAFKA_TOPIC_CANDLES", "crypto.candles.1m.v1"),
                metrics=os.getenv("KAFKA_TOPIC_METRICS", "crypto.metrics.live.v1"),
                alerts=os.getenv("KAFKA_TOPIC_ALERTS", "crypto.alerts.v1"),
            ),
            kafka_fail_on_data_loss=_parse_bool(
                os.getenv("KAFKA_FAIL_ON_DATA_LOSS"),
                default=False,
            ),
            checkpoint_dir=checkpoint_dir,
            insight_model_backend=os.getenv("INSIGHT_MODEL_BACKEND", "local_stub"),
            insight_model_name=os.getenv("INSIGHT_MODEL_NAME", "Qwen3.5-0.8B"),
            chat_model_backend=os.getenv("CHAT_MODEL_BACKEND", "mlx_qwen"),
            chat_model_name=os.getenv("CHAT_MODEL_NAME", "Qwen3.5-0.8B"),
            chat_model_source=os.getenv("CHAT_MODEL_SOURCE", "").strip(),
            chat_max_tokens=int(os.getenv("CHAT_MAX_TOKENS", "160")),
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            clickhouse_db=os.getenv("CLICKHOUSE_DB", "crypto"),
            clickhouse_user=os.getenv("CLICKHOUSE_USER", "crypto_writer"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )
