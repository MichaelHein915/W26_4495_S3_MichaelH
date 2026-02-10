import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class AppConfig:
    kafka_server: str
    topic_raw: str
    coinbase_ws: str
    symbols: list[str]
    log_level: str
    kafka_client_id: str


def _parse_symbols(raw: str) -> list[str]:
    return [symbol.strip() for symbol in raw.split(",") if symbol.strip()]


def get_config() -> AppConfig:
    default_symbols = (
        "BTC-USD,ETH-USD,SOL-USD,XRP-USD,ADA-USD,"
        "DOGE-USD,AVAX-USD,LINK-USD,LTC-USD,BCH-USD"
    )
    return AppConfig(
        kafka_server=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic_raw=os.getenv("KAFKA_TOPIC_RAW", "crypto.trades.raw"),
        coinbase_ws=os.getenv(
            "COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com"
        ),
        symbols=_parse_symbols(os.getenv("CRYPTO_SYMBOLS", default_symbols)),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "crypto-producer"),
    )
