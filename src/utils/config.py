import os
from dataclasses import dataclass, field
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

    # Multi-exchange
    exchanges: list[str] = field(default_factory=lambda: ["coinbase", "binance", "kraken"])
    binance_ws: str = "wss://stream.binance.com:9443"
    kraken_ws: str = "wss://ws.kraken.com/v2"

    # Redshift
    redshift_host: str = ""
    redshift_port: int = 5439
    redshift_db: str = "crypto"
    redshift_user: str = "admin"
    redshift_password: str = ""
    redshift_iam_role: str = ""

    # S3 staging
    s3_bucket: str = ""
    s3_staging_prefix: str = "crypto-data/"
    aws_region: str = "us-west-2"

    # Athena
    athena_database: str = "crypto_pipeline"

    # QuickSight
    quicksight_user: str = ""

    # Sink tuning
    sink_flush_interval_sec: int = 60
    sink_flush_max_records: int = 5000

    # Alerts
    slack_webhook_url: str = ""
    alert_email_to: str = ""
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_use_tls: bool = True
    alert_arbitrage_threshold_pct: float = 0.3
    alert_volume_spike_ratio: float = 2.0
    alert_price_thresholds: list[tuple[str, str, float]] = field(default_factory=list)
    alert_anomaly_enabled: bool = True
    anomaly_contamination: float = 0.05

    # News pipeline
    topic_news: str = "crypto.news.sentiment"
    news_enabled: bool = False
    news_poll_interval_sec: int = 60
    cryptopanic_api_key: str = ""
    news_sources: list[str] = field(default_factory=lambda: ["rss"])

    # AI Assistant (MLX)
    ai_enabled: bool = False
    mlx_server_url: str = "http://localhost:8080"
    mlx_model: str = "mlx-community/Llama-3.2-3B-Instruct-4bit"
    ai_insight_interval_sec: int = 60
    ai_request_timeout_sec: int = 120


def _parse_symbols(raw: str) -> list[str]:
    return [symbol.strip() for symbol in raw.split(",") if symbol.strip()]


def _parse_price_thresholds(raw: str) -> list[tuple[str, str, float]]:
    """Parse ALERT_PRICE_THRESHOLDS: 'BTC-USD:above:100000,ETH-USD:below:3000'."""
    result = []
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            symbol, direction, price_str = part.split(":")
            symbol = symbol.strip().upper()
            direction = direction.strip().lower()
            price = float(price_str.strip())
            if direction in ("above", "below") and price > 0:
                result.append((symbol, direction, price))
        except (ValueError, TypeError):
            continue
    return result


def get_config() -> AppConfig:
    default_symbols = "BTC-USD,ETH-USD,SOL-USD,XRP-USD,ADA-USD,DOGE-USD,AVAX-USD,LINK-USD,LTC-USD,BCH-USD"
    return AppConfig(
        kafka_server=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic_raw=os.getenv("KAFKA_TOPIC_RAW", "crypto.trades.raw"),
        coinbase_ws=os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com"),
        symbols=_parse_symbols(os.getenv("CRYPTO_SYMBOLS", default_symbols)),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "crypto-producer"),
        exchanges=_parse_symbols(os.getenv("EXCHANGES", "coinbase,binance,kraken")),
        binance_ws=os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443"),
        kraken_ws=os.getenv("KRAKEN_WS_URL", "wss://ws.kraken.com/v2"),
        redshift_host=os.getenv("REDSHIFT_HOST", ""),
        redshift_port=int(os.getenv("REDSHIFT_PORT", "5439")),
        redshift_db=os.getenv("REDSHIFT_DB", "crypto"),
        redshift_user=os.getenv("REDSHIFT_USER", "admin"),
        redshift_password=os.getenv("REDSHIFT_PASSWORD", ""),
        redshift_iam_role=os.getenv("REDSHIFT_IAM_ROLE", ""),
        s3_bucket=os.getenv("S3_BUCKET", ""),
        s3_staging_prefix=os.getenv("S3_STAGING_PREFIX", "crypto-data/"),
        aws_region=os.getenv("AWS_REGION", "us-west-2"),
        athena_database=os.getenv("ATHENA_DATABASE", "crypto_pipeline"),
        quicksight_user=os.getenv("QUICKSIGHT_USER", ""),
        sink_flush_interval_sec=int(os.getenv("SINK_FLUSH_INTERVAL_SEC", "60")),
        sink_flush_max_records=int(os.getenv("SINK_FLUSH_MAX_RECORDS", "5000")),
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
        alert_email_to=os.getenv("ALERT_EMAIL_TO", ""),
        smtp_host=os.getenv("SMTP_HOST", ""),
        smtp_port=int(os.getenv("SMTP_PORT", "587")),
        smtp_user=os.getenv("SMTP_USER", ""),
        smtp_password=os.getenv("SMTP_PASSWORD", ""),
        smtp_use_tls=os.getenv("SMTP_USE_TLS", "true").lower() in ("1", "true", "yes"),
        alert_arbitrage_threshold_pct=float(os.getenv("ALERT_ARBITRAGE_THRESHOLD_PCT", "0.3")),
        alert_volume_spike_ratio=float(os.getenv("ALERT_VOLUME_SPIKE_RATIO", "2.0")),
        alert_price_thresholds=_parse_price_thresholds(os.getenv("ALERT_PRICE_THRESHOLDS", "")),
        alert_anomaly_enabled=os.getenv("ALERT_ANOMALY_ENABLED", "true").lower() in ("1", "true", "yes"),
        anomaly_contamination=float(os.getenv("ANOMALY_CONTAMINATION", "0.05")),
        topic_news=os.getenv("KAFKA_TOPIC_NEWS", "crypto.news.sentiment"),
        news_enabled=os.getenv("NEWS_ENABLED", "false").lower() in ("1", "true", "yes"),
        news_poll_interval_sec=int(os.getenv("NEWS_POLL_INTERVAL_SEC", "60")),
        cryptopanic_api_key=os.getenv("CRYPTOPANIC_API_KEY", ""),
        news_sources=_parse_symbols(os.getenv("NEWS_SOURCES", "rss")),
        ai_enabled=os.getenv("AI_ENABLED", "false").lower() in ("1", "true", "yes"),
        mlx_server_url=os.getenv("MLX_SERVER_URL", "http://localhost:8080"),
        mlx_model=os.getenv("MLX_MODEL", "mlx-community/Llama-3.2-3B-Instruct-4bit"),
        ai_insight_interval_sec=int(os.getenv("AI_INSIGHT_INTERVAL_SEC", "60")),
        ai_request_timeout_sec=int(os.getenv("AI_REQUEST_TIMEOUT_SEC", "120")),
    )
