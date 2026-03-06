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
    )
