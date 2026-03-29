import logging

from crypto_analytics.logging import configure_logging
from crypto_analytics.producer import CoinbaseTradeProducer, build_kafka_producer
from crypto_analytics.settings import AppSettings


logger = logging.getLogger("crypto_analytics.producer")


def main() -> None:
    settings = AppSettings.from_env()
    configure_logging(settings.log_level)

    producer = build_kafka_producer(settings)
    service = CoinbaseTradeProducer(settings=settings, producer=producer)

    logger.info("Kafka bootstrap servers: %s", settings.kafka_bootstrap_servers)
    logger.info("Raw topic: %s", settings.kafka_topics.raw)
    logger.info("Tracked symbols: %s", ", ".join(settings.crypto_symbols))
    service.run_forever()


if __name__ == "__main__":
    main()
