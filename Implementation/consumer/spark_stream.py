import os
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json, lit, to_timestamp, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from utils.config import get_config
from utils.metrics import CONSUMER_RUNNING, start_metrics_server


config = get_config()
checkpoint_dir = os.getenv(
    "CHECKPOINT_DIR",
    str(Path(__file__).resolve().parents[2] / "data" / "checkpoints" / "raw_console"),
)
reset_checkpoint = os.getenv("RESET_CHECKPOINT", "").lower() in {"1", "true", "yes"}
spark_log_level = os.getenv("SPARK_LOG_LEVEL", "WARN")


def _build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("crypto-stream-consumer")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .getOrCreate()
    )


def main() -> None:
    start_metrics_server(default=9091)
    CONSUMER_RUNNING.set(1)
    if reset_checkpoint:
        checkpoint_path = Path(checkpoint_dir)
        if checkpoint_path.exists():
            # Reset corrupted or stale checkpoints for local dev runs.
            rmtree(checkpoint_path)

    spark = _build_spark_session()
    spark.sparkContext.setLogLevel(spark_log_level.upper())

    # Schema supports both raw Coinbase (type, product_id, price, time) and
    # normalised multi-exchange (exchange, product_id, price, size, time)
    schema = StructType(
        [
            StructField("type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("price", StringType(), True),
            StructField("time", StringType(), True),
            StructField("exchange", StringType(), True),
            StructField("size", StringType(), True),
        ]
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_server)
        .option("subscribe", config.topic_raw)
        .option("startingOffsets", "latest")
        .load()
    )

    # Accept raw Coinbase (type=ticker) OR normalised multi-exchange (exchange present)
    parsed_stream = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .where((col("type") == "ticker") | col("exchange").isNotNull())
        .where(col("price").isNotNull())
        .where(col("product_id").isNotNull())
        .withColumn("exchange", coalesce(col("exchange"), lit("coinbase")))
    )

    metrics_stream = (
        parsed_stream.withColumn("event_time", to_timestamp(col("time")))
        .withColumn("price_usd", col("price").cast(DoubleType()))
        .where(col("event_time").isNotNull())
        .where(col("price_usd").isNotNull())
        .groupBy(window(col("event_time"), "1 minute"), col("product_id"))
        .agg({"price_usd": "avg", "*": "count"})
        .withColumnRenamed("avg(price_usd)", "avg_price_usd")
        .withColumnRenamed("count(1)", "trade_count")
        .select(
            "product_id",
            "trade_count",
            "avg_price_usd",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
        )
    )

    query = (
        metrics_stream.writeStream.format("console")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("update")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
