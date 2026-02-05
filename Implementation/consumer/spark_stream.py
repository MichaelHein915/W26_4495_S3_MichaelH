import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

repo_root = Path(__file__).resolve().parents[2]
sys.path.append(str(repo_root / "src"))
from utils.config import get_config


config = get_config()
checkpoint_dir = os.getenv(
    "CHECKPOINT_DIR",
    str(Path(__file__).resolve().parents[2] / "data" / "checkpoints" / "raw_console"),
)
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
    spark = _build_spark_session()
    spark.sparkContext.setLogLevel(spark_log_level.upper())

    schema = StructType(
        [
            StructField("type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("price", StringType(), True),
            StructField("time", StringType(), True),
        ]
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_server)
        .option("subscribe", config.topic_raw)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .where(col("type") == "ticker")
        .where(col("price").isNotNull())
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
