import os
import sys
from pathlib import Path
from shutil import rmtree

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType

sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.config import get_config


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
    if reset_checkpoint:
        checkpoint_path = Path(checkpoint_dir)
        if checkpoint_path.exists():
            # Reset corrupted or stale checkpoints for local dev runs.
            rmtree(checkpoint_path)

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

    query = (
        parsed_stream.writeStream.format("console")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
