"""
Create Athena database and external tables over S3 Parquet data.

Uses partition projection so Athena automatically discovers new partitions
without running MSCK REPAIR TABLE.

Usage:
    python setup_athena.py
"""

import os
import sys
import time
from pathlib import Path

import boto3

sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))
from utils.config import get_config

config = get_config()

if not config.s3_bucket:
    print("Error: S3_BUCKET must be set in .env")
    sys.exit(1)

athena = boto3.client("athena", region_name=config.aws_region)

DATABASE = config.athena_database
RESULTS_BUCKET = os.environ.get("ATHENA_RESULTS_BUCKET", config.s3_bucket)
OUTPUT = f"s3://{RESULTS_BUCKET}/athena-results/"
PREFIX = config.s3_staging_prefix.rstrip("/")


def run_query(sql: str, description: str):
    """Execute an Athena query and wait for completion."""
    print(f"  Running: {description} ...")
    resp = athena.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": OUTPUT},
    )
    qid = resp["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        print(f"  FAILED: {reason}")
        sys.exit(1)

    print(f"  OK")
    return qid


def main():
    print("=" * 50)
    print("  Crypto Pipeline — Athena Setup")
    print("=" * 50)
    print(f"  Database: {DATABASE}")
    print(f"  S3 data:  s3://{config.s3_bucket}/{PREFIX}/")
    print()

    # ---- Create database ----
    run_query(
        f"CREATE DATABASE IF NOT EXISTS {DATABASE}",
        f"Create database '{DATABASE}'",
    )

    # ---- raw_trades table with partition projection ----
    run_query(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.raw_trades (
            trade_time    STRING,
            product_id    STRING,
            price         DOUBLE,
            size_qty      DOUBLE,
            notional_usd  DOUBLE,
            exchange      STRING,
            batch_id      STRING
        )
        PARTITIONED BY (
            `year`  STRING,
            `month` STRING,
            `day`   STRING,
            `hour`  STRING
        )
        STORED AS PARQUET
        LOCATION 's3://{config.s3_bucket}/{PREFIX}/raw_trades/'
        TBLPROPERTIES (
            'projection.enabled'          = 'true',
            'projection.year.type'        = 'integer',
            'projection.year.range'       = '2024,2030',
            'projection.year.digits'      = '4',
            'projection.month.type'       = 'integer',
            'projection.month.range'      = '1,12',
            'projection.month.digits'     = '2',
            'projection.day.type'         = 'integer',
            'projection.day.range'        = '1,31',
            'projection.day.digits'       = '2',
            'projection.hour.type'        = 'integer',
            'projection.hour.range'       = '0,23',
            'projection.hour.digits'      = '2',
            'storage.location.template'   =
                's3://{config.s3_bucket}/{PREFIX}/raw_trades/year=${{year}}/month=${{month}}/day=${{day}}/hour=${{hour}}'
        )
        """,
        "Create table 'raw_trades' (partition projection)",
    )

    # ---- candles_1m table with partition projection ----
    run_query(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.candles_1m (
            window_start   STRING,
            window_end     STRING,
            product_id     STRING,
            exchange       STRING,
            open_price     DOUBLE,
            high_price     DOUBLE,
            low_price      DOUBLE,
            close_price    DOUBLE,
            volume         DOUBLE,
            trade_count    INT,
            vwap           DOUBLE,
            batch_id       STRING
        )
        PARTITIONED BY (
            `year`  STRING,
            `month` STRING,
            `day`   STRING,
            `hour`  STRING
        )
        STORED AS PARQUET
        LOCATION 's3://{config.s3_bucket}/{PREFIX}/candles_1m/'
        TBLPROPERTIES (
            'projection.enabled'          = 'true',
            'projection.year.type'        = 'integer',
            'projection.year.range'       = '2024,2030',
            'projection.year.digits'      = '4',
            'projection.month.type'       = 'integer',
            'projection.month.range'      = '1,12',
            'projection.month.digits'     = '2',
            'projection.day.type'         = 'integer',
            'projection.day.range'        = '1,31',
            'projection.day.digits'       = '2',
            'projection.hour.type'        = 'integer',
            'projection.hour.range'       = '0,23',
            'projection.hour.digits'      = '2',
            'storage.location.template'   =
                's3://{config.s3_bucket}/{PREFIX}/candles_1m/year=${{year}}/month=${{month}}/day=${{day}}/hour=${{hour}}'
        )
        """,
        "Create table 'candles_1m' (partition projection)",
    )

    print()
    print("Setup complete. Tables are ready to query:")
    print(f"  SELECT * FROM {DATABASE}.raw_trades LIMIT 10;")
    print(f"  SELECT * FROM {DATABASE}.candles_1m  LIMIT 10;")


if __name__ == "__main__":
    main()
