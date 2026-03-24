"""
Run Athena queries against the crypto pipeline data.

Usage:
    python run_query.py                          # runs all example queries
    python run_query.py "SELECT * FROM ..."      # runs a custom query
"""

import sys
import time

import boto3

from utils.config import get_config

config = get_config()
athena = boto3.client("athena", region_name=config.aws_region)

DATABASE = config.athena_database
OUTPUT = f"s3://{config.s3_bucket}/athena-results/"

EXAMPLE_QUERIES = {
    "Recent trades (last 10)": f"""
        SELECT trade_time, product_id, price, size_qty, notional_usd
        FROM {DATABASE}.raw_trades
        ORDER BY trade_time DESC
        LIMIT 10
    """,
    "Trade count by symbol (today)": f"""
        SELECT product_id,
               COUNT(*)          AS trades,
               ROUND(AVG(price), 2)   AS avg_price,
               ROUND(MIN(price), 2)   AS low,
               ROUND(MAX(price), 2)   AS high
        FROM {DATABASE}.raw_trades
        GROUP BY product_id
        ORDER BY trades DESC
    """,
    "Latest 1-minute candles": f"""
        SELECT window_start, product_id,
               open_price, high_price, low_price, close_price,
               volume, trade_count, ROUND(vwap, 2) AS vwap
        FROM {DATABASE}.candles_1m
        ORDER BY window_start DESC
        LIMIT 10
    """,
    "Hourly volume summary": f"""
        SELECT year, month, day, hour,
               product_id,
               COUNT(*)                      AS candles,
               ROUND(SUM(volume), 4)         AS total_volume,
               ROUND(AVG(close_price), 2)    AS avg_close
        FROM {DATABASE}.candles_1m
        GROUP BY year, month, day, hour, product_id
        ORDER BY year DESC, month DESC, day DESC, hour DESC, total_volume DESC
        LIMIT 20
    """,
}


def run_query(sql: str) -> list[list[str]]:
    """Execute a query and return rows as lists of strings."""
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
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
        return [[f"QUERY FAILED: {reason}"]]

    stats = status["QueryExecution"]["Statistics"]
    scanned_mb = stats.get("DataScannedInBytes", 0) / (1024 * 1024)

    result = athena.get_query_results(QueryExecutionId=qid)
    rows = result["ResultSet"]["Rows"]

    print(f"  (scanned {scanned_mb:.2f} MB, {len(rows) - 1} rows)")
    return [[col.get("VarCharValue", "") for col in row["Data"]] for row in rows]


def print_table(rows: list[list[str]]):
    """Pretty-print rows with aligned columns."""
    if not rows:
        print("  (no results)")
        return

    widths = [max(len(str(row[i])) for row in rows) for i in range(len(rows[0]))]
    header = rows[0]
    print("  " + "  ".join(h.ljust(w) for h, w in zip(header, widths)))
    print("  " + "  ".join("-" * w for w in widths))
    for row in rows[1:]:
        print("  " + "  ".join(str(v).ljust(w) for v, w in zip(row, widths)))


def main():
    if len(sys.argv) > 1:
        sql = " ".join(sys.argv[1:])
        print(f"\nCustom query:")
        rows = run_query(sql)
        print_table(rows)
        return

    print("=" * 60)
    print("  Crypto Pipeline — Athena Query Examples")
    print("=" * 60)

    for title, sql in EXAMPLE_QUERIES.items():
        print(f"\n--- {title} ---")
        rows = run_query(sql)
        print_table(rows)

    print()


if __name__ == "__main__":
    main()
