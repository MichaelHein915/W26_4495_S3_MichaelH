"""
Apply the Redshift schema DDL via redshift-connector.

Replaces the need for psql. Reads schema.sql from the same directory
and executes each statement against the configured Redshift cluster.

Usage:
    python apply_schema.py
"""

import sys
from pathlib import Path

import redshift_connector

from utils.config import get_config

SCHEMA_FILE = Path(__file__).resolve().parent / "schema.sql"


def main():
    cfg = get_config()

    if not cfg.redshift_host or not cfg.redshift_password:
        print("Error: REDSHIFT_HOST and REDSHIFT_PASSWORD must be set in .env")
        sys.exit(1)

    print(f"Connecting to {cfg.redshift_host}:{cfg.redshift_port}/{cfg.redshift_db} ...")
    conn = redshift_connector.connect(
        host=cfg.redshift_host,
        port=cfg.redshift_port,
        database=cfg.redshift_db,
        user=cfg.redshift_user,
        password=cfg.redshift_password,
    )

    ddl = SCHEMA_FILE.read_text()
    # Split on semicolons, skip empty/comment-only fragments
    statements = [s.strip() for s in ddl.split(";") if s.strip() and not s.strip().startswith("--")]

    with conn.cursor() as cur:
        for stmt in statements:
            first_line = stmt.lstrip().split("\n")[0][:70]
            print(f"  Running: {first_line} ...")
            cur.execute(stmt)
    conn.commit()
    conn.close()

    print(f"\nSchema applied successfully ({len(statements)} statements).")


if __name__ == "__main__":
    main()
