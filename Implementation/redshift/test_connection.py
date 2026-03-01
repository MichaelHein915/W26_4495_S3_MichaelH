"""
Verify Redshift + S3 connectivity and schema readiness.

Usage:
    python test_connection.py
"""

import sys
from pathlib import Path

import boto3
import redshift_connector

sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))
from utils.config import get_config


def check_redshift(cfg) -> bool:
    print(f"[Redshift] Connecting to {cfg.redshift_host}:{cfg.redshift_port}/{cfg.redshift_db} ...")
    try:
        conn = redshift_connector.connect(
            host=cfg.redshift_host,
            port=cfg.redshift_port,
            database=cfg.redshift_db,
            user=cfg.redshift_user,
            password=cfg.redshift_password,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT CURRENT_USER, CURRENT_DATABASE(), VERSION();")
            user, db, version = cur.fetchone()
            print(f"[Redshift] Connected as {user} to {db}")
            print(f"[Redshift] {version[:80]}...")

            cur.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'crypto' ORDER BY tablename;"
            )
            tables = [row[0] for row in cur.fetchall()]
            if tables:
                print(f"[Redshift] Tables in 'crypto' schema: {', '.join(tables)}")
            else:
                print("[Redshift] Schema 'crypto' has no tables — run schema.sql first")

        conn.close()
        print("[Redshift] OK\n")
        return True
    except Exception as exc:
        print(f"[Redshift] FAILED — {exc}\n")
        return False


def check_s3(cfg) -> bool:
    print(f"[S3] Checking bucket '{cfg.s3_bucket}' in {cfg.aws_region} ...")
    try:
        s3 = boto3.client("s3", region_name=cfg.aws_region)
        s3.head_bucket(Bucket=cfg.s3_bucket)
        print(f"[S3] Bucket exists and is accessible")

        resp = s3.list_objects_v2(
            Bucket=cfg.s3_bucket,
            Prefix=cfg.s3_staging_prefix,
            MaxKeys=5,
        )
        count = resp.get("KeyCount", 0)
        print(f"[S3] Staging prefix '{cfg.s3_staging_prefix}' — {count} existing objects")
        print("[S3] OK\n")
        return True
    except Exception as exc:
        print(f"[S3] FAILED — {exc}\n")
        return False


def main():
    cfg = get_config()

    errors = []
    if not cfg.redshift_host:
        errors.append("REDSHIFT_HOST")
    if not cfg.redshift_password:
        errors.append("REDSHIFT_PASSWORD")
    if not cfg.s3_bucket:
        errors.append("S3_BUCKET")
    if not cfg.redshift_iam_role:
        errors.append("REDSHIFT_IAM_ROLE")

    if errors:
        print(f"Missing required env vars: {', '.join(errors)}")
        print("Copy config/env.example to .env and fill in the values.")
        sys.exit(1)

    print("=" * 50)
    print("  Crypto Pipeline — Connection Test")
    print("=" * 50 + "\n")

    rs_ok = check_redshift(cfg)
    s3_ok = check_s3(cfg)

    if rs_ok and s3_ok:
        print("All checks passed. Ready to run the Redshift sink.")
    else:
        failed = []
        if not rs_ok:
            failed.append("Redshift")
        if not s3_ok:
            failed.append("S3")
        print(f"Failed: {', '.join(failed)}. Check the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
