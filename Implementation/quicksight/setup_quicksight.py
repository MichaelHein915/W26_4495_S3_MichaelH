"""
Set up Amazon QuickSight dashboards over the crypto pipeline Athena tables.

Creates:
  1. An Athena data source in QuickSight
  2. Two SPICE datasets (raw_trades and candles_1m)
  3. An analysis with pre-built visuals (price trends, volume, OHLCV candles)

Prerequisites:
  - QuickSight Enterprise or Standard edition enabled in your AWS account
  - Athena tables already created (run setup_athena.py first)
  - S3 sink running so data exists in the tables
  - QUICKSIGHT_USER set in .env (your QuickSight username)

Usage:
    python setup_quicksight.py
"""

import json
import os
import sys
import time
import uuid
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))
from utils.config import get_config

config = get_config()

REQUIRED = {
    "S3_BUCKET": config.s3_bucket,
    "QUICKSIGHT_USER": config.quicksight_user,
}
missing = [k for k, v in REQUIRED.items() if not v]
if missing:
    print(f"Error: set these in .env: {', '.join(missing)}")
    sys.exit(1)

sts = boto3.client("sts", region_name=config.aws_region)
ACCOUNT_ID = sts.get_caller_identity()["Account"]

QS_IDENTITY_REGION = os.environ.get("QUICKSIGHT_IDENTITY_REGION", "us-east-1")
qs = boto3.client("quicksight", region_name=QS_IDENTITY_REGION)

DATASOURCE_ID = "crypto-pipeline-athena"
DATASET_RAW_ID = "crypto-raw-trades"
DATASET_CANDLES_ID = "crypto-candles-1m"
ANALYSIS_ID = "crypto-pipeline-analysis"

QS_USER_ARN = (
    f"arn:aws:quicksight:{QS_IDENTITY_REGION}:{ACCOUNT_ID}"
    f":user/default/{config.quicksight_user}"
)

PRINCIPAL_ARN = QS_USER_ARN


def _grant_permissions(resource_type: str, resource_id: str):
    """Grant the QuickSight user full owner permissions on a resource."""
    actions_map = {
        "datasource": [
            "quicksight:DescribeDataSource",
            "quicksight:DescribeDataSourcePermissions",
            "quicksight:PassDataSource",
            "quicksight:UpdateDataSource",
            "quicksight:DeleteDataSource",
            "quicksight:UpdateDataSourcePermissions",
        ],
        "dataset": [
            "quicksight:DescribeDataSet",
            "quicksight:DescribeDataSetPermissions",
            "quicksight:PassDataSet",
            "quicksight:DescribeIngestion",
            "quicksight:ListIngestions",
            "quicksight:UpdateDataSet",
            "quicksight:DeleteDataSet",
            "quicksight:CreateIngestion",
            "quicksight:CancelIngestion",
            "quicksight:UpdateDataSetPermissions",
        ],
        "analysis": [
            "quicksight:RestoreAnalysis",
            "quicksight:UpdateAnalysisPermissions",
            "quicksight:DeleteAnalysis",
            "quicksight:DescribeAnalysisPermissions",
            "quicksight:QueryAnalysis",
            "quicksight:DescribeAnalysis",
            "quicksight:UpdateAnalysis",
        ],
    }

    call = {
        "datasource": qs.update_data_source_permissions,
        "dataset": qs.update_data_set_permissions,
        "analysis": qs.update_analysis_permissions,
    }[resource_type]

    for attempt in range(6):
        try:
            call(
                AwsAccountId=ACCOUNT_ID,
                **{_resource_id_key(resource_type): resource_id},
                GrantPermissions=[
                    {"Principal": PRINCIPAL_ARN, "Actions": actions_map[resource_type]}
                ],
            )
            return
        except ClientError as e:
            if "CREATION_IN_PROGRESS" in str(e) and attempt < 5:
                time.sleep(3)
                continue
            raise


def _resource_id_key(resource_type: str) -> str:
    return {
        "datasource": "DataSourceId",
        "dataset": "DataSetId",
        "analysis": "AnalysisId",
    }[resource_type]


def create_data_source():
    """Create an Athena data source in QuickSight."""
    print("  Creating Athena data source ...")
    try:
        qs.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=DATASOURCE_ID,
            Name="Crypto Pipeline (Athena)",
            Type="ATHENA",
            DataSourceParameters={
                "AthenaParameters": {"WorkGroup": "primary"}
            },
            SslProperties={"DisableSsl": False},
        )
        _grant_permissions("datasource", DATASOURCE_ID)
        print("  OK — data source created")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceExistsException":
            print("  OK — data source already exists")
        else:
            raise


def _build_dataset(dataset_id: str, name: str, table: str, columns: dict):
    """Create a QuickSight dataset backed by an Athena table."""
    print(f"  Creating dataset '{name}' ...")

    physical_table_id = str(uuid.uuid4())

    physical_table = {
        physical_table_id: {
            "RelationalTable": {
                "DataSourceArn": (
                    f"arn:aws:quicksight:{QS_IDENTITY_REGION}:{ACCOUNT_ID}"
                    f":datasource/{DATASOURCE_ID}"
                ),
                "Catalog": "AwsDataCatalog",
                "Schema": config.athena_database,
                "Name": table,
                "InputColumns": [
                    {"Name": col, "Type": dtype}
                    for col, dtype in columns.items()
                ],
            }
        }
    }

    try:
        qs.create_data_set(
            AwsAccountId=ACCOUNT_ID,
            DataSetId=dataset_id,
            Name=name,
            PhysicalTableMap=physical_table,
            ImportMode="SPICE",
        )
        _grant_permissions("dataset", dataset_id)
        print(f"  OK — dataset '{name}' created")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceExistsException":
            print(f"  OK — dataset '{name}' already exists")
        else:
            raise


def create_datasets():
    """Create SPICE datasets for raw_trades and candles_1m."""
    _build_dataset(
        DATASET_RAW_ID,
        "Crypto Raw Trades",
        "raw_trades",
        {
            "trade_time": "STRING",
            "product_id": "STRING",
            "price": "DECIMAL",
            "size_qty": "DECIMAL",
            "notional_usd": "DECIMAL",
            "batch_id": "STRING",
            "year": "STRING",
            "month": "STRING",
            "day": "STRING",
            "hour": "STRING",
        },
    )

    _build_dataset(
        DATASET_CANDLES_ID,
        "Crypto Candles 1m",
        "candles_1m",
        {
            "window_start": "STRING",
            "window_end": "STRING",
            "product_id": "STRING",
            "open_price": "DECIMAL",
            "high_price": "DECIMAL",
            "low_price": "DECIMAL",
            "close_price": "DECIMAL",
            "volume": "DECIMAL",
            "trade_count": "INTEGER",
            "vwap": "DECIMAL",
            "batch_id": "STRING",
            "year": "STRING",
            "month": "STRING",
            "day": "STRING",
            "hour": "STRING",
        },
    )


def create_analysis():
    """Create a QuickSight analysis with starter visuals."""
    print("  Creating analysis ...")

    raw_ds_arn = (
        f"arn:aws:quicksight:{QS_IDENTITY_REGION}:{ACCOUNT_ID}"
        f":dataset/{DATASET_RAW_ID}"
    )
    candles_ds_arn = (
        f"arn:aws:quicksight:{QS_IDENTITY_REGION}:{ACCOUNT_ID}"
        f":dataset/{DATASET_CANDLES_ID}"
    )

    definition = {
        "DataSetIdentifierDeclarations": [
            {
                "Identifier": "raw_trades",
                "DataSetArn": raw_ds_arn,
            },
            {
                "Identifier": "candles_1m",
                "DataSetArn": candles_ds_arn,
            },
        ],
        "Sheets": [
            {
                "SheetId": "overview",
                "Name": "Crypto Overview",
                "Visuals": [
                    {
                        "KPIVisual": {
                            "VisualId": "kpi-total-trades",
                            "Title": {"Visibility": "VISIBLE", "FormatText": {"PlainText": "Total Trades"}},
                            "ChartConfiguration": {
                                "FieldWells": {
                                    "Values": [{"NumericalMeasureField": {"FieldId": "trade-count", "Column": {"DataSetIdentifier": "raw_trades", "ColumnName": "price"}, "AggregationFunction": {"SimpleNumericalAggregation": "COUNT"}}}],
                                },
                            },
                        }
                    },
                    {
                        "BarChartVisual": {
                            "VisualId": "bar-volume-by-symbol",
                            "Title": {"Visibility": "VISIBLE", "FormatText": {"PlainText": "Volume by Symbol"}},
                            "ChartConfiguration": {
                                "FieldWells": {
                                    "BarChartAggregatedFieldWells": {
                                        "Category": [{"CategoricalDimensionField": {"FieldId": "symbol", "Column": {"DataSetIdentifier": "candles_1m", "ColumnName": "product_id"}}}],
                                        "Values": [{"NumericalMeasureField": {"FieldId": "vol", "Column": {"DataSetIdentifier": "candles_1m", "ColumnName": "volume"}, "AggregationFunction": {"SimpleNumericalAggregation": "SUM"}}}],
                                    }
                                },
                            },
                        }
                    },
                    {
                        "LineChartVisual": {
                            "VisualId": "line-vwap-trend",
                            "Title": {"Visibility": "VISIBLE", "FormatText": {"PlainText": "VWAP Trend by Symbol"}},
                            "ChartConfiguration": {
                                "FieldWells": {
                                    "LineChartAggregatedFieldWells": {
                                        "Category": [{"CategoricalDimensionField": {"FieldId": "time", "Column": {"DataSetIdentifier": "candles_1m", "ColumnName": "window_start"}}}],
                                        "Values": [{"NumericalMeasureField": {"FieldId": "vwap-val", "Column": {"DataSetIdentifier": "candles_1m", "ColumnName": "vwap"}, "AggregationFunction": {"SimpleNumericalAggregation": "AVERAGE"}}}],
                                        "Colors": [{"CategoricalDimensionField": {"FieldId": "color-sym", "Column": {"DataSetIdentifier": "candles_1m", "ColumnName": "product_id"}}}],
                                    }
                                },
                            },
                        }
                    },
                ],
            },
        ],
    }

    try:
        qs.create_analysis(
            AwsAccountId=ACCOUNT_ID,
            AnalysisId=ANALYSIS_ID,
            Name="Crypto Pipeline Dashboard",
            Definition=definition,
        )
        _grant_permissions("analysis", ANALYSIS_ID)
        print("  OK — analysis created")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceExistsException":
            print("  OK — analysis already exists")
        else:
            raise


def trigger_spice_refresh():
    """Kick off a SPICE ingestion for both datasets."""
    print("  Triggering SPICE refresh ...")
    for ds_id, name in [(DATASET_RAW_ID, "raw_trades"), (DATASET_CANDLES_ID, "candles_1m")]:
        ingestion_id = str(uuid.uuid4())
        try:
            qs.create_ingestion(
                AwsAccountId=ACCOUNT_ID,
                DataSetId=ds_id,
                IngestionId=ingestion_id,
            )
            print(f"    {name}: SPICE ingestion started (id: {ingestion_id})")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "ResourceExistsException":
                print(f"    {name}: ingestion already running")
            else:
                print(f"    {name}: skipped — {e.response['Error']['Message']}")


def print_urls():
    """Print the QuickSight console URL for the analysis."""
    base = f"https://{QS_IDENTITY_REGION}.quicksight.aws.amazon.com"
    print()
    print("  Open your dashboard in QuickSight:")
    print(f"    {base}/sn/analyses/{ANALYSIS_ID}")
    print()
    print("  To publish as a shared dashboard, open the analysis and click")
    print("  Share → Publish dashboard in the QuickSight console.")


def main():
    print("=" * 55)
    print("  Crypto Pipeline — QuickSight Setup")
    print("=" * 55)
    print(f"  Account:   {ACCOUNT_ID}")
    print(f"  Region:    {config.aws_region}")
    print(f"  QS User:   {config.quicksight_user}")
    print(f"  Athena DB: {config.athena_database}")
    print()

    create_data_source()
    create_datasets()
    create_analysis()
    trigger_spice_refresh()
    print_urls()

    print("Setup complete.")


if __name__ == "__main__":
    main()
