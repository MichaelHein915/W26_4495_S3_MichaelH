# Progress Report 2

**Student:** Michael Hein (300375535)  
**Course:** CSIS 4495 – Applied Research Project  
**Project:** Real-Time Cryptocurrency Streaming Pipeline  
**Reporting Period:** Feb 24 – Mar 1, 2026

---

## Work Date/Hours Logs

| Date | Number of Hours | Description of Work Done |
|---|---|---|
| Feb 27, 2026 | 2 | Researched AWS Athena and S3 Parquet integration patterns. Designed Hive-style partitioning strategy (`year=/month=/day=/hour=`) for raw trades and candle data. Reviewed boto3 and pyarrow documentation. |
| Feb 27, 2026 | 1.5 | Implemented S3 Parquet sink (`Implementation/athena/s3_sink.py`) — consumes Kafka messages, buffers in memory, flushes raw trades and 1-minute OHLCV candles as Parquet files to S3. Code checked into repo. |
| Feb 27, 2026 | 1 | Updated centralized config (`src/utils/config.py`) and environment template (`config/env.example`) to include S3, Athena, and sink tuning variables. |
| Feb 28, 2026 | 1.5 | Implemented Athena setup script (`Implementation/athena/setup_athena.py`) — creates Athena database and external tables (`raw_trades`, `candles_1m`) with partition projection so new partitions are auto-discovered. Code checked into repo. |
| Feb 28, 2026 | 1.5 | Built Athena query runner (`Implementation/athena/run_query.py`) — executes example queries (recent trades, trade count by symbol, latest candles, hourly volume summary) and pretty-prints results. Code checked into repo. |
| Mar 1, 2026 | 1 | Removed duplicate files and fixed bugs — deleted older copies of producer, Spark consumer, and Streamlit app. Fixed import bug in `spark_stream.py`, S3 prefix mismatch in `setup_aws.sh`, and Athena hourly volume query grouping in `run_query.py`. |
| Mar 1, 2026 | 1 | Updated `README.md` — added Redshift and Athena to the architecture diagram, tech stack table, and project structure. Added "AWS Data Sinks" section with setup instructions and troubleshooting entries. |
| Mar 1, 2026 | 1 | Configured AWS credentials, provisioned S3 bucket and IAM role, created Athena database and tables, and tested full end-to-end pipeline (Coinbase → Kafka → S3 → Athena queries). Verified query results returning live trade data. |

**Total Hours This Period: ~10.5**

---

## Summary Description of Work Done

This reporting period focused on extending the pipeline with AWS data persistence and performing code quality cleanup. I implemented a complete S3 + Athena integration that allows historical analysis of streaming trade data. The S3 sink consumes from Kafka, buffers trades in memory, and periodically flushes both raw trades and pre-aggregated 1-minute OHLCV candles as Parquet files to S3 using Hive-style partitioning (`year=/month=/day=/hour=`). Athena external tables with partition projection were set up so new data is automatically queryable without manual partition repair.

I also cleaned up the codebase by removing three duplicate files (older copies of the producer, Spark consumer, and Streamlit dashboard), fixing an import path bug, resolving an S3 prefix configuration mismatch between the setup script and environment template, and correcting an Athena query that was aggregating across days instead of showing actual hourly periods.

The README was substantially updated to document the full project scope including the Redshift and Athena sinks, AWS environment variables, setup instructions, and additional troubleshooting entries. The end-to-end pipeline was tested successfully with live Coinbase data flowing through Kafka to S3 and queryable via Athena.

One issue encountered was a region mismatch — the S3 bucket was provisioned in `us-west-2` while the AWS CLI config defaulted to `ca-west-1`, causing Athena queries to fail. This was resolved by aligning the `.env` region setting to match the bucket's region. Redshift Serverless setup was attempted but requires a service subscription that has not yet been activated on the AWS account; the Athena path was used as the primary data warehouse solution.

---

## Repo Check In of Implementation Completed

The following files/folders have been checked into the repo since the last progress report:

**New files (S3 + Athena integration):**
- `Implementation/athena/s3_sink.py` — Kafka → S3 Parquet sink with Hive partitioning
- `Implementation/athena/setup_athena.py` — Athena database and table creation script
- `Implementation/athena/run_query.py` — Example Athena query runner

**Modified files:**
- `README.md` — Updated with full project scope, AWS sinks documentation, architecture diagram
- `config/env.example` — Added S3, Athena, Redshift, and sink tuning variables
- `.env` — Updated AWS region to match S3 bucket location
- `Implementation/redshift/setup_aws.sh` — Fixed S3 prefix to read from environment variable

**Deleted files (duplicate cleanup):**
- `Implementation/coinbase_producer.py` — duplicate of `Implementation/producer/coinbase_producer.py`
- `Implementation/spark_stream.py` — older version with import bug, replaced by `Implementation/consumer/spark_stream.py`
- `Implementation/streamlit_app.py` — simpler version, replaced by `Implementation/dashboard/streamlit_app.py`
- `docker-compose.yml.save` — stale backup file

---

## AI Use Section

| AI Tool Name | Version, Account Type | Specific Feature for Which the AI Tool Was Used | Value Addition |
|---|---|---|---|
| Cursor (Claude) | claude-4.6-opus, Premium | Code review — identified duplicate files, import bugs, config mismatches, and query errors across the codebase | Reviewed all flagged issues, validated fixes against actual table schemas, and verified changes worked end-to-end with live data |
| Cursor (Claude) | claude-4.6-opus, Premium | README documentation — generated updated architecture diagram, tech stack table, AWS setup instructions | Customized content to match actual project structure, verified all file paths and commands, added project-specific troubleshooting |
| Cursor (Claude) | claude-4.6-opus, Premium | Bug fixes — fixed S3 prefix mismatch, Athena query grouping, and region configuration | Diagnosed root causes by reading source code and cross-referencing config files; tested the fixes against live AWS services |
| Cursor (Claude) | claude-4.6-opus, Premium | AWS setup guidance — step-by-step configuration of AWS CLI credentials, S3, and Athena | Executed all provisioning and testing commands manually; debugged region mismatch issue independently |

---

## Appendix: AI Prompt History

**Prompt 1 — S3 sink design**
> "I need to write a Kafka consumer that reads from crypto.trades.raw, buffers messages, and periodically flushes them as Parquet files to S3 with Hive-style partitioning (year/month/day/hour). It should also compute 1-minute OHLCV candles and write those as a separate Parquet dataset. Can you help me structure this?"

**Prompt 2 — Athena table setup**
> "How do I create Athena external tables over my S3 Parquet data? I want partition projection so I don't have to run MSCK REPAIR TABLE every time new data arrives. The partitions are year, month, day, hour as strings."

**Prompt 3 — Athena query examples**
> "Write me example Athena queries for my crypto pipeline: recent trades, trade count by symbol, latest 1-minute candles, and an hourly volume summary. The tables are raw_trades and candles_1m in the crypto_pipeline database."

**Prompt 4 — Code review and cleanup**
> "How is the project going? Can you review the codebase and identify any issues, duplicates, or things that need fixing?"

**Prompt 5 — Bug fixes**
> "Can you fix all the issues you found — remove the duplicate files, fix the import bug, fix the S3 prefix mismatch, fix the Athena query, and update the README?"

**Prompt 6 — AWS configuration**
> "How do I configure AWS credentials for the Redshift and Athena sinks?"

**Prompt 7 — Athena setup error**
> "I'm getting an InvalidRequestException when running setup_athena.py — it says the S3 location is invalid. Here's the error output."

**Prompt 8 — End-to-end testing**
> "How do I test the full pipeline? I want to run the producer, S3 sink, and Athena queries together."

**Prompt 9 — README update**
> "Update the README to include documentation for the Redshift and Athena sinks, the new AWS environment variables, and setup instructions for both sink options."
