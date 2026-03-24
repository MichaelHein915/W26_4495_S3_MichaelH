# Real-Time Cryptocurrency Streaming Pipeline

Real-time market volatility and trade activity tracking using Kafka, PySpark, and live dashboards. The pipeline ingests live trade events from multiple exchanges (Coinbase, Binance, Kraken), streams them through Apache Kafka, processes them with PySpark Structured Streaming, and visualizes metrics through a Flask + HTML/JS dashboard. It also supports persisting data to AWS via Redshift Serverless and Athena over S3 Parquet.

**Course:** CSIS 4495 – Applied Research Project  
**Author:** Michael Hein (300375535)

## Architecture

```
  Coinbase / Binance / Kraken WebSocket APIs
        │
        ├──▶ coinbase_producer.py   (single exchange, Coinbase only)
        └──▶ multi_exchange_producer.py  (Coinbase + Binance + Kraken in parallel)
        │
        ▼
  Kafka topic: crypto.trades.raw
        │
        ├──▶ spark_stream.py        (PySpark windowed aggregations → console)
        ├──▶ api_server.py          (Flask REST API → HTML/JS dashboard, /health)
        ├──▶ redshift_sink.py       (Kafka → S3 Parquet → Redshift COPY)
        └──▶ s3_sink.py             (Kafka → S3 Parquet → Athena queries)
                                                              │
                                                              ▼
                                                      Amazon QuickSight
                                                    (BI dashboards via SPICE)
```

**Tracked Symbols:** BTC-USD, ETH-USD, SOL-USD, XRP-USD, ADA-USD, DOGE-USD, AVAX-USD, LINK-USD, LTC-USD, BCH-USD

## Technology Stack

| Component | Technology | Version |
|---|---|---|
| Message Broker | Apache Kafka (Confluent) | 7.5.0 |
| Coordination | Apache ZooKeeper (Confluent) | 7.5.0 |
| Containers | Docker & Docker Compose | — |
| Data Source | Coinbase WebSocket API | — |
| Stream Processing | PySpark Structured Streaming | 3.5.0 |
| Dashboard | Flask + HTML/CSS/JS + Chart.js | — |
| Data Warehouse | Amazon Redshift Serverless | — |
| Query Engine | Amazon Athena | — |
| BI Dashboards | Amazon QuickSight (SPICE) | — |
| Object Storage | Amazon S3 (Parquet + Hive partitioning) | — |
| Language | Python | 3.11+ |

## Prerequisites

Before running this project, ensure you have the following installed:

- **Docker Desktop** – [Download](https://www.docker.com/products/docker-desktop/)  
  Required to run Kafka and ZooKeeper containers.
- **Python 3.11+** – [Download](https://www.python.org/downloads/)
- **Java 11 or 17** – Required by PySpark. Verify with `java -version`.  
  Install via Homebrew on macOS: `brew install openjdk@17`
- **Git** – [Download](https://git-scm.com/downloads)
- **AWS CLI v2** (optional) – Required only for the Redshift and Athena sinks.  
  [Install guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/cryto-streaming-pipeline.git
cd cryto-streaming-pipeline
```

### 2. Create and Activate a Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate        # macOS / Linux
# venv\Scripts\activate          # Windows
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example environment file and adjust if needed:

```bash
cp config/env.example .env
```

The default `.env` values work out of the box for local development:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC_RAW` | `crypto.trades.raw` | Topic for raw trade events |
| `COINBASE_WS_URL` | `wss://ws-feed.exchange.coinbase.com` | Coinbase WebSocket endpoint |
| `CRYPTO_SYMBOLS` | `BTC-USD,ETH-USD,...` (10 symbols) | Comma-separated trading pairs |
| `EXCHANGES` | `coinbase,binance,kraken` | Comma-separated exchanges for multi-exchange producer |
| `BINANCE_WS_URL` | `wss://stream.binance.com:9443` | Binance WebSocket endpoint |
| `KRAKEN_WS_URL` | `wss://ws.kraken.com/v2` | Kraken WebSocket endpoint |
| `LOG_LEVEL` | `INFO` | Application log level |
| `SPARK_LOG_LEVEL` | `WARN` | Spark log verbosity |
| `CHECKPOINT_DIR` | `data/checkpoints/raw_console` | Spark checkpoint directory |
| `SLACK_WEBHOOK_URL` | — | Slack webhook for arbitrage & volume spike alerts |
| `ALERT_EMAIL_TO` | — | Comma-separated email addresses for alerts |
| `SMTP_HOST` | — | SMTP server (e.g. `smtp.gmail.com`) |
| `SMTP_PORT` | `587` | SMTP port |
| `SMTP_USER` | — | SMTP username (often your email) |
| `SMTP_PASSWORD` | — | SMTP password or app password |
| `ALERT_ARBITRAGE_THRESHOLD_PCT` | `0.3` | Min spread % to trigger arbitrage alert |
| `ALERT_VOLUME_SPIKE_RATIO` | `2.0` | Volume spike ratio to trigger alert |
| `ALERT_PRICE_THRESHOLDS` | — | Price alerts: `SYMBOL:above|below:PRICE` (comma-separated, e.g. `BTC-USD:above:100000,ETH-USD:below:3000`) |
| `ALERT_ANOMALY_ENABLED` | `true` | Enable ML anomaly detection (Isolation Forest) |
| `ANOMALY_CONTAMINATION` | `0.05` | Expected proportion of anomalies (0.01–0.1) |

For AWS sinks, see the [AWS Data Sinks](#aws-data-sinks-optional) section below.

### 5. Start Kafka and ZooKeeper (Docker)

Make sure Docker Desktop is running, then:

```bash
docker compose up -d
```

Verify the containers are healthy:

```bash
docker compose ps
```

You should see both `zookeeper` and `kafka` running. Wait a few seconds for Kafka to finish initializing before proceeding.

## Running the Pipeline

You can run the pipeline in two ways: **Docker Compose** (all-in-one) or **manually** (separate terminals).

### Option A: Full Pipeline with Docker Compose (Recommended)

Run the entire pipeline (Kafka, producer, consumer, dashboard) with one command:

```bash
docker compose up --build
```

Optional: copy `config/env.example` to `.env` to customize symbols, exchanges, or alerts.

This starts:

- **Zookeeper** and **Kafka** — message broker
- **Producer** — multi-exchange (Coinbase, Binance, Kraken) WebSocket → Kafka
- **Consumer** — PySpark streaming aggregations (console output)
- **Dashboard** — Flask API + UI at **http://localhost:5000**
- **Prometheus** — metrics storage at **http://localhost:9093**
- **Grafana** — dashboards at **http://localhost:3000** (admin/admin)

To run in the background:

```bash
docker compose up -d --build
```

To include the optional AWS sinks (S3/Athena, Redshift), ensure `.env` has the required AWS variables, then:

```bash
docker compose --profile aws up -d --build
```

**Note:** For local development (running producer/dashboard outside Docker), use `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` in your `.env`. The Kafka container exposes port 9092 for host access.

### Option B: Manual (Separate Terminals)

The pipeline has three layers that should be started in order. Open a separate terminal for each component.

### Step 1 — Start the Kafka Producer

**Option A: Single exchange (Coinbase only)**

```bash
python Implementation/producer/coinbase_producer.py
```

**Option B: Multi-exchange (Coinbase, Binance, Kraken)**

Runs all three exchanges in parallel, normalising trades to a common schema with an `exchange` field:

```bash
python Implementation/producer/multi_exchange_producer.py
```

To enable only specific exchanges:

```bash
EXCHANGES=coinbase,binance python Implementation/producer/multi_exchange_producer.py
```

You should see log output like:

```
INFO - Enabled exchanges: ['coinbase', 'binance', 'kraken']
INFO - [coinbase] BTC-USD: $97432.10
INFO - [binance] BTC-USDT: $97430.50
INFO - [kraken] BTC-USD: $97435.00
```

### Step 2 — Start the Spark Consumer (Optional)

The Spark consumer reads from Kafka and computes 1-minute windowed aggregations (average price and trade count per symbol):

```bash
python Implementation/consumer/spark_stream.py
```

> **Note:** On the first run, Spark will download the Kafka connector JAR automatically. This may take a minute.

To reset Spark checkpoints (useful if you encounter checkpoint errors):

```bash
RESET_CHECKPOINT=true python Implementation/consumer/spark_stream.py
```

### Step 3 — Start the Dashboard

```bash
python Implementation/dashboard/api_server.py
```

Open **http://localhost:5000** in your browser.

Features:
- Dark terminal-themed UI with Chart.js visualizations
- **Recent trades ticker** — Last 25 trades with symbol, price, size, exchange
- Sortable, filterable metrics table with sparklines
- **Volatility chart** — Horizontal bar chart of price volatility by symbol
- **Volume over time** — Line chart of total volume per 30-second bucket
- **Exchange price comparison** — Grouped bar chart comparing prices across Coinbase, Binance, Kraken (multi-exchange only)
- Real-time price trend line chart (30-second buckets)
- Volume spike alerts
- **Price threshold alerts** — Slack/email when a symbol crosses above or below a configured price
- **ML anomaly detection** — Isolation Forest detects unusual patterns in trade count, volatility, volume, and price change
- **Cross-exchange arbitrage detection** — highlights price spreads between exchanges
- **Configurable alerts** — Slack and/or email for arbitrage, volume spikes, price thresholds, and anomalies
- **Exchange filter** — filter metrics by Coinbase, Binance, or Kraken (dropdown + clickable pills)
- KPI cards (total trades, live symbols, total volume, live exchanges, data freshness)
- Exchange pills showing trade counts per exchange (click to filter)
- Configurable refresh rate (1–5 seconds) and time window (1m, 3m, 5m, 10m)
- Health endpoint at `/health` for Kafka connectivity and data freshness
- REST API documentation at `/api/docs`

To run on a different port:

```bash
DASHBOARD_PORT=8080 python Implementation/dashboard/api_server.py
```

Open **http://localhost:8080** (or `http://192.168.1.77:8080` from another device on your network) in your browser.

## AWS Data Sinks (Optional)

The pipeline can persist data to AWS for historical analysis. Two sink options are available — they can run independently or side by side.

### Additional Environment Variables

Add these to your `.env` when using AWS sinks:

| Variable | Default | Description |
|---|---|---|
| `S3_BUCKET` | — | S3 bucket for staging / Athena data |
| `S3_STAGING_PREFIX` | `crypto-data/` | Key prefix inside the bucket |
| `AWS_REGION` | `us-west-2` | AWS region |
| `REDSHIFT_HOST` | — | Redshift Serverless endpoint |
| `REDSHIFT_PORT` | `5439` | Redshift port |
| `REDSHIFT_DB` | `crypto` | Redshift database name |
| `REDSHIFT_USER` | `admin` | Redshift admin username |
| `REDSHIFT_PASSWORD` | — | Redshift admin password |
| `REDSHIFT_IAM_ROLE` | — | IAM role ARN for Redshift S3 access |
| `ATHENA_DATABASE` | `crypto_pipeline` | Athena database name |
| `QUICKSIGHT_USER` | — | Your QuickSight username (for dashboard setup) |
| `SINK_FLUSH_INTERVAL_SEC` | `60` | Seconds between sink flushes |
| `SINK_FLUSH_MAX_RECORDS` | `5000` | Max buffered records before flush |

### Option A: Redshift Serverless

Provisions infrastructure, applies the schema, and starts the sink:

```bash
# 1. Provision S3 bucket, IAM role, and Redshift Serverless (one-time)
chmod +x Implementation/redshift/setup_aws.sh
./Implementation/redshift/setup_aws.sh

# 2. Apply the Redshift schema
python Implementation/redshift/apply_schema.py

# 3. Verify connectivity
python Implementation/redshift/test_connection.py

# 4. Start the sink (runs alongside the producer)
python Implementation/redshift/redshift_sink.py
```

The sink consumes from Kafka, buffers trades, and periodically flushes raw trades and 1-minute OHLCV candles to Redshift via S3 COPY.

### Option B: S3 + Athena + QuickSight

Writes Hive-partitioned Parquet files to S3 so Athena can query them directly — no Redshift required:

```bash
# 1. Create the Athena database and external tables (one-time)
python Implementation/athena/setup_athena.py

# 2. Start the S3 sink (runs alongside the producer)
python Implementation/athena/s3_sink.py

# 3. Run example queries
python Implementation/athena/run_query.py
```

### QuickSight Dashboards

QuickSight connects to the Athena tables and provides interactive BI dashboards with SPICE (in-memory) acceleration.

**Prerequisites:**
- QuickSight enabled in your AWS account ([sign up here](https://quicksight.aws.amazon.com/))
- During QuickSight setup, grant access to your S3 bucket and Athena
- Athena tables already created (`setup_athena.py`) and S3 sink running

```bash
# 1. Set your QuickSight username in .env
#    QUICKSIGHT_USER=YourIAMUsername

# 2. Run the setup script (creates data source, datasets, and analysis)
python Implementation/quicksight/setup_quicksight.py

# 3. Open the analysis URL printed by the script, then customize visuals
#    and publish as a shared dashboard from the QuickSight console
```

The setup script creates:
- **Athena data source** — connects QuickSight to your Athena workgroup
- **Crypto Raw Trades dataset** — SPICE-backed dataset over `raw_trades`
- **Crypto Candles 1m dataset** — SPICE-backed dataset over `candles_1m`
- **Crypto Pipeline Dashboard analysis** — starter visuals (trade KPI, volume by symbol, VWAP trend)

To refresh data, re-run the setup script or configure a SPICE refresh schedule in the QuickSight console (Datasets → Schedule refresh).

## Prometheus & Grafana (Observability)

The pipeline exposes Prometheus metrics for monitoring. When running with Docker Compose, Prometheus and Grafana are started automatically.

| Service | URL | Description |
|---------|-----|-------------|
| **Prometheus** | http://localhost:9093 | Metrics storage and query UI |
| **Grafana** | http://localhost:3000 | Dashboards (login: `admin` / `admin`) |

### Metrics Exposed

| Metric | Component | Description |
|--------|-----------|-------------|
| `crypto_trades_published_total` | Producer | Trades published to Kafka per exchange |
| `crypto_kafka_send_errors_total` | Producer | Kafka send failures per exchange |
| `crypto_dashboard_events_total` | Dashboard | Events in dashboard buffer |
| `crypto_dashboard_data_freshness_seconds` | Dashboard | Seconds since last event |
| `crypto_dashboard_requests_total` | Dashboard | API request count by endpoint |
| `crypto_dashboard_request_duration_seconds` | Dashboard | API latency histogram |
| `crypto_consumer_running` | Consumer | 1 if Spark consumer is running |

The pre-provisioned **Crypto Pipeline** dashboard in Grafana shows trades per minute, data freshness, API latency, and pipeline health. Change the default admin password after first login.

## Stopping the Pipeline

1. Stop each Python process with `Ctrl+C`.
2. Shut down the Docker containers:

```bash
docker compose down
```

## Project Structure

```
cryto-streaming-pipeline/
├── docker-compose.yml              # Full pipeline: Kafka, producer, consumer, dashboard, Prometheus, Grafana
├── Dockerfile.producer             # Multi-exchange producer image
├── Dockerfile.consumer              # PySpark consumer image
├── Dockerfile.dashboard             # Flask dashboard image
├── Dockerfile.s3-sink               # S3/Athena sink image
├── Dockerfile.redshift-sink         # Redshift sink image
├── requirements.txt                # Python dependencies
├── .env                            # Environment configuration (not committed)
├── config/
│   ├── env.example                 # Environment variable template
│   ├── prometheus.yml              # Prometheus scrape config
│   └── grafana/
│       ├── provisioning/           # Grafana datasources & dashboards
│       └── dashboards/             # Pre-built Crypto Pipeline dashboard
├── pyproject.toml                  # Python packaging (pip install -e .)
├── src/
│   └── utils/
│       ├── __init__.py
│       ├── config.py               # Centralized AppConfig loader
│       ├── parse_trade.py          # Shared trade message parser (raw + normalised)
│       ├── metrics.py              # Prometheus metric definitions
│       ├── alerts.py               # Slack + email alert dispatch with cooldowns
│       ├── anomaly.py              # Isolation Forest anomaly detection
│       ├── llm_service.py          # OpenAI-compatible MLX LLM client
│       └── context_builder.py      # Market context / prompt builder for AI
├── Implementation/
│   ├── producer/
│   │   ├── coinbase_producer.py    # Single-exchange producer (Coinbase only)
│   │   ├── multi_exchange_producer.py  # Multi-exchange (Coinbase, Binance, Kraken)
│   │   ├── base_exchange.py        # Abstract base for exchange adapters
│   │   ├── exchange_coinbase.py   # Coinbase WebSocket adapter
│   │   ├── exchange_binance.py    # Binance WebSocket adapter
│   │   └── exchange_kraken.py     # Kraken WebSocket adapter
│   ├── consumer/
│   │   └── spark_stream.py         # PySpark consumer (windowed aggregations)
│   ├── dashboard/
│   │   ├── api_server.py           # Flask REST API backend (orchestrator)
│   │   ├── analytics.py            # Pure compute functions (metrics, arbitrage, etc.)
│   │   ├── ai_routes.py            # AI assistant Flask Blueprint
│   │   └── web/
│   │       ├── index.html          # Dashboard HTML frontend
│   │       ├── css/styles.css      # Dark theme styles
│   │       └── js/app.js           # Chart.js + polling logic
│   ├── redshift/
│   │   ├── redshift_sink.py        # Kafka → S3 → Redshift micro-batch sink
│   │   ├── schema.sql              # Redshift DDL (raw_trades + candles_1m)
│   │   ├── apply_schema.py         # Apply schema via redshift-connector
│   │   ├── test_connection.py      # Verify Redshift connectivity
│   │   └── setup_aws.sh            # AWS provisioning script
│   ├── athena/
│   │   ├── s3_sink.py              # Kafka → S3 Parquet (Hive partitioning)
│   │   ├── setup_athena.py         # Create Athena DB + external tables
│   │   └── run_query.py            # Example Athena queries
│   └── quicksight/
│       └── setup_quicksight.py     # QuickSight data source, datasets & analysis
├── data/
│   ├── checkpoints/                # Spark streaming checkpoints
│   └── output/                     # Data output directory
└── DocumentsAndReports/
    ├── MichaelH_Proposal.pdf       # Project proposal
    └── MichaelH_Progress_Report1.pdf
```

## Troubleshooting

| Problem | Solution |
|---|---|
| `kafka.errors.NoBrokersAvailable` | Ensure Docker containers are running: `docker compose ps` |
| Spark checkpoint errors | Reset checkpoints: `RESET_CHECKPOINT=true python Implementation/consumer/spark_stream.py` |
| `ModuleNotFoundError` | Activate your virtual environment: `source venv/bin/activate` |
| Spark cannot find Java | Install Java 11 or 17 and ensure `JAVA_HOME` is set |
| No data appearing in dashboards | Confirm the producer is running and printing trade events |
| Port 5000 already in use | Use a different port: `DASHBOARD_PORT=8080 python Implementation/dashboard/api_server.py` |
| Redshift COPY fails | Check that `REDSHIFT_IAM_ROLE` has S3 read access and the bucket/prefix are correct |
| Athena query returns no results | Ensure the S3 sink is running and files exist under `s3://<bucket>/<prefix>/` |
| QuickSight `AccessDeniedException` | Ensure QuickSight has permission to access your S3 bucket and Athena (QuickSight console → Manage QuickSight → Security & permissions) |
| QuickSight datasets show 0 rows | Trigger a SPICE refresh: re-run `setup_quicksight.py` or refresh manually in the Datasets page |
| Redshift/Athena: `exchange` column missing | For existing deployments, run `ALTER TABLE crypto.raw_trades ADD COLUMN exchange VARCHAR(20) DEFAULT 'coinbase';` and same for `crypto.candles_1m`. See `Implementation/redshift/schema.sql` for migration notes |
| Docker: producer/consumer can't connect to Kafka | Wait for Kafka healthcheck to pass (~30s). Check `docker compose ps` — Kafka must show "healthy" before dependent services start |
| Docker: consumer OOM or slow startup | PySpark downloads JARs on first run. Increase Docker memory (Docker Desktop → Settings → Resources) or wait a few minutes |
