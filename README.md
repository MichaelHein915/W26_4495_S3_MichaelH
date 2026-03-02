# Real-Time Cryptocurrency Streaming Pipeline

Real-time market volatility and trade activity tracking using Kafka, PySpark, and live dashboards. The pipeline ingests live trade events from the Coinbase WebSocket API, streams them through Apache Kafka, processes them with PySpark Structured Streaming, and visualizes metrics through two dashboard frontends. It also supports persisting data to AWS via Redshift Serverless and Athena over S3 Parquet.

**Course:** CSIS 4495 – Applied Research Project  
**Author:** Michael Hein (300375535)

## Architecture

```
Coinbase WebSocket API (ticker channel)
        │
        ▼
  coinbase_producer.py ── WebSocket → Kafka
        │
        ▼
  Kafka topic: crypto.trades.raw
        │
        ├──▶ spark_stream.py        (PySpark windowed aggregations → console)
        ├──▶ streamlit_app.py       (Streamlit real-time dashboard)
        ├──▶ api_server.py          (Flask REST API → HTML/JS dashboard)
        ├──▶ redshift_sink.py       (Kafka → S3 Parquet → Redshift COPY)
        └──▶ s3_sink.py             (Kafka → S3 Parquet → Athena queries)
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
| Dashboard (Option A) | Flask + HTML/CSS/JS + Chart.js | — |
| Dashboard (Option B) | Streamlit | ≥1.32.0 |
| Data Warehouse | Amazon Redshift Serverless | — |
| Query Engine | Amazon Athena | — |
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
| `LOG_LEVEL` | `INFO` | Application log level |
| `SPARK_LOG_LEVEL` | `WARN` | Spark log verbosity |
| `CHECKPOINT_DIR` | `data/checkpoints/raw_console` | Spark checkpoint directory |

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

The pipeline has three layers that should be started in order. Open a separate terminal for each component.

### Step 1 — Start the Kafka Producer

The producer connects to the Coinbase WebSocket API and publishes live trade events to Kafka:

```bash
python Implementation/producer/coinbase_producer.py
```

You should see log output like:

```
INFO - Starting Producer for ['BTC-USD', 'ETH-USD', ...]...
INFO - Streaming BTC-USD: $97432.10
INFO - Streaming ETH-USD: $2741.55
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

### Step 3 — Start a Dashboard

Choose **one** of the two dashboard options:

#### Option A: Flask + HTML/JS Dashboard

```bash
python Implementation/dashboard/api_server.py
```

Open **http://localhost:5000** in your browser.

Features:
- Dark terminal-themed UI with Chart.js visualizations
- Sortable, filterable metrics table
- Real-time price trend line chart (30-second buckets)
- Volume spike alerts
- KPI cards (total trades, live symbols, total volume, data freshness)
- Configurable refresh rate (1–5 seconds)

To run on a different port:

```bash
DASHBOARD_PORT=8080 python Implementation/dashboard/api_server.py
```

Open **http://localhost:8080** (or `http://192.168.1.77:8080` from another device on your network) in your browser.

#### Option B: Streamlit Dashboard

```bash
streamlit run Implementation/dashboard/streamlit_app.py
```

Open the URL shown in terminal output (default: **http://localhost:8501**).

Features:
- Rolling 3-minute metrics with VWAP and volatility
- Live bar chart (avg price + trade count per symbol)
- 30-second price trend line chart
- Volume spike detection and alerts
- Kafka connection status and data freshness indicators
- Sidebar controls for refresh interval and live toggle

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

### Option B: S3 + Athena

Writes Hive-partitioned Parquet files to S3 so Athena can query them directly — no Redshift required:

```bash
# 1. Create the Athena database and external tables (one-time)
python Implementation/athena/setup_athena.py

# 2. Start the S3 sink (runs alongside the producer)
python Implementation/athena/s3_sink.py

# 3. Run example queries
python Implementation/athena/run_query.py
```

## Stopping the Pipeline

1. Stop each Python process with `Ctrl+C`.
2. Shut down the Docker containers:

```bash
docker compose down
```

## Project Structure

```
cryto-streaming-pipeline/
├── docker-compose.yml              # Kafka + ZooKeeper containers
├── requirements.txt                # Python dependencies
├── .env                            # Environment configuration (not committed)
├── config/
│   └── env.example                 # Environment variable template
├── src/
│   └── utils/
│       └── config.py               # Centralized AppConfig loader
├── Implementation/
│   ├── producer/
│   │   └── coinbase_producer.py    # Kafka producer (Coinbase WebSocket → Kafka)
│   ├── consumer/
│   │   └── spark_stream.py         # PySpark consumer (windowed aggregations)
│   ├── dashboard/
│   │   ├── api_server.py           # Flask REST API backend
│   │   ├── streamlit_app.py        # Streamlit dashboard (full-featured)
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
│   └── athena/
│       ├── s3_sink.py              # Kafka → S3 Parquet (Hive partitioning)
│       ├── setup_athena.py         # Create Athena DB + external tables
│       └── run_query.py            # Example Athena queries
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
