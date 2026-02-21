# Real-Time Cryptocurrency Streaming Pipeline

Real-time market volatility and trade activity tracking using Kafka, PySpark, and live dashboards. The pipeline ingests live trade events from the Coinbase WebSocket API, streams them through Apache Kafka, processes them with PySpark Structured Streaming, and visualizes metrics through two dashboard frontends.

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
        └──▶ api_server.py          (Flask REST API → HTML/JS dashboard)
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
| Language | Python | 3.11+ |

## Prerequisites

Before running this project, ensure you have the following installed:

- **Docker Desktop** – [Download](https://www.docker.com/products/docker-desktop/)  
  Required to run Kafka and ZooKeeper containers.
- **Python 3.11+** – [Download](https://www.python.org/downloads/)
- **Java 11 or 17** – Required by PySpark. Verify with `java -version`.  
  Install via Homebrew on macOS: `brew install openjdk@17`
- **Git** – [Download](https://git-scm.com/downloads)

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
python Implementation/coinbase_producer.py
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
│   ├── coinbase_producer.py        # Kafka producer (Coinbase WebSocket → Kafka)
│   ├── producer/
│   │   └── coinbase_producer.py    # Producer (organized copy)
│   ├── consumer/
│   │   └── spark_stream.py         # PySpark consumer (windowed aggregations)
│   ├── spark_stream.py             # Spark consumer (simple version)
│   ├── streamlit_app.py            # Streamlit dashboard (standalone)
│   └── dashboard/
│       ├── api_server.py           # Flask REST API backend
│       ├── streamlit_app.py        # Streamlit dashboard (full-featured)
│       └── web/
│           ├── index.html          # Dashboard HTML frontend
│           ├── css/styles.css      # Dark theme styles
│           └── js/app.js           # Chart.js + polling logic
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
