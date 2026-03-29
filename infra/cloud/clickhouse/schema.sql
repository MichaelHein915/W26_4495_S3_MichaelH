-- ClickHouse schema for crypto-analytics cloud deployment
-- Partition strategy: PARTITION BY toYYYYMMDD(window_start)
-- Sort key:           ORDER BY (symbol, window_start)  ← fast per-symbol time queries

CREATE DATABASE IF NOT EXISTS crypto;

-- ── Raw trade events (append-only audit log) ──────────────────────────────
CREATE TABLE IF NOT EXISTS crypto.raw_trades (
    product_id   String,
    event_time   DateTime64(3, 'UTC'),
    price_usd    Float64,
    size_qty     Float64,
    trade_id     String,
    source       String       DEFAULT 'coinbase',
    received_at  DateTime64(3, 'UTC'),
    _ingested_at DateTime     DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (product_id, event_time)
TTL toDateTime(event_time) + INTERVAL 30 DAY     -- keep 30 days of raw ticks
SETTINGS index_granularity = 8192;

-- ── 1-minute OHLCV candles (primary analytics table) ─────────────────────
CREATE TABLE IF NOT EXISTS crypto.candles_1m (
    product_id   String,
    window_start DateTime('UTC'),
    window_end   DateTime('UTC'),
    open_price   Float64,
    high_price   Float64,
    low_price    Float64,
    close_price  Float64,
    volume_qty   Float64,
    trade_count  UInt32,
    vwap_usd     Float64,
    _written_at  DateTime     DEFAULT now()
)
ENGINE = ReplacingMergeTree(_written_at)    -- deduplicates re-processed windows
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (product_id, window_start)         -- Grafana + React query on this key
SETTINGS index_granularity = 8192;

-- ── Live metrics (latest window per symbol — dashboard overview) ───────────
CREATE TABLE IF NOT EXISTS crypto.live_metrics (
    product_id        String,
    window_start      DateTime('UTC'),
    window_end        DateTime('UTC'),
    last_price_usd    Float64,
    avg_price_usd     Float64,
    price_change_pct  Float64,
    volume_qty        Float64,
    trade_count       UInt32,
    volatility_usd    Float64,
    vwap_usd          Float64,
    notional_volume_usd Float64  MATERIALIZED last_price_usd * volume_qty,
    _written_at       DateTime   DEFAULT now()
)
ENGINE = ReplacingMergeTree(_written_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (product_id, window_start)
SETTINGS index_granularity = 8192;

-- ── Volume spike alerts ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS crypto.alerts (
    product_id          String,
    window_start        DateTime('UTC'),
    severity            LowCardinality(String),   -- 'low' | 'medium' | 'high'
    spike_ratio         Float64,
    volume_qty          Float64,
    baseline_volume_qty Float64,
    _written_at         DateTime  DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (product_id, window_start)
TTL toDateTime(window_start) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- ── Materialized view: latest price per symbol (fast dashboard summary) ───
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.symbol_latest
ENGINE = ReplacingMergeTree()
ORDER BY product_id
AS SELECT
    product_id,
    argMax(last_price_usd,  window_start) AS last_price_usd,
    argMax(vwap_usd,        window_start) AS vwap_usd,
    argMax(volatility_usd,  window_start) AS volatility_usd,
    argMax(volume_qty,      window_start) AS volume_qty,
    argMax(price_change_pct, window_start) AS price_change_pct,
    max(window_start)                     AS latest_window
FROM crypto.live_metrics
GROUP BY product_id;

-- ── Writer user (used by Spark + FastAPI) ─────────────────────────────────
-- Note: CLICKHOUSE_USER / CLICKHOUSE_PASSWORD env vars in docker-compose
-- create the user automatically. This grant runs after schema init.
-- If your ClickHouse version requires manual grants, run:
-- GRANT ALL ON crypto.* TO crypto_writer;
