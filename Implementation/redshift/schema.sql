-- ============================================================
-- Redshift DDL for crypto streaming pipeline
-- Run once against your Redshift Serverless database:
--   psql -h $REDSHIFT_HOST -p 5439 -U admin -d crypto -f schema.sql
-- ============================================================

CREATE SCHEMA IF NOT EXISTS crypto;

-- Raw trade events from the Coinbase ticker channel.
-- Append-only fact table; one row per trade received from Kafka.
CREATE TABLE IF NOT EXISTS crypto.raw_trades (
    trade_time      TIMESTAMP       NOT NULL,
    product_id      VARCHAR(20)     NOT NULL,
    price           DECIMAL(18,8)   NOT NULL,
    size_qty        DECIMAL(18,8),
    notional_usd    DECIMAL(18,8),
    batch_id        VARCHAR(64),
    loaded_at       TIMESTAMP       DEFAULT GETDATE()
)
DISTKEY(product_id)
SORTKEY(trade_time);

-- Pre-aggregated 1-minute OHLCV candles.
-- Computed client-side from the raw buffer before each flush.
CREATE TABLE IF NOT EXISTS crypto.candles_1m (
    window_start    TIMESTAMP       NOT NULL,
    window_end      TIMESTAMP       NOT NULL,
    product_id      VARCHAR(20)     NOT NULL,
    open_price      DECIMAL(18,8),
    high_price      DECIMAL(18,8),
    low_price       DECIMAL(18,8),
    close_price     DECIMAL(18,8),
    volume          DECIMAL(18,8),
    trade_count     INTEGER,
    vwap            DECIMAL(18,8),
    batch_id        VARCHAR(64),
    loaded_at       TIMESTAMP       DEFAULT GETDATE()
)
DISTKEY(product_id)
SORTKEY(window_start);
