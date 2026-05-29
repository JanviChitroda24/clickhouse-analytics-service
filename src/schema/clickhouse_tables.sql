-- ============================================================
-- ClickHouse Schema — Stock Analytics Data Service
-- ============================================================
-- Engine Choices:
--   ReplacingMergeTree → raw_trades (Kafka at-least-once dedup)
--   MergeTree          → vwap_1min (pre-aggregated, no dedup needed)
--   MergeTree          → company_metadata (small reference table)
-- ============================================================

CREATE DATABASE IF NOT EXISTS stock_analytics;


-- ────────────────────────────────────────────────────────────
-- 1. RAW TRADES — ReplacingMergeTree
-- ────────────────────────────────────────────────────────────
-- Why ReplacingMergeTree:
--   Kafka delivers at-least-once. Duplicates are expected.
--   ReplacingMergeTree keeps the latest version (by _loaded_at)
--   per ORDER BY key on background merge.
--   For immediate dedup: SELECT ... FINAL (adds overhead).
--
-- Why ORDER BY (ticker, trade_time, trade_id):
--   Sparse primary index stores one entry per granule (8192 rows).
--   Ticker at position 1 → queries filtering by ticker skip entire granules.
--   trade_time at position 2 → time-range filters after ticker.
--   trade_id at position 3 → unique per row for ReplacingMergeTree.
--
-- Why PARTITION BY toYYYYMM(trade_time):
--   Monthly partitions. "Last 24 hours" only reads current month's partition.

CREATE TABLE IF NOT EXISTS stock_analytics.raw_trades
(
    trade_id       String,
    ticker         LowCardinality(String),  -- 25 distinct values → dictionary encoding
    price          Float64,
    quantity       Int32,
    trade_time     DateTime64(3),           -- millisecond precision
    side           LowCardinality(String),  -- BUY / SELL / UNKNOWN
    trade_type     LowCardinality(String),  -- MARKET / LIMIT / BLOCK
    bid_price      Float64,
    ask_price      Float64,
    source         LowCardinality(String),  -- simulator / finnhub_live
    _loaded_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMM(trade_time)
ORDER BY (ticker, trade_time, trade_id)
SETTINGS index_granularity = 8192;


-- ────────────────────────────────────────────────────────────
-- 2. VWAP 1-MINUTE — MergeTree
-- ────────────────────────────────────────────────────────────
-- Pre-aggregated VWAP from Week 3 Spark pipeline.
-- No dedup needed — each (ticker, window_start) is computed once.

CREATE TABLE IF NOT EXISTS stock_analytics.vwap_1min
(
    ticker         LowCardinality(String),
    window_start   DateTime,
    window_end     DateTime,
    vwap           Float64,
    total_volume   Int64,
    trade_count    Int32,
    high_price     Float64,
    low_price      Float64,
    buy_pressure   Float64,
    _loaded_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (ticker, window_start);


-- ────────────────────────────────────────────────────────────
-- 3. COMPANY METADATA — MergeTree
-- ────────────────────────────────────────────────────────────
-- Small reference table (25 rows). Used for JOINs.

CREATE TABLE IF NOT EXISTS stock_analytics.company_metadata
(
    ticker         String,
    company_name   String,
    sector         LowCardinality(String),
    industry       LowCardinality(String),
    market_cap     Int64,
    country        LowCardinality(String),
    exchange       LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY ticker;
