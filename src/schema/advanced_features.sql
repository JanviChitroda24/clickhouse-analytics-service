-- ============================================================
-- Advanced ClickHouse Features
-- TTL, Dictionaries, Projections
-- ============================================================
-- Run via: python -m src.setup_advanced_features
-- Idempotent where ClickHouse supports IF NOT EXISTS / MODIFY TTL.


-- 1. TTL — auto-delete raw trades older than 90 days
-- Materialized views (VWAP, daily summary) are separate tables and persist.
ALTER TABLE stock_analytics.raw_trades
    MODIFY TTL trade_time + INTERVAL 90 DAY;


-- 2. Dictionary — in-memory hash map for company_metadata lookups
-- HASHED layout supports String primary keys (FLAT requires UInt64 keys).
DROP DICTIONARY IF EXISTS stock_analytics.company_dict;

CREATE DICTIONARY stock_analytics.company_dict
(
    ticker String,
    company_name String,
    sector String,
    industry String,
    market_cap Int64,
    country String,
    exchange String
)
PRIMARY KEY ticker
SOURCE(CLICKHOUSE(TABLE 'company_metadata' DB 'stock_analytics'))
LIFETIME(MIN 300 MAX 600)
LAYOUT(HASHED());


-- 3. Projection — alternative physical sort (time-first)
-- ReplacingMergeTree requires deduplicate_merge_projection_mode before ADD PROJECTION.
ALTER TABLE stock_analytics.raw_trades
    MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';

ALTER TABLE stock_analytics.raw_trades
    ADD PROJECTION IF NOT EXISTS proj_time_first
    (SELECT * ORDER BY (trade_time, ticker));
