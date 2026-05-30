-- ============================================================
-- Skip Indexes — Granule-Level Data Skipping (Hour 7)
-- ============================================================
-- ClickHouse reads in granules (8192 rows). Skip indexes store per-granule
-- metadata so the engine can skip blocks that cannot match the WHERE clause.
-- ADD INDEX alone applies to new inserts only — MATERIALIZE builds metadata
-- for existing parts (same idea as MV backfill).
-- ============================================================


-- SET index on ticker — distinct values per granule
-- Skips granules whose set does not contain the filtered ticker.
-- ticker is in ORDER BY (primary index helps too); useful at granule boundaries.
ALTER TABLE stock_analytics.raw_trades
    ADD INDEX IF NOT EXISTS idx_ticker ticker TYPE set(0) GRANULARITY 1;


-- MINMAX index on price — min/max per granule
-- Skips granules whose [min, max] range does not overlap the query range.
ALTER TABLE stock_analytics.raw_trades
    ADD INDEX IF NOT EXISTS idx_price price TYPE minmax GRANULARITY 1;


-- BLOOM_FILTER on trade_type — probabilistic membership per granule
-- "Definitely not here" → skip. 0.01 = ~1% false positive rate.
ALTER TABLE stock_analytics.raw_trades
    ADD INDEX IF NOT EXISTS idx_trade_type trade_type TYPE bloom_filter(0.01) GRANULARITY 1;


-- Build skip-index metadata for data already on disk (async background work)
ALTER TABLE stock_analytics.raw_trades MATERIALIZE INDEX idx_ticker;
ALTER TABLE stock_analytics.raw_trades MATERIALIZE INDEX idx_price;
ALTER TABLE stock_analytics.raw_trades MATERIALIZE INDEX idx_trade_type;
