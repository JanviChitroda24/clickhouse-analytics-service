-- ============================================================
-- Materialized Views — Pre-Computed Analytics 
-- ============================================================
-- MVs fire on every INSERT into raw_trades (including Kafka MV path).
-- They do NOT retroactively process existing rows — run setup_materialized_views
-- backfill after create.
--
-- Query patterns:
--   AggregatingMergeTree → sumMerge(), countMerge(), maxMerge(), minMerge()
--   SummingMergeTree     → plain SELECT (numeric cols auto-sum on merge)
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- MV 1: Real-time VWAP — AggregatingMergeTree
-- ────────────────────────────────────────────────────────────
-- Stores intermediate aggregation STATES (sumState), not final VWAP.
-- Multiple Kafka batches for same (ticker, minute) merge correctly.
-- Query: sumMerge(price_volume_sum) / sumMerge(volume_sum)

CREATE MATERIALIZED VIEW IF NOT EXISTS stock_analytics.mv_realtime_vwap
ENGINE = AggregatingMergeTree()
ORDER BY (ticker, minute)
AS SELECT
    ticker,
    toStartOfMinute(trade_time) AS minute,
    sumState(toFloat64(price * quantity)) AS price_volume_sum,
    sumState(toFloat64(quantity))         AS volume_sum,
    countState()                          AS trade_count,
    maxState(price)                       AS high_price,
    minState(price)                       AS low_price
FROM stock_analytics.raw_trades
GROUP BY ticker, minute;


-- ────────────────────────────────────────────────────────────
-- MV 2: Daily Summary — SummingMergeTree
-- ────────────────────────────────────────────────────────────
-- Auto-sums numeric columns on merge for same (ticker, trade_date).
-- Caveat: max/min/avg are summed across batches, not true extrema.
-- Acceptable for portfolio; production would use maxState/minState.

CREATE MATERIALIZED VIEW IF NOT EXISTS stock_analytics.mv_daily_summary
ENGINE = SummingMergeTree()
ORDER BY (ticker, trade_date)
AS SELECT
    ticker,
    toDate(trade_time) AS trade_date,
    count()            AS trade_count,
    sum(quantity)      AS total_volume,
    max(price)         AS high_price,
    min(price)         AS low_price,
    avg(price)         AS avg_price
FROM stock_analytics.raw_trades
GROUP BY ticker, trade_date;


-- ────────────────────────────────────────────────────────────
-- MV 3: Hourly Stats — SummingMergeTree
-- ────────────────────────────────────────────────────────────
-- Hourly volume + price_stddev for volatility / anomaly signals.

CREATE MATERIALIZED VIEW IF NOT EXISTS stock_analytics.mv_hourly_stats
ENGINE = SummingMergeTree()
ORDER BY (ticker, hour)
AS SELECT
    ticker,
    toStartOfHour(trade_time) AS hour,
    count()                   AS trade_count,
    sum(quantity)             AS total_volume,
    avg(price)                AS avg_price,
    stddevPop(price)          AS price_stddev
FROM stock_analytics.raw_trades
GROUP BY ticker, hour;


-- ────────────────────────────────────────────────────────────
-- Sector Summary — manual refresh (MVs cannot JOIN)
-- ────────────────────────────────────────────────────────────
-- Populated via INSERT ... SELECT ... JOIN in setup_materialized_views.
-- Dagster periodic refresh planned for the FastAPI layer.

CREATE TABLE IF NOT EXISTS stock_analytics.sector_summary
(
    sector          LowCardinality(String),
    trade_date      Date,
    ticker_count    Int32,
    trade_count     Int64,
    total_volume    Int64,
    avg_price       Float64,
    total_notional  Float64
)
ENGINE = MergeTree()
ORDER BY (sector, trade_date);
