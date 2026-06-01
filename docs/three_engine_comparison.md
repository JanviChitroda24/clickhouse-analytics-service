# Three-Engine Comparison: ClickHouse vs ElasticSearch vs SQL Server

**Generated:** 2026-06-01 12:37 UTC

**Dataset:** 85,815 identical rows in all three engines

## Benchmark Results

| Query | ClickHouse (ms) | SQL Server (ms) | ElasticSearch (ms) | Winner | Why |
|-------|----------------:|----------------:|-------------------:|:------:|-----|
| VWAP aggregation (all tickers) | 78 | 278 | 127 | CH | Columnar CH reads price+quantity only; SQL reads full rows per group. |
| Single ticker filter (AAPL stats) | 32 | 29 | 17 | ES | All three use indexes: CH primary index, SQL B-tree, ES term lookup. |
| Top movers (price range per ticker) | 29 | 52 | 10 | ES | Min/max per ticker: CH scans price column; SQL scans full rows. |
| Buy/sell pressure | 137 | 59 | 10 | ES | CH countIf vs SQL CASE vs ES filter aggregations. |
| Time-range filter (recent trades) | 22 | 28 | 18 | ES | CH partition pruning + columnar; SQL B-tree on trade_time still reads rows. |

## Engine Strengths

### ClickHouse (columnar OLAP)
- Fastest for aggregations on large scans (reads only needed columns)
- Materialized views and skip indexes for analytics
- Best for: dashboards, VWAP, time-series, OLAP

### ElasticSearch (inverted index)
- Autocomplete, fuzzy match, full-text search, relevance ranking
- Denormalized documents avoid JOINs for sector/company fields
- Best for: search bars, discovery, text queries

### SQL Server (row-store OLTP)
- ACID transactions, stored procedures, row-level updates/deletes
- B-tree indexes excel at point lookups and transactional workloads
- Best for: ledgers, order management, systems of record

## When to Use Which

| Workload | Engine |
|----------|--------|
| Aggregations at scale | ClickHouse |
| Full-text / autocomplete | ElasticSearch |
| Transactional updates / ACID | SQL Server |
