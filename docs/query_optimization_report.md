# ClickHouse Query Optimization Report

**Generated:** 2026-06-01 21:04 UTC
**Dataset:** 85,815 rows in `raw_trades`
**Engine:** ReplacingMergeTree, ORDER BY (ticker, trade_time, trade_id), PARTITION BY toYYYYMM(trade_time)

## Executive Summary

- **Materialized Views:** Average 3.8x speedup over raw table aggregations
- **Skip Indexes:** Average 68% row reduction vs full scan baseline
- **ORDER BY Impact:** ticker filter reads 24,676 rows vs 85,815 for non-indexed column
- **Production Queries:** 8/8 under 100ms

## 1. Materialized Views

| Query | Raw Table (ms) | Materialized View (ms) | Speedup |
|-------|---------------:|----------------------:|--------:|
| VWAP (AAPL, per minute) | 11.6 | 3.1 | 3.7x |
| Daily summary (all tickers) | 11.4 | 2.2 | 5.2x |
| Hourly stats (NVDA) | 7.6 | 3.0 | 2.6x |
| Sector summary (JOIN) | 11.8 | 3.1 | 3.8x |

MVs use AggregatingMergeTree (VWAP) and SummingMergeTree (daily/hourly).
They fire automatically on every INSERT — no manual refresh needed.

## 2. Skip Indexes

Baseline (no index, `WHERE source = 'simulator'`): **85,815 rows read** (full scan)

| Index Type | Filter | Rows Read | Reduction |
|------------|--------|----------:|----------:|
| SET + BLOOM_FILTER | `ticker='AAPL' AND trade_type='block'` | 8,292 | 90.3% |
| MINMAX | `price BETWEEN 200 AND 210` | 61,239 | 28.6% |
| BLOOM_FILTER | `trade_type = 'block'` | 12,500 | 85.4% |

Skip indexes store per-granule (8,192 rows) metadata.
set(0) stores distinct values, minmax stores range, bloom_filter uses probabilistic membership.

## 3. Partition Pruning + ORDER BY Impact

| Filter | Column Position | Rows Read |
|--------|-----------------|----------:|
| `WHERE ticker = 'AAPL'` | Col 1 (ORDER BY) | 24,676 |
| `WHERE side = 'BUY'` | Not in ORDER BY | 85,815 |

Filtering on `ticker` (ORDER BY position 1) reads **24,676** rows.
Filtering on `side` (not in ORDER BY) reads **85,815** rows — full scan.
Column position in ORDER BY determines primary index effectiveness.

## 4. Production Analytics Queries

| Query | Time (ms) | Rows | Status |
|-------|----------:|-----:|--------|
| Top Movers | 9.7 | 10 | PASS |
| VWAP Deviation | 14.7 | 20 | PASS |
| Anomaly by Sector | 21.0 | 5 | PASS |
| Sector Performance | 11.3 | 5 | PASS |
| Buy/Sell Pressure | 7.7 | 28 | PASS |
| Rolling VWAP (AAPL) | 4.7 | 26 | PASS |
| Intraday Range | 5.2 | 28 | PASS |
| Cross-Correlation | 21.4 | 20 | PASS |

**8/8 queries under 100ms** on 85,815 rows.

## Optimization Architecture

```
Layer 1: Materialized Views     → Pre-computed aggregations (AggregatingMergeTree)
Layer 2: Skip Indexes           → Per-granule data skipping (set, minmax, bloom_filter)
Layer 3: Partition Pruning       → Monthly partitions skip entire months
Layer 4: ORDER BY Primary Index  → Sparse index binary search on sort key columns
```
