# ClickHouse Query Optimization Report

**Generated:** 2026-05-30 22:35 UTC
**Dataset:** 85,815 rows in `raw_trades`
**Engine:** ReplacingMergeTree, ORDER BY (ticker, trade_time, trade_id), PARTITION BY toYYYYMM(trade_time)

## Executive Summary

- **Materialized Views:** Average 4.5x speedup over raw table aggregations
- **Skip Indexes:** Average 11% row reduction vs full scan baseline
- **ORDER BY Impact:** ticker filter reads 24,676 rows vs 12,500 for non-indexed column
- **Production Queries:** 6/8 under 100ms

## 1. Materialized Views

| Query | Raw Table (ms) | Materialized View (ms) | Speedup |
|-------|---------------:|----------------------:|--------:|
| VWAP (AAPL, per minute) | 34.4 | 12.5 | 2.8x |
| Daily summary (all tickers) | 24.9 | 6.3 | 3.9x |
| Hourly stats (NVDA) | 22.6 | 5.8 | 3.9x |
| Sector summary (JOIN) | 33.8 | 4.7 | 7.2x |

MVs use AggregatingMergeTree (VWAP) and SummingMergeTree (daily/hourly).
They fire automatically on every INSERT — no manual refresh needed.

## 2. Skip Indexes

Baseline (no index, `WHERE source = 'simulator'`): **12,500 rows read** (full scan)

| Index Type | Filter | Rows Read | Reduction |
|------------|--------|----------:|----------:|
| SET + BLOOM_FILTER | `ticker='AAPL' AND trade_type='block'` | 8,192 | 34.5% |
| MINMAX | `price BETWEEN 200 AND 210` | 12,500 | 0.0% |
| BLOOM_FILTER | `trade_type = 'block'` | 12,500 | 0.0% |

Skip indexes store per-granule (8,192 rows) metadata.
set(0) stores distinct values, minmax stores range, bloom_filter uses probabilistic membership.

## 3. Partition Pruning + ORDER BY Impact

| Filter | Column Position | Rows Read |
|--------|-----------------|----------:|
| `WHERE ticker = 'AAPL'` | Col 1 (ORDER BY) | 24,676 |
| `WHERE side = 'BUY'` | Not in ORDER BY | 12,500 |

Filtering on `ticker` (ORDER BY position 1) reads **24,676** rows.
Filtering on `side` (not in ORDER BY) reads **12,500** rows — full scan.
Column position in ORDER BY determines primary index effectiveness.

## 4. Production Analytics Queries

| Query | Time (ms) | Rows | Status |
|-------|----------:|-----:|--------|
| Top Movers | 65.9 | 10 | PASS |
| VWAP Deviation | 200.2 | 20 | SLOW |
| Anomaly by Sector | 121.6 | 5 | SLOW |
| Sector Performance | 69.4 | 5 | PASS |
| Buy/Sell Pressure | 14.1 | 28 | PASS |
| Rolling VWAP (AAPL) | 12.2 | 26 | PASS |
| Intraday Range | 16.7 | 28 | PASS |
| Cross-Correlation | 58.6 | 20 | PASS |

**6/8 queries under 100ms** on 85,815 rows.

## Optimization Architecture

```
Layer 1: Materialized Views     → Pre-computed aggregations (AggregatingMergeTree)
Layer 2: Skip Indexes           → Per-granule data skipping (set, minmax, bloom_filter)
Layer 3: Partition Pruning       → Monthly partitions skip entire months
Layer 4: ORDER BY Primary Index  → Sparse index binary search on sort key columns
```
