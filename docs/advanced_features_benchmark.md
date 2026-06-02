# Advanced ClickHouse Features Benchmark

**Generated:** 2026-06-02 01:51 UTC

**Methodology:** 5 runs per query, averaged

## dictGet vs JOIN (sector grouping)

| Approach | Avg (ms) | Rows |
|----------|---------:|-----:|
| JOIN | 40.9 | 5 |
| dictGet | 11.7 | 6 |

**Speedup:** 3.5x

## Projection vs primary sort

**Time filter:** `trade_time > toDateTime64('2026-06-01 23:09:02', 3) - INTERVAL 1 DAY`

| Query pattern | Avg (ms) |
|---------------|---------:|
| Time-first (projection) | 10.0 |
| Ticker-first (ORDER BY) | 7.7 |

## TTL

- `raw_trades`: 90-day TTL on `trade_time`
- MVs (`mv_realtime_vwap`, etc.): persist after raw rows expire

## EXPLAIN highlights (time-first)

- `Expression ((Project names + Projection))`
- `            Parts: 2/5`
- `            Granules: 2/14`
- `            Parts: 2/2`
- `            Granules: 2/2`
- `          PrimaryKey`
- `            Parts: 2/2`
- `            Granules: 2/2`

## Enriched VWAP (dictGet bonus)

**Latency:** 11.2ms

