# API Load Test Report

**Generated:** 2026-06-01 11:49 UTC

**Concurrency:** 50 simultaneous requests per endpoint

**Endpoints tested:** 11

## Results

| Endpoint | p50 (ms) | p95 (ms) | p99 (ms) | Avg (ms) | Errors |
|----------|---------:|---------:|---------:|---------:|-------:|
| Health check | 289 | 472 | 474 | 290 | 0 |
| VWAP (AAPL, 5min) | 191 | 465 | 474 | 245 | 0 |
| Top movers | 538 | 926 | 948 | 601 | 0 |
| Sector performance | 1144 | 2247 | 2266 | 1254 | 0 |
| Anomalies | 3090 | 3978 | 3995 | 3046 | 0 |
| Market summary | 235 | 269 | 273 | 227 | 0 |
| Trades (AAPL) | 713 | 1547 | 1560 | 891 | 0 |
| Autocomplete | 616 | 924 | 931 | 641 | 0 |
| Full-text search | 607 | 763 | 804 | 617 | 0 |
| Fuzzy search | 499 | 647 | 675 | 512 | 0 |
| Similar tickers | 488 | 675 | 678 | 529 | 0 |

## Summary
- **Median p50:** 538ms
- **Median p99:** 804ms
- **Total errors:** 0/550
- **Status:** Slow endpoints (p99 >= 500ms): Top movers, Sector performance, Anomalies, Trades (AAPL), Autocomplete, Full-text search, Fuzzy search, Similar tickers

## Architecture

```
N concurrent clients -> FastAPI (uvicorn)
    |-> /analytics/* -> ClickHouse (pooled client, TTL cache)
    |-> /search/*    -> ElasticSearch (pooled client, TTL cache)
```
