# Data Quality Report

**Generated:** 2026-06-01 16:36 UTC

**Status:** ✅ ALL PASSED (7/7)

## Check Results

| Check | Status | Details |
|-------|--------|---------|
| Cross-engine row count | ✅ | CH=85,815, ES=85,815, SQL=85,815 |
| ClickHouse MV consistency | ✅ | MV VWAP=282.1514, Raw VWAP=282.1514, diff=0.000000 |
| ES index health | ✅ | Cluster status: green |
| Freshness check (CH vs ES) | ✅ | CH latest=2026-05-29 21:04:17, ES latest=2026-05-29 21:04:17, diff=0s |
| Schema drift detection | ✅ | All 14 fields present with correct types |
| Null validation | ✅ | No nulls in critical fields (price, quantity, ticker, trade_time) |
| Range validation | ✅ | All prices in range $1–$2000 (actual: $44.13–$1080.00) |

## Checks Explained

1. **Cross-engine row count** — ClickHouse, ES, and SQL Server should have identical row counts
2. **MV consistency** — Materialized view VWAP matches fresh raw_trades calculation
3. **ES index health** — Cluster status green or yellow (not red)
4. **Freshness** — Latest timestamps in CH and ES within 5 minutes of each other
5. **Schema drift** — ES index has all 14 expected fields with correct types
6. **Null validation** — No nulls in price, quantity, ticker, trade_time
7. **Range validation** — All prices between $1 and $2,000
