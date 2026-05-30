# ClickHouse Pipeline Verification Report

**Generated:** 2026-05-30T11:44:41.895415+00:00

**Pipeline:** Producer → Kafka (Redpanda) → ClickHouse Kafka Engine → raw_trades

## Row Counts
| Table | Rows |
|-------|------|
| raw_trades | 85,815 |
| vwap_1min | 2,460 |
| company_metadata | 25 |

## Ticker Distribution
| Ticker | Trades |
|--------|--------|
| AAPL | 13,544 |
| TSLA | 10,598 |
| NVDA | 9,566 |
| GOOGL | 7,947 |
| AMZN | 6,241 |
| MSFT | 5,941 |
| AVGO | 5,591 |
| WMT | 4,250 |
| META | 3,356 |
| KO | 2,544 |
| BAC | 2,369 |
| CVX | 1,675 |
| JPM | 1,650 |
| HD | 1,471 |
| V | 1,356 |
| COST | 1,181 |
| PG | 1,092 |
| LLY | 1,010 |
| PEP | 855 |
| ABBV | 737 |
| JNJ | 700 |
| MA | 694 |
| MRK | 591 |
| TMO | 496 |
| BRK-B | 310 |
| UNH | 20 |
| XOM | 15 |
| BRK.B | 15 |

## Data Freshness
- Earliest trade: `2026-05-26 14:10:52.361000`
- Latest trade: `2026-05-29 21:04:17`
- Seconds behind real-time: `52824`

## Price Sanity Check
| Ticker | Min | Max | Avg | Trades |
|--------|-----|-----|-----|--------|
| AAPL | $207.65 | $311.82 | $304.41 | 13,544 |
| ABBV | $191.78 | $214.47 | $205.55 | 737 |
| AMZN | $211.01 | $268.05 | $257.64 | 6,241 |
| AVGO | $181.30 | $435.25 | $408.95 | 5,591 |
| BAC | $44.13 | $52.32 | $50.81 | 2,369 |
| BRK-B | $474.01 | $482.39 | $478.69 | 310 |
| BRK.B | $473.84 | $475.42 | $474.69 | 15 |
| COST | $948.67 | $1004.78 | $989.55 | 1,181 |
| CVX | $153.47 | $188.53 | $180.77 | 1,675 |
| GOOGL | $184.66 | $389.22 | $375.12 | 7,947 |
| HD | $308.99 | $396.63 | $327.84 | 1,471 |
| JNJ | $164.74 | $231.27 | $200.24 | 700 |
| JPM | $218.14 | $309.46 | $283.18 | 1,650 |
| KO | $63.44 | $80.69 | $78.50 | 2,544 |
| LLY | $937.19 | $1080.00 | $1030.83 | 1,010 |
| MA | $492.20 | $545.08 | $523.71 | 694 |
| META | $517.78 | $611.15 | $585.04 | 3,356 |
| MRK | $120.50 | $130.89 | $125.49 | 591 |
| MSFT | $414.16 | $473.06 | $422.80 | 5,941 |
| NVDA | $136.88 | $218.18 | $208.09 | 9,566 |
| PEP | $146.83 | $173.18 | $156.03 | 855 |
| PG | $143.26 | $171.49 | $151.28 | 1,092 |
| TMO | $446.29 | $581.18 | $529.25 | 496 |
| TSLA | $270.69 | $434.54 | $417.63 | 10,598 |
| UNH | $518.94 | $521.25 | $519.99 | 20 |
| V | $302.85 | $326.79 | $319.15 | 1,356 |
| WMT | $94.10 | $119.29 | $116.94 | 4,250 |
| XOM | $114.75 | $115.17 | $115.04 | 15 |

## Source Distribution
| Source | Rows |
|--------|------|
| finnhub_live | 73,315 |
| simulator | 12,000 |
| kafka_test | 500 |

## Architecture
```
Producer (simulator) → Kafka (stock_trades, 3 partitions)
    → ClickHouse Kafka Engine (kafka_trades)
    → MV (mv_kafka_to_trades)
    → ReplacingMergeTree (raw_trades)
```

## Status: NEEDS REVIEW (stale data)