# Real-Time Analytics Data Service — ClickHouse + ElasticSearch

A dual-engine analytics platform that routes stock trade queries to the right engine: **ClickHouse** for OLAP analytics, **ElasticSearch** for search. Fed by Kafka in real-time, served via FastAPI, orchestrated with Dagster.

Built as an extension of [kafka-spark-streaming-pipeline](https://github.com/JanviChitroda24/kafka-spark-streaming-pipeline) — same data, same Kafka topic, three query engines.

## Architecture

```
Finnhub / Simulator
        ↓
  Kafka (Redpanda)
   ┌────┼────────────────────┐
   ↓    ↓                    ↓
ClickHouse   ES Consumer   SQL Server
(Kafka engine)  (Python)   (benchmark)
   ↓            ↓
   ├────────────┤
   ↓            ↓
 FastAPI Data Service
 ┌──────┴──────┐
/analytics    /search
(ClickHouse) (ElasticSearch)
       ↓
 Streamlit Dashboard
```

## Key Results

| Metric | Value |
|--------|-------|
| ClickHouse VWAP query | **18ms** (materialized view) |
| ElasticSearch autocomplete | **10ms** (FST completion) |
| Skip index row reduction | **85%** (bloom_filter on trade_type) |
| MV speedup vs raw table | **4-6x** (AggregatingMergeTree) |
| dictGet vs JOIN | **3.5x faster** (in-memory dictionary) |
| API tests | **16/16 passing** |
| Recovery tests | **3/3 passing** (crash, degradation, partition) |
| Cross-engine data quality | **7/7 checks** (row counts, MV consistency, schema drift) |

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| OLAP Engine | **ClickHouse** | Columnar storage, MergeTree family, materialized views, skip indexes |
| Search Engine | **ElasticSearch** | Inverted index, completion suggester, fuzzy matching, relevance ranking |
| Benchmark DB | **SQL Server** | Row-store baseline for columnar vs row-store comparison |
| Message Broker | **Redpanda** | Kafka-compatible, feeds all engines from same topic |
| Data Service | **FastAPI** | Dual-engine query routing, TTL caching, cursor pagination |
| Orchestration | **Dagster** | 6-asset pipeline with quality gates and daily schedule |
| Dashboard | **Streamlit** | VWAP charts, autocomplete search, engine toggle |

## ClickHouse Optimization — Four Layers

```
Layer 1: Materialized Views     → Pre-computed VWAP, daily/hourly summaries (5x speedup)
Layer 2: Skip Indexes           → set, minmax, bloom_filter (85% fewer rows read)
Layer 3: Partition Pruning      → Monthly partitions skip entire months
Layer 4: ORDER BY Primary Index → Sparse index binary search on ticker-first sort
```

**Advanced features:** TTL (90-day data lifecycle), Dictionaries (3.5x faster than JOINs), Projections (dual sort orders).

## ElasticSearch — Four Features ClickHouse Can't Do

1. **Autocomplete** — Completion field + FST, prefix lookup in microseconds
2. **Fuzzy matching** — Edit distance on inverted index terms ("Micorsoft" → Microsoft)
3. **Full-text search** — Tokenized, relevance-ranked, multi-field
4. **More Like This** — Document similarity (AAPL → MSFT, NVDA as Technology peers)

## API Endpoints

### Analytics (ClickHouse-backed)
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/analytics/vwap/{ticker}` | VWAP time-series with configurable granularity |
| `GET /api/v1/analytics/top-movers` | Tickers ranked by price range % |
| `GET /api/v1/analytics/sectors/performance` | Sector aggregation via JOIN |
| `GET /api/v1/analytics/anomalies` | Trades deviating > N stddev from mean |
| `GET /api/v1/analytics/market/summary` | Dashboard homepage (bundled overview) |
| `GET /api/v1/analytics/trades` | Cursor-based pagination (not OFFSET) |

### Search (ElasticSearch-backed)
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/search/autocomplete` | Type-ahead ticker/company suggestions |
| `GET /api/v1/search/trades` | Full-text search with relevance ranking |
| `GET /api/v1/search/fuzzy` | Typo-tolerant search |
| `GET /api/v1/search/similar/{ticker}` | Find related tickers (MLT) |

### Production Patterns
- **TTL caching** — Market summary 60s, VWAP 10s, search 30s
- **Cursor-based pagination** — O(1) per page vs OFFSET's O(n)
- **Graceful degradation** — Analytics works when ES is down, search works when CH is down
- **Request logging middleware** — Every request logged with timing

## Quick Start

```bash
# 1. Clone and setup
git clone https://github.com/JanviChitroda24/clickhouse-analytics-service.git
cd clickhouse-analytics-service
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Start infrastructure
docker compose up -d   # ClickHouse, Redpanda, ElasticSearch, SQL Server

# 3. Setup databases
python -m src.setup_clickhouse              # Create tables
python -m src.setup_materialized_views      # Create + backfill MVs
python -m src.setup_skip_indexes            # Add skip indexes
python -m src.setup_advanced_features       # TTL, dictionaries, projections

# 4. Load data
python -m src.bulk_loader                   # Delta Lake → ClickHouse
python -m src.es_bulk_loader                # ClickHouse → ElasticSearch
python -m src.sqlserver_setup               # ClickHouse → SQL Server

# 5. Verify
python -m src.data_quality_checks           # 7 cross-engine checks

# 6. Start API + Dashboard
uvicorn src.api.main:app --port 8000        # API at localhost:8000
streamlit run dashboard/app.py              # Dashboard at localhost:8501
```

## Dagster Pipeline

6 assets with dependency chain:

```
check_infrastructure → verify_ingestion → run_quality_checks
    → refresh_materialized_views → refresh_sector_summary → generate_benchmark_report
```

Daily schedule: 5 PM Mon-Fri (after market close).

```bash
dagster dev -m dagster_pipeline   # UI at localhost:3000
```

## Recovery & Fault Tolerance

| Failure | Impact | Recovery |
|---------|--------|----------|
| ClickHouse crash | Ingestion pauses, analytics down | Auto-restart, Kafka offset replay, zero data loss |
| ES crash | Search 503, analytics unaffected | Auto-reconnect after restart |
| Kafka pause | All ingestion pauses | Auto-resume after unpause |

Tested with `python -m src.recovery_test` — 3/3 passing.

## Documentation

| Document | Description |
|----------|-------------|
| [Query Engine Deep Dive](docs/query_engine_deep_dive.md) | All 4 JD engines compared |
| [Engine Comparison Report](docs/engine_comparison_report.md) | CH vs ES head-to-head benchmarks |
| [Three-Engine Comparison](docs/three_engine_comparison.md) | CH vs ES vs SQL Server |
| [Query Optimization Report](docs/query_optimization_report.md) | MV, skip index, partition benchmarks |
| [Data Quality Report](docs/data_quality_report.md) | 7 cross-engine consistency checks |
| [Recovery Test Report](docs/recovery_test_report.md) | Crash and degradation test results |
| [API Load Test Report](docs/api_load_test_report.md) | 50-concurrent-request benchmarks |
| [Advanced Features Benchmark](docs/advanced_features_benchmark.md) | TTL, dictionaries, projections |

## Design Decisions

| Decision | Why |
|----------|-----|
| ClickHouse for OLAP | Columnar reads only needed columns — 3-10x faster than row-store for aggregations |
| ElasticSearch for search | Inverted index + FST for autocomplete, fuzzy, full-text — impossible in ClickHouse |
| SQL Server for benchmark | Row-store baseline proving the columnar advantage with real numbers |
| ReplacingMergeTree | Kafka at-least-once delivery produces duplicates — eventual dedup on merge |
| ORDER BY (ticker, trade_time) | Ticker-first because most queries filter by ticker |
| PARTITION BY toYYYYMM | Monthly partitions — coarse enough to avoid overhead, fine enough to skip data |
| Denormalized ES documents | ES has no JOINs — sector/company embedded in each trade document |
| FastAPI dual routing | Analytics → ClickHouse, search → ES. Consumers don't know which engine handles their request. |
| Cursor pagination | OFFSET is O(page_number). Cursor is O(1) — page 500 is as fast as page 1. |

## Connection to Week 3

This project extends the [Kafka-Spark Streaming Pipeline](https://github.com/JanviChitroda24/kafka-spark-streaming-pipeline):
- **Same Kafka topic** (`stock_trades`) feeds both Spark (Week 3) and ClickHouse (this project)
- **Same producer** generates trade events for both pipelines
- **Delta Lake output** from Week 3 is bulk-loaded into ClickHouse as historical data
- **Company metadata** from Week 1 enriches trade data in both systems
