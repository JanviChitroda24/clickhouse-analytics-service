# Query Engine Deep Dive: ClickHouse vs ElasticSearch vs SQL Server vs HBase

> The ByteDance JD lists four query engines. This document covers all four —
> three built and benchmarked in this project, one discussed theoretically.

---

## ClickHouse — Columnar OLAP Engine

**Built and benchmarked in this project.**

| Aspect | Detail |
|--------|--------|
| **Storage model** | Columnar (MergeTree family) — each column stored as a separate file |
| **Index type** | Sparse primary index (one entry per 8,192 rows) + skip indexes (set, minmax, bloom_filter) |
| **Query language** | SQL with extensions (countIf, toStartOfMinute, dictGet, sumState/sumMerge) |
| **Partitioning** | PARTITION BY — monthly time partitions, entire months skipped on time-range queries |
| **Replication** | ReplicatedMergeTree for multi-node; single-node in this project |

### Strengths
- **Columnar I/O:** `avg(price)` reads only the price column file — 1/10th the data vs row-store
- **MergeTree family:** ReplacingMergeTree for Kafka dedup, AggregatingMergeTree for incremental pre-computed analytics
- **Materialized views:** Trigger on INSERT — no manual refresh. VWAP pre-computed on arrival.
- **Skip indexes:** Per-granule metadata eliminates granules without matching values. bloom_filter on trade_type reduced rows read by 85%.
- **Dictionaries:** In-memory hash maps replacing JOINs. dictGet() is 3.5x faster than JOIN.
- **Projections:** Alternative sort orders — ticker-first AND time-first queries both use optimal sort.
- **TTL:** Automatic data lifecycle — raw trades expire after 90 days, MVs persist indefinitely.
- **Native Kafka engine:** Direct consumption from Kafka, no middleware Python consumer needed.

### Weaknesses
- **No full-text search:** No inverted index, no tokenization, no fuzzy matching, no relevance ranking.
- **No ACID transactions:** Append-only. Updates are async mutations that rewrite data parts.
- **Expensive JOINs:** Functional but not optimized like SQL Server/Postgres. Dictionaries are the workaround.
- **Eventual dedup:** ReplacingMergeTree deduplicates on background merge, not on insert.

### Project Benchmarks
- VWAP aggregation: **20ms** (columnar scan, 2 columns out of 10)
- MV VWAP query: **18ms** (pre-computed, 2,576 rows vs 85K raw)
- Skip index (bloom_filter): **85% fewer rows read** vs full scan
- dictGet vs JOIN: **3.5x faster** (11.7ms vs 40.9ms)
- Kafka ingestion: Zero data loss on crash — consumer resumes from offset

### When to Use
- Analytics dashboards (VWAP, sector performance, top movers)
- Time-series aggregations at scale
- Real-time metrics on streaming data
- Log analytics with pre-computed rollups

### When NOT to Use
- Full-text search (use ElasticSearch)
- Transactional workloads with row-level updates (use SQL Server)
- Systems requiring immediate consistency (eventual dedup)

---

## ElasticSearch — Inverted Index Search Engine

**Built and benchmarked in this project.**

| Aspect | Detail |
|--------|--------|
| **Storage model** | Inverted index (Lucene segments) — term → document ID mapping |
| **Index type** | Inverted index for text, doc_values for aggregations, FST for completion |
| **Query language** | JSON Query DSL (bool, match, term, fuzzy, completion suggest) |
| **Refresh model** | Near-real-time — documents searchable after refresh_interval (5s in this project) |
| **Scaling** | Shards (horizontal partitioning) + replicas (fault tolerance) |

### Strengths
- **Full-text search:** Tokenized, lowercased, relevance-ranked. "financial services" finds matching sectors.
- **Autocomplete:** Completion field type with FST — prefix lookup in microseconds, not milliseconds.
- **Fuzzy matching:** Edit distance on inverted index terms. "Micorsoft" → Microsoft (1 edit).
- **Relevance ranking:** TF-IDF scoring. "Apple" matches "Apple Inc" better than "Pineapple Corp."
- **More Like This:** Document similarity via term overlap. AAPL → MSFT, NVDA (Technology peers).
- **Denormalization:** Sector/company embedded in each document — no JOINs needed at query time.
- **filter context:** Structured filters (term, range) are cacheable and skip scoring overhead.

### Weaknesses
- **Expensive aggregations:** Reads full JSON documents to aggregate — 3-5x slower than ClickHouse for VWAP.
- **No JOINs:** Document store — each query operates on one index. Cross-index queries require multiple calls.
- **High memory usage:** Inverted index + FST + doc_values consume significant heap.
- **Refresh delay:** Documents not searchable for 1-5 seconds after indexing.
- **No native Kafka engine:** Requires external Python consumer for Kafka ingestion.

### Project Benchmarks
- Autocomplete: **10-20ms** (FST prefix lookup)
- Full-text search: **14ms** ("financial services" → 6,379 matching trades)
- Fuzzy search: **28ms** ("Micorsoft" → MSFT)
- More Like This: **42-66ms** (AAPL → Technology peers)
- Sector grouping: **12ms** (denormalized, no JOIN — faster than ClickHouse's 44ms JOIN)

### When to Use
- Search bars with autocomplete
- Typo-tolerant search
- Full-text search with relevance ranking
- Document similarity and recommendations
- Log search (ELK stack)

### When NOT to Use
- Heavy OLAP aggregations at scale (use ClickHouse)
- Transactional updates (use SQL Server)
- Complex multi-table JOINs (use SQL Server/Postgres)

---

## SQL Server — Row-Store OLTP Database

**Benchmarked in this project (Azure SQL Edge on ARM64).**

| Aspect | Detail |
|--------|--------|
| **Storage model** | Row-store (B-tree pages) + optional columnstore indexes |
| **Index type** | B-tree (clustered + non-clustered) |
| **Query language** | T-SQL |
| **Transaction model** | Full ACID — atomic, consistent, isolated, durable |
| **Update model** | In-place row-level updates and deletes |

### Strengths
- **ACID transactions:** Transfer money between accounts atomically. ClickHouse can't do this.
- **Row-level updates:** `UPDATE trades SET status = 'confirmed' WHERE trade_id = 'abc'` — instant. ClickHouse mutations are async.
- **Optimized JOINs:** 30+ years of JOIN optimization. Complex multi-table queries are SQL Server's specialty.
- **Stored procedures:** Business logic in the database. Reduces network round trips.
- **B-tree indexes:** O(log n) point lookups. Find one row by trade_id in <5ms.
- **Enterprise ecosystem:** SSRS, SSIS, SSAS, Power BI integration.

### Weaknesses
- **Slow analytics:** Row-store reads all columns per row. `avg(price)` reads 10 columns to use 1.
- **No full-text search:** Basic `LIKE '%term%'` with no relevance ranking, no fuzzy matching.
- **Licensing costs:** Enterprise edition is expensive. ClickHouse and ES are open-source.
- **Scaling:** Vertical scaling primarily. Horizontal scaling requires Always On AG or sharding.

### Project Benchmarks
- VWAP aggregation: **278ms** (reads all 10 columns per row — 3.5x slower than ClickHouse)
- Single ticker filter: **29ms** (B-tree index on ticker — competitive with ClickHouse)
- Buy/sell pressure: **59ms** (CASE WHEN is less elegant than ClickHouse's countIf)

### When to Use
- Order management systems (ACID required)
- Banking and financial ledgers (transactions required)
- Mixed OLTP+OLAP workloads (columnstore indexes help)
- Enterprise applications with stored procedure business logic

### When NOT to Use
- Pure analytics at scale (ClickHouse is 3-10x faster)
- Full-text search (ElasticSearch is purpose-built)
- Append-only event streams (ClickHouse's MergeTree is designed for this)

---

## HBase — Wide-Column NoSQL Store

**Discussed for JD completeness — not built in this project (requires Hadoop cluster).**

| Aspect | Detail |
|--------|--------|
| **Storage model** | Wide-column (LSM-tree), runs on HDFS |
| **Index type** | Row key only — no secondary indexes natively (requires Phoenix) |
| **Query language** | Java API / Thrift / REST — no SQL without Apache Phoenix |
| **Consistency model** | Strong consistency per row, eventual across regions |
| **Scaling** | Horizontal on HDFS — petabyte scale |

### Strengths
- **Petabyte-scale random access:** Billions of rows with fast key-based lookup.
- **Wide, sparse columns:** Each row can have different columns. Good for schema-flexible data.
- **Hadoop integration:** Sits on HDFS, works with MapReduce, Spark, Hive.
- **Auto-sharding:** RegionServer splits handle growing data automatically.
- **Strong per-row consistency:** Read-after-write guarantee for individual rows.

### Weaknesses
- **Row key only access:** No secondary indexes natively. Query patterns must be designed around the row key.
- **No SQL:** Java API by default. Apache Phoenix adds SQL but adds latency.
- **Complex operations:** ZooKeeper + HDFS + HBase RegionServers — many moving parts.
- **High latency for analytics:** Not designed for aggregations — ClickHouse is 10-100x faster.
- **No full-text search:** No inverted index.

### When to Use
- IoT sensor data at petabyte scale (billions of time-series readings)
- User profile stores (user_id → attributes, activity, preferences)
- Real-time random access to very large datasets
- When Hadoop/HDFS is already the data platform

### When NOT to Use
- Analytics and aggregations (use ClickHouse)
- Full-text search (use ElasticSearch)
- Transactional systems (use SQL Server/Postgres)
- Small datasets (overkill — Postgres is simpler)

### Why Not Built in This Project
HBase requires a full Hadoop stack: HDFS (distributed filesystem), ZooKeeper (coordination), and HBase RegionServers. This is too heavy for a Docker-on-laptop setup. The project covers HBase theoretically so you can answer "when would you use HBase?" in interviews.

---

## Architecture Decision Matrix

| Need | Engine | Why |
|------|--------|-----|
| Dashboard analytics (VWAP, metrics) | **ClickHouse** | Columnar, sub-second aggregations on millions of rows |
| Search bar / autocomplete | **ElasticSearch** | FST completion suggester, inverted index, relevance ranking |
| Fuzzy matching / typo tolerance | **ElasticSearch** | Edit distance on inverted index terms |
| Transactional writes + reads | **SQL Server** | ACID guarantees, row-level updates, stored procedures |
| Petabyte random access | **HBase** | LSM-tree, horizontal scaling on HDFS |
| Real-time log aggregation | **ClickHouse** | High insert throughput + fast aggregation |
| Full-text document search | **ElasticSearch** | Tokenized, relevance-ranked, multi-field |
| Order management / banking | **SQL Server** | ACID transactions, complex JOINs |
| IoT sensor data (billions) | **HBase** | Wide-column, sparse data, row-key access |
| Mixed OLTP + analytics | **SQL Server** | Columnstore indexes for analytics, B-tree for OLTP |

## This Project's Architecture

```
Producer → Kafka (Redpanda) ─┬→ ClickHouse (native Kafka engine) → OLAP analytics
                              ├→ Python consumer → ElasticSearch → Search features
                              └→ SQL Server (benchmark baseline)
                                        ↓
                                  FastAPI Data Service
                               ┌────────┴────────┐
                            /analytics          /search
                           (ClickHouse)     (ElasticSearch)
                                        ↓
                                  Streamlit Dashboard
```

**Routing logic:** Analytics queries → ClickHouse. Search queries → ElasticSearch. The FastAPI service abstracts the engine choice — consumers don't know which database handles their request.

---

## Interview Quick Reference

**"Tell me about your query engine experience."**
> I built a real-time analytics data service using ClickHouse and ElasticSearch, fed by Kafka. ClickHouse handles OLAP — materialized views with AggregatingMergeTree for pre-computed VWAP, skip indexes reducing rows read by 85%, TTL for data lifecycle. ElasticSearch handles search — autocomplete via FST, fuzzy matching, full-text with relevance ranking. I also benchmarked SQL Server to prove the columnar vs row-store difference. FastAPI routes queries to the right engine.

**"Why ClickHouse over ElasticSearch for analytics?"**
> ClickHouse is columnar — it reads only the columns a query needs. For VWAP (needs price and quantity out of 10 columns), ClickHouse reads 1/5th the data ES reads. My benchmark showed ClickHouse at 20ms vs ES at 45ms for the same VWAP query. But ES wins for search — autocomplete in 10ms, fuzzy matching, relevance ranking. That's why I use both.

**"When would you use HBase?"**
> Petabyte-scale random access where the access pattern is single-row lookups by key. IoT sensor data with billions of time-series rows, user profile stores. HBase runs on HDFS with auto-sharding. I wouldn't use it for analytics (ClickHouse) or search (ES) or transactions (SQL Server). It's for massive-scale key-value access.

**"How did you optimize ClickHouse query performance?"**
> Four layers. Materialized views — pre-computed aggregations, 5x speedup. Skip indexes — bloom_filter reduced rows read by 85%. Partition pruning — monthly partitions skip entire months. Projections — alternative sort orders so both ticker-first and time-first queries are fast. Plus dictionaries replacing JOINs (3.5x faster) and TTL for automatic data lifecycle.
