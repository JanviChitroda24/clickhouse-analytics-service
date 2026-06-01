# Recovery & Fault Tolerance Test Report

**Generated:** 2026-06-01 23:09 UTC

## ClickHouse crash mid-ingest

**Status:** ✅ PASS

**Detail:** Kafka buffered 500 messages during outage. Zero data loss.

**Steps:**
- Before count: 85,815
- docker stop clickhouse — container stopped
- Sent 500 events to Kafka while ClickHouse was down
- docker start clickhouse — container restarted
- After count: 86,315 (new rows: 500)

## ElasticSearch crash

**Status:** ✅ PASS

**Detail:** Analytics worked during ES outage. Search returned 503. After restart, search recovered.

**Steps:**
- docker stop elasticsearch
- Analytics while ES down: {'vwap': '200 (working)', 'top_movers': '200 (working)'}
- Search while ES down: {'autocomplete': '503 (503 expected)', 'fuzzy': '503 (503 expected)'}
- Health status: degraded
- docker start elasticsearch — restarted

## Kafka pause (network partition)

**Status:** ✅ PASS

**Detail:** Consumer resumed after partition. 200 new rows ingested.

**Steps:**
- Before count: 86,315
- docker pause redpanda — container frozen
- docker unpause redpanda — container resumed
- Sent 200 events after unpause
- After count: 86,515 (new rows: 200)

## Key Findings

- **Kafka buffering:** Messages sent while ClickHouse is down should replay on restart.
- **Graceful degradation:** Analytics (ClickHouse) survive ES crash; search returns 503.
- **Network partition:** Consumer tolerates Redpanda pause and resumes without manual fix.

## Interview Talking Points

- ClickHouse crash: Kafka consumer resumes from last committed offset → zero data loss
- ES crash: FastAPI returns 503 for search, 200 for analytics → graceful degradation
- Network partition: consumer self-heals after broker unpause
