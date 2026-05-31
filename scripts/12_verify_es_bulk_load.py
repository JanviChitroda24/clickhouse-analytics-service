#!/usr/bin/env python3
"""
12 — Bulk load ClickHouse trades into ElasticSearch (Hour 12).

Usage:
    python3 scripts/12_verify_es_bulk_load.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        from src.es_bulk_loader import bulk_load, verify

        bulk_load()
        es_count = verify()
        ch_count = __import__("src.clickhouse_client", fromlist=["execute_query"]).execute_query(
            "SELECT count() FROM stock_analytics.raw_trades"
        )[0][0]
        if es_count != ch_count:
            print(f"CHECK: ES {es_count} vs CH {ch_count}", file=sys.stderr)
            return 1
        print("ES bulk load: OK")
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
