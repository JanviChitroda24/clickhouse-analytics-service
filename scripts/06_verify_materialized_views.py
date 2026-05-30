#!/usr/bin/env python3
"""
06 — Verify materialized views exist and have rows after backfill.

Run after: python -m src.setup_materialized_views

Usage:
    python3 scripts/06_verify_materialized_views.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

EXPECTED = {
    "mv_realtime_vwap",
    "mv_daily_summary",
    "mv_hourly_stats",
    "sector_summary",
}


def main() -> int:
    try:
        from src.clickhouse_client import execute_query

        rows = execute_query("""
            SELECT name, engine
            FROM system.tables
            WHERE database = 'stock_analytics'
            ORDER BY name
        """)
        found = {name for name, _ in rows}

        print("--- Hour 6 objects ---")
        missing = EXPECTED - found
        for name in sorted(EXPECTED):
            if name not in found:
                print(f"  MISSING  {name}")
                continue
            engine = next(e for n, e in rows if n == name)
            count = execute_query(f"SELECT count() FROM stock_analytics.{name}")[0][0]
            status = "OK" if count > 0 else "EMPTY"
            print(f"  {status:<6} {name:<25} {engine:<25} {count:>8} rows")

        if missing:
            print(f"\nMissing: {missing}")
            print("Run: python -m src.setup_materialized_views")
            return 1

        empty = [
            n
            for n in EXPECTED
            if execute_query(f"SELECT count() FROM stock_analytics.{n}")[0][0] == 0
        ]
        if empty:
            print(f"\nEmpty tables (backfill needed): {empty}")
            return 1

        print("\nMaterialized views: OK")
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
