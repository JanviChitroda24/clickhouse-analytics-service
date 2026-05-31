#!/usr/bin/env python3
"""
ClickHouse schema verification.

Run after: python -m src.setup_clickhouse

Checks:
  - Tables exist in stock_analytics
  - Engine + sort key match expectations
  - SHOW CREATE TABLE snippet per table
  - Row counts

Usage:
    python3 scripts/02_verify_schema.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

EXPECTED_TABLES = {
    "raw_trades": {
        "engine": "ReplacingMergeTree",
        "sort_key": "ticker, trade_time, trade_id",
    },
    "vwap_1min": {
        "engine": "MergeTree",
        "sort_key": "ticker, window_start",
    },
    "company_metadata": {
        "engine": "MergeTree",
        "sort_key": "ticker",
    },
}


def main() -> int:
    try:
        from src.clickhouse_client import execute_query

        print("--- Tables (system.tables) ---")
        rows = execute_query("""
            SELECT name, engine, sorting_key
            FROM system.tables
            WHERE database = 'stock_analytics'
            ORDER BY name
        """)

        if not rows:
            print("No tables in stock_analytics.")
            print("Run: python -m src.setup_clickhouse")
            return 1

        found = {name: (engine, sort_key) for name, engine, sort_key in rows}

        print(f"{'Table':<22} {'Engine':<22} {'Sort Key'}")
        print("-" * 70)
        for name, engine, sort_key in rows:
            print(f"{name:<22} {engine:<22} {sort_key}")

        all_ok = True
        for table, expected in EXPECTED_TABLES.items():
            if table not in found:
                print(f"\nMISSING: stock_analytics.{table}")
                all_ok = False
                continue

            engine, sort_key = found[table]
            if expected["engine"] not in engine:
                print(f"\nFAIL {table}: expected engine containing '{expected['engine']}', got '{engine}'")
                all_ok = False
            if sort_key != expected["sort_key"]:
                print(f"\nFAIL {table}: expected sort key '{expected['sort_key']}', got '{sort_key}'")
                all_ok = False

        print("\n--- SHOW CREATE TABLE ---")
        for table in EXPECTED_TABLES:
            if table not in found:
                continue
            result = execute_query(f"SHOW CREATE TABLE stock_analytics.{table}")
            ddl = result[0][0]
            print(f"\n--- {table} ---")
            print(ddl[:200] + ("..." if len(ddl) > 200 else ""))

        print("\n--- Row counts ---")
        for table in EXPECTED_TABLES:
            if table not in found:
                continue
            count = execute_query(f"SELECT count() FROM stock_analytics.{table}")[0][0]
            print(f"{table}: {count} rows")

        if all_ok:
            print("\nSchema: OK")
            return 0

        print("\nSchema: FAILED")
        return 1
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        print("Tip: run 01_verify_connection.py first, then setup_clickhouse.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
