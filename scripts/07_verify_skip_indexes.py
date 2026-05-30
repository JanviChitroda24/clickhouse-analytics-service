#!/usr/bin/env python3
"""
07 — Verify skip indexes exist on raw_trades.

Run after: python -m src.setup_skip_indexes

Usage:
    python3 scripts/07_verify_skip_indexes.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

EXPECTED = {"idx_ticker", "idx_price", "idx_trade_type"}


def main() -> int:
    try:
        from src.clickhouse_client import execute_query

        rows = execute_query("""
            SELECT name, type_full, expr
            FROM system.data_skipping_indices
            WHERE database = 'stock_analytics' AND table = 'raw_trades'
            ORDER BY name
        """)
        found = {name for name, _, _ in rows}

        print("--- skip indexes on raw_trades ---")
        for name, type_full, expr in rows:
            print(f"  {name:<18} {type_full:<30} {expr}")

        missing = EXPECTED - found
        if missing:
            print(f"\nMISSING: {missing}")
            print("Run: python -m src.setup_skip_indexes")
            return 1

        print("\nSkip indexes: OK (run benchmark_indexes for rows_read stats)")
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
