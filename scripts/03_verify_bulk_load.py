#!/usr/bin/env python3
"""
Verify bulk load: Delta row counts vs ClickHouse.

Run after: python -m src.bulk_loader

Usage:
    python3 scripts/03_verify_bulk_load.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        from deltalake import DeltaTable

        from src.clickhouse_client import execute_query
        from src.config import get_settings

        settings = get_settings()
        raw_path = settings.resolve_path(settings.delta_raw_trades_path)
        vwap_path = settings.resolve_path(settings.delta_vwap_1min_path)

        delta_raw = len(DeltaTable(str(raw_path)).to_pandas())
        delta_vwap = len(DeltaTable(str(vwap_path)).to_pandas())

        ch_raw = execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]
        ch_vwap = execute_query("SELECT count() FROM stock_analytics.vwap_1min")[0][0]
        ch_meta = execute_query("SELECT count() FROM stock_analytics.company_metadata")[0][0]

        print("--- Delta vs ClickHouse ---")
        print(f"raw_trades:      Delta {delta_raw:>8}  |  CH {ch_raw:>8}  |  {'OK' if delta_raw == ch_raw else 'MISMATCH'}")
        print(f"vwap_1min:       Delta {delta_vwap:>8}  |  CH {ch_vwap:>8}  |  {'OK' if delta_vwap == ch_vwap else 'MISMATCH'}")
        print(f"company_metadata: (CSV)           |  CH {ch_meta:>8}  |  {'OK' if ch_meta >= 25 else 'CHECK'}")

        if delta_raw == ch_raw and delta_vwap == ch_vwap and ch_meta >= 25:
            print("\nBulk load verification: OK")
            return 0

        print("\nBulk load verification: FAILED")
        return 1
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
