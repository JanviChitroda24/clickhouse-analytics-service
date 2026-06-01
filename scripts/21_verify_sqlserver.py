#!/usr/bin/env python3
"""
Verify SQL Server setup and optional three-engine report.

Usage:
    python3 scripts/21_verify_sqlserver.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    from src.clickhouse_client import execute_query as ch_query
    from src.sqlserver_client import close_sqlserver, execute_query as sql_query

    try:
        ch_count = int(ch_query("SELECT count() FROM stock_analytics.raw_trades")[0][0])
        sql_count = int(
            sql_query("SELECT COUNT(*) FROM stock_analytics.dbo.raw_trades")[0][0]
        )
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        print(
            "Ensure SQL Server is running: docker compose up -d sqlserver",
            file=sys.stderr,
        )
        print("Then: python -m src.sqlserver_setup", file=sys.stderr)
        return 1
    finally:
        close_sqlserver()

    print(f"ClickHouse rows: {ch_count:,}")
    print(f"SQL Server rows: {sql_count:,}")

    if ch_count != sql_count:
        print("FAILED: row count mismatch", file=sys.stderr)
        return 1

    report = Path(__file__).resolve().parent.parent / "docs" / "three_engine_comparison.md"
    if report.exists():
        print(f"OK report exists: {report}")
    else:
        print("Note: run python -m src.sqlserver_benchmark to generate report")

    print("SQL Server verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
