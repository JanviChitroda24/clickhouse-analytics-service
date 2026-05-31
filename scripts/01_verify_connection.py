#!/usr/bin/env python3
"""
ClickHouse connection smoke test.

Run after: docker compose up -d

Usage:
    python3 scripts/01_verify_connection.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        from src.clickhouse_client import execute_query

        version = execute_query("SELECT version()")[0][0]
        execute_query("SELECT 1")

        print(f"ClickHouse version: {version}")
        print("Connection: OK")
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        print("Tip: wait ~30s after `docker compose up -d`, then retry.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
