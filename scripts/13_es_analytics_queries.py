#!/usr/bin/env python3
"""
Run ES analytics queries and head-to-head CH comparison.

Usage:
    python3 scripts/13_es_analytics_queries.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        from src.es_analytics_queries import main as run_queries

        run_queries()
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
