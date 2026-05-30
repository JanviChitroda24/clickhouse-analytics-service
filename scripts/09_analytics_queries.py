#!/usr/bin/env python3
"""
09 — Run 8 production analytics queries with timing (Hour 9).

Usage:
    python3 scripts/09_analytics_queries.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        from src.analytics_queries import main as run_queries

        run_queries()
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
