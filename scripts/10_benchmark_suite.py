#!/usr/bin/env python3
"""
10 — Run comprehensive benchmark suite and generate optimization report (Hour 10).

Usage:
    python3 scripts/10_benchmark_suite.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        from src.benchmark_suite import main as run_suite

        run_suite()
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
