#!/usr/bin/env python3
"""
08 — Run partition pruning + ORDER BY benchmark (Hour 8).

Usage:
    python3 scripts/08_benchmark_partitions.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        from src.benchmark_partitions import main as run_benchmark

        run_benchmark()
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
