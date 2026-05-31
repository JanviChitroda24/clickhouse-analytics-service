#!/usr/bin/env python3
"""
Head-to-head engine comparison.

Usage:
    python3 scripts/15_engine_comparison.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        from src.engine_comparison import main as run_comparison

        run_comparison()
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
