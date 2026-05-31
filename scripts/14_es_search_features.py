#!/usr/bin/env python3
"""
14 — ES search features demo (Hour 14).

Usage:
    python3 scripts/14_es_search_features.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        from src.es_search_features import main as run_features

        run_features()
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
