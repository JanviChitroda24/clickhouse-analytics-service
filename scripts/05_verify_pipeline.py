#!/usr/bin/env python3
"""
End-to-end pipeline verification.

Run while or after Week 3 producer at 100 eps for 2 minutes.
Generates docs/clickhouse_verification_report.md.

Usage:
    python3 scripts/05_verify_pipeline.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        from src.verify_pipeline import main as verify_main

        verify_main()
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
