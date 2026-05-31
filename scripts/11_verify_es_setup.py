#!/usr/bin/env python3
"""
ElasticSearch connection + index mapping verification.

Usage:
    python3 scripts/11_verify_es_setup.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        from src.es_client import get_es_client
        from src.es_index_setup import create_index, verify_index

        client = get_es_client()
        version = client.info()["version"]["number"]
        print(f"ElasticSearch version: {version}")

        create_index(delete_existing=True)
        if not verify_index():
            return 1

        print("ES setup: OK")
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        print("Tip: wait 30-60s after `docker compose up -d`, then retry.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
