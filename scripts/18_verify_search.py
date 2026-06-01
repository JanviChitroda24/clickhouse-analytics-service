#!/usr/bin/env python3
"""
Verify ElasticSearch search API endpoints.

Prerequisites: uvicorn src.api.main:app --reload --port 8000

Usage:
    python3 scripts/18_verify_search.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        import httpx
    except ImportError:
        print("FAILED: httpx not installed", file=sys.stderr)
        return 1

    base = "http://localhost:8000"
    endpoints = [
        ("/api/v1/search/autocomplete?q=Goo&limit=5", "autocomplete"),
        ("/api/v1/search/trades?q=financial+services&limit=5", "trades"),
        ("/api/v1/search/fuzzy?q=Micorsoft&limit=5", "fuzzy"),
        ("/api/v1/search/similar/AAPL?limit=5", "similar"),
    ]

    try:
        with httpx.Client(base_url=base, timeout=30.0) as client:
            for path, name in endpoints:
                resp = client.get(path)
                if resp.status_code != 200:
                    print(f"FAILED {name}: HTTP {resp.status_code}", file=sys.stderr)
                    print(resp.text[:500], file=sys.stderr)
                    return 1
                data = resp.json()
                if name == "autocomplete":
                    assert isinstance(data, list)
                elif name == "similar":
                    assert isinstance(data, list)
                    for item in data:
                        assert item["ticker"] != "AAPL"
                else:
                    assert "total_hits" in data
                    assert "results" in data
                print(f"OK {name}: {resp.status_code}")
    except httpx.ConnectError:
        print(
            "FAILED: cannot connect to localhost:8000 — start server with:\n"
            "  uvicorn src.api.main:app --reload --port 8000",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1

    print("Search API verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
