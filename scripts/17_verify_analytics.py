#!/usr/bin/env python3
"""
Verify ClickHouse analytics API endpoints.

Prerequisites: uvicorn src.api.main:app --reload --port 8000

Usage:
    python3 scripts/17_verify_analytics.py
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
        ("/api/v1/analytics/vwap/AAPL?granularity=5min&limit=5", "vwap"),
        ("/api/v1/analytics/top-movers?limit=5", "top_movers"),
        ("/api/v1/analytics/sectors/performance", "sectors"),
        ("/api/v1/analytics/anomalies?min_deviation=2.0&limit=5", "anomalies"),
        ("/api/v1/analytics/market/summary", "market_summary"),
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
                if name == "vwap":
                    assert data.get("ticker") == "AAPL"
                    assert "data" in data
                elif name == "market_summary":
                    assert "total_trades" in data
                    assert "top_movers" in data
                else:
                    assert isinstance(data, list)
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

    print("Analytics API verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
