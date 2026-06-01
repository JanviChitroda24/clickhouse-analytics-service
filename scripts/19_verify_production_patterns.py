#!/usr/bin/env python3
"""
Verify production patterns in the FastAPI service:
  - TTL caching (smoke: endpoint returns, second call should be cache-friendly)
  - Cursor-based pagination (smoke: next_cursor enables the next page)
  - Request logging + graceful degradation (smoke: endpoints don't 500)

Prerequisites: server running
  uvicorn src.api.main:app --reload --port 8000
"""

import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    base = "http://localhost:8000"

    with httpx.Client(base_url=base, timeout=30.0) as client:
        # Market summary (cached)
        for i in range(2):
            resp = client.get("/api/v1/analytics/market/summary")
            if resp.status_code != 200:
                print(f"FAILED market_summary call {i+1}: HTTP {resp.status_code}", file=sys.stderr)
                print(resp.text[:500], file=sys.stderr)
                return 1
            data = resp.json()
            assert "top_movers" in data
            assert "sector_performance" in data
            assert "total_trades" in data

        # Trades cursor pagination
        resp1 = client.get("/api/v1/analytics/trades", params={"ticker": "AAPL", "limit": 5})
        if resp1.status_code != 200:
            print(f"FAILED trades page 1: HTTP {resp1.status_code}", file=sys.stderr)
            print(resp1.text[:500], file=sys.stderr)
            return 1
        page1 = resp1.json()
        assert page1["ticker"] == "AAPL"
        assert isinstance(page1["trades"], list)
        assert 0 < len(page1["trades"]) <= 5
        next_cursor = page1.get("next_cursor")
        assert next_cursor is not None

        resp2 = client.get(
            "/api/v1/analytics/trades",
            params={"ticker": "AAPL", "after": next_cursor, "limit": 5},
        )
        if resp2.status_code != 200:
            print(f"FAILED trades page 2: HTTP {resp2.status_code}", file=sys.stderr)
            print(resp2.text[:500], file=sys.stderr)
            return 1
        page2 = resp2.json()
        assert isinstance(page2["trades"], list)
        assert len(page2["trades"]) <= 5

        # Search endpoints (cached + degraded to 503 only if ES is down)
        resp_auto = client.get("/api/v1/search/autocomplete", params={"q": "Goo", "limit": 5})
        if resp_auto.status_code != 200:
            print(f"FAILED autocomplete: HTTP {resp_auto.status_code}", file=sys.stderr)
            print(resp_auto.text[:500], file=sys.stderr)
            return 1

    print("Production patterns verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

