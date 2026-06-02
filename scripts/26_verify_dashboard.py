#!/usr/bin/env python3
"""Check FastAPI is reachable before starting the Streamlit dashboard."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dashboard.api_client import AnalyticsApiClient


def main() -> int:
    client = AnalyticsApiClient()
    health = client.health()
    if health.error:
        print(f"API not reachable: {health.error}")
        print("Start: uvicorn src.api.main:app --reload --port 8000")
        return 1
    print(f"API OK — status={(health.data or {}).get('status')}, {health.latency_ms:.0f} ms")
    print("Run: streamlit run dashboard/app.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
