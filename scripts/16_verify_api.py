#!/usr/bin/env python3
"""
Verify FastAPI health endpoints.

Prerequisites: uvicorn src.api.main:app --reload --port 8000

Usage:
    python3 scripts/16_verify_api.py
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
    try:
        with httpx.Client(base_url=base, timeout=10.0) as client:
            health = client.get("/health")
            health.raise_for_status()
            data = health.json()
            print("GET /health:", data["status"])
            print("  clickhouse:", data["clickhouse"]["status"], data["clickhouse"].get("version"))
            print("  elasticsearch:", data["elasticsearch"]["status"], data["elasticsearch"].get("version"))

            if data["status"] not in ("healthy", "degraded"):
                print("FAILED: unexpected overall status", file=sys.stderr)
                return 1
            if data["clickhouse"]["status"] != "healthy":
                print("FAILED: ClickHouse unhealthy", file=sys.stderr)
                return 1
            if data["elasticsearch"]["status"] != "healthy":
                print("FAILED: ElasticSearch unhealthy", file=sys.stderr)
                return 1

            docs = client.get("/docs")
            if docs.status_code != 200:
                print("FAILED: /docs not reachable", file=sys.stderr)
                return 1
            print("GET /docs: OK (Swagger UI)")
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

    print("API verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
