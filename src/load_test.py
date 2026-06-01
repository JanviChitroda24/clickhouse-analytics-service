"""
Concurrent load test for all public API endpoints.

Sends many parallel requests per route, computes p50/p95/p99 latencies,
and writes docs/api_load_test_report.md for portfolio documentation.

Prerequisites:
    uvicorn src.api.main:app --reload --port 8000

Usage:
    python -m src.load_test
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)
# Keep load-test output readable (httpx logs every request at INFO by default).
logging.getLogger("httpx").setLevel(logging.WARNING)

BASE_URL = "http://localhost:8000"
REPO_ROOT = Path(__file__).resolve().parent.parent
REPORT_PATH = REPO_ROOT / "docs" / "api_load_test_report.md"

# (HTTP method, path with query string, human-readable label)
ENDPOINTS: list[tuple[str, str, str]] = [
    ("GET", "/health", "Health check"),
    ("GET", "/api/v1/analytics/vwap/AAPL?granularity=5min&limit=10", "VWAP (AAPL, 5min)"),
    ("GET", "/api/v1/analytics/top-movers?limit=5", "Top movers"),
    ("GET", "/api/v1/analytics/sectors/performance", "Sector performance"),
    ("GET", "/api/v1/analytics/anomalies?limit=10", "Anomalies"),
    ("GET", "/api/v1/analytics/market/summary", "Market summary"),
    ("GET", "/api/v1/analytics/trades?ticker=AAPL&limit=10", "Trades (AAPL)"),
    ("GET", "/api/v1/search/autocomplete?q=Goo", "Autocomplete"),
    ("GET", "/api/v1/search/trades?q=financial+services&limit=5", "Full-text search"),
    ("GET", "/api/v1/search/fuzzy?q=Micorsoft&limit=5", "Fuzzy search"),
    ("GET", "/api/v1/search/similar/AAPL?limit=5", "Similar tickers"),
]


def percentile(data: list[float], p: float) -> float:
    """Compute the p-th percentile (linear interpolation)."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100)
    lower = int(k)
    upper = min(lower + 1, len(sorted_data) - 1)
    weight = k - lower
    return sorted_data[lower] + weight * (sorted_data[upper] - sorted_data[lower])


async def hit_endpoint(
    client: httpx.AsyncClient,
    path: str,
    concurrency: int,
) -> tuple[list[float], int]:
    """Fire `concurrency` parallel GETs; return successful latencies (ms) and error count."""
    times: list[float] = []
    errors = 0

    async def single_request() -> None:
        nonlocal errors
        try:
            start = time.perf_counter()
            response = await client.get(f"{BASE_URL}{path}")
            elapsed_ms = (time.perf_counter() - start) * 1000
            if response.status_code == 200:
                times.append(elapsed_ms)
            else:
                errors += 1
        except Exception:
            errors += 1

    await asyncio.gather(*(single_request() for _ in range(concurrency)))
    return times, errors


async def run_load_test(concurrency: int = 50) -> list[dict]:
    """Run load test across all endpoints; return per-endpoint stats."""
    results: list[dict] = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        logger.info("Warming up first endpoints...")
        for _, path, _ in ENDPOINTS[:3]:
            await client.get(f"{BASE_URL}{path}")

        logger.info("Load test: %d concurrent requests per endpoint", concurrency)

        for _method, path, name in ENDPOINTS:
            times, errors = await hit_endpoint(client, path, concurrency)

            if times:
                p50 = percentile(times, 50)
                p95 = percentile(times, 95)
                p99 = percentile(times, 99)
                avg = statistics.mean(times)
                min_t = min(times)
                max_t = max(times)
            else:
                p50 = p95 = p99 = avg = min_t = max_t = 0.0

            result = {
                "name": name,
                "path": path.split("?")[0],
                "requests": concurrency,
                "success": len(times),
                "errors": errors,
                "p50": p50,
                "p95": p95,
                "p99": p99,
                "avg": avg,
                "min": min_t,
                "max": max_t,
            }
            results.append(result)

            status = "OK" if errors == 0 else f"{errors} errors"
            logger.info(
                "%-25s p50=%6.0fms p95=%6.0fms p99=%6.0fms %s",
                name,
                p50,
                p95,
                p99,
                status,
            )

    return results


def generate_report(results: list[dict], concurrency: int) -> None:
    """Write markdown summary to docs/api_load_test_report.md."""
    lines: list[str] = [
        "# API Load Test Report",
        "",
        f"**Generated:** {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"**Concurrency:** {concurrency} simultaneous requests per endpoint",
        "",
        f"**Endpoints tested:** {len(results)}",
        "",
        "## Results",
        "",
        "| Endpoint | p50 (ms) | p95 (ms) | p99 (ms) | Avg (ms) | Errors |",
        "|----------|---------:|---------:|---------:|---------:|-------:|",
    ]

    for row in results:
        lines.append(
            f"| {row['name']} | {row['p50']:.0f} | {row['p95']:.0f} | "
            f"{row['p99']:.0f} | {row['avg']:.0f} | {row['errors']} |"
        )

    lines.append("")
    lines.append("## Summary")

    all_p50 = [r["p50"] for r in results if r["p50"] > 0]
    all_p99 = [r["p99"] for r in results if r["p99"] > 0]
    total_errors = sum(r["errors"] for r in results)

    if all_p50:
        lines.append(f"- **Median p50:** {statistics.median(all_p50):.0f}ms")
    if all_p99:
        lines.append(f"- **Median p99:** {statistics.median(all_p99):.0f}ms")
    lines.append(f"- **Total errors:** {total_errors}/{concurrency * len(results)}")

    if results and all(r["p99"] < 500 for r in results if r["p99"] > 0):
        lines.append("- **Status:** All endpoints p99 < 500ms under load")
    else:
        slow = [r["name"] for r in results if r["p99"] >= 500]
        if slow:
            lines.append(f"- **Status:** Slow endpoints (p99 >= 500ms): {', '.join(slow)}")

    lines.extend(
        [
            "",
            "## Architecture",
            "",
            "```",
            "N concurrent clients -> FastAPI (uvicorn)",
            "    |-> /analytics/* -> ClickHouse (pooled client, TTL cache)",
            "    |-> /search/*    -> ElasticSearch (pooled client, TTL cache)",
            "```",
            "",
        ]
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Report saved: %s", REPORT_PATH)


async def main() -> None:
    logger.info("=" * 60)
    logger.info("API Load Test")
    logger.info("=" * 60)

    concurrency = 50
    results = await run_load_test(concurrency)
    generate_report(results, concurrency)
    logger.info("Load test complete.")


if __name__ == "__main__":
    asyncio.run(main())
