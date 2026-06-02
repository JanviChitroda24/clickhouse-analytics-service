"""
Benchmark advanced ClickHouse features: dictGet vs JOIN, projection usage.

Compares sector grouping via JOIN vs dictionary lookup, and runs EXPLAIN
on time-first vs ticker-first filters to show projection selection.

Usage:
    python -m src.setup_advanced_features   # run first
    python -m src.benchmark_advanced
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.clickhouse_client import execute_query

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_REPORT_PATH = _REPO_ROOT / "docs" / "advanced_features_benchmark.md"

_BENCHMARK_RUNS = 5


def timed_query(sql: str, runs: int = _BENCHMARK_RUNS) -> tuple[float, list[tuple]]:
    """Return average latency (ms) and last result set."""
    times: list[float] = []
    result: list[tuple] = []
    for _ in range(runs):
        start = time.perf_counter()
        result = execute_query(sql)
        times.append((time.perf_counter() - start) * 1000)
    return sum(times) / len(times), result


def _time_filter_clause() -> str:
    """Build a time-range predicate using the latest trade in the table."""
    latest = execute_query("SELECT max(trade_time) FROM stock_analytics.raw_trades")[0][0]
    if latest is None:
        return "trade_time > now() - INTERVAL 1 DAY"
    return f"trade_time > toDateTime64('{latest}', 3) - INTERVAL 1 DAY"


def benchmark_dict_vs_join() -> dict[str, Any]:
    """Compare sector aggregation via JOIN vs dictGet()."""
    logger.info("Benchmark 1: dictGet() vs JOIN")
    logger.info("-" * 50)

    join_ms, join_result = timed_query(
        """
        SELECT m.sector, count() AS trades, round(avg(t.price), 2) AS avg_price
        FROM stock_analytics.raw_trades t
        INNER JOIN stock_analytics.company_metadata m ON t.ticker = m.ticker
        GROUP BY m.sector
        ORDER BY trades DESC
        """
    )

    dict_ms, dict_result = timed_query(
        """
        SELECT
            dictGet('company_dict', 'sector', ticker) AS sector,
            count() AS trades,
            round(avg(price), 2) AS avg_price
        FROM stock_analytics.raw_trades
        GROUP BY sector
        ORDER BY trades DESC
        """
    )

    speedup = join_ms / dict_ms if dict_ms > 0 else 0.0
    logger.info("  JOIN:     %7.1fms  (%s sectors)", join_ms, len(join_result))
    logger.info("  dictGet:  %7.1fms  (%s sectors)", dict_ms, len(dict_result))
    logger.info("  Speedup:  %.1fx", speedup)

    for row in dict_result[:5]:
        logger.info("    %s", row)

    return {
        "join_ms": join_ms,
        "dict_ms": dict_ms,
        "speedup": speedup,
        "join_rows": len(join_result),
        "dict_rows": len(dict_result),
    }


def benchmark_projection() -> dict[str, Any]:
    """Compare time-first filter (projection) vs ticker-first filter (primary sort)."""
    logger.info("")
    logger.info("Benchmark 2: Projection (time-first vs ticker-first)")
    logger.info("-" * 50)

    time_predicate = _time_filter_clause()

    proj_ms, proj_result = timed_query(
        f"""
        SELECT count(), round(avg(price), 2), max(price), min(price)
        FROM stock_analytics.raw_trades
        WHERE {time_predicate}
        """
    )

    orig_ms, orig_result = timed_query(
        """
        SELECT count(), round(avg(price), 2), max(price), min(price)
        FROM stock_analytics.raw_trades
        WHERE ticker = 'AAPL'
        """
    )

    logger.info(
        "  Time-first (projection path): %7.1fms  (%s rows)",
        proj_ms,
        proj_result[0][0] if proj_result else 0,
    )
    logger.info(
        "  Ticker-first (primary sort):  %7.1fms  (%s rows)",
        orig_ms,
        orig_result[0][0] if orig_result else 0,
    )

    explain_lines: list[str] = []
    logger.info("")
    logger.info("  EXPLAIN indexes (time-first query):")
    try:
        explain = execute_query(
            f"""
            EXPLAIN indexes = 1
            SELECT count(), avg(price)
            FROM stock_analytics.raw_trades
            WHERE {time_predicate}
            """
        )
        for row in explain:
            line = row[0]
            if any(token in line for token in ("Projection", "proj_", "PrimaryKey", "Granules", "Parts")):
                logger.info("    %s", line)
                explain_lines.append(line)
    except Exception as exc:
        logger.warning("  EXPLAIN not available or failed: %s", exc)

    return {
        "proj_ms": proj_ms,
        "orig_ms": orig_ms,
        "time_predicate": time_predicate,
        "explain_lines": explain_lines,
    }


def benchmark_dict_enriched_vwap() -> dict[str, Any]:
    """Sector + company enrichment via dictGet in one aggregation query."""
    logger.info("")
    logger.info("Bonus: dictGet-enriched VWAP top 10")
    logger.info("-" * 50)

    ms, result = timed_query(
        """
        SELECT
            dictGet('company_dict', 'sector', ticker) AS sector,
            dictGet('company_dict', 'company_name', ticker) AS company,
            ticker,
            count() AS trades,
            round(sum(price * quantity) / sum(quantity), 2) AS vwap
        FROM stock_analytics.raw_trades
        GROUP BY ticker
        ORDER BY vwap DESC
        LIMIT 10
        """
    )

    logger.info("  Enriched VWAP query: %.1fms", ms)
    for row in result[:5]:
        logger.info("    %s", row)

    return {"enriched_ms": ms, "top_rows": result[:5]}


def generate_report(
    dict_bench: dict[str, Any],
    proj_bench: dict[str, Any],
    enriched: dict[str, Any],
) -> None:
    """Write summary markdown to docs/advanced_features_benchmark.md."""
    lines = [
        "# Advanced ClickHouse Features Benchmark",
        "",
        f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"**Methodology:** {_BENCHMARK_RUNS} runs per query, averaged",
        "",
        "## dictGet vs JOIN (sector grouping)",
        "",
        "| Approach | Avg (ms) | Rows |",
        "|----------|---------:|-----:|",
        f"| JOIN | {dict_bench['join_ms']:.1f} | {dict_bench['join_rows']} |",
        f"| dictGet | {dict_bench['dict_ms']:.1f} | {dict_bench['dict_rows']} |",
        "",
        f"**Speedup:** {dict_bench['speedup']:.1f}x",
        "",
        "## Projection vs primary sort",
        "",
        f"**Time filter:** `{proj_bench['time_predicate']}`",
        "",
        "| Query pattern | Avg (ms) |",
        "|---------------|---------:|",
        f"| Time-first (projection) | {proj_bench['proj_ms']:.1f} |",
        f"| Ticker-first (ORDER BY) | {proj_bench['orig_ms']:.1f} |",
        "",
        "## TTL",
        "",
        "- `raw_trades`: 90-day TTL on `trade_time`",
        "- MVs (`mv_realtime_vwap`, etc.): persist after raw rows expire",
        "",
        "## EXPLAIN highlights (time-first)",
        "",
    ]

    if proj_bench.get("explain_lines"):
        for line in proj_bench["explain_lines"]:
            lines.append(f"- `{line}`")
    else:
        lines.append("- (Run locally with ClickHouse up for EXPLAIN output)")

    lines.extend(
        [
            "",
            "## Enriched VWAP (dictGet bonus)",
            "",
            f"**Latency:** {enriched['enriched_ms']:.1f}ms",
            "",
        ]
    )

    _REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Report written: %s", _REPORT_PATH)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Advanced Features Benchmark")
    logger.info("=" * 60)

    dict_bench = benchmark_dict_vs_join()
    proj_bench = benchmark_projection()
    enriched = benchmark_dict_enriched_vwap()

    generate_report(dict_bench, proj_bench, enriched)

    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary:")
    logger.info(
        "  dictGet vs JOIN: %.1fms vs %.1fms (%.1fx)",
        dict_bench["dict_ms"],
        dict_bench["join_ms"],
        dict_bench["speedup"],
    )
    logger.info("  Time-first (projection): %.1fms", proj_bench["proj_ms"])
    logger.info("  Ticker-first (original): %.1fms", proj_bench["orig_ms"])
    logger.info("  TTL: 90-day retention on raw_trades (MVs persist)")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
