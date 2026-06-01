"""
Three-engine benchmark: ClickHouse vs ElasticSearch vs SQL Server.

Runs the same analytics-style queries on identical trade data and writes
docs/three_engine_comparison.md with timings and winner per category.

Usage:
    python -m src.sqlserver_benchmark

Prerequisites: ClickHouse populated, ES index loaded, SQL Server setup complete.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from src.clickhouse_client import execute_query as ch_query
from src.config import get_settings
from src.es_client import get_es_client
from src.sqlserver_client import close_sqlserver, execute_query as sql_query

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BENCHMARK_RUNS = 5
_REPO_ROOT = Path(__file__).resolve().parent.parent
REPORT_PATH = _REPO_ROOT / "docs" / "three_engine_comparison.md"


def _response_data(result: Any) -> dict:
    return result.body if hasattr(result, "body") else result


def time_query(fn: Callable[..., Any], *args: Any, runs: int = BENCHMARK_RUNS) -> tuple[float, Any]:
    """Average elapsed ms over multiple runs."""
    times: list[float] = []
    result = None
    for _ in range(runs):
        start = time.perf_counter()
        result = fn(*args)
        times.append((time.perf_counter() - start) * 1000)
    return sum(times) / len(times), result


def time_es(body: dict, runs: int = BENCHMARK_RUNS) -> float:
    """Average ES search/aggs latency in ms."""
    client = get_es_client()
    index_name = get_settings().es_index_trades
    times: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        client.search(index=index_name, **body)
        times.append((time.perf_counter() - start) * 1000)
    return sum(times) / len(times)


BENCHMARKS: list[dict[str, Any]] = [
    {
        "name": "VWAP aggregation (all tickers)",
        "ch": """
            SELECT ticker,
                   round(sum(price * quantity) / sum(quantity), 2) AS vwap,
                   count() AS trades
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY trades DESC
            LIMIT 10
        """,
        "sql": """
            SELECT TOP 10 ticker,
                   SUM(price * quantity) / NULLIF(SUM(quantity), 0) AS vwap,
                   COUNT(*) AS trades
            FROM stock_analytics.dbo.raw_trades
            GROUP BY ticker
            ORDER BY COUNT(*) DESC
        """,
        "es": {
            "size": 0,
            "aggs": {
                "by_ticker": {
                    "terms": {"field": "ticker", "size": 10},
                    "aggs": {"vwap": {"avg": {"field": "price"}}},
                }
            },
        },
        "note": "Columnar CH reads price+quantity only; SQL reads full rows per group.",
    },
    {
        "name": "Single ticker filter (AAPL stats)",
        "ch": """
            SELECT count(), avg(price), max(price), min(price)
            FROM stock_analytics.raw_trades
            WHERE ticker = 'AAPL'
        """,
        "sql": """
            SELECT COUNT(*), AVG(price), MAX(price), MIN(price)
            FROM stock_analytics.dbo.raw_trades
            WHERE ticker = 'AAPL'
        """,
        "es": {
            "size": 0,
            "query": {"term": {"ticker": "AAPL"}},
            "aggs": {"stats": {"extended_stats": {"field": "price"}}},
        },
        "note": "All three use indexes: CH primary index, SQL B-tree, ES term lookup.",
    },
    {
        "name": "Top movers (price range per ticker)",
        "ch": """
            SELECT ticker,
                   max(price) - min(price) AS price_range,
                   count() AS trades
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY price_range DESC
            LIMIT 10
        """,
        "sql": """
            SELECT TOP 10 ticker,
                   MAX(price) - MIN(price) AS price_range,
                   COUNT(*) AS trades
            FROM stock_analytics.dbo.raw_trades
            GROUP BY ticker
            ORDER BY MAX(price) - MIN(price) DESC
        """,
        "es": {
            "size": 0,
            "aggs": {
                "by_ticker": {
                    "terms": {"field": "ticker", "size": 10},
                    "aggs": {
                        "high": {"max": {"field": "price"}},
                        "low": {"min": {"field": "price"}},
                    },
                }
            },
        },
        "note": "Min/max per ticker: CH scans price column; SQL scans full rows.",
    },
    {
        "name": "Buy/sell pressure",
        "ch": """
            SELECT ticker,
                   countIf(side = 'BUY') AS buys,
                   countIf(side = 'SELL') AS sells
            FROM stock_analytics.raw_trades
            GROUP BY ticker
        """,
        "sql": """
            SELECT ticker,
                   SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) AS buys,
                   SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) AS sells
            FROM stock_analytics.dbo.raw_trades
            GROUP BY ticker
        """,
        "es": {
            "size": 0,
            "aggs": {
                "by_ticker": {
                    "terms": {"field": "ticker", "size": 30},
                    "aggs": {
                        "buys": {"filter": {"term": {"side": "BUY"}}},
                        "sells": {"filter": {"term": {"side": "SELL"}}},
                    },
                }
            },
        },
        "note": "CH countIf vs SQL CASE vs ES filter aggregations.",
    },
    {
        "name": "Time-range filter (recent trades)",
        "ch": """
            SELECT count(), avg(price)
            FROM stock_analytics.raw_trades
            WHERE trade_time > '2026-05-26 15:00:00'
        """,
        "sql": """
            SELECT COUNT(*), AVG(price)
            FROM stock_analytics.dbo.raw_trades
            WHERE trade_time > '2026-05-26 15:00:00'
        """,
        "es": {
            "size": 0,
            "query": {"range": {"trade_time": {"gte": "2026-05-26T15:00:00"}}},
            "aggs": {"avg_price": {"avg": {"field": "price"}}},
        },
        "note": "CH partition pruning + columnar; SQL B-tree on trade_time still reads rows.",
    },
]


def run_benchmarks() -> list[dict[str, Any]]:
    """Run each benchmark on CH, SQL Server, and ES."""
    results: list[dict[str, Any]] = []

    for bench in BENCHMARKS:
        name = bench["name"]
        logger.info("%s", name)

        ch_ms, _ = time_query(ch_query, bench["ch"])
        logger.info("  CH:  %7.1fms", ch_ms)

        sql_ms, _ = time_query(sql_query, bench["sql"])
        logger.info("  SQL: %7.1fms", sql_ms)

        es_ms = time_es(bench["es"])
        logger.info("  ES:  %7.1fms", es_ms)

        times = {"CH": ch_ms, "SQL": sql_ms, "ES": es_ms}
        winner = min(times, key=times.get)

        logger.info("  Winner: %s", winner)

        results.append(
            {
                "name": name,
                "ch": ch_ms,
                "sql": sql_ms,
                "es": es_ms,
                "winner": winner,
                "note": bench["note"],
            }
        )

    return results


def generate_report(results: list[dict[str, Any]]) -> None:
    """Write markdown comparison report."""
    ch_count = int(ch_query("SELECT count() FROM stock_analytics.raw_trades")[0][0])

    lines = [
        "# Three-Engine Comparison: ClickHouse vs ElasticSearch vs SQL Server",
        "",
        f"**Generated:** {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"**Dataset:** {ch_count:,} identical rows in all three engines",
        "",
        "## Benchmark Results",
        "",
        "| Query | ClickHouse (ms) | SQL Server (ms) | ElasticSearch (ms) | Winner | Why |",
        "|-------|----------------:|----------------:|-------------------:|:------:|-----|",
    ]

    for row in results:
        lines.append(
            f"| {row['name']} | {row['ch']:.0f} | {row['sql']:.0f} | {row['es']:.0f} | "
            f"{row['winner']} | {row['note']} |"
        )

    lines.extend(
        [
            "",
            "## Engine Strengths",
            "",
            "### ClickHouse (columnar OLAP)",
            "- Fastest for aggregations on large scans (reads only needed columns)",
            "- Materialized views and skip indexes for analytics",
            "- Best for: dashboards, VWAP, time-series, OLAP",
            "",
            "### ElasticSearch (inverted index)",
            "- Autocomplete, fuzzy match, full-text search, relevance ranking",
            "- Denormalized documents avoid JOINs for sector/company fields",
            "- Best for: search bars, discovery, text queries",
            "",
            "### SQL Server (row-store OLTP)",
            "- ACID transactions, stored procedures, row-level updates/deletes",
            "- B-tree indexes excel at point lookups and transactional workloads",
            "- Best for: ledgers, order management, systems of record",
            "",
            "## When to Use Which",
            "",
            "| Workload | Engine |",
            "|----------|--------|",
            "| Aggregations at scale | ClickHouse |",
            "| Full-text / autocomplete | ElasticSearch |",
            "| Transactional updates / ACID | SQL Server |",
            "",
        ]
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Report saved: %s", REPORT_PATH)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Three-engine benchmark: CH vs ES vs SQL Server")
    logger.info("=" * 60)

    try:
        results = run_benchmarks()

        logger.info("")
        logger.info("%-35s %8s %8s %8s %8s", "Query", "CH", "SQL", "ES", "Winner")
        logger.info("-" * 72)
        for row in results:
            logger.info(
                "  %-33s %7.0f %7.0f %7.0f %8s",
                row["name"][:33],
                row["ch"],
                row["sql"],
                row["es"],
                row["winner"],
            )

        generate_report(results)
    finally:
        close_sqlserver()

    logger.info("Three-engine benchmark complete.")


if __name__ == "__main__":
    main()
