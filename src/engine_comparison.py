"""
Head-to-head benchmark: ClickHouse vs ElasticSearch (Hour 15).

Runs 8 query categories across both engines (or marks N/A where only one can run),
records timing, declares winners, and writes docs/engine_comparison_report.md.

Categories:
  - Aggregations (VWAP, sector, buy/sell) — columnar vs doc store
  - Time-range filters — partition pruning vs inverted index
  - Point lookups — sparse primary index vs term lookup
  - Search-only (full-text, fuzzy, autocomplete) — ES only

Usage:
    python -m src.engine_comparison

Prerequisites: ClickHouse raw_trades populated, ES stock-trades index (Hour 12).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.clickhouse_client import execute_query as ch_query
from src.config import get_settings
from src.es_client import get_es_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BENCHMARK_RUNS = 5
_REPO_ROOT = Path(__file__).resolve().parent.parent
REPORT_PATH = _REPO_ROOT / "docs" / "engine_comparison_report.md"


def _response_data(result: Any) -> dict:
    """Normalize elasticsearch-py 8.x ObjectApiResponse to dict."""
    return result.body if hasattr(result, "body") else result


def _hit_total(hits_section: dict) -> int:
    """ES 7+ total may be int or {value: N}."""
    total = hits_section.get("total", 0)
    if isinstance(total, dict):
        return int(total.get("value", 0))
    return int(total)


def time_ch(sql: str, runs: int = BENCHMARK_RUNS) -> tuple[float, int]:
    """Run ClickHouse SQL `runs` times; return average ms and row count."""
    times: list[float] = []
    result: list | None = None
    for _ in range(runs):
        start = time.perf_counter()
        result = ch_query(sql)
        times.append((time.perf_counter() - start) * 1000)
    return sum(times) / len(times), len(result or [])


def time_es(body: dict, runs: int = BENCHMARK_RUNS) -> tuple[float, int]:
    """Run ES search `runs` times; return average ms and hit/bucket count."""
    client = get_es_client()
    index_name = get_settings().es_index_trades
    times: list[float] = []
    result: dict | None = None
    for _ in range(runs):
        start = time.perf_counter()
        raw = client.search(index=index_name, **body)
        times.append((time.perf_counter() - start) * 1000)
        result = _response_data(raw)

    count = 0
    if result:
        if "aggregations" in result:
            for agg in result["aggregations"].values():
                if "buckets" in agg:
                    count = max(count, len(agg["buckets"]))
        else:
            count = _hit_total(result.get("hits", {}))
    return sum(times) / len(times), count


def time_es_suggest(body: dict, runs: int = BENCHMARK_RUNS) -> tuple[float, int]:
    """Time ES completion suggester; return average ms and suggestion count."""
    client = get_es_client()
    index_name = get_settings().es_index_trades
    times: list[float] = []
    result: dict | None = None
    for _ in range(runs):
        start = time.perf_counter()
        raw = client.search(index=index_name, **body)
        times.append((time.perf_counter() - start) * 1000)
        result = _response_data(raw)

    suggestions = (
        result.get("suggest", {}).get("ticker-suggest", [{}])[0].get("options", [])
        if result
        else []
    )
    return sum(times) / len(times), len(suggestions)


# ── Comparison definitions ────────────────────────────────────────
# Each entry: CH SQL (or None), ES body, expected winner, architectural why.
# Hour 13 tested analytics only (biased toward CH). Hour 15 adds search categories.

COMPARISONS: list[dict[str, Any]] = [
    {
        "name": "Aggregation: VWAP by ticker",
        "category": "aggregation",
        "ch_sql": """
            SELECT ticker,
                   round(sum(price * quantity) / sum(quantity), 2) AS vwap,
                   count() AS trades
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY trades DESC
            LIMIT 10
        """,
        "es_body": {
            "size": 0,
            "aggs": {
                "by_ticker": {
                    "terms": {"field": "ticker", "size": 10, "order": {"_count": "desc"}},
                    "aggs": {
                        "avg_price": {"avg": {"field": "price"}},
                        "volume": {"sum": {"field": "quantity"}},
                    },
                }
            },
        },
        "expected_winner": "ClickHouse",
        "why": "Columnar storage reads only price + quantity columns. ES reads full JSON documents.",
    },
    {
        "name": "Time-range: trades last 3 days",
        "category": "time_range",
        "ch_sql": """
            SELECT ticker, count(), avg(price)
            FROM stock_analytics.raw_trades
            WHERE trade_time > now() - INTERVAL 3 DAY
            GROUP BY ticker
            ORDER BY count() DESC
            LIMIT 10
        """,
        "es_body": {
            "size": 0,
            "query": {"range": {"trade_time": {"gte": "now-3d"}}},
            "aggs": {
                "by_ticker": {
                    "terms": {"field": "ticker", "size": 10, "order": {"_count": "desc"}},
                    "aggs": {"avg_price": {"avg": {"field": "price"}}},
                }
            },
        },
        "expected_winner": "ClickHouse",
        "why": "Partition pruning skips entire months. Columnar reads fewer bytes.",
    },
    {
        "name": "Point lookup: single ticker stats",
        "category": "point_lookup",
        "ch_sql": """
            SELECT ticker, count(), avg(price), max(price), min(price)
            FROM stock_analytics.raw_trades
            WHERE ticker = 'AAPL'
            GROUP BY ticker
        """,
        "es_body": {
            "size": 0,
            "query": {"bool": {"filter": [{"term": {"ticker": "AAPL"}}]}},
            "aggs": {"price_stats": {"extended_stats": {"field": "price"}}},
        },
        "expected_winner": "Competitive",
        "why": "CH uses primary index binary search. ES uses inverted index term lookup. Both fast.",
    },
    {
        "name": "Sector performance (JOIN vs denormalized)",
        "category": "aggregation",
        "ch_sql": """
            SELECT m.sector,
                   count() AS trades,
                   round(avg(t.price), 2) AS avg_price,
                   count(DISTINCT t.ticker) AS tickers
            FROM stock_analytics.raw_trades t
            INNER JOIN stock_analytics.company_metadata m ON t.ticker = m.ticker
            GROUP BY m.sector
            ORDER BY trades DESC
        """,
        "es_body": {
            "size": 0,
            "aggs": {
                "by_sector": {
                    "terms": {"field": "sector", "size": 20},
                    "aggs": {
                        "avg_price": {"avg": {"field": "price"}},
                        "ticker_count": {"cardinality": {"field": "ticker"}},
                    },
                }
            },
        },
        "expected_winner": "ES (denormalized)",
        "why": "ES has sector embedded in each doc — no JOIN. CH must JOIN company_metadata.",
    },
    {
        "name": "Full-text: search 'financial services'",
        "category": "search",
        "ch_sql": None,
        "es_body": {
            "query": {
                "multi_match": {
                    "query": "financial services",
                    "fields": ["company_name", "sector"],
                }
            },
            "size": 5,
            "collapse": {"field": "ticker"},
            "_source": ["ticker", "sector"],
        },
        "expected_winner": "ES only",
        "why": "ClickHouse has no full-text search. ES inverted index + relevance ranking.",
    },
    {
        "name": "Fuzzy match: 'APPL' (typo for AAPL)",
        "category": "search",
        "ch_sql": None,
        "es_body": {
            "query": {
                "match": {
                    "ticker_text": {
                        "query": "APPL",
                        "fuzziness": "AUTO",
                    }
                }
            },
            "size": 3,
            "_source": ["ticker", "company_name"],
        },
        "expected_winner": "ES only",
        "why": "ClickHouse has no edit-distance matching. ES uses inverted index for fuzzy lookup.",
    },
    {
        "name": "Autocomplete: prefix 'App'",
        "category": "search",
        "ch_sql": None,
        "es_body": {
            "suggest": {
                "ticker-suggest": {
                    "prefix": "App",
                    "completion": {"field": "suggest", "size": 5, "skip_duplicates": True},
                }
            }
        },
        "es_type": "suggest",
        "expected_winner": "ES only",
        "why": "ClickHouse has no completion data structure. ES FST returns in microseconds.",
    },
    {
        "name": "Complex agg: buy/sell pressure all tickers",
        "category": "aggregation",
        "ch_sql": """
            SELECT ticker,
                   countIf(side = 'BUY') AS buys,
                   countIf(side = 'SELL') AS sells,
                   round(countIf(side = 'BUY') / count() * 100, 2) AS buy_pct
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY buy_pct DESC
        """,
        "es_body": {
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
        "expected_winner": "Competitive",
        "why": "CH has countIf() (native). ES uses filter aggs (also efficient). Close match.",
    },
]


def _determine_winner(ch_ms: float | None, es_ms: float | None, ch_sql: str | None) -> str:
    """Pick winner label for scorecard and report table."""
    if ch_ms is None and es_ms is not None:
        return "ES only"
    if ch_ms is not None and es_ms is None:
        return "CH only"
    if ch_ms is not None and es_ms is not None:
        if abs(ch_ms - es_ms) < 5.0:
            return "Competitive"
        return "CH" if ch_ms < es_ms else "ES"
    return "N/A"


def run_comparison() -> list[dict[str, Any]]:
    """Execute all comparison pairs and collect timing + winner metadata."""
    results: list[dict[str, Any]] = []

    for comp in COMPARISONS:
        name = comp["name"]
        logger.info("\n  %s", name)

        ch_ms: float | None = None
        ch_rows = 0
        if comp.get("ch_sql"):
            try:
                ch_ms, ch_rows = time_ch(comp["ch_sql"])
                logger.info("    CH:  %7.1fms  (%d rows)", ch_ms, ch_rows)
            except Exception as exc:
                logger.error("    CH:  FAILED — %s", exc)

        es_ms: float | None = None
        es_rows = 0
        try:
            if comp.get("es_type") == "suggest":
                es_ms, es_rows = time_es_suggest(comp["es_body"])
            else:
                es_ms, es_rows = time_es(comp["es_body"])
            logger.info("    ES:  %7.1fms  (%d hits/suggestions)", es_ms, es_rows)
        except Exception as exc:
            logger.error("    ES:  FAILED — %s", exc)

        winner = _determine_winner(ch_ms, es_ms, comp.get("ch_sql"))
        results.append(
            {
                "name": name,
                "category": comp["category"],
                "ch_ms": ch_ms,
                "es_ms": es_ms,
                "winner": winner,
                "expected": comp["expected_winner"],
                "why": comp["why"],
            }
        )
        logger.info("    Winner: %s  (expected: %s)", winner, comp["expected_winner"])

    return results


def generate_report(results: list[dict[str, Any]]) -> str:
    """Write docs/engine_comparison_report.md and return report text."""
    ch_count = ch_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]
    es_count = get_es_client().count(index=get_settings().es_index_trades)["count"]

    lines: list[str] = [
        "# Query Engine Comparison: ClickHouse vs ElasticSearch",
        "",
        f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "## Test Conditions",
        f"- **Dataset:** {ch_count:,} trades in ClickHouse, {es_count:,} docs in ElasticSearch, 25 tickers",
        "- **Hardware:** Docker on MacBook (local dev)",
        "- **ClickHouse:** ReplacingMergeTree, ORDER BY (ticker, trade_time, trade_id)",
        "- **ElasticSearch:** 1 shard, 0 replicas, 5s refresh interval",
        f"- **Methodology:** {BENCHMARK_RUNS} runs per query, averaged",
        "",
        "## Results",
        "",
        "| Query | ClickHouse | ElasticSearch | Winner | Why |",
        "|-------|----------:|-------------:|--------|-----|",
    ]

    for row in results:
        ch_str = f"{row['ch_ms']:.1f}ms" if row["ch_ms"] is not None else "N/A"
        es_str = f"{row['es_ms']:.1f}ms" if row["es_ms"] is not None else "N/A"
        lines.append(
            f"| {row['name']} | {ch_str} | {es_str} | {row['winner']} | {row['why']} |"
        )

    ch_wins = sum(1 for r in results if r["winner"] == "CH")
    es_wins = sum(1 for r in results if r["winner"] in ("ES", "ES only"))
    competitive = sum(1 for r in results if r["winner"] in ("Competitive", "N/A", "CH only"))

    lines.extend(
        [
            "",
            "## Scorecard",
            f"- **ClickHouse wins:** {ch_wins}",
            f"- **ElasticSearch wins:** {es_wins} (including ES-only features)",
            f"- **Competitive / N/A:** {competitive}",
            "",
            "## When to Use Which Engine",
            "",
            "### Use ClickHouse for:",
            "- Time-series analytics and OLAP dashboards",
            "- Heavy aggregations (VWAP, sector performance, correlations)",
            "- Queries touching many rows with few columns",
            "- Materialized views for pre-computed metrics",
            "- SQL-based analytics with JOINs",
            "",
            "### Use ElasticSearch for:",
            "- Autocomplete and type-ahead suggestions",
            "- Fuzzy matching and typo tolerance",
            "- Full-text search with relevance ranking",
            "- Document similarity (More Like This)",
            "- Queries where data is denormalized (no JOINs needed)",
            "",
            "### Use Both Together:",
            "- FastAPI routes analytics → ClickHouse, search → ElasticSearch",
            "- Same Kafka topic feeds both engines simultaneously",
            "- ClickHouse for dashboard panels, ES for the search bar",
            "",
            "## Architecture",
            "```",
            "Producer → Kafka (stock_trades)",
            "    ├→ ClickHouse (native Kafka engine) → OLAP queries via FastAPI /analytics/*",
            "    └→ Python consumer → ElasticSearch → Search queries via FastAPI /search/*",
            "```",
            "",
            "## Key Insight",
            "",
            "Absolute timings depend on dataset size and hardware. The **why** column is stable:",
            "columnar storage vs inverted index vs FST completion — each engine wins where its",
            "data structure matches the query pattern.",
            "",
        ]
    )

    report = "\n".join(lines)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report, encoding="utf-8")
    logger.info("\nReport saved: %s", REPORT_PATH)
    return report


def main() -> None:
    logger.info("=" * 60)
    logger.info("Engine Comparison: ClickHouse vs ElasticSearch (Hour 15)")
    logger.info("=" * 60)

    results = run_comparison()

    logger.info("\n" + "=" * 60)
    logger.info("Summary Table")
    logger.info("-" * 60)
    logger.info("  %-40s %8s %8s %10s", "Query", "CH", "ES", "Winner")
    logger.info("  " + "-" * 60)
    for row in results:
        ch_str = f"{row['ch_ms']:.1f}" if row["ch_ms"] is not None else "N/A"
        es_str = f"{row['es_ms']:.1f}" if row["es_ms"] is not None else "N/A"
        logger.info("  %-40s %8s %8s %10s", row["name"], ch_str, es_str, row["winner"])

    generate_report(results)
    logger.info("\nDay 3 capstone complete — engine comparison report generated.")


if __name__ == "__main__":
    main()
