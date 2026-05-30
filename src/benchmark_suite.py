"""
Comprehensive benchmark suite (Hour 10).

Re-runs all Day 2 benchmarks in one pass and writes docs/query_optimization_report.md:
  - Section 1: Materialized views vs raw (Hour 6)
  - Section 2: Skip index row reduction (Hour 7)
  - Section 3: ORDER BY / primary index impact (Hour 8)
  - Section 4: Production analytics query timings (Hour 9)

Usage:
    python -m src.benchmark_suite

Prerequisites: Hours 2–9 (schema, data, MVs, skip indexes materialized).
"""

import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from src.clickhouse_client import execute_command, execute_query

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BENCHMARK_RUNS = 5
TARGET_MS = 100
_REPO_ROOT = Path(__file__).resolve().parent.parent
REPORT_PATH = _REPO_ROOT / "docs" / "query_optimization_report.md"


def timed_query(query: str, runs: int = BENCHMARK_RUNS) -> tuple[float, list | None]:
    """Run query multiple times; return average latency (ms) and last result set."""
    times: list[float] = []
    result = None
    for _ in range(runs):
        start = time.perf_counter()
        result = execute_query(query)
        times.append((time.perf_counter() - start) * 1000)
    return sum(times) / len(times), result


def get_rows_read(query: str) -> tuple[float, int, list | None]:
    """
    Execute query once with a UUID SQL comment marker; read read_rows from query_log.

    Uses markers (same pattern as benchmark_indexes.py) instead of fragile
    substring matching on query text.
    """
    marker = f"suite_{uuid.uuid4().hex[:12]}"
    tagged_query = f"/* {marker} */ {query.strip()}"

    execute_command("SYSTEM FLUSH LOGS")

    start = time.perf_counter()
    result = execute_query(tagged_query)
    elapsed_ms = (time.perf_counter() - start) * 1000

    time.sleep(0.5)
    execute_command("SYSTEM FLUSH LOGS")

    stats = execute_query(f"""
        SELECT read_rows
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query LIKE '%{marker}%'
          AND query NOT LIKE '%system.query_log%'
        ORDER BY event_time DESC
        LIMIT 1
    """)
    rows_read = int(stats[0][0]) if stats else 0
    return elapsed_ms, rows_read, result


# ── Section 1: Materialized Views (Hour 6) ───────────────────────

def benchmark_mvs() -> list[tuple[str, float, float]]:
    """Compare MV queries vs equivalent raw_trades aggregations."""
    logger.info("Section 1: Materialized Views...")
    results: list[tuple[str, float, float]] = []

    mv_ms, _ = timed_query("""
        SELECT ticker, minute,
               sumMerge(price_volume_sum) / sumMerge(volume_sum) AS vwap,
               countMerge(trade_count) AS trades
        FROM stock_analytics.mv_realtime_vwap
        WHERE ticker = 'AAPL'
        GROUP BY ticker, minute
        ORDER BY minute DESC
        LIMIT 10
    """)
    raw_ms, _ = timed_query("""
        SELECT ticker, toStartOfMinute(trade_time) AS minute,
               sum(price * quantity) / sum(quantity) AS vwap,
               count() AS trades
        FROM stock_analytics.raw_trades
        WHERE ticker = 'AAPL'
        GROUP BY ticker, minute
        ORDER BY minute DESC
        LIMIT 10
    """)
    results.append(("VWAP (AAPL, per minute)", raw_ms, mv_ms))

    mv_ms, _ = timed_query("""
        SELECT ticker, trade_date, trade_count, total_volume, high_price, low_price
        FROM stock_analytics.mv_daily_summary
        ORDER BY trade_date DESC, ticker
        LIMIT 50
    """)
    raw_ms, _ = timed_query("""
        SELECT ticker, toDate(trade_time) AS trade_date,
               count() AS trade_count, sum(quantity) AS total_volume,
               max(price) AS high_price, min(price) AS low_price
        FROM stock_analytics.raw_trades
        GROUP BY ticker, toDate(trade_time)
        ORDER BY trade_date DESC, ticker
        LIMIT 50
    """)
    results.append(("Daily summary (all tickers)", raw_ms, mv_ms))

    mv_ms, _ = timed_query("""
        SELECT ticker, hour, trade_count, total_volume, avg_price, price_stddev
        FROM stock_analytics.mv_hourly_stats
        WHERE ticker = 'NVDA'
        ORDER BY hour DESC
        LIMIT 20
    """)
    raw_ms, _ = timed_query("""
        SELECT ticker, toStartOfHour(trade_time) AS hour,
               count() AS trade_count, sum(quantity) AS total_volume,
               avg(price) AS avg_price, stddevPop(price) AS price_stddev
        FROM stock_analytics.raw_trades
        WHERE ticker = 'NVDA'
        GROUP BY ticker, toStartOfHour(trade_time)
        ORDER BY hour DESC
        LIMIT 20
    """)
    results.append(("Hourly stats (NVDA)", raw_ms, mv_ms))

    mv_ms, _ = timed_query("""
        SELECT sector, trade_date, ticker_count, trade_count, total_volume
        FROM stock_analytics.sector_summary
        ORDER BY trade_date DESC, total_volume DESC
        LIMIT 20
    """)
    raw_ms, _ = timed_query("""
        SELECT m.sector, toDate(t.trade_time) AS trade_date,
               uniqExact(t.ticker) AS ticker_count,
               count() AS trade_count, sum(t.quantity) AS total_volume
        FROM stock_analytics.raw_trades t
        INNER JOIN stock_analytics.company_metadata m ON t.ticker = m.ticker
        GROUP BY m.sector, toDate(t.trade_time)
        ORDER BY trade_date DESC, total_volume DESC
        LIMIT 20
    """)
    results.append(("Sector summary (JOIN)", raw_ms, mv_ms))

    for name, raw_ms, mv_ms in results:
        speedup = raw_ms / mv_ms if mv_ms > 0 else 0
        logger.info("  %-35s Raw: %6.1fms  MV: %6.1fms  (%.1fx)", name, raw_ms, mv_ms, speedup)

    return results


# ── Section 2: Skip Indexes (Hour 7) ─────────────────────────────

def benchmark_skip_indexes() -> tuple[list[tuple[str, str, int, int]], int]:
    """Measure row reduction from skip indexes vs control full scan."""
    logger.info("Section 2: Skip Indexes...")
    results: list[tuple[str, str, int, int]] = []

    _, control_rows, _ = get_rows_read("""
        SELECT count(), avg(price)
        FROM stock_analytics.raw_trades
        WHERE source = 'simulator'
    """)

    _, indexed_rows, _ = get_rows_read("""
        SELECT count(), avg(price)
        FROM stock_analytics.raw_trades
        WHERE ticker = 'AAPL' AND trade_type = 'block'
    """)
    results.append(("SET + BLOOM_FILTER", "ticker='AAPL' AND trade_type='block'", control_rows, indexed_rows))

    _, indexed_rows, _ = get_rows_read("""
        SELECT ticker, count(), avg(price)
        FROM stock_analytics.raw_trades
        WHERE price BETWEEN 200 AND 210
        GROUP BY ticker
    """)
    results.append(("MINMAX", "price BETWEEN 200 AND 210", control_rows, indexed_rows))

    _, indexed_rows, _ = get_rows_read("""
        SELECT ticker, count()
        FROM stock_analytics.raw_trades
        WHERE trade_type = 'block'
        GROUP BY ticker
    """)
    results.append(("BLOOM_FILTER", "trade_type = 'block'", control_rows, indexed_rows))

    for idx_type, query_desc, baseline, indexed in results:
        reduction = round((1 - indexed / baseline) * 100, 1) if baseline > 0 else 0
        logger.info(
            "  %-25s baseline=%s  indexed=%s  reduction=%s%%",
            idx_type,
            f"{baseline:,}",
            f"{indexed:,}",
            reduction,
        )

    return results, control_rows


# ── Section 3: ORDER BY / Primary Index (Hour 8) ─────────────────

def benchmark_partitions_orderby() -> list[tuple[str, str, int]]:
    """
    Compare rows_read for ORDER BY column vs non-indexed column.

    ticker is position 1 in ORDER BY (ticker, trade_time, trade_id).
    side is not in ORDER BY — expects full granule scan unless skip-indexed.
    """
    logger.info("Section 3: Partition Pruning + ORDER BY...")
    results: list[tuple[str, str, int]] = []

    _, ticker_rows, _ = get_rows_read("""
        SELECT count(), avg(price), max(price), min(price)
        FROM stock_analytics.raw_trades
        WHERE ticker = 'AAPL'
    """)

    _, side_rows, _ = get_rows_read("""
        SELECT count(), avg(price), max(price), min(price)
        FROM stock_analytics.raw_trades
        WHERE side = 'BUY'
    """)

    results.append(("WHERE ticker = 'AAPL'", "Col 1 (ORDER BY)", ticker_rows))
    results.append(("WHERE side = 'BUY'", "Not in ORDER BY", side_rows))

    for query_desc, col_type, rows in results:
        logger.info("  %-40s %-20s rows_read=%s", query_desc, col_type, f"{rows:,}")

    return results


# ── Section 4: Production Analytics (Hour 9) ─────────────────────

def benchmark_analytics() -> list[tuple[str, float, int, str]]:
    """Time all 8 production analytics queries from Hour 9."""
    logger.info("Section 4: Production Analytics Queries...")

    queries: dict[str, str] = {
        "Top Movers": """
            SELECT ticker, max(price) AS high, min(price) AS low,
                   round(max(price) - min(price), 2) AS range,
                   round((max(price) - min(price)) / min(price) * 100, 2) AS range_pct
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY range_pct DESC
            LIMIT 10
        """,
        "VWAP Deviation": """
            SELECT t.ticker, t.price, v.vwap,
                   round((t.price - v.vwap) / v.vwap * 100, 4) AS deviation_pct
            FROM stock_analytics.raw_trades t
            INNER JOIN stock_analytics.vwap_1min v
              ON t.ticker = v.ticker
              AND toStartOfMinute(t.trade_time) = v.window_start
            ORDER BY abs((t.price - v.vwap) / v.vwap * 100) DESC
            LIMIT 20
        """,
        "Anomaly by Sector": """
            SELECT sector, total_trades, anomalies,
                   round(anomalies / total_trades * 100, 2) AS anomaly_pct
            FROM (
                SELECT m.sector,
                       count() AS total_trades,
                       countIf(
                           t.price > s.avg_p + 2 * s.std_p
                           OR t.price < s.avg_p - 2 * s.std_p
                       ) AS anomalies
                FROM stock_analytics.raw_trades t
                INNER JOIN stock_analytics.company_metadata m ON t.ticker = m.ticker
                INNER JOIN (
                    SELECT ticker, avg(price) AS avg_p, stddevPop(price) AS std_p
                    FROM stock_analytics.raw_trades
                    GROUP BY ticker
                ) s ON t.ticker = s.ticker
                GROUP BY m.sector
            )
            ORDER BY anomaly_pct DESC
        """,
        "Sector Performance": """
            SELECT m.sector, round(sum(t.quantity * t.price), 2) AS notional,
                   round(avg(t.price), 2) AS avg_price, uniqExact(t.ticker) AS tickers
            FROM stock_analytics.raw_trades t
            INNER JOIN stock_analytics.company_metadata m ON t.ticker = m.ticker
            GROUP BY m.sector
            ORDER BY notional DESC
        """,
        "Buy/Sell Pressure": """
            SELECT ticker, countIf(side = 'BUY') AS buys, countIf(side = 'SELL') AS sells,
                   round(countIf(side = 'BUY') / count() * 100, 2) AS buy_pct
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY buy_pct DESC
        """,
        "Rolling VWAP (AAPL)": """
            SELECT toStartOfFiveMinutes(trade_time) AS bucket,
                   round(sum(price * quantity) / sum(quantity), 2) AS vwap,
                   sum(quantity) AS volume, count() AS trades
            FROM stock_analytics.raw_trades
            WHERE ticker = 'AAPL'
            GROUP BY bucket
            ORDER BY bucket
        """,
        "Intraday Range": """
            SELECT ticker, max(price) AS high, min(price) AS low,
                   round(max(price) - min(price), 2) AS range
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY range DESC
        """,
        "Cross-Correlation": """
            SELECT a.ticker AS t_a, b.ticker AS t_b,
                   round(corr(a.price, b.price), 4) AS correlation
            FROM (
                SELECT ticker, toStartOfMinute(trade_time) AS minute, avg(price) AS price
                FROM stock_analytics.raw_trades
                GROUP BY ticker, minute
            ) a
            INNER JOIN (
                SELECT ticker, toStartOfMinute(trade_time) AS minute, avg(price) AS price
                FROM stock_analytics.raw_trades
                GROUP BY ticker, minute
            ) b ON a.minute = b.minute AND a.ticker < b.ticker
            GROUP BY a.ticker, b.ticker
            HAVING count() > 10
            ORDER BY abs(correlation) DESC
            LIMIT 20
        """,
    }

    results: list[tuple[str, float, int, str]] = []
    for name, sql in queries.items():
        avg_ms, result = timed_query(sql)
        status = "PASS" if avg_ms < TARGET_MS else "SLOW"
        rows = len(result) if result else 0
        results.append((name, avg_ms, rows, status))
        logger.info("  [%s] %-25s %7.1fms  (%d rows)", status, name, avg_ms, rows)

    return results


# ── Report Generator ─────────────────────────────────────────────

def generate_report(
    mv_results: list[tuple[str, float, float]],
    skip_results: list[tuple[str, str, int, int]],
    skip_baseline: int,
    partition_results: list[tuple[str, str, int]],
    analytics_results: list[tuple[str, float, int, str]],
) -> str:
    """Assemble markdown report and write to docs/query_optimization_report.md."""
    total_rows = execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]

    speedups = [raw / mv if mv > 0 else 0 for _, raw, mv in mv_results]
    avg_mv_speedup = sum(speedups) / len(speedups) if speedups else 0

    indexed_rows_list = [indexed for _, _, _, indexed in skip_results]
    avg_reduction = (
        sum((1 - indexed / skip_baseline) * 100 for indexed in indexed_rows_list) / len(indexed_rows_list)
        if skip_baseline > 0 and indexed_rows_list
        else 0
    )

    ticker_rows = next(rows for desc, _, rows in partition_results if "ticker" in desc)
    side_rows = next(rows for desc, _, rows in partition_results if "side" in desc)

    passed = sum(1 for _, ms, _, status in analytics_results if status == "PASS")

    lines: list[str] = [
        "# ClickHouse Query Optimization Report",
        "",
        f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"**Dataset:** {total_rows:,} rows in `raw_trades`",
        "**Engine:** ReplacingMergeTree, ORDER BY (ticker, trade_time, trade_id), PARTITION BY toYYYYMM(trade_time)",
        "",
        "## Executive Summary",
        "",
        f"- **Materialized Views:** Average {avg_mv_speedup:.1f}x speedup over raw table aggregations",
        f"- **Skip Indexes:** Average {avg_reduction:.0f}% row reduction vs full scan baseline",
        f"- **ORDER BY Impact:** ticker filter reads {ticker_rows:,} rows vs {side_rows:,} for non-indexed column",
        f"- **Production Queries:** {passed}/{len(analytics_results)} under {TARGET_MS}ms",
        "",
        "## 1. Materialized Views",
        "",
        "| Query | Raw Table (ms) | Materialized View (ms) | Speedup |",
        "|-------|---------------:|----------------------:|--------:|",
    ]

    for name, raw_ms, mv_ms in mv_results:
        speedup = raw_ms / mv_ms if mv_ms > 0 else 0
        lines.append(f"| {name} | {raw_ms:.1f} | {mv_ms:.1f} | {speedup:.1f}x |")

    lines.extend([
        "",
        "MVs use AggregatingMergeTree (VWAP) and SummingMergeTree (daily/hourly).",
        "They fire automatically on every INSERT — no manual refresh needed.",
        "",
        "## 2. Skip Indexes",
        "",
        f"Baseline (no index, `WHERE source = 'simulator'`): **{skip_baseline:,} rows read** (full scan)",
        "",
        "| Index Type | Filter | Rows Read | Reduction |",
        "|------------|--------|----------:|----------:|",
    ])

    for idx_type, query_desc, _, indexed in skip_results:
        reduction = round((1 - indexed / skip_baseline) * 100, 1) if skip_baseline > 0 else 0
        lines.append(f"| {idx_type} | `{query_desc}` | {indexed:,} | {reduction}% |")

    lines.extend([
        "",
        "Skip indexes store per-granule (8,192 rows) metadata.",
        "set(0) stores distinct values, minmax stores range, bloom_filter uses probabilistic membership.",
        "",
        "## 3. Partition Pruning + ORDER BY Impact",
        "",
        "| Filter | Column Position | Rows Read |",
        "|--------|-----------------|----------:|",
    ])

    for query_desc, col_type, rows in partition_results:
        lines.append(f"| `{query_desc}` | {col_type} | {rows:,} |")

    lines.extend([
        "",
        f"Filtering on `ticker` (ORDER BY position 1) reads **{ticker_rows:,}** rows.",
        f"Filtering on `side` (not in ORDER BY) reads **{side_rows:,}** rows — full scan.",
        "Column position in ORDER BY determines primary index effectiveness.",
        "",
        "## 4. Production Analytics Queries",
        "",
        "| Query | Time (ms) | Rows | Status |",
        "|-------|----------:|-----:|--------|",
    ])

    for name, ms, rows, status in analytics_results:
        lines.append(f"| {name} | {ms:.1f} | {rows} | {status} |")

    lines.extend([
        "",
        f"**{passed}/{len(analytics_results)} queries under {TARGET_MS}ms** on {total_rows:,} rows.",
        "",
        "## Optimization Architecture",
        "",
        "```",
        "Layer 1: Materialized Views     → Pre-computed aggregations (AggregatingMergeTree)",
        "Layer 2: Skip Indexes           → Per-granule data skipping (set, minmax, bloom_filter)",
        "Layer 3: Partition Pruning       → Monthly partitions skip entire months",
        "Layer 4: ORDER BY Primary Index  → Sparse index binary search on sort key columns",
        "```",
        "",
    ])

    report = "\n".join(lines)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report, encoding="utf-8")

    logger.info("Report saved: %s (%d lines)", REPORT_PATH, len(lines))
    return report


def main() -> None:
    logger.info("=" * 60)
    logger.info("Comprehensive Benchmark Suite (Hour 10)")
    logger.info("=" * 60)

    total = execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]
    logger.info("Dataset: %s rows in raw_trades", f"{total:,}")
    logger.info("")

    mv_results = benchmark_mvs()
    logger.info("")
    skip_results, skip_baseline = benchmark_skip_indexes()
    logger.info("")
    partition_results = benchmark_partitions_orderby()
    logger.info("")
    analytics_results = benchmark_analytics()

    generate_report(mv_results, skip_results, skip_baseline, partition_results, analytics_results)

    logger.info("")
    logger.info("=" * 60)
    logger.info("Day 2 complete — optimization report generated.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
