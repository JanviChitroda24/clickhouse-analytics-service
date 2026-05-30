"""
Benchmark skip indexes on raw_trades using system.query_log.

Primary metric: read_rows (deterministic) — not wall-clock time alone.
Compare indexed filter queries vs control query on unindexed column (source).

Usage:
    python -m src.benchmark_indexes

Prerequisite: python -m src.setup_skip_indexes (indexes + MATERIALIZE)
"""

import logging
import time
import uuid

from src.clickhouse_client import execute_command, execute_query

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def run_with_stats(name: str, query: str) -> tuple[float, int | str, list]:
    """
    Execute query with a unique SQL comment marker, then read read_rows from query_log.

    Marker avoids fragile substring matching when multiple similar queries run.
    """
    marker = f"bench_{uuid.uuid4().hex[:12]}"
    tagged_query = f"/* {marker} */ {query.strip()}"

    execute_command("SYSTEM FLUSH LOGS")

    start = time.perf_counter()
    result = execute_query(tagged_query)
    elapsed_ms = (time.perf_counter() - start) * 1000

    time.sleep(0.5)
    execute_command("SYSTEM FLUSH LOGS")

    stats = execute_query(f"""
        SELECT read_rows, read_bytes, result_rows
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query LIKE '%{marker}%'
          AND query NOT LIKE '%system.query_log%'
        ORDER BY event_time DESC
        LIMIT 1
    """)

    if stats:
        rows_read, bytes_read, result_rows = stats[0]
        logger.info(
            "  %-45s %7.1fms  rows_read=%s  bytes=%s  result=%s",
            name,
            elapsed_ms,
            rows_read,
            bytes_read,
            result_rows,
        )
        return elapsed_ms, rows_read, result

    logger.warning("  %-45s %7.1fms  rows_read=N/A (check query_log enabled)", name, elapsed_ms)
    return elapsed_ms, "N/A", result


def benchmark_set_and_bloom() -> int | str:
    """SET (ticker) + bloom_filter (trade_type) — composite filter."""
    logger.info("")
    logger.info("Benchmark 1: ticker + trade_type (SET + bloom_filter)")
    _, rows, _ = run_with_stats(
        "ticker=AAPL AND trade_type=block",
        """
        SELECT count(), avg(price)
        FROM stock_analytics.raw_trades
        WHERE ticker = 'AAPL' AND trade_type = 'block'
        """,
    )
    return rows


def benchmark_minmax() -> int | str:
    """MINMAX on price — range filter on non-ORDER BY column."""
    logger.info("")
    logger.info("Benchmark 2: price BETWEEN 200 AND 210 (MINMAX)")
    _, rows, _ = run_with_stats(
        "price BETWEEN 200 AND 210",
        """
        SELECT ticker, count(), avg(price)
        FROM stock_analytics.raw_trades
        WHERE price BETWEEN 200 AND 210
        GROUP BY ticker
        ORDER BY count() DESC
        """,
    )
    return rows


def benchmark_bloom_only() -> int | str:
    """Bloom filter on trade_type alone."""
    logger.info("")
    logger.info("Benchmark 3: trade_type = block (bloom_filter)")
    _, rows, _ = run_with_stats(
        "trade_type = block",
        """
        SELECT ticker, count(), avg(price), avg(quantity)
        FROM stock_analytics.raw_trades
        WHERE trade_type = 'block'
        GROUP BY ticker
        ORDER BY count() DESC
        """,
    )
    return rows


def benchmark_control_no_index() -> int | str:
    """
    Control: filter on source — no skip index defined.
    Expect rows_read ≈ full table (baseline for comparison).
    """
    logger.info("")
    logger.info("Control: source = simulator (no skip index on source)")
    _, rows, _ = run_with_stats(
        "source = simulator (no index)",
        """
        SELECT count(), avg(price)
        FROM stock_analytics.raw_trades
        WHERE source = 'simulator'
        """,
    )
    return rows


def show_explain() -> None:
    """EXPLAIN indexes = 1 — shows which granules/indexes the optimizer uses."""
    logger.info("")
    logger.info("EXPLAIN indexes = 1 (ticker + price + trade_type):")
    rows = execute_query("""
        EXPLAIN indexes = 1
        SELECT ticker, avg(price), count()
        FROM stock_analytics.raw_trades
        WHERE ticker = 'AAPL'
          AND price BETWEEN 200 AND 220
          AND trade_type = 'block'
        GROUP BY ticker
    """)
    for row in rows:
        logger.info("  %s", row[0])


def main() -> None:
    logger.info("=" * 60)
    logger.info("Skip Index Benchmark — raw_trades")
    logger.info("=" * 60)

    total = execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]
    logger.info("Total rows in raw_trades: %s", total)

    benchmark_set_and_bloom()
    benchmark_minmax()
    benchmark_bloom_only()
    control_rows = benchmark_control_no_index()
    show_explain()

    logger.info("")
    logger.info("=" * 60)
    logger.info("Key takeaway: indexed queries should read fewer rows than control.")
    logger.info("Control (no index on source) rows_read ≈ %s — baseline full/partition scan.", control_rows)
    logger.info("At scale, bloom_filter on trade_type often shows ~85%% fewer rows read.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
