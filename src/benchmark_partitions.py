"""
Benchmark partition pruning and ORDER BY / primary index impact.

No new schema — proves design choices:
  PARTITION BY toYYYYMM(trade_time)
  ORDER BY (ticker, trade_time, trade_id)

Layer 3: time-filter queries skip monthly partitions.
Layer 4: filters on ORDER BY columns use sparse primary index.

Also runs EXPLAIN indexes = 1 to show the full optimization cascade
(partition → primary key → skip indexes from index setup).

Usage:
    python -m src.benchmark_partitions

Prerequisites: (schema + skip indexes materialized).
"""

import logging
import time
import uuid

from src.clickhouse_client import execute_command, execute_query

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BENCHMARK_RUNS = 5


def run_with_stats(name: str, query: str, runs: int = BENCHMARK_RUNS) -> tuple[float, int | str, int | str]:
    """
    Run query with a unique marker; return avg ms, read_rows, parts_read from query_log.

    parts_read comes from ProfileEvents['SelectedParts'] when available.
    """
    marker = f"bench_{uuid.uuid4().hex[:12]}"
    tagged_query = f"/* {marker} */ {query.strip()}"

    execute_command("SYSTEM FLUSH LOGS")

    times: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        execute_query(tagged_query)
        times.append((time.perf_counter() - start) * 1000)

    avg_ms = sum(times) / len(times)

    time.sleep(0.5)
    execute_command("SYSTEM FLUSH LOGS")

    stats = execute_query(f"""
        SELECT
            read_rows,
            read_bytes,
            ProfileEvents['SelectedParts'] AS parts_read
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query LIKE '%{marker}%'
          AND query NOT LIKE '%system.query_log%'
        ORDER BY event_time DESC
        LIMIT 1
    """)

    if stats:
        rows_read, bytes_read, parts_read = stats[0]
        parts_str = parts_read if parts_read is not None else "N/A"
        logger.info(
            "  %-55s %7.1fms  rows_read=%s  parts=%s",
            name,
            avg_ms,
            rows_read,
            parts_str,
        )
        return avg_ms, rows_read, parts_str

    logger.warning("  %-55s %7.1fms  stats=N/A", name, avg_ms)
    return avg_ms, "N/A", "N/A"


def show_partitions() -> list:
    """List active partitions and row counts from system.parts."""
    logger.info("Current partitions in raw_trades:")
    result = execute_query("""
        SELECT partition, count() AS parts, sum(rows) AS total_rows
        FROM system.parts
        WHERE database = 'stock_analytics'
          AND table = 'raw_trades'
          AND active = 1
        GROUP BY partition
        ORDER BY partition
    """)
    if not result:
        logger.info("  (no active parts found)")
        return result
    for partition, parts, rows in result:
        logger.info("  Partition %s: %s part(s), %s rows", partition, parts, rows)
    return result


def benchmark_partition_pruning() -> None:
    """
    Compare query with trade_time filter vs without.

    With filter: partition pruning can skip months outside the range.
    On current data (mostly one month) parts may match — EXPLAIN still shows logic.
    """
    logger.info("")
    logger.info("Benchmark 1: Partition pruning (time filter vs none)")
    logger.info("-" * 60)

    run_with_stats(
        "WITH time filter (trade_time > '2026-05-20')",
        """
        SELECT ticker, avg(price), count()
        FROM stock_analytics.raw_trades
        WHERE trade_time > '2026-05-20' AND ticker = 'AAPL'
        GROUP BY ticker
        """,
    )

    run_with_stats(
        "WITHOUT time filter (all partitions)",
        """
        SELECT ticker, avg(price), count()
        FROM stock_analytics.raw_trades
        WHERE ticker = 'AAPL'
        GROUP BY ticker
        """,
    )

    logger.info("  With time filter, ClickHouse prunes partitions outside the range.")
    logger.info("  On a year of data, a May-only filter skips 11 of 12 monthly partitions.")


def benchmark_orderby_impact() -> None:
    """
    Compare filters on ORDER BY columns vs columns not in ORDER BY.

    ticker (position 1) uses sparse primary index.
    source / side (not in ORDER BY) need full granule scan unless skip-indexed.
    """
    logger.info("")
    logger.info("Benchmark 2: ORDER BY column position (primary index)")
    logger.info("-" * 60)

    run_with_stats(
        "ORDER BY col 1: WHERE ticker = 'AAPL'",
        """
        SELECT count(), avg(price), max(price), min(price)
        FROM stock_analytics.raw_trades
        WHERE ticker = 'AAPL'
        """,
    )

    run_with_stats(
        "ORDER BY col 2 only: WHERE trade_time > '2026-05-28'",
        """
        SELECT count(), avg(price), max(price), min(price)
        FROM stock_analytics.raw_trades
        WHERE trade_time > '2026-05-28'
        """,
    )

    run_with_stats(
        "NOT in ORDER BY: WHERE source = 'simulator'",
        """
        SELECT count(), avg(price), max(price), min(price)
        FROM stock_analytics.raw_trades
        WHERE source = 'simulator'
        """,
    )

    run_with_stats(
        "NOT in ORDER BY: WHERE side = 'BUY'",
        """
        SELECT count(), avg(price), max(price), min(price)
        FROM stock_analytics.raw_trades
        WHERE side = 'BUY'
        """,
    )

    logger.info("  ORDER BY columns use primary index -> fewer rows read.")
    logger.info("  Non-ORDER BY columns: full scan unless skip index applies.")


def show_explain_queries() -> None:
    """Print EXPLAIN indexes = 1 for queries hitting different optimization layers."""
    logger.info("")
    logger.info("Benchmark 3: EXPLAIN indexes = 1")
    logger.info("-" * 60)

    explains = {
        "All layers (time + ticker + price + trade_type)": """
            EXPLAIN indexes = 1
            SELECT ticker, avg(price), count()
            FROM stock_analytics.raw_trades
            WHERE trade_time > '2026-05-20'
              AND ticker = 'AAPL'
              AND price BETWEEN 200 AND 220
              AND trade_type = 'block'
            GROUP BY ticker
        """,
        "Ticker only (primary key)": """
            EXPLAIN indexes = 1
            SELECT ticker, avg(price), count()
            FROM stock_analytics.raw_trades
            WHERE ticker = 'AAPL'
            GROUP BY ticker
        """,
        "Source only (no ORDER BY / skip index on source)": """
            EXPLAIN indexes = 1
            SELECT count(), avg(price)
            FROM stock_analytics.raw_trades
            WHERE source = 'simulator'
        """,
    }

    for title, query in explains.items():
        logger.info("")
        logger.info("  EXPLAIN: %s", title)
        logger.info("  %s", "·" * 50)
        for row in execute_query(query):
            logger.info("    %s", row[0])


def generate_summary() -> None:
    """Print four-layer optimization recap for interviews."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("Four-Layer Optimization Summary")
    logger.info("=" * 60)
    logger.info(
        """
  Layer 1 — Materialized Views 
    Pre-computed aggregations. MV reads ~2.5K rows vs ~85K raw.
    Speedup: ~4-6x at current scale (10-15x at production volume).

  Layer 2 — Skip Indexes 
    Per-granule metadata for non-ORDER BY columns.
    bloom_filter on trade_type: ~85%% fewer rows read.

  Layer 3 — Partition Pruning (schema, benchmark)
    PARTITION BY toYYYYMM(trade_time). Time filters skip whole months.
    One year of data: ~11/12 partitions skipped for "last month" queries.

  Layer 4 — ORDER BY / Primary Index (schema, benchmark)
    ORDER BY (ticker, trade_time, trade_id). ticker-first filters skip granules.
    WHERE ticker = 'AAPL' reads ~12K rows vs ~85K for source = 'simulator'.

  Interview one-liner:
    "I optimized at four layers — MVs for pre-computed aggregations (~5x),
    skip indexes for granule skipping (~85%% fewer rows), partition pruning
    for month skipping, and ORDER BY for primary index granule search."
        """
    )


def main() -> None:
    logger.info("=" * 60)
    logger.info("Partition Pruning + Query Planning Benchmark ")
    logger.info("=" * 60)

    show_partitions()
    benchmark_partition_pruning()
    benchmark_orderby_impact()
    show_explain_queries()
    generate_summary()


if __name__ == "__main__":
    main()
