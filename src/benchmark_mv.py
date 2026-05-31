"""
Benchmark materialized view queries vs raw raw_trades aggregations.

Proves MVs return equivalent results with lower latency.
Numbers feed query optimization report and interview talking points.

Usage:
    python -m src.benchmark_mv
"""

import logging
import time

from src.clickhouse_client import execute_query

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BENCHMARK_RUNS = 5


def benchmark(name: str, query: str, runs: int = BENCHMARK_RUNS) -> tuple[float, list]:
    """Run query multiple times; return average latency (ms) and last result set."""
    times: list[float] = []
    result = None
    for _ in range(runs):
        start = time.perf_counter()
        result = execute_query(query)
        times.append((time.perf_counter() - start) * 1000)
    avg_ms = sum(times) / len(times)
    row_count = len(result) if result else 0
    logger.info("  %-40s %8.1fms  (%d rows)", name, avg_ms, row_count)
    return avg_ms, result


def test_vwap() -> tuple[float, float]:
    """VWAP per minute: AggregatingMergeTree + sumMerge vs raw GROUP BY."""
    logger.info("")
    logger.info("VWAP benchmark (AAPL, latest 10 minutes):")

    mv_time, _ = benchmark("MV (mv_realtime_vwap)", """
        SELECT
            ticker,
            minute,
            sumMerge(price_volume_sum) / sumMerge(volume_sum) AS vwap,
            countMerge(trade_count) AS trades
        FROM stock_analytics.mv_realtime_vwap
        WHERE ticker = 'AAPL'
        GROUP BY ticker, minute
        ORDER BY minute DESC
        LIMIT 10
    """)

    raw_time, _ = benchmark("RAW (raw_trades GROUP BY)", """
        SELECT
            ticker,
            toStartOfMinute(trade_time) AS minute,
            sum(price * quantity) / sum(quantity) AS vwap,
            count() AS trades
        FROM stock_analytics.raw_trades
        WHERE ticker = 'AAPL'
        GROUP BY ticker, minute
        ORDER BY minute DESC
        LIMIT 10
    """)

    if mv_time > 0:
        logger.info("  Speedup: %.1fx", raw_time / mv_time)
    return mv_time, raw_time


def test_daily_summary() -> tuple[float, float]:
    """Daily rollups: SummingMergeTree MV vs raw aggregation."""
    logger.info("")
    logger.info("Daily summary benchmark (50 rows):")

    mv_time, _ = benchmark("MV (mv_daily_summary)", """
        SELECT ticker, trade_date, trade_count, total_volume, high_price, low_price
        FROM stock_analytics.mv_daily_summary
        ORDER BY trade_date DESC, ticker
        LIMIT 50
    """)

    raw_time, _ = benchmark("RAW (raw_trades GROUP BY)", """
        SELECT
            ticker,
            toDate(trade_time) AS trade_date,
            count() AS trade_count,
            sum(quantity) AS total_volume,
            max(price) AS high_price,
            min(price) AS low_price
        FROM stock_analytics.raw_trades
        GROUP BY ticker, toDate(trade_time)
        ORDER BY trade_date DESC, ticker
        LIMIT 50
    """)

    if mv_time > 0:
        logger.info("  Speedup: %.1fx", raw_time / mv_time)
    return mv_time, raw_time


def test_hourly_stats() -> tuple[float, float]:
    """Hourly stats: MV vs raw for NVDA."""
    logger.info("")
    logger.info("Hourly stats benchmark (NVDA, 20 hours):")

    mv_time, _ = benchmark("MV (mv_hourly_stats)", """
        SELECT ticker, hour, trade_count, total_volume, avg_price, price_stddev
        FROM stock_analytics.mv_hourly_stats
        WHERE ticker = 'NVDA'
        ORDER BY hour DESC
        LIMIT 20
    """)

    raw_time, _ = benchmark("RAW (raw_trades GROUP BY)", """
        SELECT
            ticker,
            toStartOfHour(trade_time) AS hour,
            count() AS trade_count,
            sum(quantity) AS total_volume,
            avg(price) AS avg_price,
            stddevPop(price) AS price_stddev
        FROM stock_analytics.raw_trades
        WHERE ticker = 'NVDA'
        GROUP BY ticker, toStartOfHour(trade_time)
        ORDER BY hour DESC
        LIMIT 20
    """)

    if mv_time > 0:
        logger.info("  Speedup: %.1fx", raw_time / mv_time)
    return mv_time, raw_time


def test_sector() -> None:
    """Sector-level denormalized table (JOIN pre-computed offline)."""
    logger.info("")
    logger.info("Sector summary (manual JOIN table):")

    benchmark("sector_summary", """
        SELECT
            sector,
            trade_date,
            ticker_count,
            trade_count,
            total_volume,
            round(avg_price, 2),
            round(total_notional, 2)
        FROM stock_analytics.sector_summary
        ORDER BY trade_date DESC, total_notional DESC
        LIMIT 20
    """)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Materialized View Benchmark")
    logger.info("=" * 60)

    vwap_mv, vwap_raw = test_vwap()
    daily_mv, daily_raw = test_daily_summary()
    hourly_mv, hourly_raw = test_hourly_stats()
    test_sector()

    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary (avg of %d runs each):", BENCHMARK_RUNS)
    if vwap_mv > 0:
        logger.info("  VWAP:   MV %.1fms vs Raw %.1fms (%.1fx)", vwap_mv, vwap_raw, vwap_raw / vwap_mv)
    if daily_mv > 0:
        logger.info("  Daily:  MV %.1fms vs Raw %.1fms (%.1fx)", daily_mv, daily_raw, daily_raw / daily_mv)
    if hourly_mv > 0:
        logger.info("  Hourly: MV %.1fms vs Raw %.1fms (%.1fx)", hourly_mv, hourly_raw, hourly_raw / hourly_mv)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
