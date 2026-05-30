"""
8 production analytics queries for stock trading data (Hour 9).

Each query answers a real dashboard/analyst question and exercises a different
part of the optimization stack (MVs, skip indexes, partition pruning, JOINs).

Runs each query 5 times, averages latency, flags any query >= 100ms target.

Usage:
    python -m src.analytics_queries

Prerequisites: Hours 2–8 (schema, bulk load, MVs, skip indexes).
"""

import logging
import time
from typing import Any

from src.clickhouse_client import execute_query
from src.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

TARGET_MS = 100
BENCHMARK_RUNS = 5

# Named queries: description + SQL. No time filters tied to now()/today() so results
# work on historical bulk-loaded data (May 2026), not only "live" clock time.
QUERIES: dict[str, dict[str, str]] = {

    # Uses: ORDER BY ticker (primary index), full-table scan per ticker group — OK at 85K
    "top_movers": {
        "description": "Top 10 tickers by intraday price range (largest swing)",
        "sql": """
            SELECT ticker,
                   max(price) AS high,
                   min(price) AS low,
                   max(price) - min(price) AS price_range,
                   round((max(price) - min(price)) / min(price) * 100, 2) AS range_pct,
                   count() AS trades
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY range_pct DESC
            LIMIT 10
        """,
    },

    # Uses: JOIN raw_trades + pre-aggregated vwap_1min from Week 3 bulk load
    "vwap_deviation": {
        "description": "Top 20 trades deviating from minute-level VWAP",
        "sql": """
            SELECT t.ticker,
                   t.price,
                   v.vwap,
                   round((t.price - v.vwap) / v.vwap * 100, 4) AS deviation_pct,
                   t.quantity,
                   t.side
            FROM stock_analytics.raw_trades t
            INNER JOIN stock_analytics.vwap_1min v
              ON t.ticker = v.ticker
              AND toStartOfMinute(t.trade_time) = v.window_start
            ORDER BY abs((t.price - v.vwap) / v.vwap * 100) DESC
            LIMIT 20
        """,
    },

    # Uses: JOIN raw_trades + company_metadata + per-ticker stats subquery
    "anomaly_by_sector": {
        "description": "Anomaly rate (price > 2 stddev from ticker mean) by sector",
        "sql": """
            SELECT sector,
                   total_trades,
                   anomalies,
                   round(anomalies / total_trades * 100, 2) AS anomaly_rate_pct
            FROM (
                SELECT m.sector,
                       count() AS total_trades,
                       countIf(
                           t.price > stats.avg_price + 2 * stats.stddev_price
                           OR t.price < stats.avg_price - 2 * stats.stddev_price
                       ) AS anomalies
                FROM stock_analytics.raw_trades t
                INNER JOIN stock_analytics.company_metadata m ON t.ticker = m.ticker
                INNER JOIN (
                    SELECT ticker,
                           avg(price) AS avg_price,
                           stddevPop(price) AS stddev_price
                    FROM stock_analytics.raw_trades
                    GROUP BY ticker
                ) stats ON t.ticker = stats.ticker
                GROUP BY m.sector
            )
            ORDER BY anomaly_rate_pct DESC
        """,
    },

    # Uses: JOIN raw_trades + company_metadata — portfolio manager morning view
    "sector_performance": {
        "description": "Sector notional volume, avg price, ticker count",
        "sql": """
            SELECT m.sector,
                   round(sum(t.quantity * t.price), 2) AS total_notional,
                   round(avg(t.price), 2) AS avg_price,
                   uniqExact(t.ticker) AS tickers,
                   count() AS trades
            FROM stock_analytics.raw_trades t
            INNER JOIN stock_analytics.company_metadata m ON t.ticker = m.ticker
            GROUP BY m.sector
            ORDER BY total_notional DESC
        """,
    },

    # Uses: GROUP BY ticker — volatility screening (same shape as top_movers, ranked by range)
    "intraday_range": {
        "description": "Tickers ranked by price range (high - low)",
        "sql": """
            SELECT ticker,
                   max(price) AS high,
                   min(price) AS low,
                   round(max(price) - min(price), 2) AS range,
                   round((max(price) - min(price)) / min(price) * 100, 2) AS range_pct,
                   count() AS trades
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY range_pct DESC
        """,
    },

    # Uses: countIf() — ClickHouse idiom for conditional aggregation
    "buy_sell_pressure": {
        "description": "Buy vs sell pressure ratio per ticker",
        "sql": """
            SELECT ticker,
                   countIf(side = 'BUY') AS buys,
                   countIf(side = 'SELL') AS sells,
                   countIf(side NOT IN ('BUY', 'SELL')) AS unknown,
                   count() AS total,
                   round(countIf(side = 'BUY') / count() * 100, 2) AS buy_pct
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY buy_pct DESC
        """,
    },

    # Uses: toStartOfFiveMinutes() — becomes FastAPI parameterized endpoint in Hour 17
    "rolling_vwap_aapl": {
        "description": "5-minute VWAP buckets for AAPL (fixed ticker)",
        "sql": """
            SELECT toStartOfFiveMinutes(trade_time) AS bucket,
                   round(sum(price * quantity) / sum(quantity), 2) AS vwap,
                   sum(quantity) AS volume,
                   count() AS trades
            FROM stock_analytics.raw_trades
            WHERE ticker = 'AAPL'
            GROUP BY bucket
            ORDER BY bucket
        """,
    },

    # Uses: self-JOIN + corr() — heaviest query; may exceed 100ms on 85K rows
    "cross_correlation": {
        "description": "Top 20 ticker pairs by minute-level price correlation",
        "sql": """
            SELECT a.ticker AS ticker_a,
                   b.ticker AS ticker_b,
                   round(corr(a.price, b.price), 4) AS price_correlation
            FROM (
                SELECT ticker,
                       toStartOfMinute(trade_time) AS minute,
                       avg(price) AS price
                FROM stock_analytics.raw_trades
                GROUP BY ticker, minute
            ) a
            INNER JOIN (
                SELECT ticker,
                       toStartOfMinute(trade_time) AS minute,
                       avg(price) AS price
                FROM stock_analytics.raw_trades
                GROUP BY ticker, minute
            ) b ON a.minute = b.minute AND a.ticker < b.ticker
            GROUP BY a.ticker, b.ticker
            HAVING count() > 10
            ORDER BY abs(price_correlation) DESC
            LIMIT 20
        """,
    },
}

# Parameterized variant — ClickHouse {ticker:String} syntax (no f-string SQL injection)
ROLLING_VWAP_PARAM_SQL = """
    SELECT toStartOfFiveMinutes(trade_time) AS bucket,
           round(sum(price * quantity) / sum(quantity), 2) AS vwap,
           sum(quantity) AS volume,
           count() AS trades
    FROM stock_analytics.raw_trades
    WHERE ticker = {ticker:String}
    GROUP BY bucket
    ORDER BY bucket
"""


def run_query(
    name: str,
    info: dict[str, str],
    runs: int = BENCHMARK_RUNS,
    parameters: dict[str, Any] | None = None,
) -> tuple[float | None, list | None, str | None]:
    """Run query `runs` times; return average ms, last result set, or error message."""
    sql = info["sql"]
    desc = info["description"]
    times: list[float] = []
    result = None

    for _ in range(runs):
        start = time.perf_counter()
        try:
            result = execute_query(sql, parameters=parameters)
            times.append((time.perf_counter() - start) * 1000)
        except Exception as exc:
            logger.error("  FAIL %s: %s", name, exc)
            return None, None, str(exc)

    avg_ms = sum(times) / len(times)
    status = "OK" if avg_ms < TARGET_MS else "SLOW"
    row_count = len(result) if result else 0
    logger.info(
        "  [%s] %-28s %7.1fms  (%d rows)  — %s",
        status,
        name,
        avg_ms,
        row_count,
        desc,
    )
    return avg_ms, result, None


def run_parameterized_rolling_vwap(ticker: str = "NVDA", runs: int = BENCHMARK_RUNS) -> float | None:
    """Demonstrate {ticker:String} parameterized query — pattern for FastAPI Hour 17."""
    info = {"description": f"5-min VWAP buckets for {ticker} (parameterized)", "sql": ROLLING_VWAP_PARAM_SQL}
    avg_ms, _, err = run_query(
        "rolling_vwap_param",
        info,
        runs=runs,
        parameters={"ticker": ticker},
    )
    if err:
        return None
    return avg_ms


def show_sample(name: str, result: list | None, max_rows: int = 3) -> None:
    """Print first few result rows (truncated for readability)."""
    if not result:
        return
    for row in result[:max_rows]:
        display = str(row)
        if len(display) > 120:
            display = display[:120] + "..."
        logger.info("    %s", display)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Production Analytics Queries (target: <%dms each)", TARGET_MS)
    logger.info("=" * 60)

    total_rows = execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]
    logger.info("raw_trades: %s rows", total_rows)
    logger.info("")

    results: dict[str, float] = {}
    failed: list[str] = []
    slow: list[tuple[str, float]] = []

    for name, info in QUERIES.items():
        avg_ms, result, error = run_query(name, info)
        if error:
            failed.append(name)
            continue
        if avg_ms is not None:
            results[name] = avg_ms
        show_sample(name, result)
        logger.info("")
        if avg_ms is not None and avg_ms >= TARGET_MS:
            slow.append((name, avg_ms))

    # Bonus: parameterized rolling VWAP (different ticker than fixed AAPL query)
    logger.info("Parameterized query (FastAPI pattern):")
    param_ms = run_parameterized_rolling_vwap(get_settings().tickers[4])  # NVDA
    if param_ms is not None:
        results["rolling_vwap_param"] = param_ms
        if param_ms >= TARGET_MS:
            slow.append(("rolling_vwap_param", param_ms))
    logger.info("")

    logger.info("=" * 60)
    logger.info("Summary")
    logger.info("-" * 60)
    for name, ms in results.items():
        flag = "OK" if ms < TARGET_MS else "SLOW"
        logger.info("  %-30s %7.1fms  %s", name, ms, flag)

    logger.info("-" * 60)
    core_results = {n: ms for n, ms in results.items() if n in QUERIES}
    passed = sum(1 for ms in core_results.values() if ms < TARGET_MS)
    logger.info("  Passed (<%dms): %d/%d", TARGET_MS, passed, len(QUERIES))

    if slow:
        logger.info("  Slow (>=%dms): %s", TARGET_MS, ", ".join(n for n, _ in slow))
    if failed:
        logger.info("  Failed: %s", ", ".join(failed))

    if not slow and not failed:
        logger.info("")
        logger.info("All 8 queries under %dms — production ready.", TARGET_MS)
    elif not failed:
        logger.info("")
        logger.info("Some queries above %dms — cross_correlation self-JOIN often slowest.", TARGET_MS)

    logger.info("=" * 60)


if __name__ == "__main__":
    main()
