"""
Create materialized views and backfill from existing raw_trades data.

ClickHouse MVs only process INSERTs that arrive AFTER the MV is created.
Historical bulk-load + Kafka pipeline rows are backfilled with INSERT INTO ... SELECT.

Usage:
    python -m src.setup_materialized_views

Re-run safe: TRUNCATE + backfill refreshes MV targets (SummingMergeTree would
double-count if backfilled twice without truncate).
"""

import logging
from pathlib import Path

from src.clickhouse_client import execute_command, execute_query
from src.setup_clickhouse import run_sql_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent / "schema"

# Never SELECT count() from Kafka engine — it consumes messages
SKIP_ROW_COUNT = {"kafka_trades"}


def create_views() -> None:
    """Execute materialized_views.sql (IF NOT EXISTS)."""
    run_sql_file(SCHEMA_DIR / "materialized_views.sql")


def truncate_mv_targets() -> None:
    """Clear MV targets before backfill so re-runs do not double-count."""
    for table in (
        "mv_realtime_vwap",
        "mv_daily_summary",
        "mv_hourly_stats",
        "sector_summary",
    ):
        logger.info("  Truncating %s...", table)
        execute_command(f"TRUNCATE TABLE IF EXISTS stock_analytics.{table}")


def backfill_vwap() -> None:
    """Backfill AggregatingMergeTree VWAP states from raw_trades."""
    logger.info("Backfilling mv_realtime_vwap...")
    execute_command("""
        INSERT INTO stock_analytics.mv_realtime_vwap
        SELECT
            ticker,
            toStartOfMinute(trade_time) AS minute,
            sumState(toFloat64(price * quantity)) AS price_volume_sum,
            sumState(toFloat64(quantity))         AS volume_sum,
            countState()                          AS trade_count,
            maxState(price)                       AS high_price,
            minState(price)                       AS low_price
        FROM stock_analytics.raw_trades
        GROUP BY ticker, minute
    """)
    count = execute_query("SELECT count() FROM stock_analytics.mv_realtime_vwap")[0][0]
    logger.info("  mv_realtime_vwap: %s rows", count)


def backfill_daily() -> None:
    """Backfill daily per-ticker rollups into SummingMergeTree MV."""
    logger.info("Backfilling mv_daily_summary...")
    execute_command("""
        INSERT INTO stock_analytics.mv_daily_summary
        SELECT
            ticker,
            toDate(trade_time) AS trade_date,
            count()            AS trade_count,
            sum(quantity)      AS total_volume,
            max(price)         AS high_price,
            min(price)         AS low_price,
            avg(price)         AS avg_price
        FROM stock_analytics.raw_trades
        GROUP BY ticker, trade_date
    """)
    count = execute_query("SELECT count() FROM stock_analytics.mv_daily_summary")[0][0]
    logger.info("  mv_daily_summary: %s rows", count)


def backfill_hourly() -> None:
    """Backfill hourly stats (volume + price stddev) from raw_trades."""
    logger.info("Backfilling mv_hourly_stats...")
    execute_command("""
        INSERT INTO stock_analytics.mv_hourly_stats
        SELECT
            ticker,
            toStartOfHour(trade_time) AS hour,
            count()                   AS trade_count,
            sum(quantity)             AS total_volume,
            avg(price)                AS avg_price,
            stddevPop(price)          AS price_stddev
        FROM stock_analytics.raw_trades
        GROUP BY ticker, hour
    """)
    count = execute_query("SELECT count() FROM stock_analytics.mv_hourly_stats")[0][0]
    logger.info("  mv_hourly_stats: %s rows", count)


def backfill_sector() -> None:
    """
    Populate sector_summary via JOIN — MVs cannot JOIN in their definition.
    Aggregates trades by sector + day using company_metadata.
    """
    logger.info("Populating sector_summary...")
    execute_command("""
        INSERT INTO stock_analytics.sector_summary
        SELECT
            m.sector,
            toDate(t.trade_time) AS trade_date,
            uniqExact(t.ticker)  AS ticker_count,
            count()              AS trade_count,
            sum(t.quantity)      AS total_volume,
            avg(t.price)         AS avg_price,
            sum(t.price * t.quantity) AS total_notional
        FROM stock_analytics.raw_trades t
        INNER JOIN stock_analytics.company_metadata m ON t.ticker = m.ticker
        GROUP BY m.sector, toDate(t.trade_time)
    """)
    count = execute_query("SELECT count() FROM stock_analytics.sector_summary")[0][0]
    logger.info("  sector_summary: %s rows", count)


def verify() -> None:
    """
    List stock_analytics objects with engines and row counts.

    Skips ClickHouse internal MV storage (`.inner_id.*`) — dots break unquoted SQL.
    Skips Kafka engine tables — SELECT consumes messages.
    """
    rows = execute_query("""
        SELECT name, engine
        FROM system.tables
        WHERE database = 'stock_analytics'
          AND name NOT LIKE '.%'
        ORDER BY name
    """)
    logger.info("")
    logger.info("%-35s %-25s %10s", "Object", "Engine", "Rows")
    logger.info("-" * 72)
    for name, engine in rows:
        if name in SKIP_ROW_COUNT or engine == "Kafka":
            logger.info("%-35s %-25s %10s", name, engine, "(virtual)")
            continue
        count = execute_query(
            f"SELECT count() FROM stock_analytics.`{name}`"
        )[0][0]
        logger.info("%-35s %-25s %10d", name, engine, count)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Materialized Views Setup + Backfill")
    logger.info("=" * 60)

    create_views()

    logger.info("")
    logger.info("Truncating MV targets (idempotent re-run)...")
    truncate_mv_targets()

    logger.info("")
    logger.info("Backfilling from raw_trades...")
    backfill_vwap()
    backfill_daily()
    backfill_hourly()
    backfill_sector()

    verify()

    logger.info("")
    logger.info("Materialized views ready — run: python -m src.benchmark_mv")


if __name__ == "__main__":
    main()
