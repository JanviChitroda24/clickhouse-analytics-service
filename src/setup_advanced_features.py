"""
Setup advanced ClickHouse features: TTL, dictionary, and projection.

- TTL: raw_trades rows expire after 90 days (MVs keep aggregated history)
- Dictionary: company_dict for dictGet() instead of JOIN
- Projection: time-first sort order for time-range queries

Usage:
    python -m src.setup_advanced_features
"""

import logging
from pathlib import Path

from src.clickhouse_client import execute_command, execute_query
from src.setup_clickhouse import run_sql_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent / "schema"


def apply_ddl() -> None:
    """Run advanced_features.sql (TTL, dictionary, projection definition)."""
    run_sql_file(SCHEMA_DIR / "advanced_features.sql")


def materialize_projection() -> None:
    """
    Build the time-first projection on existing raw_trades data.

    Without MATERIALIZE, only new inserts use the projection.
    """
    logger.info("Materializing projection proj_time_first (may take a moment)...")
    try:
        execute_command(
            """
            ALTER TABLE stock_analytics.raw_trades
            MATERIALIZE PROJECTION proj_time_first
            """
        )
        logger.info("  Projection materialized on existing data")
    except Exception as exc:
        message = str(exc).lower()
        if "already" in message or "nothing to do" in message:
            logger.info("  Projection already materialized or in progress")
        else:
            raise


def verify_dictionary() -> None:
    """Smoke-test dictGet for a known ticker."""
    sector, name = execute_query(
        """
        SELECT
            dictGet('company_dict', 'sector', 'AAPL') AS sector,
            dictGet('company_dict', 'company_name', 'AAPL') AS name
        """
    )[0]
    logger.info("  dictGet verify: AAPL → sector=%s, name=%s", sector, name)
    if not sector:
        raise RuntimeError("dictGet returned empty sector for AAPL")


def verify_all() -> None:
    """Print TTL, dictionary, and projection status from system tables."""
    logger.info("")
    logger.info("Verification:")

    ttl_rows = execute_query(
        """
        SELECT engine_full
        FROM system.tables
        WHERE database = 'stock_analytics' AND name = 'raw_trades'
        """
    )
    if ttl_rows:
        engine_full = ttl_rows[0][0]
        has_ttl = "TTL" in engine_full.upper()
        logger.info("  raw_trades TTL in engine definition: %s", "yes" if has_ttl else "check engine_full")

    dict_rows = execute_query(
        """
        SELECT name, status, element_count, last_successful_update_time
        FROM system.dictionaries
        WHERE database = 'stock_analytics'
        ORDER BY name
        """
    )
    for name, status, count, updated in dict_rows:
        logger.info("  Dictionary %s: status=%s, entries=%s, updated=%s", name, status, count, updated)

    proj_rows = execute_query(
        """
        SELECT name, sorting_key
        FROM system.projections
        WHERE database = 'stock_analytics' AND table = 'raw_trades'
        ORDER BY name
        """
    )
    if proj_rows:
        for name, sort_key in proj_rows:
            logger.info("  Projection %s: sort_key=%s", name, sort_key)
    else:
        logger.warning("  No projections found on raw_trades")


def main() -> None:
    logger.info("=" * 60)
    logger.info("Advanced ClickHouse Features Setup")
    logger.info("=" * 60)

    apply_ddl()
    logger.info("DDL applied (TTL, dictionary, projection definition)")

    materialize_projection()
    verify_dictionary()
    verify_all()

    logger.info("")
    logger.info("Advanced features ready — run: python -m src.benchmark_advanced")


if __name__ == "__main__":
    main()
