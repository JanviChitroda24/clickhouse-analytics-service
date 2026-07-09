# Author: Janvi Chitroda | github.com/JanviChitroda24
"""
Create skip indexes on raw_trades and materialize them on existing data.

Skip indexes only apply to parts inserted AFTER ADD INDEX unless you
MATERIALIZE — same backfill idea as MVs.

Usage:
    python -m src.setup_skip_indexes
"""

import logging
from pathlib import Path

from src.clickhouse_client import execute_query
from src.setup_clickhouse import run_sql_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent / "schema"


def create_indexes() -> None:
    """Run skip_indexes.sql — 3 ADD INDEX + 3 MATERIALIZE statements."""
    run_sql_file(SCHEMA_DIR / "skip_indexes.sql")


def verify() -> None:
    """List skip indexes registered on raw_trades."""
    rows = execute_query("""
        SELECT name, type_full, expr, granularity
        FROM system.data_skipping_indices
        WHERE database = 'stock_analytics' AND table = 'raw_trades'
        ORDER BY name
    """)
    logger.info("")
    logger.info("%-20s %-25s %-15s %s", "Index", "Type", "Column", "Granularity")
    logger.info("-" * 70)
    if not rows:
        logger.warning("  No skip indexes found on raw_trades")
        return
    for name, type_full, expr, gran in rows:
        logger.info("  %-18s %-25s %-15s %s", name, type_full, expr, gran)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Skip Indexes Setup — raw_trades")
    logger.info("=" * 60)

    create_indexes()
    verify()

    logger.info("")
    logger.info("Skip indexes ready — run: python -m src.benchmark_indexes")


if __name__ == "__main__":
    main()
