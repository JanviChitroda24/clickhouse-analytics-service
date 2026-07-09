# Author: Janvi Chitroda | github.com/JanviChitroda24
"""
ClickHouse schema setup — executes CREATE TABLE statements.

Usage: python -m src.setup_clickhouse
Idempotent: all statements use IF NOT EXISTS.
"""

import logging
from pathlib import Path

from src.clickhouse_client import execute_command, get_ch_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent / "schema"


def run_sql_file(filepath: Path):
    """Execute a SQL file with multiple statements separated by semicolons."""
    content = filepath.read_text()
    statements = []
    current = []

    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("--") and not current:
            continue
        current.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt and stmt != ";":
                statements.append(stmt)
            current = []

    logger.info("Found %s statements in %s", len(statements), filepath.name)

    for i, stmt in enumerate(statements, 1):
        first_line = next(
            (line.strip() for line in stmt.split("\n") if line.strip() and not line.strip().startswith("--")),
            "unknown",
        )
        try:
            execute_command(stmt.rstrip(";"))
            logger.info("  [%s/%s] OK %s", i, len(statements), first_line[:80])
        except Exception as exc:
            logger.error("  [%s/%s] FAIL %s", i, len(statements), first_line[:80])
            logger.error("     Error: %s", exc)
            raise


def verify_tables():
    """Print all tables in stock_analytics with their engines."""
    client = get_ch_client()
    result = client.query("""
        SELECT name, engine, sorting_key, partition_key
        FROM system.tables
        WHERE database = 'stock_analytics'
        ORDER BY name
    """)

    logger.info("")
    logger.info("=" * 70)
    logger.info("%-30s %-25s %-30s", "Table", "Engine", "Sort Key")
    logger.info("-" * 70)
    for row in result.result_rows:
        name, engine, sort_key, _part_key = row
        logger.info("%-30s %-25s %-30s", name, engine, sort_key)
    logger.info("=" * 70)
    logger.info("Total: %s tables/views", result.row_count)


def main():
    logger.info("=" * 60)
    logger.info("ClickHouse Schema Setup — stock_analytics")
    logger.info("=" * 60)

    execute_command("CREATE DATABASE IF NOT EXISTS stock_analytics")
    logger.info("Database stock_analytics exists")

    schema_file = SCHEMA_DIR / "clickhouse_tables.sql"
    run_sql_file(schema_file)

    verify_tables()
    logger.info("\nSchema setup complete — ready for (bulk load)!")


if __name__ == "__main__":
    main()
