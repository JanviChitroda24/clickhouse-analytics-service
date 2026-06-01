"""
SQL Server setup: database, raw_trades table, B-tree indexes, bulk load from ClickHouse.

Loads the same trade rows as ClickHouse so three-engine benchmarks compare
identical data on row-store vs columnar vs inverted-index engines.

Usage:
    python -m src.sqlserver_setup
"""

from __future__ import annotations

import logging
import time

import pandas as pd

from src.clickhouse_client import execute_query as ch_query
from src.clickhouse_client import execute_query_df as ch_query_df
from src.config import get_settings
from src.sqlserver_client import close_sqlserver, execute_command, execute_query, get_sqlserver_conn

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def create_database() -> None:
    """Create stock_analytics database if it does not exist (runs on master)."""
    execute_command(
        """
        IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'stock_analytics')
        CREATE DATABASE stock_analytics
        """,
        database="master",
    )
    logger.info("Database stock_analytics ready")


def create_table() -> None:
    """Create raw_trades with clustered PK and B-tree indexes for filter paths."""
    settings = get_settings()
    db = settings.sqlserver_database

    execute_command(
        f"""
        IF NOT EXISTS (SELECT * FROM {db}.sys.tables WHERE name = 'raw_trades')
        CREATE TABLE {db}.dbo.raw_trades (
            trade_id       VARCHAR(50) NOT NULL PRIMARY KEY,
            ticker         VARCHAR(10) NOT NULL,
            price          FLOAT NOT NULL,
            quantity       INT NOT NULL,
            trade_time     DATETIME2(3) NOT NULL,
            side           VARCHAR(10),
            trade_type     VARCHAR(20),
            bid_price      FLOAT,
            ask_price      FLOAT,
            source         VARCHAR(20)
        )
        """,
        database=db,
    )
    logger.info("Table raw_trades ready")

    for index_sql, label in [
        (
            f"""
            IF NOT EXISTS (SELECT * FROM {db}.sys.indexes WHERE name = 'idx_ticker')
            CREATE INDEX idx_ticker ON {db}.dbo.raw_trades (ticker)
            """,
            "idx_ticker",
        ),
        (
            f"""
            IF NOT EXISTS (SELECT * FROM {db}.sys.indexes WHERE name = 'idx_trade_time')
            CREATE INDEX idx_trade_time ON {db}.dbo.raw_trades (trade_time)
            """,
            "idx_trade_time",
        ),
        (
            f"""
            IF NOT EXISTS (SELECT * FROM {db}.sys.indexes WHERE name = 'idx_ticker_time')
            CREATE INDEX idx_ticker_time ON {db}.dbo.raw_trades (ticker, trade_time)
            """,
            "idx_ticker_time",
        ),
    ]:
        try:
            execute_command(index_sql, database=db)
            logger.info("Index %s ready", label)
        except Exception as exc:
            logger.info("Index %s skipped (may exist): %s", label, exc)


def load_data() -> None:
    """Bulk copy trades from ClickHouse into SQL Server in batches."""
    settings = get_settings()
    db = settings.sqlserver_database

    logger.info("Reading trades from ClickHouse...")
    df = ch_query_df(
        """
        SELECT trade_id, ticker, price, quantity, trade_time,
               side, trade_type, bid_price, ask_price, source
        FROM stock_analytics.raw_trades
        ORDER BY ticker, trade_time
        """
    )
    logger.info("Read %s rows from ClickHouse", f"{len(df):,}")

    existing = execute_query(f"SELECT COUNT(*) FROM {db}.dbo.raw_trades", database=db)
    if existing and existing[0][0] > 0:
        logger.info("Truncating existing %s rows in SQL Server", f"{existing[0][0]:,}")
        execute_command(f"TRUNCATE TABLE {db}.dbo.raw_trades", database=db)

    conn = get_sqlserver_conn(db)
    cursor = conn.cursor()
    batch_size = 1000
    start = time.perf_counter()

    for offset in range(0, len(df), batch_size):
        batch = df.iloc[offset : offset + batch_size]
        values = []
        for _, row in batch.iterrows():
            trade_time = row["trade_time"]
            if hasattr(trade_time, "to_pydatetime"):
                trade_time = trade_time.to_pydatetime()

            values.append(
                (
                    str(row["trade_id"]),
                    str(row["ticker"]),
                    float(row["price"]),
                    int(row["quantity"]),
                    trade_time,
                    str(row["side"]),
                    str(row["trade_type"]),
                    float(row["bid_price"]) if pd.notna(row["bid_price"]) else 0.0,
                    float(row["ask_price"]) if pd.notna(row["ask_price"]) else 0.0,
                    str(row["source"]),
                )
            )

        cursor.executemany(
            """
            INSERT INTO dbo.raw_trades
                (trade_id, ticker, price, quantity, trade_time,
                 side, trade_type, bid_price, ask_price, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            values,
        )

        inserted = min(offset + batch_size, len(df))
        if inserted % 10000 == 0 or inserted == len(df):
            logger.info("Inserted %s/%s rows", f"{inserted:,}", f"{len(df):,}")

    cursor.close()
    elapsed = time.perf_counter() - start
    logger.info("Loaded %s rows in %.1fs", f"{len(df):,}", elapsed)


def verify() -> bool:
    """Confirm row counts match between ClickHouse and SQL Server."""
    settings = get_settings()
    db = settings.sqlserver_database

    ch_count = int(ch_query("SELECT count() FROM stock_analytics.raw_trades")[0][0])
    sql_count = int(execute_query(f"SELECT COUNT(*) FROM {db}.dbo.raw_trades", database=db)[0][0])

    logger.info("ClickHouse: %s rows", f"{ch_count:,}")
    logger.info("SQL Server: %s rows", f"{sql_count:,}")
    match = ch_count == sql_count
    logger.info("Match: %s", "yes" if match else "NO")
    return match


def main() -> None:
    logger.info("=" * 60)
    logger.info("SQL Server setup (third query engine)")
    logger.info("=" * 60)

    try:
        create_database()
        close_sqlserver()
        create_table()
        load_data()
        ok = verify()
    finally:
        close_sqlserver()

    if not ok:
        raise SystemExit("Row count mismatch between ClickHouse and SQL Server")
    logger.info("SQL Server setup complete. Run: python -m src.sqlserver_benchmark")


if __name__ == "__main__":
    main()
