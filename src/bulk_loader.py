"""
Bulk load Week 3 Delta Lake output into ClickHouse.

Reads:
  - Delta: raw_trades, vwap_1min (paths from .env)
  - CSV: company_metadata (data/seeds/)

Writes:
  - stock_analytics.raw_trades
  - stock_analytics.vwap_1min
  - stock_analytics.company_metadata

Week 3 Delta raw_trades columns (from stream_processor.py):
  trade_id, ticker, price, quantity, side, trade_type, exchange, source,
  event_time, kafka_ingest_time, dollar_volume

ClickHouse raw_trades expects trade_time (not event_time) and bid/ask columns
(not in Delta) — we map event_time → trade_time and default bid/ask to 0.0.

Uses pandas as intermediary — fine for <100K rows.
At production scale: ClickHouse native file() or s3() + INSERT SELECT.

Usage:
    python -m src.bulk_loader
"""

import logging

import pandas as pd
from deltalake import DeltaTable

from src.clickhouse_client import execute_query, insert_dataframe
from src.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def _strip_timezone(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """ClickHouse DateTime64 columns need timezone-naive pandas datetimes."""
    for col in columns:
        if col in df.columns and hasattr(df[col].dtype, "tz") and df[col].dt.tz is not None:
            df[col] = df[col].dt.tz_localize(None)
    return df


def load_raw_trades() -> int:
    """Load raw_trades from Delta Lake into ClickHouse."""
    settings = get_settings()
    path = settings.resolve_path(settings.delta_raw_trades_path)

    logger.info("Reading Delta table: %s", path)
    df = DeltaTable(str(path)).to_pandas()
    logger.info("  Delta raw_trades: %s rows", len(df))

    if df.empty:
        logger.warning("  No data in Delta raw_trades — skipping")
        return 0

    # Week 3 Spark writes event_time; ClickHouse schema uses trade_time
    column_map = {
        "trade_id": "trade_id",
        "ticker": "ticker",
        "price": "price",
        "quantity": "quantity",
        "event_time": "trade_time",
        "side": "side",
        "trade_type": "trade_type",
        "source": "source",
    }

    available = [c for c in column_map if c in df.columns]
    missing = set(column_map) - set(available)
    if missing:
        logger.warning("  Delta missing expected columns: %s", sorted(missing))

    df = df[available].rename(columns=column_map)

    if "trade_time" in df.columns:
        df["trade_time"] = pd.to_datetime(df["trade_time"], utc=True)
        df = _strip_timezone(df, ["trade_time"])

    # Not stored in Week 3 Delta bronze table — required by ClickHouse schema
    df["bid_price"] = 0.0
    df["ask_price"] = 0.0

    if "quantity" in df.columns:
        df["quantity"] = df["quantity"].fillna(0).astype(int)

    for col in ["price", "bid_price", "ask_price"]:
        df[col] = df[col].fillna(0.0)

    rows = insert_dataframe("raw_trades", df)
    logger.info("  Loaded %s rows into stock_analytics.raw_trades", rows)
    return rows


def load_vwap_1min() -> int:
    """Load vwap_1min from Delta Lake into ClickHouse."""
    settings = get_settings()
    path = settings.resolve_path(settings.delta_vwap_1min_path)

    logger.info("Reading Delta table: %s", path)
    df = DeltaTable(str(path)).to_pandas()
    logger.info("  Delta vwap_1min: %s rows", len(df))

    if df.empty:
        logger.warning("  No data in Delta vwap_1min — skipping")
        return 0

    # Spark may write window as struct {start, end} on older runs
    if "window" in df.columns:
        df["window_start"] = pd.to_datetime(df["window"].apply(lambda x: x["start"]))
        df["window_end"] = pd.to_datetime(df["window"].apply(lambda x: x["end"]))
        df = df.drop(columns=["window"])
    else:
        df["window_start"] = pd.to_datetime(df["window_start"])
        df["window_end"] = pd.to_datetime(df["window_end"])

    df = _strip_timezone(df, ["window_start", "window_end"])

    ch_columns = [
        "ticker",
        "window_start",
        "window_end",
        "vwap",
        "total_volume",
        "trade_count",
        "high_price",
        "low_price",
        "buy_pressure",
    ]

    for col in ch_columns:
        if col not in df.columns:
            if col in ("vwap", "high_price", "low_price", "buy_pressure"):
                df[col] = 0.0
            elif col in ("total_volume", "trade_count"):
                df[col] = 0

    df = df[ch_columns]

    df["total_volume"] = df["total_volume"].fillna(0).astype(int)
    df["trade_count"] = df["trade_count"].fillna(0).astype(int)
    for col in ("vwap", "high_price", "low_price", "buy_pressure"):
        df[col] = df[col].fillna(0.0)

    rows = insert_dataframe("vwap_1min", df)
    logger.info("  Loaded %s rows into stock_analytics.vwap_1min", rows)
    return rows


def load_company_metadata() -> int:
    """Load company metadata from CSV seed file."""
    settings = get_settings()
    path = settings.resolve_path(settings.company_metadata_csv)

    logger.info("Reading CSV: %s", path)
    df = pd.read_csv(path)
    logger.info("  company_metadata: %s rows", len(df))

    df.columns = df.columns.str.lower().str.strip()

    ch_columns = [
        "ticker",
        "company_name",
        "sector",
        "industry",
        "market_cap",
        "country",
        "exchange",
    ]

    available = [c for c in ch_columns if c in df.columns]
    df = df[available]

    for col in ("company_name", "sector", "industry", "country", "exchange"):
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    if "market_cap" in df.columns:
        df["market_cap"] = df["market_cap"].fillna(0).astype(int)

    rows = insert_dataframe("company_metadata", df)
    logger.info("  Loaded %s rows into stock_analytics.company_metadata", rows)
    return rows


def verify_counts(delta_raw_count: int | None = None) -> bool:
    """Print row counts and compare raw_trades to Delta source."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("Verification — Row Counts")
    logger.info("-" * 50)

    tables = ["raw_trades", "vwap_1min", "company_metadata"]
    for table in tables:
        count = execute_query(f"SELECT count() FROM stock_analytics.{table}")[0][0]
        logger.info("  %-25s %8s rows", table, count)

    if delta_raw_count is not None:
        ch_raw = execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]
        match = delta_raw_count == ch_raw
        logger.info("")
        logger.info("Delta raw_trades:      %s", delta_raw_count)
        logger.info("ClickHouse raw_trades: %s", ch_raw)
        logger.info("Match: %s", "OK" if match else "MISMATCH")

    logger.info("")
    logger.info("Sample raw_trades (5 rows):")
    sample = execute_query("""
        SELECT ticker, price, quantity, side, trade_time
        FROM stock_analytics.raw_trades
        LIMIT 5
    """)
    for row in sample:
        logger.info("  %s", row)

    logger.info("")
    logger.info("Tickers in raw_trades (top 10):")
    tickers = execute_query("""
        SELECT ticker, count() AS cnt
        FROM stock_analytics.raw_trades
        GROUP BY ticker
        ORDER BY cnt DESC
        LIMIT 10
    """)
    for ticker, cnt in tickers:
        logger.info("  %-8s %6s trades", ticker, cnt)

    logger.info("=" * 50)

    if delta_raw_count is None:
        return True
    ch_raw = execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]
    return delta_raw_count == ch_raw


def main():
    logger.info("=" * 60)
    logger.info("Bulk Load — Delta Lake → ClickHouse")
    logger.info("=" * 60)

    settings = get_settings()
    delta_path = settings.resolve_path(settings.delta_raw_trades_path)
    delta_raw_count = len(DeltaTable(str(delta_path)).to_pandas())

    raw_count = load_raw_trades()
    vwap_count = load_vwap_1min()
    meta_count = load_company_metadata()

    counts_match = verify_counts(delta_raw_count=delta_raw_count)

    logger.info("")
    if raw_count > 0 and meta_count > 0 and counts_match:
        logger.info("Bulk load complete — ready for (Kafka engine)!")
    elif raw_count > 0 and meta_count > 0:
        logger.warning("Loaded data but raw_trades count does not match Delta — investigate")
    else:
        logger.warning("Some tables empty — check Delta paths in .env")


if __name__ == "__main__":
    main()
