"""
Shared ES document building for the FastAPI layer.

ClickHouse stores trades and metadata in separate tables (JOIN at query time).
ES documents must be self-contained — sector, company_name, ticker_text, and
suggest are embedded on every trade before indexing.

Used by es_bulk_loader.py and es_kafka_consumer.py.
"""

from __future__ import annotations

from typing import Any

from src.clickhouse_client import execute_query
from src.config import get_settings


def load_metadata() -> dict[str, dict[str, str]]:
    """Load ticker -> {company_name, sector} from ClickHouse company_metadata."""
    rows = execute_query("""
        SELECT ticker, company_name, sector
        FROM stock_analytics.company_metadata
    """)
    metadata = {
        ticker: {
            "company_name": company_name or "Unknown",
            "sector": sector or "Unknown",
        }
        for ticker, company_name, sector in rows
    }
    return metadata


def format_trade_time(value: Any) -> str:
    """Normalize datetime to ISO string for ES date field."""
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def build_es_doc(trade: dict[str, Any], metadata: dict[str, dict[str, str]]) -> dict[str, Any]:
    """
    Convert a trade dict to an elasticsearch.helpers.bulk action.

    Accepts ClickHouse rows (trade_time) or Kafka JSON (timestamp).
    Adds denormalized sector/company_name and completion suggest inputs.
    """
    settings = get_settings()
    ticker = trade.get("ticker", "")
    meta = metadata.get(ticker, {"company_name": "Unknown", "sector": "Unknown"})
    company_name = meta["company_name"]
    trade_time = trade.get("trade_time") or trade.get("timestamp")

    return {
        "_index": settings.es_index_trades,
        "_id": trade.get("trade_id"),
        "_source": {
            "trade_id": trade.get("trade_id"),
            "ticker": ticker,
            "ticker_text": ticker,
            "price": float(trade.get("price", 0)),
            "quantity": int(trade.get("quantity", 0)),
            "trade_time": format_trade_time(trade_time),
            "side": trade.get("side", "UNKNOWN"),
            "trade_type": trade.get("trade_type", "unknown"),
            "bid_price": float(trade.get("bid_price", 0)),
            "ask_price": float(trade.get("ask_price", 0)),
            "source": trade.get("source", "unknown"),
            "sector": meta["sector"],
            "company_name": company_name,
            # autocomplete — match by ticker, company name, or both
            "suggest": {
                "input": [
                    ticker,
                    company_name,
                    f"{ticker} {company_name}",
                ],
            },
        },
    }
