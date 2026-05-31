"""
Bulk load trades from ClickHouse into ElasticSearch.

Reads typed, cleaned data from raw_trades (same 85K rows as ClickHouse).
Denormalizes each document with sector, company_name, ticker_text, and suggest
before bulk indexing — ES has no efficient JOINs.

Usage:
    python -m src.es_bulk_loader

Prerequisites: index created, ClickHouse bulk-loaded.
"""

import logging

from elasticsearch import helpers

from src.clickhouse_client import execute_query, execute_query_df
from src.config import get_settings
from src.es_client import get_es_client
from src.es_document import build_es_doc, load_metadata

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 5000
BULK_CHUNK_SIZE = 500


def generate_es_docs(metadata: dict, batch_size: int = BATCH_SIZE):
    """
    Stream trades from ClickHouse in batches — avoids loading 85K rows at once.

    Yields bulk actions for elasticsearch.helpers.bulk().
    """
    total = execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]
    logger.info("Total trades to index: %s", f"{total:,}")

    offset = 0
    while offset < total:
        df = execute_query_df(f"""
            SELECT trade_id, ticker, price, quantity, trade_time,
                   side, trade_type, bid_price, ask_price, source
            FROM stock_analytics.raw_trades
            ORDER BY ticker, trade_time
            LIMIT {batch_size} OFFSET {offset}
        """)

        for _, row in df.iterrows():
            yield build_es_doc(row.to_dict(), metadata)

        offset += batch_size
        logger.info("  Processed %s/%s trades", f"{min(offset, total):,}", f"{total:,}")


def bulk_load() -> int:
    """Index all raw_trades into ES via helpers.bulk()."""
    client = get_es_client()
    metadata = load_metadata()
    logger.info("Loaded metadata for %s tickers", len(metadata))

    logger.info("Bulk indexing into ElasticSearch...")
    es = client.options(request_timeout=120)
    success, errors = helpers.bulk(
        es,
        generate_es_docs(metadata),
        chunk_size=BULK_CHUNK_SIZE,
        raise_on_error=False,
    )

    logger.info("Indexed %s documents", f"{success:,}")
    if errors:
        logger.warning("%s errors during indexing", len(errors))
        for err in errors[:5]:
            logger.warning("  %s", err)

    return success


def verify() -> int:
    """Compare ES doc count with ClickHouse; print sample AAPL document."""
    client = get_es_client()
    index_name = get_settings().es_index_trades

    client.indices.refresh(index=index_name)

    es_count = client.count(index=index_name)["count"]
    ch_count = execute_query("SELECT count() FROM stock_analytics.raw_trades")[0][0]

    logger.info("")
    logger.info("Verification:")
    logger.info("  ClickHouse raw_trades: %s", f"{ch_count:,}")
    logger.info("  ElasticSearch docs:    %s", f"{es_count:,}")

    if es_count == ch_count:
        logger.info("  Counts match")
    else:
        logger.warning("  Mismatch: diff = %s", abs(ch_count - es_count))

    result = client.search(
        index=index_name,
        query={"term": {"ticker": "AAPL"}},
        size=1,
    )
    hits = result["hits"]["hits"]
    if hits:
        doc = hits[0]["_source"]
        logger.info("")
        logger.info("Sample AAPL doc:")
        logger.info("  ticker: %s, price: %s, sector: %s", doc["ticker"], doc["price"], doc["sector"])
        logger.info("  company_name: %s", doc["company_name"])
        logger.info("  suggest inputs: %s", doc["suggest"]["input"])

    return es_count


def main() -> None:
    logger.info("=" * 60)
    logger.info("ES Bulk Load — ClickHouse -> ElasticSearch")
    logger.info("=" * 60)

    count = bulk_load()
    verify()

    logger.info("")
    logger.info("ES bulk load complete — %s documents indexed", f"{count:,}")


if __name__ == "__main__":
    main()
