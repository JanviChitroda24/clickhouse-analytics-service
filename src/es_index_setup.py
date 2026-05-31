"""
ElasticSearch index setup — creates stock-trades index with mappings.

Mapping design:
  - keyword: exact match + aggregations (ticker, side, trade_type, sector)
  - text: full-text + fuzzy search (company_name, ticker_text)
  - completion: autocomplete suggest field (suggest)
  - Denormalized sector + company_name on each doc (no JOINs in ES)

Usage:
    python -m src.es_index_setup
    python -m src.es_index_setup --keep   # skip delete if index exists

Prerequisites: docker compose up -d (elasticsearch healthy on :9200)
"""

import argparse
import logging

from elasticsearch import ApiError, NotFoundError

from src.config import get_settings
from src.es_client import get_es_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# Index body — settings + mappings. Equivalent to ORDER BY decision in ClickHouse:
# field types here determine what queries are possible and how fast they are.
TRADE_INDEX_MAPPING = {
    "settings": {
        # Single-node dev: 1 shard, 0 replicas (no second node for replica copies)
        "number_of_shards": 1,
        "number_of_replicas": 0,
        # 5s refresh: better bulk load throughput vs default 1s (near-real-time lag)
        "refresh_interval": "5s",
    },
    "mappings": {
        "properties": {
            # Identifiers — keyword for exact lookups and dedup
            "trade_id": {"type": "keyword"},

            # Dual-field ticker pattern: keyword for term/aggs, text for fuzzy match
            "ticker": {"type": "keyword"},
            "ticker_text": {"type": "text", "analyzer": "standard"},

            # Numeric — range filters and metric aggregations
            "price": {"type": "float"},
            "quantity": {"type": "integer"},
            "bid_price": {"type": "float"},
            "ask_price": {"type": "float"},

            # Temporal — range queries and date_histogram aggs
            "trade_time": {"type": "date"},

            # Categorical — low cardinality, exact filter only
            "side": {"type": "keyword"},
            "trade_type": {"type": "keyword"},
            "source": {"type": "keyword"},

            # Denormalized from company_metadata (ES document model — avoid JOINs)
            "sector": {"type": "keyword"},
            "company_name": {"type": "text", "analyzer": "standard"},

            # Autocomplete — completion suggester (ClickHouse has no equivalent)
            "suggest": {
                "type": "completion",
                "analyzer": "simple",
                "preserve_separators": True,
                "preserve_position_increments": True,
                "max_input_length": 50,
            },
        }
    },
}

FIELD_NOTES = {
    "keyword": "exact match + aggs",
    "text": "full-text search",
    "completion": "autocomplete",
    "date": "range + date_histogram",
    "float": "numeric range + aggs",
    "integer": "numeric range + aggs",
}


def index_exists(client, index_name: str) -> bool:
    """
    Check if index exists via GET (not HEAD exists()).

    ES 8.x + elasticsearch-py: indices.exists() can send headers that 8.12 rejects.
    """
    try:
        client.indices.get(index=index_name)
        return True
    except NotFoundError:
        return False
    except ApiError as exc:
        if exc.meta.status == 404:
            return False
        raise


def create_index(delete_existing: bool = True) -> str:
    """Create stock-trades index. Recreates when delete_existing=True."""
    client = get_es_client()
    index_name = get_settings().es_index_trades

    if index_exists(client, index_name):
        if delete_existing:
            logger.info("Deleting existing index: %s", index_name)
            client.indices.delete(index=index_name)
        else:
            logger.info("Index '%s' already exists — skipping creation", index_name)
            return index_name

    client.indices.create(
        index=index_name,
        settings=TRADE_INDEX_MAPPING["settings"],
        mappings=TRADE_INDEX_MAPPING["mappings"],
    )
    logger.info("Created index: %s", index_name)
    return index_name


def verify_index() -> bool:
    """Print cluster health, mapping summary, settings, and doc count."""
    client = get_es_client()
    index_name = get_settings().es_index_trades

    health = client.cluster.health()
    logger.info("Cluster: %s, status: %s", health["cluster_name"], health["status"])

    exists = index_exists(client, index_name)
    logger.info("Index '%s' exists: %s", index_name, exists)
    if not exists:
        return False

    mapping = client.indices.get_mapping(index=index_name)
    properties = mapping[index_name]["mappings"]["properties"]

    logger.info("")
    logger.info("Mapping (%d fields):", len(properties))
    logger.info("%-20s %-15s %s", "Field", "Type", "Notes")
    logger.info("-" * 55)
    for field, config in sorted(properties.items()):
        field_type = config.get("type", "object")
        notes = FIELD_NOTES.get(field_type, "")
        if field_type == "text":
            notes = f"full-text (analyzer: {config.get('analyzer', 'standard')})"
        logger.info("  %-18s %-15s %s", field, field_type, notes)

    settings = client.indices.get_settings(index=index_name)
    idx_settings = settings[index_name]["settings"]["index"]
    logger.info("")
    logger.info("Settings:")
    logger.info("  Shards: %s", idx_settings.get("number_of_shards", "?"))
    logger.info("  Replicas: %s", idx_settings.get("number_of_replicas", "?"))
    logger.info("  Refresh interval: %s", idx_settings.get("refresh_interval", "?"))

    count = client.count(index=index_name)["count"]
    logger.info("  Documents: %s", count)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Create and verify ES stock-trades index")
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Do not delete existing index; skip creation if present",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("ElasticSearch Index Setup — %s", get_settings().es_index_trades)
    logger.info("=" * 60)

    create_index(delete_existing=not args.keep)
    ok = verify_index()

    if ok:
        logger.info("")
        logger.info("ES index ready — proceed to (bulk load)")
    else:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
