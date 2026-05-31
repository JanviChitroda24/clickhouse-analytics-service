"""
ElasticSearch client wrapper.

Singleton connection — same pattern as clickhouse_client.py.
Uses elasticsearch-py 8.x (pinned in requirements) for ES 8.12 server compatibility.

Usage:
    from src.es_client import get_es_client
    client = get_es_client()
    client.info()
"""

from elasticsearch import Elasticsearch

from src.config import get_settings

_client: Elasticsearch | None = None


def get_es_client() -> Elasticsearch:
    """Return a cached Elasticsearch client configured from Settings."""
    global _client
    if _client is None:
        settings = get_settings()
        _client = Elasticsearch(
            settings.elasticsearch_url,
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=3,
        )
    return _client


def close_es_client() -> None:
    """Close the cached Elasticsearch client (called on FastAPI shutdown)."""
    global _client
    if _client is not None:
        _client.close()
        _client = None
