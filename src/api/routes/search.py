"""
ElasticSearch-backed search endpoints.

Four search features ClickHouse cannot do natively:
  1. Autocomplete — completion suggester (FST on `suggest` field)
  2. Full-text search — tokenized multi_match with relevance ranking + filters
  3. Fuzzy search — typo-tolerant match queries (analyzes input before fuzzy match)
  4. Similar tickers — More Like This on sector + company_name

Each route builds an ES query body, executes via the shared ES client, maps hits to
Pydantic models, and logs elapsed milliseconds for observability.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from src.api.models import AutocompleteResult, SearchHit, SearchResponse, SimilarTicker
from src.api.cache import autocomplete_cache, search_cache
from src.config import get_settings
from src.es_client import get_es_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/search", tags=["search"])

# Fields returned for trade-level search results (full-text + fuzzy)
_TRADE_SOURCE = [
    "trade_id",
    "ticker",
    "company_name",
    "sector",
    "price",
    "quantity",
    "side",
    "trade_time",
]


def _as_dict(result: Any) -> dict:
    """Normalize elasticsearch-py 8.x ObjectApiResponse to a plain dict."""
    return result.body if hasattr(result, "body") else result


def _hit_total(hits_section: dict) -> int:
    """ES 7+ total may be int or {value: N}."""
    total = hits_section.get("total", 0)
    if isinstance(total, dict):
        return int(total.get("value", 0))
    return int(total)


def _parse_trade_hits(hits: list[dict]) -> list[SearchHit]:
    """Map ES hit documents to SearchHit models."""
    return [
        SearchHit(
            trade_id=h["_source"].get("trade_id", ""),
            ticker=h["_source"].get("ticker", ""),
            company_name=h["_source"].get("company_name", "Unknown"),
            sector=h["_source"].get("sector", "Unknown"),
            price=float(h["_source"].get("price", 0)),
            quantity=int(h["_source"].get("quantity", 0)),
            side=h["_source"].get("side", ""),
            trade_time=str(h["_source"].get("trade_time", "")),
            score=float(h.get("_score") or 0),
        )
        for h in hits
    ]


def _index_name() -> str:
    return get_settings().es_index_trades


# ── 1. Autocomplete ─────────────────────────────────────────────


@router.get("/autocomplete", response_model=list[AutocompleteResult])
async def autocomplete(
    q: str = Query(..., min_length=1, max_length=50, description="Prefix to autocomplete"),
    limit: int = Query(5, ge=1, le=20),
) -> list[AutocompleteResult]:
    """
    Ticker/company type-ahead using ES completion suggester.

    Designed for search-bar dropdowns — prefix lookup via FST is microseconds.
    ClickHouse has no completion structure; LIKE 'Goo%' would full-scan and miss
    partial company-name matches.
    """
    cache_key = f"autocomplete:{q}:{limit}"
    # TTL caching: avoid re-running completion lookups during rapid typing.
    if cache_key in autocomplete_cache:
        logger.info("Autocomplete '%s': CACHE HIT", q)
        return autocomplete_cache[cache_key]

    client = get_es_client()
    start = time.perf_counter()
    try:
        result = _as_dict(
            client.search(
                index=_index_name(),
                suggest={
                    "ticker-suggest": {
                        "prefix": q,
                        "completion": {
                            "field": "suggest",
                            "size": limit,
                            "skip_duplicates": True,
                        },
                    }
                },
            )
        )
    except Exception as exc:
        logger.error("Autocomplete failed for '%s': %s", q, exc)
        raise HTTPException(status_code=503, detail=f"ElasticSearch unavailable: {exc}")

    elapsed = (time.perf_counter() - start) * 1000
    suggestions = result["suggest"]["ticker-suggest"][0]["options"]
    logger.info("Autocomplete '%s': %d results, %.1fms", q, len(suggestions), elapsed)

    response = [
        AutocompleteResult(
            ticker=s["_source"]["ticker"],
            company_name=s["_source"].get("company_name", "Unknown"),
            score=s.get("_score"),
        )
        for s in suggestions
    ]
    autocomplete_cache[cache_key] = response
    return response


# ── 2. Full-Text Search ─────────────────────────────────────────


@router.get("/trades", response_model=SearchResponse)
async def search_trades(
    q: str = Query(..., min_length=1, description="Search query"),
    ticker: str | None = Query(None, description="Filter by ticker"),
    side: str | None = Query(None, pattern="^(BUY|SELL)$", description="Filter by side"),
    min_price: float | None = Query(None, ge=0, description="Minimum price"),
    max_price: float | None = Query(None, ge=0, description="Maximum price"),
    limit: int = Query(20, ge=1, le=100),
) -> SearchResponse:
    """
    Full-text search across company names, sectors, and tickers with optional filters.

    Uses bool query: `must` for text scoring (multi_match), `filter` for structured
    constraints (term/range) that do not affect relevance — ES best practice.
    ClickHouse equivalent: LIKE '%financial%' — no tokenization, no ranking.
    """
    cache_key = f"search:trades:{q}:{ticker}:{side}:{min_price}:{max_price}:{limit}"
    if cache_key in search_cache:
        logger.info("Search trades '%s': CACHE HIT", q)
        return search_cache[cache_key]

    client = get_es_client()
    start = time.perf_counter()

    # Text query affects relevance score
    must = [
        {
            "multi_match": {
                "query": q,
                "fields": ["company_name^2", "sector", "ticker_text"],
                "type": "best_fields",
                "fuzziness": "AUTO",
            }
        }
    ]

    # Structured filters — yes/no inclusion only, cacheable, no scoring overhead
    filters: list[dict] = []
    if ticker:
        filters.append({"term": {"ticker": ticker.upper()}})
    if side:
        filters.append({"term": {"side": side}})
    if min_price is not None:
        filters.append({"range": {"price": {"gte": min_price}}})
    if max_price is not None:
        filters.append({"range": {"price": {"lte": max_price}}})

    bool_query: dict[str, Any] = {"must": must}
    if filters:
        bool_query["filter"] = filters

    try:
        result = _as_dict(
            client.search(
                index=_index_name(),
                query={"bool": bool_query},
                size=limit,
                _source=_TRADE_SOURCE,
            )
        )
    except Exception as exc:
        logger.error("Search trades failed for '%s': %s", q, exc)
        raise HTTPException(status_code=503, detail=f"ElasticSearch unavailable: {exc}")
    elapsed = (time.perf_counter() - start) * 1000

    hits = result["hits"]["hits"]
    total = _hit_total(result["hits"])

    logger.info("Search '%s': %d total, %d returned, %.1fms", q, total, len(hits), elapsed)

    response = SearchResponse(
        query=q,
        total_hits=total,
        took_ms=round(elapsed, 1),
        results=_parse_trade_hits(hits),
    )
    search_cache[cache_key] = response
    return response


# ── 3. Fuzzy Search ─────────────────────────────────────────────


@router.get("/fuzzy", response_model=SearchResponse)
async def fuzzy_search(
    q: str = Query(..., min_length=1, description="Search with typo tolerance"),
    limit: int = Query(10, ge=1, le=50),
) -> SearchResponse:
    """
    Typo-tolerant search — 'APPL' finds AAPL, 'Micorsoft' finds Microsoft.

    Uses `match` with fuzziness (not raw `fuzzy` query) so the analyzer lowercases
    input before edit-distance matching — fixes case-sensitivity issues on tickers.
    Results collapsed by ticker so one row per symbol, not one per trade.
    """
    cache_key = f"search:fuzzy:{q}:{limit}"
    if cache_key in search_cache:
        logger.info("Search fuzzy '%s': CACHE HIT", q)
        return search_cache[cache_key]

    client = get_es_client()
    start = time.perf_counter()

    try:
        result = _as_dict(
            client.search(
                index=_index_name(),
                query={
                    "bool": {
                        "should": [
                            {
                                "match": {
                                    "ticker_text": {
                                        "query": q,
                                        "fuzziness": "AUTO",
                                        "boost": 3,
                                    }
                                }
                            },
                            {
                                "match": {
                                    "company_name": {
                                        "query": q,
                                        "fuzziness": "AUTO",
                                        "boost": 2,
                                    }
                                }
                            },
                            {
                                "match": {
                                    "sector": {
                                        "query": q,
                                        "fuzziness": "AUTO",
                                    }
                                }
                            },
                        ],
                        "minimum_should_match": 1,
                    }
                },
                size=limit,
                collapse={"field": "ticker"},
                _source=_TRADE_SOURCE,
            )
        )
    except Exception as exc:
        logger.error("Fuzzy search failed for '%s': %s", q, exc)
        raise HTTPException(status_code=503, detail=f"ElasticSearch unavailable: {exc}")
    elapsed = (time.perf_counter() - start) * 1000

    hits = result["hits"]["hits"]
    total = _hit_total(result["hits"])

    logger.info("Fuzzy '%s': %d total, %d returned, %.1fms", q, total, len(hits), elapsed)

    response = SearchResponse(
        query=q,
        total_hits=total,
        took_ms=round(elapsed, 1),
        results=_parse_trade_hits(hits),
    )
    search_cache[cache_key] = response
    return response


# ── 4. Similar Tickers ──────────────────────────────────────────


@router.get("/similar/{ticker}", response_model=list[SimilarTicker])
async def find_similar(
    ticker: str,
    limit: int = Query(5, ge=1, le=20),
) -> list[SimilarTicker]:
    """
    Find tickers similar to the given symbol (typically same sector / name overlap).

    Two-step flow: fetch one seed document, then More Like This on sector +
    company_name. Collapsed by ticker for a sidebar "related tickers" widget.
    ClickHouse has no built-in document similarity.
    """
    index = _index_name()
    ticker_upper = ticker.upper()

    cache_key = f"search:similar:{ticker_upper}:{limit}"
    if cache_key in search_cache:
        logger.info("Similar '%s': CACHE HIT", ticker_upper)
        return search_cache[cache_key]

    client = get_es_client()
    start = time.perf_counter()

    try:
        sample = _as_dict(
            client.search(
                index=index,
                query={"term": {"ticker": ticker_upper}},
                size=1,
            )
        )
    except Exception as exc:
        logger.error("Similar seed lookup failed for '%s': %s", ticker_upper, exc)
        raise HTTPException(status_code=503, detail=f"ElasticSearch unavailable: {exc}")

    if not sample["hits"]["hits"]:
        raise HTTPException(status_code=404, detail=f"Ticker '{ticker_upper}' not found")

    doc_id = sample["hits"]["hits"][0]["_id"]

    try:
        result = _as_dict(
            client.search(
                index=index,
                query={
                    "more_like_this": {
                        "fields": ["sector", "company_name"],
                        "like": [{"_index": index, "_id": doc_id}],
                        "min_term_freq": 1,
                        "min_doc_freq": 1,
                        "max_query_terms": 10,
                    }
                },
                size=limit * 5,
                collapse={"field": "ticker"},
                _source=["ticker", "company_name", "sector"],
            )
        )
    except Exception as exc:
        logger.error("Similar MLT query failed for '%s': %s", ticker_upper, exc)
        raise HTTPException(status_code=503, detail=f"ElasticSearch unavailable: {exc}")
    elapsed = (time.perf_counter() - start) * 1000

    hits = result["hits"]["hits"]
    similar = [
        SimilarTicker(
            ticker=h["_source"]["ticker"],
            company_name=h["_source"].get("company_name", "Unknown"),
            sector=h["_source"].get("sector", "Unknown"),
            score=round(float(h.get("_score") or 0), 4),
        )
        for h in hits
        if h["_source"]["ticker"] != ticker_upper
    ][:limit]

    logger.info("Similar to %s: %d results, %.1fms", ticker_upper, len(similar), elapsed)

    search_cache[cache_key] = similar
    return similar
