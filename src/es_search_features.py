"""
ElasticSearch search features ClickHouse cannot do (Hour 14).

Four capabilities that justify the dual-engine architecture:
  1. Autocomplete — completion suggester (FST on suggest field from Hour 12)
  2. Fuzzy matching — edit distance on analyzed text fields
  3. Full-text search — tokenized multi_match with relevance ranking
  4. More Like This — document similarity via term overlap

Usage:
    python -m src.es_search_features

Prerequisites: Hour 11 index, Hour 12 bulk load (85K docs with suggest field).
"""

from __future__ import annotations

import logging
import time
from typing import Any

from src.config import get_settings
from src.es_client import get_es_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def _response_data(result: Any) -> dict:
    """Normalize elasticsearch-py 8.x ObjectApiResponse to dict."""
    return result.body if hasattr(result, "body") else result


def timed_search(client, body: dict) -> tuple[float, dict]:
    """Run ES search/suggest request; return elapsed ms and parsed response dict."""
    start = time.perf_counter()
    result = client.search(index=get_settings().es_index_trades, **body)
    elapsed = (time.perf_counter() - start) * 1000
    return elapsed, _response_data(result)


def _hit_total(hits_section: dict) -> int:
    """ES 7+ total may be int or {value: N}."""
    total = hits_section.get("total", 0)
    if isinstance(total, dict):
        return int(total.get("value", 0))
    return int(total)


# ── Feature 1: Autocomplete ───────────────────────────────────────


def test_autocomplete(client) -> None:
    """
    Completion suggester — type-ahead on every keystroke (<10ms target).

    Uses Hour 12 `suggest` field with inputs: ticker, company_name, combined.
    ClickHouse: no FST — LIKE 'Goo%' is full scan, won't match company names.
    """
    logger.info("=" * 60)
    logger.info("Feature 1: AUTOCOMPLETE (Completion Suggester)")
    logger.info("  ClickHouse: impossible — no completion data structure")
    logger.info("-" * 60)

    test_cases = [
        ("AAP", "Ticker prefix — should find AAPL"),
        ("Goo", "Company prefix — should find GOOGL (Alphabet)"),
        ("Mic", "Company prefix — should find MSFT (Microsoft)"),
        ("NV", "Ticker prefix — should find NVDA"),
        ("App", "Matches AAPL ticker and Apple company name inputs"),
    ]

    for prefix, description in test_cases:
        body = {
            "suggest": {
                "ticker-suggest": {
                    "prefix": prefix,
                    "completion": {
                        "field": "suggest",
                        "size": 5,
                        "skip_duplicates": True,
                    },
                }
            }
        }

        elapsed, result = timed_search(client, body)
        options = result["suggest"]["ticker-suggest"][0]["options"]
        matches = []
        for opt in options[:3]:
            src = opt.get("_source") or {}
            if src.get("ticker"):
                matches.append(f"{src['ticker']} ({src.get('company_name', '?')})")
            else:
                matches.append(opt.get("text", str(opt)))

        logger.info("  '%s' -> %s  (%.1fms)  — %s", prefix, matches, elapsed, description)


# ── Feature 2: Fuzzy Matching ─────────────────────────────────────


def test_fuzzy_matching(client) -> None:
    """
    Fuzzy query — Levenshtein edit distance on inverted index terms.

    ClickHouse: WHERE ticker = 'APPL' returns 0 rows (exact match only).
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("Feature 2: FUZZY MATCHING (Edit Distance)")
    logger.info("  ClickHouse: WHERE ticker = 'APPL' -> 0 rows (exact match only)")
    logger.info("-" * 60)

    test_cases = [
        ("APPL", "ticker_text", "Typo: APPL -> AAPL (1 edit)"),
        ("GOGL", "ticker_text", "Typo: GOGL -> GOOGL (1 edit)"),
        ("Appel", "company_name", "Typo: Appel -> Apple (1 edit)"),
        ("Micorsoft", "company_name", "Typo: Micorsoft -> Microsoft (1 edit)"),
        ("TSAL", "ticker_text", "Transposition: TSAL -> TSLA (1 swap)"),
    ]

    for query_text, field, description in test_cases:
        body = {
            "query": {
                "fuzzy": {
                    field: {
                        "value": query_text,
                        "fuzziness": "AUTO",
                    }
                }
            },
            "size": 3,
            "_source": ["ticker", "company_name", "price"],
        }

        elapsed, result = timed_search(client, body)
        hits = result["hits"]["hits"]
        total = _hit_total(result["hits"])
        matches = [h["_source"]["ticker"] for h in hits]

        logger.info(
            "  '%s' on %s -> %s (%d total)  (%.1fms)  — %s",
            query_text,
            field,
            matches,
            total,
            elapsed,
            description,
        )


# ── Feature 3: Full-Text Search ───────────────────────────────────


def test_fulltext_search(client) -> None:
    """
    multi_match on company_name + sector — TF-IDF relevance ranking.

    ClickHouse: LIKE '%Goldman%' — no ranking, full scan, no cross-field match.
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("Feature 3: FULL-TEXT SEARCH (Inverted Index + Relevance)")
    logger.info("  ClickHouse: LIKE '%%Goldman%%' — no relevance ranking, full scan")
    logger.info("-" * 60)

    test_cases = [
        ("Apple technology", "Should find AAPL (Apple Inc, Technology sector)"),
        ("financial services", "Should find JPM, V, MA (Financial Services sector)"),
        ("energy oil", "Should find XOM, CVX (energy/oil companies)"),
        ("consumer retail", "Should find WMT, COST, HD (consumer/retail)"),
        ("healthcare pharma", "Should find JNJ, MRK, ABBV, LLY (healthcare)"),
    ]

    for query_text, description in test_cases:
        body = {
            "query": {
                "multi_match": {
                    "query": query_text,
                    "fields": ["company_name^2", "sector"],
                    "type": "best_fields",
                    "fuzziness": "AUTO",
                }
            },
            "size": 5,
            "_source": ["ticker", "company_name", "sector"],
            "collapse": {"field": "ticker"},
        }

        elapsed, result = timed_search(client, body)
        hits = result["hits"]["hits"]
        matches = [
            f"{h['_source']['ticker']} ({h['_source'].get('sector', '?')})"
            for h in hits
        ]

        logger.info("  '%s' -> %s  (%.1fms)  — %s", query_text, matches, elapsed, description)


# ── Feature 4: More Like This ─────────────────────────────────────


def _run_mlt(client, seed_ticker: str, label: str) -> None:
    """Find documents similar to one trade doc for the given ticker."""
    index_name = get_settings().es_index_trades
    sample = _response_data(
        client.search(
            index=index_name,
            query={"term": {"ticker": seed_ticker}},
            size=1,
        )
    )
    hits = sample["hits"]["hits"]
    if not hits:
        logger.warning("  No %s documents found — skipping MLT", seed_ticker)
        return

    doc_id = hits[0]["_id"]
    seed = hits[0]["_source"]

    body = {
        "query": {
            "more_like_this": {
                "fields": ["sector", "company_name"],
                "like": [{"_index": index_name, "_id": doc_id}],
                "min_term_freq": 1,
                "min_doc_freq": 1,
                "max_query_terms": 10,
            }
        },
        "size": 10,
        "_source": ["ticker", "company_name", "sector"],
        "collapse": {"field": "ticker"},
    }

    elapsed, result = timed_search(client, body)
    mlt_hits = result["hits"]["hits"]
    matches = [
        f"{h['_source']['ticker']} ({h['_source'].get('company_name', '?')}, "
        f"{h['_source'].get('sector', '?')})"
        for h in mlt_hits
    ]

    logger.info(
        "  Similar to %s (%s, %s):",
        seed_ticker,
        seed.get("company_name", "?"),
        seed.get("sector", "?"),
    )
    for match in matches:
        logger.info("    -> %s", match)
    logger.info("  (%s) (%.1fms)", label, elapsed)


def test_more_like_this(client) -> None:
    """
    MLT — extract significant terms from a seed doc, find overlapping documents.

    ClickHouse: no built-in document similarity — would need custom cosine logic.
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("Feature 4: MORE LIKE THIS (Document Similarity)")
    logger.info("  ClickHouse: no built-in similarity — custom cosine only")
    logger.info("-" * 60)

    _run_mlt(client, "AAPL", "Technology peers")
    logger.info("")
    _run_mlt(client, "JPM", "Financial Services peers")


def print_summary() -> None:
    """Recap — why these four features justify ES alongside ClickHouse."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary: ES Features ClickHouse Cannot Do")
    logger.info("-" * 60)
    logger.info("  1. Autocomplete    — completion field + FST (O(prefix length))")
    logger.info("  2. Fuzzy matching  — edit distance on inverted index terms")
    logger.info("  3. Full-text search — tokenized, relevance-ranked, multi-field")
    logger.info("  4. More Like This  — document similarity via term overlap")
    logger.info("")
    logger.info("  Dual-engine split:")
    logger.info("    ClickHouse -> analytics (VWAP, aggregations, time-series)")
    logger.info("    ElasticSearch -> search (autocomplete, fuzzy, full-text, MLT)")
    logger.info("=" * 60)


def main() -> None:
    logger.info("=" * 60)
    logger.info("ES-Specific Search Features (ClickHouse Can't Do These)")
    logger.info("=" * 60)

    client = get_es_client()
    index_name = get_settings().es_index_trades
    doc_count = client.count(index=index_name)["count"]
    logger.info("ES index '%s': %s documents", index_name, f"{doc_count:,}")
    logger.info("")

    test_autocomplete(client)
    test_fuzzy_matching(client)
    test_fulltext_search(client)
    test_more_like_this(client)
    print_summary()


if __name__ == "__main__":
    main()
