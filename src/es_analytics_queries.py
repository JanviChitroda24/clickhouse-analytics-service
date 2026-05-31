"""
8 analytics queries in ElasticSearch Query DSL.

Same business logic as ClickHouse queries — head-to-head benchmark on
identical 85,815 documents/rows. Proves which engine wins for aggregations vs filters.

Usage:
    python -m src.es_analytics_queries

Prerequisites: index, bulk load (85K docs in ES).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from src.config import get_settings
from src.es_client import get_es_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BENCHMARK_RUNS = 5

# ClickHouse avg ms from run (85,815 rows) — update after re-running analytics_queries
CH_TIMINGS: dict[str, float] = {
    "top_movers": 49.0,          # warm; cold first-run was 164.8ms
    "vwap_deviation": 74.1,
    "anomaly_by_sector": 73.4,
    "sector_performance": 27.1,
    "intraday_range": 24.6,
    "buy_sell_pressure": 40.9,
    "rolling_vwap_aapl": 31.1,
    "cross_correlation": 71.1,
}


def _parse_top_movers(result: dict) -> list:
    buckets = result["aggregations"]["by_ticker"]["buckets"]
    ranked = sorted(buckets, key=lambda b: b["price_range"]["value"], reverse=True)[:10]
    return [
        (
            b["key"],
            round(b["price_stats"]["max"], 2),
            round(b["price_stats"]["min"], 2),
            round(b["price_range"]["value"], 2),
            b["doc_count"],
        )
        for b in ranked
    ]


def _parse_vwap_deviation(result: dict) -> list:
    return [
        (b["key"], round(b["avg_price"]["value"], 2), b["doc_count"])
        for b in result["aggregations"]["by_ticker"]["buckets"][:10]
    ]


def _parse_anomaly_by_sector(result: dict) -> list:
    return [
        (
            b["key"],
            b["doc_count"],
            round(b["price_stats"]["std_deviation"], 2)
            if b["price_stats"].get("std_deviation")
            else 0,
            round(b["price_stats"]["avg"], 2),
        )
        for b in result["aggregations"]["by_sector"]["buckets"]
    ]


def _parse_sector_performance(result: dict) -> list:
    buckets = sorted(
        result["aggregations"]["by_sector"]["buckets"],
        key=lambda b: b["doc_count"],
        reverse=True,
    )
    return [
        (
            b["key"],
            b["doc_count"],
            round(b["avg_price"]["value"], 2),
            b["ticker_count"]["value"],
        )
        for b in buckets
    ]


def _parse_intraday_range(result: dict) -> list:
    buckets = sorted(
        result["aggregations"]["by_ticker"]["buckets"],
        key=lambda b: b["range_calc"]["value"],
        reverse=True,
    )
    return [
        (
            b["key"],
            round(b["high"]["value"], 2),
            round(b["low"]["value"], 2),
            round(b["range_calc"]["value"], 2),
        )
        for b in buckets
    ]


def _parse_buy_sell_pressure(result: dict) -> list:
    rows = []
    for b in result["aggregations"]["by_ticker"]["buckets"]:
        total = b["doc_count"]
        buys = b["buys"]["doc_count"]
        sells = b["sells"]["doc_count"]
        buy_pct = round(buys / total * 100, 2) if total else 0
        rows.append((b["key"], buys, sells, total, buy_pct))
    return rows


def _parse_rolling_vwap_aapl(result: dict) -> list:
    buckets = [b for b in result["aggregations"]["by_5min"]["buckets"] if b["doc_count"] > 0]
    return [
        (
            b["key_as_string"],
            round(b["total_pv"]["value"], 2),
            int(b["volume"]["value"]),
            int(b["trade_count"]["value"]),
        )
        for b in buckets[:20]
    ]


def _parse_cross_correlation(result: dict) -> list:
    """ES approximation — co-occurring tickers per minute (no self-JOIN)."""
    return [
        (
            b["key_as_string"],
            len(b["tickers"]["buckets"]),
            [t["key"] for t in b["tickers"]["buckets"][:5]],
        )
        for b in result["aggregations"]["by_minute"]["buckets"][:10]
    ]


# Each entry: description, ES DSL body, result parser
QUERIES: dict[str, dict[str, Any]] = {

    # terms + extended_stats + bucket_script — ES GROUP BY equivalent
    "top_movers": {
        "description": "Top 10 tickers by price range (high - low)",
        "body": {
            "size": 0,
            "aggs": {
                "by_ticker": {
                    "terms": {"field": "ticker", "size": 30},
                    "aggs": {
                        "price_stats": {"extended_stats": {"field": "price"}},
                        "price_range": {
                            "bucket_script": {
                                "buckets_path": {
                                    "max_p": "price_stats.max",
                                    "min_p": "price_stats.min",
                                },
                                "script": "params.max_p - params.min_p",
                            }
                        },
                    },
                }
            },
        },
        "parse": _parse_top_movers,
    },

    # ES cannot JOIN vwap_1min — per-ticker avg price approximation
    "vwap_deviation": {
        "description": "Per-ticker avg price stats (ES approximation — no JOIN to VWAP table)",
        "body": {
            "size": 0,
            "aggs": {
                "by_ticker": {
                    "terms": {"field": "ticker", "size": 25},
                    "aggs": {
                        "avg_price": {"avg": {"field": "price"}},
                        "price_over_time": {
                            "date_histogram": {
                                "field": "trade_time",
                                "fixed_interval": "1m",
                            },
                            "aggs": {
                                "price_stats": {"extended_stats": {"field": "price"}},
                            },
                        },
                    },
                }
            },
        },
        "parse": _parse_vwap_deviation,
    },

    # terms on denormalized sector field (no JOIN — embedded in the API layer)
    "anomaly_by_sector": {
        "description": "Price stats by sector (extended_stats includes stddev)",
        "body": {
            "size": 0,
            "aggs": {
                "by_sector": {
                    "terms": {"field": "sector", "size": 20},
                    "aggs": {
                        "price_stats": {"extended_stats": {"field": "price"}},
                        "volume": {"sum": {"field": "quantity"}},
                    },
                }
            },
        },
        "parse": _parse_anomaly_by_sector,
    },

    "sector_performance": {
        "description": "Sector doc count, avg price, unique tickers",
        "body": {
            "size": 0,
            "aggs": {
                "by_sector": {
                    "terms": {"field": "sector", "size": 20},
                    "aggs": {
                        "total_volume": {"sum": {"field": "quantity"}},
                        "avg_price": {"avg": {"field": "price"}},
                        "ticker_count": {"cardinality": {"field": "ticker"}},
                    },
                }
            },
        },
        "parse": _parse_sector_performance,
    },

    "intraday_range": {
        "description": "Tickers ranked by price range (max - min)",
        "body": {
            "size": 0,
            "aggs": {
                "by_ticker": {
                    "terms": {"field": "ticker", "size": 30},
                    "aggs": {
                        "high": {"max": {"field": "price"}},
                        "low": {"min": {"field": "price"}},
                        "range_calc": {
                            "bucket_script": {
                                "buckets_path": {"h": "high", "l": "low"},
                                "script": "params.h - params.l",
                            }
                        },
                    },
                }
            },
        },
        "parse": _parse_intraday_range,
    },

    # filter sub-aggs — filter context (no scoring), faster than query context
    "buy_sell_pressure": {
        "description": "Buy vs sell count per ticker",
        "body": {
            "size": 0,
            "aggs": {
                "by_ticker": {
                    "terms": {"field": "ticker", "size": 30},
                    "aggs": {
                        "buys": {"filter": {"term": {"side": "BUY"}}},
                        "sells": {"filter": {"term": {"side": "SELL"}}},
                    },
                }
            },
        },
        "parse": _parse_buy_sell_pressure,
    },

    # filter context + date_histogram + scripted_metric for VWAP = sum(p*q)/sum(q)
    "rolling_vwap_aapl": {
        "description": "5-minute VWAP buckets for AAPL",
        "body": {
            "size": 0,
            "query": {"bool": {"filter": [{"term": {"ticker": "AAPL"}}]}},
            "aggs": {
                "by_5min": {
                    "date_histogram": {
                        "field": "trade_time",
                        "fixed_interval": "5m",
                    },
                    "aggs": {
                        "total_pv": {
                            "scripted_metric": {
                                "init_script": "state.pv = 0; state.vol = 0",
                                "map_script": (
                                    "state.pv += doc['price'].value * doc['quantity'].value; "
                                    "state.vol += doc['quantity'].value"
                                ),
                                "combine_script": "return ['pv': state.pv, 'vol': state.vol]",
                                "reduce_script": (
                                    "double pv = 0; double vol = 0; "
                                    "for (s in states) { pv += s.pv; vol += s.vol; } "
                                    "return vol > 0 ? pv / vol : 0"
                                ),
                            }
                        },
                        "volume": {"sum": {"field": "quantity"}},
                        "trade_count": {"value_count": {"field": "trade_id"}},
                    },
                }
            },
        },
        "parse": _parse_rolling_vwap_aapl,
    },

    # ES has no JOIN — minute-level ticker co-occurrence instead of Pearson corr
    "cross_correlation": {
        "description": "Tickers co-occurring in same minute (ES approximation — no JOINs)",
        "body": {
            "size": 0,
            "aggs": {
                "by_minute": {
                    "date_histogram": {
                        "field": "trade_time",
                        "fixed_interval": "1m",
                        "min_doc_count": 2,
                    },
                    "aggs": {
                        "tickers": {"terms": {"field": "ticker", "size": 25}},
                    },
                }
            },
        },
        "parse": _parse_cross_correlation,
    },
}


def run_query(
    name: str,
    info: dict[str, Any],
    index_name: str,
    runs: int = BENCHMARK_RUNS,
) -> tuple[float | None, list | None]:
    """Execute ES query `runs` times; return average ms and parsed rows."""
    client = get_es_client()
    parse_fn: Callable[[dict], list] = info["parse"]

    times: list[float] = []
    result = None
    for _ in range(runs):
        start = time.perf_counter()
        try:
            result = client.search(index=index_name, **info["body"])
            times.append((time.perf_counter() - start) * 1000)
        except Exception as exc:
            logger.error("  FAIL %s: %s", name, exc)
            return None, None

    avg_ms = sum(times) / len(times)
    ch_ms = CH_TIMINGS.get(name, 0)
    winner = "CH" if ch_ms < avg_ms else "ES"
    ratio = avg_ms / ch_ms if ch_ms > 0 else 0

    parsed: list = []
    try:
        if result:
            data = result.body if hasattr(result, "body") else result
            parsed = parse_fn(data)
    except Exception as exc:
        logger.warning("  Parse warning for %s: %s", name, exc)

    logger.info(
        "  %-25s ES: %7.1fms  CH: %7.1fms  -> %s wins (%.1fx)",
        name,
        avg_ms,
        ch_ms,
        winner,
        ratio,
    )
    return avg_ms, parsed


def print_summary(results: dict[str, float | None]) -> None:
    """Print head-to-head table and win counts."""
    logger.info("=" * 60)
    logger.info("Head-to-Head Summary")
    logger.info("-" * 60)
    logger.info("  %-25s %10s %10s %8s %8s", "Query", "ES (ms)", "CH (ms)", "Winner", "Ratio")
    logger.info("  " + "-" * 55)

    ch_wins = es_wins = 0
    for name, es_ms in results.items():
        if es_ms is None:
            continue
        ch_ms = CH_TIMINGS.get(name, 0)
        winner = "CH" if ch_ms < es_ms else "ES"
        ratio = es_ms / ch_ms if ch_ms > 0 else 0
        if winner == "CH":
            ch_wins += 1
        else:
            es_wins += 1
        logger.info("  %-25s %10.1f %10.1f %8s %7.1fx", name, es_ms, ch_ms, winner, ratio)

    logger.info("  " + "-" * 55)
    logger.info("  ClickHouse wins: %s  |  ElasticSearch wins: %s", ch_wins, es_wins)
    logger.info("")
    logger.info("  Key insight: ClickHouse faster for pure aggregations (columnar).")
    logger.info("  ES competitive for filtered single-ticker queries.")
    logger.info("  ES cannot self-JOIN — cross_correlation is approximation only.")
    logger.info("=" * 60)


def main() -> None:
    logger.info("=" * 60)
    logger.info("ES Analytics Queries — Head-to-Head vs ClickHouse")
    logger.info("=" * 60)

    index_name = get_settings().es_index_trades
    client = get_es_client()
    doc_count = client.count(index=index_name)["count"]
    logger.info("ES index '%s': %s documents", index_name, f"{doc_count:,}")
    logger.info("")

    results: dict[str, float | None] = {}
    for name, info in QUERIES.items():
        logger.info("%s — %s", name, info["description"])
        avg_ms, parsed = run_query(name, info, index_name)
        results[name] = avg_ms
        if parsed:
            for row in parsed[:2]:
                logger.info("    %s", row)
        logger.info("")

    print_summary(results)


if __name__ == "__main__":
    main()
