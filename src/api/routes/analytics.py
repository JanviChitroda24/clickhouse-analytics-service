"""
ClickHouse-backed analytics endpoints.

Five OLAP routes for dashboard panels — each wraps production SQL from
analytics_queries.py with HTTP parameters, parameterized ClickHouse syntax,
Pydantic response models, and per-request timing logs.
"""

from __future__ import annotations

from datetime import datetime
import logging
import time

from fastapi import APIRouter, HTTPException, Query

from src.api.models import (
    AnomalyTrade,
    MarketSummary,
    SectorPerformance,
    TopMover,
    VWAPPoint,
    VWAPResponse,
    TradeBrowseItem,
    TradeBrowseResponse,
)
from src.api.cache import market_summary_cache, vwap_cache
from src.clickhouse_client import execute_query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])

# Whitelist granularity → ClickHouse time-bucket expression (never interpolate raw user strings)
GRANULARITY_MAP: dict[str, str] = {
    "1min": "toStartOfMinute(trade_time)",
    "5min": "toStartOfFiveMinutes(trade_time)",
    "1h": "toStartOfHour(trade_time)",
    "1d": "toDate(trade_time)",
}


@router.get("/vwap/{ticker}", response_model=VWAPResponse)
async def get_vwap(
    ticker: str,
    granularity: str = Query("5min", pattern="^(1min|5min|1h|1d)$"),
    limit: int = Query(100, ge=1, le=1000),
) -> VWAPResponse:
    """
    VWAP time-series for one ticker.

    Returns bucketed volume-weighted average price for charting.
    User-supplied ticker uses {ticker:String} — not string concatenation.
    """
    ticker_upper = ticker.upper()
    cache_key = f"vwap:{ticker_upper}:{granularity}:{limit}"
    if cache_key in vwap_cache:
        logger.info("VWAP %s (%s): CACHE HIT", ticker_upper, granularity)
        return vwap_cache[cache_key]

    bucket_fn = GRANULARITY_MAP[granularity]
    start = time.perf_counter()

    try:
        rows = execute_query(
            f"""
            SELECT
                {bucket_fn} AS window_start,
                round(sum(price * quantity) / sum(quantity), 4) AS vwap,
                sum(quantity) AS volume,
                count() AS trade_count
            FROM stock_analytics.raw_trades
            WHERE ticker = {{ticker:String}}
            GROUP BY window_start
            ORDER BY window_start DESC
            LIMIT {{limit:UInt32}}
            """,
            parameters={"ticker": ticker_upper, "limit": limit},
        )
    except Exception as exc:
        logger.error("VWAP query failed for %s: %s", ticker_upper, exc)
        raise HTTPException(status_code=503, detail=f"ClickHouse unavailable: {exc}")

    elapsed = (time.perf_counter() - start) * 1000
    logger.info(
        "VWAP %s (%s): %.1fms, %d points",
        ticker_upper,
        granularity,
        elapsed,
        len(rows),
    )

    response = VWAPResponse(
        ticker=ticker_upper,
        granularity=granularity,
        data=[
            VWAPPoint(
                ticker=ticker_upper,
                window_start=row[0],
                vwap=float(row[1]),
                volume=int(row[2]),
                trade_count=int(row[3]),
            )
            for row in rows
        ],
    )
    vwap_cache[cache_key] = response
    return response


@router.get("/top-movers", response_model=list[TopMover])
async def get_top_movers(
    limit: int = Query(10, ge=1, le=50),
) -> list[TopMover]:
    """
    Tickers with the largest intraday price range (% swing).

    Leaderboard panel — highlights names that moved the most.
    """
    start = time.perf_counter()

    try:
        rows = execute_query(
            """
            SELECT ticker,
                   max(price) AS high,
                   min(price) AS low,
                   round(max(price) - min(price), 2) AS price_range,
                   round((max(price) - min(price)) / min(price) * 100, 2) AS range_pct,
                   count() AS trades
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY range_pct DESC
            LIMIT {limit:UInt32}
            """,
            parameters={"limit": limit},
        )
    except Exception as exc:
        logger.error("Top movers query failed: %s", exc)
        raise HTTPException(status_code=503, detail=f"ClickHouse unavailable: {exc}")

    elapsed = (time.perf_counter() - start) * 1000
    logger.info("Top movers: %.1fms, %d rows", elapsed, len(rows))

    return [
        TopMover(
            ticker=row[0],
            high=float(row[1]),
            low=float(row[2]),
            price_range=float(row[3]),
            range_pct=float(row[4]),
            trades=int(row[5]),
        )
        for row in rows
    ]


@router.get("/sectors/performance", response_model=list[SectorPerformance])
async def get_sector_performance() -> list[SectorPerformance]:
    """
    Sector-level aggregation via JOIN to company_metadata.

    Portfolio view — notional volume and activity by sector.
    """
    start = time.perf_counter()

    try:
        rows = execute_query(
            """
            SELECT m.sector,
                   round(sum(t.quantity * t.price), 2) AS total_notional,
                   round(avg(t.price), 2) AS avg_price,
                   count(DISTINCT t.ticker) AS tickers,
                   count() AS trades
            FROM stock_analytics.raw_trades t
            INNER JOIN stock_analytics.company_metadata m ON t.ticker = m.ticker
            GROUP BY m.sector
            ORDER BY total_notional DESC
            """
        )
    except Exception as exc:
        logger.error("Sector performance query failed: %s", exc)
        raise HTTPException(status_code=503, detail=f"ClickHouse unavailable: {exc}")

    elapsed = (time.perf_counter() - start) * 1000
    logger.info("Sector performance: %.1fms, %d sectors", elapsed, len(rows))

    return [
        SectorPerformance(
            sector=row[0],
            total_notional=float(row[1]),
            avg_price=float(row[2]),
            ticker_count=int(row[3]),
            trade_count=int(row[4]),
        )
        for row in rows
    ]


@router.get("/anomalies", response_model=list[AnomalyTrade])
async def get_anomalies(
    ticker: str | None = Query(None, description="Optional ticker filter"),
    min_deviation: float = Query(2.0, ge=0.5, le=5.0),
    limit: int = Query(50, ge=1, le=200),
) -> list[AnomalyTrade]:
    """
    Trades deviating more than N standard deviations from ticker mean.

    Risk / surveillance panel — catches fat-finger or unusual prints.
    """
    start = time.perf_counter()

    ticker_filter = "AND t.ticker = {ticker:String}" if ticker else ""
    params: dict[str, object] = {"min_dev": min_deviation, "limit": limit}
    if ticker:
        params["ticker"] = ticker.upper()

    try:
        rows = execute_query(
            f"""
            SELECT t.trade_id,
                   t.ticker,
                   t.price,
                   s.avg_price,
                   round((t.price - s.avg_price) / s.avg_price * 100, 4) AS deviation_pct,
                   t.side,
                   t.trade_time
            FROM stock_analytics.raw_trades t
            INNER JOIN (
                SELECT ticker,
                       avg(price) AS avg_price,
                       stddevPop(price) AS stddev_price
                FROM stock_analytics.raw_trades
                GROUP BY ticker
            ) s ON t.ticker = s.ticker
            WHERE abs(t.price - s.avg_price) > {{min_dev:Float64}} * s.stddev_price
                {ticker_filter}
            ORDER BY abs(deviation_pct) DESC
            LIMIT {{limit:UInt32}}
            """,
            parameters=params,
        )
    except Exception as exc:
        logger.error("Anomalies query failed: %s", exc)
        raise HTTPException(status_code=503, detail=f"ClickHouse unavailable: {exc}")

    elapsed = (time.perf_counter() - start) * 1000
    logger.info(
        "Anomalies (threshold=%.1f): %.1fms, %d rows",
        min_deviation,
        elapsed,
        len(rows),
    )

    return [
        AnomalyTrade(
            trade_id=str(row[0]),
            ticker=row[1],
            price=float(row[2]),
            avg_price=float(row[3]),
            deviation_pct=float(row[4]),
            side=row[5],
            trade_time=row[6],
        )
        for row in rows
    ]


@router.get("/trades", response_model=TradeBrowseResponse)
async def browse_trades(
    ticker: str = Query(..., description="Ticker symbol"),
    after: datetime | None = Query(None, description="Cursor — trade_time of last seen trade"),
    limit: int = Query(100, ge=1, le=500),
) -> TradeBrowseResponse:
    """
    Browse raw trades with cursor-based pagination.

    Cursor-based approach avoids OFFSET rescans by using `trade_time > after`
    combined with `ORDER BY trade_time`.
    """
    ticker_upper = ticker.upper()
    after_sql = "AND trade_time > {after:DateTime64}" if after else ""

    start = time.perf_counter()
    try:
        rows = execute_query(
            f"""
            SELECT
                trade_id,
                ticker,
                price,
                quantity,
                trade_time,
                side,
                trade_type,
                source
            FROM stock_analytics.raw_trades
            WHERE ticker = {{ticker:String}}
                {after_sql}
            ORDER BY trade_time
            LIMIT {{limit:UInt32}}
            """,
            parameters={
                "ticker": ticker_upper,
                "after": after,
                "limit": limit,
            },
        )
    except Exception as exc:
        logger.error("Trades browse query failed for %s: %s", ticker_upper, exc)
        raise HTTPException(status_code=503, detail=f"ClickHouse unavailable: {exc}")

    elapsed = (time.perf_counter() - start) * 1000
    logger.info(
        "Trades browse %s (after=%s): %.1fms, %d rows",
        ticker_upper,
        after.isoformat() if after else None,
        elapsed,
        len(rows),
    )

    trades = [
        TradeBrowseItem(
            trade_id=str(row[0]),
            ticker=row[1],
            price=float(row[2]),
            quantity=int(row[3]),
            trade_time=row[4],
            side=row[5],
            trade_type=row[6],
            source=row[7],
        )
        for row in rows
    ]

    next_cursor = trades[-1].trade_time if trades else None
    has_more = len(trades) == limit
    return TradeBrowseResponse(
        ticker=ticker_upper,
        trades=trades,
        count=len(trades),
        next_cursor=next_cursor,
        has_more=has_more,
    )


@router.get("/market/summary", response_model=MarketSummary)
async def get_market_summary() -> MarketSummary:
    """
    Dashboard homepage — one call for overview stats, top movers, and sectors.

    Avoids three separate HTTP round-trips for the main screen.
    """
    cache_key = "market_summary"
    if cache_key in market_summary_cache:
        logger.info("Market summary: CACHE HIT")
        return market_summary_cache[cache_key]

    start = time.perf_counter()
    try:
        overview = execute_query(
            """
            SELECT count() AS trades,
                   sum(quantity) AS volume,
                   uniqExact(ticker) AS tickers,
                   max(trade_time) AS latest
            FROM stock_analytics.raw_trades
            """
        )

        movers = execute_query(
            """
            SELECT ticker,
                   max(price) AS high,
                   min(price) AS low,
                   round(max(price) - min(price), 2) AS price_range,
                   round((max(price) - min(price)) / min(price) * 100, 2) AS range_pct,
                   count() AS trades
            FROM stock_analytics.raw_trades
            GROUP BY ticker
            ORDER BY range_pct DESC
            LIMIT 5
            """
        )

        sectors = execute_query(
            """
            SELECT m.sector,
                   round(sum(t.quantity * t.price), 2),
                   round(avg(t.price), 2),
                   count(DISTINCT t.ticker),
                   count()
            FROM stock_analytics.raw_trades t
            INNER JOIN stock_analytics.company_metadata m ON t.ticker = m.ticker
            GROUP BY m.sector
            ORDER BY sum(t.quantity * t.price) DESC
            """
        )
    except Exception as exc:
        logger.error("Market summary query failed: %s", exc)
        raise HTTPException(status_code=503, detail=f"ClickHouse unavailable: {exc}")

    elapsed = (time.perf_counter() - start) * 1000
    logger.info("Market summary: %.1fms", elapsed)

    row = overview[0]
    response = MarketSummary(
        total_trades=int(row[0]),
        total_volume=int(row[1]),
        total_tickers=int(row[2]),
        latest_trade=row[3],
        top_movers=[
            TopMover(
                ticker=r[0],
                high=float(r[1]),
                low=float(r[2]),
                price_range=float(r[3]),
                range_pct=float(r[4]),
                trades=int(r[5]),
            )
            for r in movers
        ],
        sector_performance=[
            SectorPerformance(
                sector=r[0],
                total_notional=float(r[1]),
                avg_price=float(r[2]),
                ticker_count=int(r[3]),
                trade_count=int(r[4]),
            )
            for r in sectors
        ],
    )

    market_summary_cache[cache_key] = response
    return response
