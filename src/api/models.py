"""
Pydantic response models for the Stock Analytics Data Service.

Every API response has a defined shape — validation, OpenAPI docs, and type safety.
Analytics and search endpoints return these models instead of raw dicts from CH/ES.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

# ── Health ──────────────────────────────────────────────────────

class EngineHealth(BaseModel):
 """Per-engine health probe result."""

 engine: str
 status: str # healthy | unhealthy | degraded
 version: str | None = None
 latency_ms: float | None = None
 error: str | None = None

class HealthResponse(BaseModel):
 """Combined health — overall status + both engines."""

 status: str # healthy | degraded | unhealthy
 clickhouse: EngineHealth
 elasticsearch: EngineHealth

# ── Analytics (ClickHouse-backed) ───────────────────────────────

class VWAPPoint(BaseModel):
 ticker: str
 window_start: datetime
 vwap: float
 volume: int
 trade_count: int

class VWAPResponse(BaseModel):
 ticker: str
 granularity: str
 data: list[VWAPPoint]

class TopMover(BaseModel):
 ticker: str
 high: float
 low: float
 price_range: float
 range_pct: float
 trades: int

class SectorPerformance(BaseModel):
 sector: str
 total_notional: float
 avg_price: float
 ticker_count: int
 trade_count: int

class MarketSummary(BaseModel):
 total_trades: int
 total_volume: int
 total_tickers: int
 latest_trade: datetime | None = None
 top_movers: list[TopMover]
 sector_performance: list[SectorPerformance]

class AnomalyTrade(BaseModel):
 trade_id: str
 ticker: str
 price: float
 avg_price: float
 deviation_pct: float
 side: str
 trade_time: datetime

class BuySellPressure(BaseModel):
 ticker: str
 buys: int
 sells: int
 total: int
 buy_pct: float


class TradeBrowseItem(BaseModel):
    """Single trade document for cursor-based browsing."""

    trade_id: str
    ticker: str
    price: float
    quantity: int
    trade_time: datetime
    side: str
    trade_type: str
    source: str


class TradeBrowseResponse(BaseModel):
    """Cursor-based paginated trade browsing response."""

    ticker: str
    trades: list[TradeBrowseItem]
    count: int
    next_cursor: datetime | None = None
    has_more: bool

# ── Search (ElasticSearch-backed) ───────────────────────────────

class AutocompleteResult(BaseModel):
 ticker: str
 company_name: str
 score: float | None = None

class SearchHit(BaseModel):
 trade_id: str
 ticker: str
 company_name: str
 sector: str
 price: float
 quantity: int
 side: str
 trade_time: str
 score: float

class SearchResponse(BaseModel):
 query: str
 total_hits: int
 results: list[SearchHit]
 took_ms: float

class SimilarTicker(BaseModel):
 ticker: str
 company_name: str
 sector: str
 score: float
