"""
API integration tests for the Stock Analytics Data Service.

Uses httpx AsyncClient with ASGITransport to call the FastAPI app in-process
(no separate uvicorn process required for pytest).

Prerequisites: ClickHouse and ElasticSearch running with populated data.

Usage:
    pytest tests/test_api.py -v
    pytest tests/test_api.py -v -m integration
"""

from __future__ import annotations

from urllib.parse import quote

import pytest
from httpx import ASGITransport, AsyncClient

from src.api.main import app

transport = ASGITransport(app=app)


@pytest.fixture
async def client():
    """In-process HTTP client bound to the FastAPI application."""
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client


# ── Health ──────────────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.asyncio
async def test_root(client: AsyncClient) -> None:
    response = await client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "service" in data
    assert "engines" in data


@pytest.mark.integration
@pytest.mark.asyncio
async def test_health(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ("healthy", "degraded", "unhealthy")
    assert "clickhouse" in data
    assert "elasticsearch" in data


@pytest.mark.integration
@pytest.mark.asyncio
async def test_health_clickhouse(client: AsyncClient) -> None:
    response = await client.get("/health/clickhouse")
    assert response.status_code == 200
    data = response.json()
    assert data["engine"] == "clickhouse"
    assert data["status"] in ("healthy", "unhealthy")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_health_elasticsearch(client: AsyncClient) -> None:
    response = await client.get("/health/elasticsearch")
    assert response.status_code == 200
    data = response.json()
    assert data["engine"] == "elasticsearch"
    assert data["status"] in ("healthy", "unhealthy")


# ── Analytics (ClickHouse) ──────────────────────────────────────


@pytest.mark.integration
@pytest.mark.asyncio
async def test_vwap_aapl(client: AsyncClient) -> None:
    response = await client.get("/api/v1/analytics/vwap/AAPL?granularity=5min&limit=10")
    assert response.status_code == 200
    data = response.json()
    assert data["ticker"] == "AAPL"
    assert data["granularity"] == "5min"
    assert len(data["data"]) > 0
    point = data["data"][0]
    assert "vwap" in point
    assert "volume" in point
    assert point["vwap"] > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_vwap_invalid_granularity(client: AsyncClient) -> None:
    response = await client.get("/api/v1/analytics/vwap/AAPL?granularity=banana")
    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.asyncio
async def test_top_movers(client: AsyncClient) -> None:
    response = await client.get("/api/v1/analytics/top-movers?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert len(data) <= 5
    if len(data) >= 2:
        assert data[0]["range_pct"] >= data[-1]["range_pct"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sector_performance(client: AsyncClient) -> None:
    response = await client.get("/api/v1/analytics/sectors/performance")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert "sector" in data[0]
    assert "total_notional" in data[0]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_anomalies(client: AsyncClient) -> None:
    response = await client.get("/api/v1/analytics/anomalies?min_deviation=2.0&limit=10")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if data:
        assert "deviation_pct" in data[0]
        assert abs(data[0]["deviation_pct"]) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_market_summary(client: AsyncClient) -> None:
    response = await client.get("/api/v1/analytics/market/summary")
    assert response.status_code == 200
    data = response.json()
    assert data["total_trades"] > 0
    assert data["total_tickers"] > 0
    assert len(data["top_movers"]) > 0
    assert len(data["sector_performance"]) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trades_pagination(client: AsyncClient) -> None:
    response = await client.get("/api/v1/analytics/trades?ticker=AAPL&limit=5")
    assert response.status_code == 200
    page1 = response.json()
    assert page1["count"] == 5
    assert page1["has_more"] is True
    assert page1["next_cursor"] is not None

    cursor = quote(str(page1["next_cursor"]), safe="")
    response2 = await client.get(f"/api/v1/analytics/trades?ticker=AAPL&after={cursor}&limit=5")
    assert response2.status_code == 200
    page2 = response2.json()
    assert page2["count"] == 5
    assert page2["trades"][0]["trade_time"] >= page1["trades"][-1]["trade_time"]


# ── Search (ElasticSearch) ──────────────────────────────────────


@pytest.mark.integration
@pytest.mark.asyncio
async def test_autocomplete(client: AsyncClient) -> None:
    response = await client.get("/api/v1/search/autocomplete?q=AAP")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    tickers = {item["ticker"] for item in data}
    assert "AAPL" in tickers


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_trades(client: AsyncClient) -> None:
    response = await client.get("/api/v1/search/trades?q=financial+services")
    assert response.status_code == 200
    data = response.json()
    assert data["total_hits"] > 0
    assert len(data["results"]) > 0
    assert data["results"][0]["score"] > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fuzzy_search(client: AsyncClient) -> None:
    response = await client.get("/api/v1/search/fuzzy?q=Micorsoft")
    assert response.status_code == 200
    data = response.json()
    assert data["total_hits"] > 0
    tickers = {hit["ticker"] for hit in data["results"]}
    assert "MSFT" in tickers


@pytest.mark.integration
@pytest.mark.asyncio
async def test_similar_tickers(client: AsyncClient) -> None:
    response = await client.get("/api/v1/search/similar/AAPL?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    tickers = {item["ticker"] for item in data}
    assert "AAPL" not in tickers


@pytest.mark.integration
@pytest.mark.asyncio
async def test_similar_not_found(client: AsyncClient) -> None:
    response = await client.get("/api/v1/search/similar/ZZZZZ")
    assert response.status_code == 404
