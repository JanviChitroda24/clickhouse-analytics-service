"""
Stock Analytics Data Service — FastAPI application shell.

Dual-engine REST layer:
  - Analytics → ClickHouse
  - Search    → ElasticSearch

Usage:
    uvicorn src.api.main:app --reload --port 8000

Swagger UI: http://localhost:8000/docs
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.models import EngineHealth, HealthResponse
from src.api.routes import analytics, search

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def _es_info_dict(info: Any) -> dict:
    """Normalize elasticsearch-py 8.x ObjectApiResponse to dict."""
    return info.body if hasattr(info, "body") else info


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup: connect to ClickHouse and ElasticSearch once, store on app.state.
    Shutdown: close pooled clients cleanly (no leaked connections).
    """
    logger.info("Starting Stock Analytics Data Service...")

    try:
        from src.clickhouse_client import get_ch_client

        ch = get_ch_client()
        version_row = ch.query("SELECT version()").result_rows
        ch_version = version_row[0][0] if version_row else "unknown"
        app.state.ch_client = ch
        logger.info("ClickHouse connected: %s", ch_version)
    except Exception as exc:
        logger.error("ClickHouse connection failed: %s", exc)
        app.state.ch_client = None

    try:
        from src.es_client import get_es_client

        es = get_es_client()
        app.state.es_client = es
        es_version = _es_info_dict(es.info()).get("version", {}).get("number", "unknown")
        logger.info("ElasticSearch connected: v%s", es_version)
    except Exception as exc:
        logger.error("ElasticSearch connection failed: %s", exc)
        app.state.es_client = None

    logger.info("Data Service ready — Swagger UI at http://localhost:8000/docs")
    yield

    logger.info("Shutting down...")
    from src.clickhouse_client import close_client
    from src.es_client import close_es_client

    close_client()
    close_es_client()
    logger.info("Connections closed.")


app = FastAPI(
    title="Stock Analytics Data Service",
    description=(
        "Dual-engine real-time analytics API.\n\n"
        "- **Analytics endpoints** → ClickHouse (OLAP, materialized views)\n"
        "- **Search endpoints** → ElasticSearch (autocomplete, fuzzy, full-text)\n\n"
        "Both engines consume from the same Kafka topic in real-time."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(analytics.router)
app.include_router(search.router)


async def _check_clickhouse() -> EngineHealth:
    """Run SELECT version() — proves CH is reachable."""
    try:
        from src.clickhouse_client import execute_query

        start = time.perf_counter()
        result = execute_query("SELECT version()")
        latency = (time.perf_counter() - start) * 1000
        version = result[0][0] if result else "unknown"
        return EngineHealth(
            engine="clickhouse",
            status="healthy",
            version=str(version),
            latency_ms=round(latency, 1),
        )
    except Exception as exc:
        return EngineHealth(engine="clickhouse", status="unhealthy", error=str(exc))


async def _check_elasticsearch() -> EngineHealth:
    """Ping ES cluster — proves search engine is reachable."""
    try:
        from src.es_client import get_es_client

        client = get_es_client()
        start = time.perf_counter()
        info = _es_info_dict(client.info())
        latency = (time.perf_counter() - start) * 1000
        version = info.get("version", {}).get("number", "unknown")
        return EngineHealth(
            engine="elasticsearch",
            status="healthy",
            version=version,
            latency_ms=round(latency, 1),
        )
    except Exception as exc:
        return EngineHealth(engine="elasticsearch", status="unhealthy", error=str(exc))


@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health_check() -> HealthResponse:
    """
    Check both engine connections.

    Returns partial (degraded) health if one engine is down — analytics may still
    work if ES fails; search may still work if CH fails (graceful degradation).
    """
    ch_health = await _check_clickhouse()
    es_health = await _check_elasticsearch()

    if ch_health.status == "healthy" and es_health.status == "healthy":
        overall = "healthy"
    elif ch_health.status == "unhealthy" and es_health.status == "unhealthy":
        overall = "unhealthy"
    else:
        overall = "degraded"

    return HealthResponse(
        status=overall,
        clickhouse=ch_health,
        elasticsearch=es_health,
    )


@app.get("/health/clickhouse", response_model=EngineHealth, tags=["health"])
async def health_clickhouse() -> EngineHealth:
    """ClickHouse-only health probe."""
    return await _check_clickhouse()


@app.get("/health/elasticsearch", response_model=EngineHealth, tags=["health"])
async def health_elasticsearch() -> EngineHealth:
    """ElasticSearch-only health probe."""
    return await _check_elasticsearch()


@app.get("/", tags=["info"])
async def root() -> dict[str, Any]:
    """Service metadata and doc links."""
    return {
        "service": "Stock Analytics Data Service",
        "version": "1.0.0",
        "engines": {
            "analytics": "ClickHouse (OLAP)",
            "search": "ElasticSearch (inverted index)",
        },
        "docs": "/docs",
        "health": "/health",
    }
