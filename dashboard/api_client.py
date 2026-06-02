"""
HTTP client for the Stock Analytics FastAPI service.

Wraps httpx with client-side latency measurement so the Streamlit UI can
display round-trip times per engine (ClickHouse vs ElasticSearch routes).
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

import httpx

DEFAULT_API_BASE = os.getenv("ANALYTICS_API_BASE", "http://localhost:8000")


@dataclass
class TimedResponse:
    """API JSON payload plus measured client round-trip time in milliseconds."""

    data: Any
    latency_ms: float
    status_code: int
    error: str | None = None


class AnalyticsApiClient:
    """Thin wrapper around FastAPI analytics and search endpoints."""

    def __init__(self, base_url: str = DEFAULT_API_BASE, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, params: dict | None = None) -> TimedResponse:
        url = f"{self.base_url}{path}"
        try:
            with httpx.Client(timeout=self.timeout) as client:
                t0 = time.perf_counter()
                response = client.get(url, params=params or {})
                latency_ms = (time.perf_counter() - t0) * 1000
                if response.status_code >= 400:
                    return TimedResponse(
                        data=None,
                        latency_ms=latency_ms,
                        status_code=response.status_code,
                        error=response.text[:500],
                    )
                return TimedResponse(
                    data=response.json(),
                    latency_ms=latency_ms,
                    status_code=response.status_code,
                )
        except Exception as exc:
            return TimedResponse(
                data=None,
                latency_ms=0.0,
                status_code=0,
                error=str(exc),
            )

    def health(self) -> TimedResponse:
        return self._get("/health")

    def market_summary(self) -> TimedResponse:
        return self._get("/api/v1/analytics/market/summary")

    def autocomplete(self, q: str, limit: int = 8) -> TimedResponse:
        return self._get("/api/v1/search/autocomplete", {"q": q, "limit": limit})

    def vwap(self, ticker: str, granularity: str = "5min", limit: int = 100) -> TimedResponse:
        return self._get(
            f"/api/v1/analytics/vwap/{ticker.upper()}",
            {"granularity": granularity, "limit": limit},
        )

    def top_movers(self, limit: int = 10) -> TimedResponse:
        return self._get("/api/v1/analytics/top-movers", {"limit": limit})

    def sector_performance(self) -> TimedResponse:
        return self._get("/api/v1/analytics/sectors/performance")

    def anomalies(
        self,
        ticker: str | None = None,
        min_deviation: float = 2.0,
        limit: int = 50,
    ) -> TimedResponse:
        params: dict[str, Any] = {"min_deviation": min_deviation, "limit": limit}
        if ticker:
            params["ticker"] = ticker.upper()
        return self._get("/api/v1/analytics/anomalies", params)

    def browse_trades_ch(self, ticker: str, limit: int = 20) -> TimedResponse:
        """ClickHouse cursor browse — structured filter by ticker."""
        return self._get(
            "/api/v1/analytics/trades",
            {"ticker": ticker.upper(), "limit": limit},
        )

    def search_trades_es(self, ticker: str, limit: int = 20) -> TimedResponse:
        """ElasticSearch full-text search filtered to one ticker."""
        return self._get(
            "/api/v1/search/trades",
            {"q": ticker, "ticker": ticker.upper(), "limit": limit},
        )
