"""
ClickHouse-backed analytics routes.

Stub router — endpoints for VWAP, sectors, top movers, etc.
"""

from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])

# Planned: GET /vwap/{ticker}, /sectors/performance, /market/summary,...
