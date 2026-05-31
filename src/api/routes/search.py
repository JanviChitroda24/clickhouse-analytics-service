"""
ElasticSearch-backed search routes.

Stub router — endpoints for autocomplete, fuzzy, full-text, MLT.
"""

from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/search", tags=["search"])

# Planned: GET /autocomplete, /query, /similar/{ticker},...
