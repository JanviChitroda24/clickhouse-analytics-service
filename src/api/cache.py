"""
TTL caches for API responses.

These live in-process and dramatically reduce repeat load when dashboards refresh
frequently (e.g., same market summary polled every few seconds).
"""

from cachetools import TTLCache

# Market summary updates slowly and is expensive (multiple ClickHouse sub-queries).
market_summary_cache: TTLCache = TTLCache(maxsize=1, ttl=60)

# VWAP changes faster (minutes), but the dashboard typically refreshes frequently.
vwap_cache: TTLCache = TTLCache(maxsize=500, ttl=10)

# Search results are stable across short windows.
autocomplete_cache: TTLCache = TTLCache(maxsize=500, ttl=60)
search_cache: TTLCache = TTLCache(maxsize=500, ttl=30)

