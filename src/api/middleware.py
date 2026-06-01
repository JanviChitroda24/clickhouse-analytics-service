"""
API request logging middleware.

Logs every request method/path/status with elapsed time to support debugging
and performance monitoring.
"""

from __future__ import annotations

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log request/response timing for observability."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        start = time.perf_counter()
        try:
            response: Response = await call_next(request)
            status = response.status_code
        except Exception:
            elapsed = (time.perf_counter() - start) * 1000
            # Health endpoints can be noisy; keep them out of the main log stream.
            if request.url.path not in (
                "/health",
                "/health/clickhouse",
                "/health/elasticsearch",
            ):
                logger.exception(
                    "%s %s -> 500 (%.0fms) [exception]",
                    request.method,
                    request.url.path,
                    elapsed,
                )
            raise

        elapsed = (time.perf_counter() - start) * 1000
        if request.url.path not in (
            "/health",
            "/health/clickhouse",
            "/health/elasticsearch",
        ):
            logger.info(
                "%s %s -> %s (%.0fms)",
                request.method,
                request.url.path,
                status,
                elapsed,
            )
        return response

