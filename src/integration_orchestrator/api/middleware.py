"""HTTP middleware.

Three concerns, kept separate so each can be reasoned about on its own:
correlation propagation, access logging with metrics, and a body size limit.

Order matters. Correlation is outermost so that everything inner — including the
access log and any error handler — sees the bound identifiers.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Awaitable, Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.status import HTTP_413_CONTENT_TOO_LARGE
from starlette.types import ASGIApp

from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.domain.enums import ErrorCategory
from integration_orchestrator.observability.correlation import correlation_scope

logger = logging.getLogger("integration_orchestrator.access")

CORRELATION_HEADER = "X-Correlation-ID"
REQUEST_ID_HEADER = "X-Request-ID"

#: Paths excluded from the access log. They are polled continuously by
#: orchestrators and scrapers, and logging them buries everything else.
QUIET_PATHS = frozenset({"/health/live", "/health/ready", "/metrics"})

RequestHandler = Callable[[Request], Awaitable[Response]]


class CorrelationMiddleware(BaseHTTPMiddleware):
    """Binds a correlation id to the request context.

    An inbound ``X-Correlation-ID`` is honoured so a trace started by an upstream
    caller stays intact; otherwise one is generated. A separate request id is
    always generated, because the correlation id may span many HTTP calls while
    the request id identifies exactly one.
    """

    async def dispatch(self, request: Request, call_next: RequestHandler) -> Response:
        correlation_id = request.headers.get(CORRELATION_HEADER) or str(uuid.uuid4())
        request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid.uuid4())
        # Truncated because it is echoed into headers and logs, and an
        # unbounded caller-supplied value is a log-flooding vector.
        correlation_id = correlation_id.strip()[:128]

        request.state.correlation_id = correlation_id
        request.state.request_id = request_id

        with correlation_scope(correlation_id=correlation_id, request_id=request_id):
            response = await call_next(request)

        response.headers[CORRELATION_HEADER] = correlation_id
        response.headers[REQUEST_ID_HEADER] = request_id
        return response


class AccessLogMiddleware(BaseHTTPMiddleware):
    """Logs one structured line per request and records API metrics."""

    def __init__(self, app: ASGIApp, *, metrics: MetricsSink) -> None:
        super().__init__(app)
        self._metrics = metrics

    async def dispatch(self, request: Request, call_next: RequestHandler) -> Response:
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            # The exception handler will turn this into a 500; the access log
            # still needs to record that the request happened and how long it
            # took before failing.
            self._record(request, status=500, elapsed=time.perf_counter() - started)
            raise

        elapsed = time.perf_counter() - started
        self._record(request, status=response.status_code, elapsed=elapsed)
        response.headers["X-Response-Time-Ms"] = f"{elapsed * 1000:.1f}"
        return response

    def _record(self, request: Request, *, status: int, elapsed: float) -> None:
        route = _route_template(request)
        labels = {"method": request.method, "route": route}
        self._metrics.increment("api_requests_total", labels={**labels, "status": str(status)})
        self._metrics.observe("api_request_duration_seconds", elapsed, labels=labels)

        if request.url.path in QUIET_PATHS:
            return
        logger.info(
            "handled an api request",
            extra={
                "method": request.method,
                "path": request.url.path,
                "route": route,
                "http_status": status,
                "duration_ms": round(elapsed * 1000, 2),
                "client": request.client.host if request.client else None,
                "principal": getattr(request.state, "principal_subject", None),
            },
        )


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    """Rejects oversized bodies before they are buffered.

    Enforced from ``Content-Length`` rather than by reading and measuring: the
    point of the limit is to avoid holding a large body in memory at all.
    """

    def __init__(self, app: ASGIApp, *, max_bytes: int) -> None:
        super().__init__(app)
        self._max_bytes = max_bytes

    async def dispatch(self, request: Request, call_next: RequestHandler) -> Response:
        declared = request.headers.get("content-length")
        if declared is not None and declared.isdigit() and int(declared) > self._max_bytes:
            return JSONResponse(
                status_code=HTTP_413_CONTENT_TOO_LARGE,
                content={
                    "error": {
                        "code": "payload_too_large",
                        "message": (f"the request body exceeds the {self._max_bytes} byte limit"),
                        "category": ErrorCategory.VALIDATION.value,
                        "retryable": False,
                    }
                },
            )
        return await call_next(request)


def _route_template(request: Request) -> str:
    """Return the matched route pattern rather than the concrete path.

    Using the raw path as a metric label would create one time series per request
    id, which is the classic way to take down a Prometheus server.
    """
    route = request.scope.get("route")
    path_format = getattr(route, "path_format", None) or getattr(route, "path", None)
    if isinstance(path_format, str):
        return path_format
    return "unmatched"
