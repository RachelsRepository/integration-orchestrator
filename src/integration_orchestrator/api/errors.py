"""HTTP translation of the normalized error model.

Every failure leaves the API through this module, which guarantees three things.
Responses share one envelope, so clients write one error handler. Status codes are
derived from the error *category* rather than chosen at each call site, so the same
condition cannot be a 409 in one endpoint and a 400 in another. And anything that
is not a :class:`DomainError` becomes an opaque 500: an unexpected exception has an
unknown message, and unknown messages are exactly how connection strings and
provider secrets end up in a client's logs.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
    HTTP_422_UNPROCESSABLE_CONTENT,
    HTTP_429_TOO_MANY_REQUESTS,
    HTTP_500_INTERNAL_SERVER_ERROR,
    HTTP_502_BAD_GATEWAY,
    HTTP_503_SERVICE_UNAVAILABLE,
    HTTP_504_GATEWAY_TIMEOUT,
)

from integration_orchestrator.domain.enums import ErrorCategory
from integration_orchestrator.domain.errors import (
    BulkheadRejectedError,
    CircuitOpenError,
    DomainError,
    IdempotencyConflictError,
    ProviderRateLimitError,
)
from integration_orchestrator.observability.correlation import current_correlation_id

logger = logging.getLogger(__name__)

CORRELATION_HEADER = "X-Correlation-ID"

STATUS_BY_CATEGORY: dict[ErrorCategory, int] = {
    ErrorCategory.VALIDATION: HTTP_400_BAD_REQUEST,
    ErrorCategory.AUTHENTICATION: HTTP_401_UNAUTHORIZED,
    ErrorCategory.AUTHORIZATION: HTTP_403_FORBIDDEN,
    ErrorCategory.NOT_FOUND: HTTP_404_NOT_FOUND,
    ErrorCategory.CONFLICT: HTTP_409_CONFLICT,
    ErrorCategory.UNSUPPORTED_OPERATION: HTTP_422_UNPROCESSABLE_CONTENT,
    # A provider rejecting our request body is our problem, not the caller's:
    # the mapping is wrong somewhere, so it is reported as a bad gateway rather
    # than blamed on the client.
    ErrorCategory.PROVIDER_VALIDATION: HTTP_502_BAD_GATEWAY,
    ErrorCategory.PROVIDER_AUTHENTICATION: HTTP_502_BAD_GATEWAY,
    ErrorCategory.PROVIDER_RATE_LIMIT: HTTP_429_TOO_MANY_REQUESTS,
    ErrorCategory.PROVIDER_TIMEOUT: HTTP_504_GATEWAY_TIMEOUT,
    ErrorCategory.PROVIDER_UNAVAILABLE: HTTP_503_SERVICE_UNAVAILABLE,
    ErrorCategory.INTERNAL: HTTP_500_INTERNAL_SERVER_ERROR,
}


def status_for(error: DomainError) -> int:
    """Map a domain error onto an HTTP status code."""
    return STATUS_BY_CATEGORY.get(error.category, HTTP_500_INTERNAL_SERVER_ERROR)


def error_payload(error: DomainError) -> dict[str, Any]:
    detail = error.detail()
    payload = detail.to_dict()
    payload.setdefault("correlation_id", current_correlation_id())
    return {"error": payload}


def _headers(error: DomainError) -> dict[str, str]:
    """Attach the headers a client needs in order to react correctly."""
    headers: dict[str, str] = {}
    correlation_id = error.correlation_id or current_correlation_id()
    if correlation_id:
        headers[CORRELATION_HEADER] = correlation_id
    if isinstance(error, (ProviderRateLimitError, CircuitOpenError)) and error.retry_after_seconds:
        headers["Retry-After"] = str(int(error.retry_after_seconds))
    if error.category is ErrorCategory.AUTHENTICATION:
        headers["WWW-Authenticate"] = 'Bearer realm="integration-orchestrator"'
    return headers


def register_exception_handlers(app: FastAPI) -> None:
    """Install every exception handler on the application."""

    @app.exception_handler(DomainError)
    async def _domain_error(request: Request, exc: DomainError) -> JSONResponse:
        status = status_for(exc)
        log = logger.warning if status < HTTP_500_INTERNAL_SERVER_ERROR else logger.error
        log(
            "request failed with a domain error",
            extra={
                "path": request.url.path,
                "method": request.method,
                "error_code": exc.code,
                "error_category": exc.category.value,
                "http_status": status,
                "retryable": exc.retryable,
            },
        )
        return JSONResponse(status_code=status, content=error_payload(exc), headers=_headers(exc))

    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=HTTP_400_BAD_REQUEST,
            content={
                "error": {
                    "code": "request_validation_failed",
                    "message": "the request body or parameters are not valid",
                    "category": ErrorCategory.VALIDATION.value,
                    "retryable": False,
                    "correlation_id": current_correlation_id(),
                    "metadata": {"violations": _violations(exc)},
                }
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        # Covers framework-raised failures such as 404 for an unmatched route and
        # 405 for a wrong method, so those share the same envelope as everything
        # else instead of Starlette's default ``{"detail": ...}``.
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": _code_for_status(exc.status_code),
                    "message": str(exc.detail),
                    "category": _category_for_status(exc.status_code).value,
                    "retryable": exc.status_code >= HTTP_500_INTERNAL_SERVER_ERROR,
                    "correlation_id": current_correlation_id(),
                }
            },
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        logger.exception(
            "unhandled exception while serving a request",
            extra={"path": request.url.path, "method": request.method},
        )
        return JSONResponse(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": {
                    "code": "internal_error",
                    "message": "an unexpected error occurred",
                    "category": ErrorCategory.INTERNAL.value,
                    "retryable": True,
                    "correlation_id": current_correlation_id(),
                }
            },
        )


def _violations(exc: RequestValidationError) -> list[dict[str, Any]]:
    """Summarise validation errors without echoing the submitted values.

    Pydantic includes the offending input in its error objects. Reflecting that
    back would put whatever the client sent — potentially a credential in the
    wrong field — into logs and error responses.
    """
    violations: list[dict[str, Any]] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error.get("loc", ()))
        violations.append(
            {
                "field": location,
                "message": error.get("msg", "invalid value"),
                "type": error.get("type", "value_error"),
            }
        )
    return violations[:20]


def _code_for_status(status: int) -> str:
    return {
        HTTP_400_BAD_REQUEST: "bad_request",
        HTTP_401_UNAUTHORIZED: "authentication_failed",
        HTTP_403_FORBIDDEN: "insufficient_scope",
        HTTP_404_NOT_FOUND: "not_found",
        HTTP_409_CONFLICT: "conflict",
        HTTP_429_TOO_MANY_REQUESTS: "rate_limited",
    }.get(status, "http_error")


def _category_for_status(status: int) -> ErrorCategory:
    if status == HTTP_401_UNAUTHORIZED:
        return ErrorCategory.AUTHENTICATION
    if status == HTTP_403_FORBIDDEN:
        return ErrorCategory.AUTHORIZATION
    if status == HTTP_404_NOT_FOUND:
        return ErrorCategory.NOT_FOUND
    if status == HTTP_409_CONFLICT:
        return ErrorCategory.CONFLICT
    if status >= HTTP_500_INTERNAL_SERVER_ERROR:
        return ErrorCategory.INTERNAL
    return ErrorCategory.VALIDATION


#: Errors that carry a ``Retry-After`` and are safe for a client to repeat.
RETRYABLE_ERRORS = (ProviderRateLimitError, CircuitOpenError, BulkheadRejectedError)

#: Conflicts that indicate a client bug rather than a transient race.
CLIENT_CONFLICTS = (IdempotencyConflictError,)
