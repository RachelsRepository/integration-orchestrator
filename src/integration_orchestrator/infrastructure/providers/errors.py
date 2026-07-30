"""Classification of provider transport failures.

Deciding what to retry is the single most consequential judgement in an
integration platform. Retrying too little turns a transient blip into a customer
visible failure; retrying too much amplifies an outage and, for non-idempotent
operations, creates duplicates.

The rules:

* **Retry** connection errors, timeouts, 429, 502, 503 and 504. All of these mean
  "the request did not get a considered answer", so asking again is reasonable.
* **Do not retry** 400, 403, 404, 409 and 422. The provider considered the
  request and rejected it. The same request will be rejected again.
* **401 is special.** It is not retried on the same credentials, but the caller
  performs exactly one credential-refresh retry, because the usual cause is a
  token that was rotated or revoked rather than a genuine authorization failure.
* **500 is not retried automatically.** Unlike 502/503/504, which come from
  infrastructure in front of the application, a 500 normally means the provider's
  own code failed on this specific payload. Replaying it tends to reproduce it,
  and the durable retry path will pick it up later if it really was transient.
"""

from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from integration_orchestrator.domain.errors import (
    ProviderAuthenticationError,
    ProviderError,
    ProviderNotFoundError,
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderUnavailableError,
    ProviderValidationError,
)

RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 502, 503, 504})
NON_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({400, 401, 403, 404, 409, 422})

_MAX_ERROR_MESSAGE_LENGTH = 500


def classify_response(
    response: httpx.Response,
    *,
    provider: str,
    correlation_id: str | None = None,
) -> ProviderError:
    """Turn a non-success HTTP response into a normalized provider error."""
    status = response.status_code
    provider_code, message = _extract_provider_error(response)
    retry_after = parse_retry_after(response.headers.get("retry-after"))
    metadata: dict[str, Any] = {"http_status": status}

    if status == 429:
        return ProviderRateLimitError(
            message or "the provider applied rate limiting",
            provider=provider,
            provider_code=provider_code,
            correlation_id=correlation_id,
            retry_after_seconds=retry_after,
            metadata=metadata,
        )
    if status == 401:
        return ProviderAuthenticationError(
            message or "the provider rejected our credentials",
            provider=provider,
            provider_code=provider_code,
            correlation_id=correlation_id,
            metadata=metadata,
        )
    if status == 403:
        return ProviderAuthenticationError(
            message or "the provider denied access to this operation",
            provider=provider,
            provider_code=provider_code,
            correlation_id=correlation_id,
            metadata=metadata,
        )
    if status == 404:
        return ProviderNotFoundError(
            message or "the provider does not recognise this operation",
            provider=provider,
            provider_code=provider_code,
            correlation_id=correlation_id,
            metadata=metadata,
        )
    if status in (400, 409, 422):
        return ProviderValidationError(
            message or "the provider rejected the request",
            provider=provider,
            provider_code=provider_code,
            correlation_id=correlation_id,
            metadata=metadata,
        )
    if status in RETRYABLE_STATUS_CODES:
        return ProviderUnavailableError(
            message or f"the provider returned a transient error ({status})",
            provider=provider,
            provider_code=provider_code,
            correlation_id=correlation_id,
            retry_after_seconds=retry_after,
            metadata=metadata,
        )

    return ProviderUnavailableError(
        message or f"the provider returned an unexpected status ({status})",
        provider=provider,
        provider_code=provider_code,
        correlation_id=correlation_id,
        # See the module docstring: a 5xx that is not 502/503/504 is treated as
        # deterministic until the durable retry path proves otherwise.
        retryable=status not in (500, 501, 505),
        metadata=metadata,
    )


def classify_transport_error(
    error: Exception,
    *,
    provider: str,
    correlation_id: str | None = None,
) -> ProviderError:
    """Turn an httpx transport exception into a normalized provider error."""
    if isinstance(error, httpx.TimeoutException):
        return ProviderTimeoutError(
            "the provider did not respond within its timeout budget",
            provider=provider,
            correlation_id=correlation_id,
            metadata={"transport_error": type(error).__name__},
        )
    if isinstance(error, httpx.TransportError):
        return ProviderUnavailableError(
            "the provider could not be reached",
            provider=provider,
            correlation_id=correlation_id,
            metadata={"transport_error": type(error).__name__},
        )
    return ProviderUnavailableError(
        "the provider call failed unexpectedly",
        provider=provider,
        correlation_id=correlation_id,
        retryable=False,
        metadata={"transport_error": type(error).__name__},
    )


def parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header.

    Supports both forms in the specification: delay in seconds and an HTTP date.
    Providers use both, and ignoring the header means backing off for the wrong
    amount of time exactly when the provider has told us the right answer.
    """
    if not value:
        return None
    stripped = value.strip()
    try:
        seconds = float(stripped)
    except ValueError:
        pass
    else:
        return max(0.0, seconds)

    try:
        when = parsedate_to_datetime(stripped)
    except (TypeError, ValueError, IndexError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - datetime.now(tz=UTC)).total_seconds())


def _extract_provider_error(response: httpx.Response) -> tuple[str | None, str | None]:
    """Pull a provider error code and message out of a response body.

    Providers disagree about where these live, so several common shapes are
    checked. The extracted message is truncated and only ever used in audit
    metadata and normalized errors, never echoed verbatim to API callers.
    """
    try:
        body = response.json()
    except ValueError:
        return None, None
    if not isinstance(body, dict):
        return None, None

    error = body.get("error")
    if isinstance(error, dict):
        code = error.get("code") or error.get("type")
        message = error.get("message") or error.get("detail")
        return _stringify(code), _truncate(_stringify(message))

    code = body.get("code") or body.get("error_code") or body.get("errorCode")
    message = (
        body.get("message")
        or body.get("detail")
        or body.get("error_description")
        or (error if isinstance(error, str) else None)
    )
    return _stringify(code), _truncate(_stringify(message))


def _stringify(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _truncate(value: str | None) -> str | None:
    if value is None:
        return None
    return value[:_MAX_ERROR_MESSAGE_LENGTH]
