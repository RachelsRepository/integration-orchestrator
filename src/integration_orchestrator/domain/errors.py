"""Normalized domain error model.

All failures crossing a layer boundary are expressed as a :class:`DomainError`
carrying a stable machine-readable code, a category, and an explicit
retryability decision. Nothing else is allowed to reach the API surface: the
error handler translates these into RFC-style problem responses and treats any
other exception as an opaque internal error so that stack traces and provider
secrets can never leak.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from integration_orchestrator.domain.enums import ErrorCategory, RequestStatus

# Categories whose failures are, by default, worth retrying. Individual errors
# can override this, because a 429 with a permanent quota exhaustion message is
# not the same as a 429 from a burst.
_RETRYABLE_BY_DEFAULT: frozenset[ErrorCategory] = frozenset(
    {
        ErrorCategory.PROVIDER_RATE_LIMIT,
        ErrorCategory.PROVIDER_TIMEOUT,
        ErrorCategory.PROVIDER_UNAVAILABLE,
    }
)


@dataclass(frozen=True, slots=True)
class ErrorDetail:
    """Serialisable, redaction-safe description of a failure."""

    code: str
    message: str
    category: ErrorCategory
    retryable: bool
    provider: str | None = None
    provider_code: str | None = None
    correlation_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "category": self.category.value,
            "retryable": self.retryable,
        }
        if self.provider:
            payload["provider"] = self.provider
        if self.provider_code:
            payload["provider_code"] = self.provider_code
        if self.correlation_id:
            payload["correlation_id"] = self.correlation_id
        if self.metadata:
            payload["metadata"] = self.metadata
        return payload


class DomainError(Exception):
    """Base class for every error the platform raises deliberately."""

    code: str = "internal_error"
    category: ErrorCategory = ErrorCategory.INTERNAL

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        category: ErrorCategory | None = None,
        retryable: bool | None = None,
        provider: str | None = None,
        provider_code: str | None = None,
        correlation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code or type(self).code
        self.category = category or type(self).category
        self.retryable = (
            retryable if retryable is not None else self.category in _RETRYABLE_BY_DEFAULT
        )
        self.provider = provider
        self.provider_code = provider_code
        self.correlation_id = correlation_id
        self.metadata: dict[str, Any] = dict(metadata or {})

    def detail(self) -> ErrorDetail:
        return ErrorDetail(
            code=self.code,
            message=self.message,
            category=self.category,
            retryable=self.retryable,
            provider=self.provider,
            provider_code=self.provider_code,
            correlation_id=self.correlation_id,
            metadata=dict(self.metadata),
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(code={self.code!r}, category={self.category.value!r})"


# ---------------------------------------------------------------------------
# Request-level errors
# ---------------------------------------------------------------------------


class ValidationError(DomainError):
    """The caller supplied something the platform cannot accept."""

    code = "validation_failed"
    category = ErrorCategory.VALIDATION


class NotFoundError(DomainError):
    """A referenced aggregate does not exist."""

    code = "not_found"
    category = ErrorCategory.NOT_FOUND


class ConflictError(DomainError):
    """The request conflicts with existing state."""

    code = "conflict"
    category = ErrorCategory.CONFLICT


class IdempotencyConflictError(ConflictError):
    """An idempotency key was reused with a different request body.

    Replaying a key with an identical body is a legitimate retry and returns the
    original result. Replaying it with a different body is a client bug that
    would otherwise silently create divergent state, so it is rejected.
    """

    code = "idempotency_key_reused"


class InvalidStateTransitionError(DomainError):
    """An operation attempted a transition the state machine forbids."""

    code = "invalid_state_transition"
    category = ErrorCategory.CONFLICT

    def __init__(
        self,
        current: RequestStatus,
        requested: RequestStatus,
        *,
        aggregate_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            f"cannot transition from '{current.value}' to '{requested.value}'",
            correlation_id=correlation_id,
            retryable=False,
            metadata={
                "current_status": current.value,
                "requested_status": requested.value,
                **({"aggregate_id": aggregate_id} if aggregate_id else {}),
            },
        )
        self.current = current
        self.requested = requested


class ConcurrencyConflictError(ConflictError):
    """An optimistic concurrency check failed; the aggregate changed underneath."""

    code = "concurrent_modification"

    def __init__(self, aggregate_id: str, *, correlation_id: str | None = None) -> None:
        super().__init__(
            "the request was modified concurrently; retry the operation",
            correlation_id=correlation_id,
            retryable=True,
            metadata={"aggregate_id": aggregate_id},
        )


class UnsupportedOperationError(DomainError):
    """The selected provider cannot perform the requested operation."""

    code = "unsupported_operation"
    category = ErrorCategory.UNSUPPORTED_OPERATION

    def __init__(
        self,
        message: str,
        *,
        provider: str | None = None,
        correlation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message,
            provider=provider,
            correlation_id=correlation_id,
            retryable=False,
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Authentication and authorization
# ---------------------------------------------------------------------------


class AuthenticationError(DomainError):
    """The caller's credentials are missing or invalid."""

    code = "authentication_failed"
    category = ErrorCategory.AUTHENTICATION


class AuthorizationError(DomainError):
    """The caller is authenticated but lacks the required scope."""

    code = "insufficient_scope"
    category = ErrorCategory.AUTHORIZATION

    def __init__(
        self,
        required_scope: str,
        *,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            f"the required scope '{required_scope}' is not granted to this token",
            correlation_id=correlation_id,
            retryable=False,
            metadata={"required_scope": required_scope},
        )
        self.required_scope = required_scope


# ---------------------------------------------------------------------------
# Provider errors
# ---------------------------------------------------------------------------


class ProviderError(DomainError):
    """Base class for failures originating at or below a provider boundary."""

    code = "provider_error"
    category = ErrorCategory.PROVIDER_UNAVAILABLE

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        code: str | None = None,
        category: ErrorCategory | None = None,
        retryable: bool | None = None,
        provider_code: str | None = None,
        correlation_id: str | None = None,
        retry_after_seconds: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            message,
            code=code,
            category=category,
            retryable=retryable,
            provider=provider,
            provider_code=provider_code,
            correlation_id=correlation_id,
            metadata=metadata,
        )
        self.retry_after_seconds = retry_after_seconds


class ProviderValidationError(ProviderError):
    """The provider rejected the request payload. Never retried automatically."""

    code = "provider_rejected_request"
    category = ErrorCategory.PROVIDER_VALIDATION

    def __init__(self, message: str, *, provider: str, **kwargs: Any) -> None:
        kwargs.setdefault("retryable", False)
        super().__init__(message, provider=provider, **kwargs)


class ProviderAuthenticationError(ProviderError):
    """The provider rejected our credentials.

    Marked non-retryable at the resilience layer: the caller invalidates the
    cached token and performs exactly one credential-refresh retry rather than
    hammering the provider with the same rejected token.
    """

    code = "provider_authentication_failed"
    category = ErrorCategory.PROVIDER_AUTHENTICATION

    def __init__(self, message: str, *, provider: str, **kwargs: Any) -> None:
        kwargs.setdefault("retryable", False)
        super().__init__(message, provider=provider, **kwargs)


class ProviderRateLimitError(ProviderError):
    """The provider applied rate limiting. Honour ``Retry-After`` when present."""

    code = "provider_rate_limited"
    category = ErrorCategory.PROVIDER_RATE_LIMIT

    def __init__(self, message: str, *, provider: str, **kwargs: Any) -> None:
        kwargs.setdefault("retryable", True)
        super().__init__(message, provider=provider, **kwargs)


class ProviderTimeoutError(ProviderError):
    """A provider call exceeded its timeout budget.

    A timeout is genuinely ambiguous: the provider may or may not have created
    the operation. This is precisely the case reconciliation exists to resolve.
    """

    code = "provider_timeout"
    category = ErrorCategory.PROVIDER_TIMEOUT

    def __init__(self, message: str, *, provider: str, **kwargs: Any) -> None:
        kwargs.setdefault("retryable", True)
        super().__init__(message, provider=provider, **kwargs)


class ProviderUnavailableError(ProviderError):
    """The provider returned a transient server error or could not be reached."""

    code = "provider_unavailable"
    category = ErrorCategory.PROVIDER_UNAVAILABLE

    def __init__(self, message: str, *, provider: str, **kwargs: Any) -> None:
        kwargs.setdefault("retryable", True)
        super().__init__(message, provider=provider, **kwargs)


class ProviderNotFoundError(ProviderError):
    """The provider does not recognise the referenced operation."""

    code = "provider_operation_not_found"
    category = ErrorCategory.PROVIDER_VALIDATION

    def __init__(self, message: str, *, provider: str, **kwargs: Any) -> None:
        kwargs.setdefault("retryable", False)
        super().__init__(message, provider=provider, **kwargs)


class ProviderResponseError(ProviderError):
    """The provider replied with something the adapter cannot interpret.

    Deliberately non-retryable: repeating a call that produced an unparseable
    response is unlikely to help and risks duplicate side effects.
    """

    code = "provider_response_malformed"
    category = ErrorCategory.PROVIDER_UNAVAILABLE

    def __init__(self, message: str, *, provider: str, **kwargs: Any) -> None:
        kwargs.setdefault("retryable", False)
        super().__init__(message, provider=provider, **kwargs)


class ProviderNotConfiguredError(ValidationError):
    """The requested provider is unknown or disabled."""

    code = "provider_not_configured"

    def __init__(self, provider: str, *, correlation_id: str | None = None) -> None:
        super().__init__(
            f"provider '{provider}' is not configured or is disabled",
            correlation_id=correlation_id,
            retryable=False,
            metadata={"provider": provider},
        )


class CircuitOpenError(ProviderError):
    """The provider-scoped circuit breaker is open and rejected the call.

    Retryable, because the condition is expected to clear when the open window
    elapses. Rejecting fast here is what stops a failing provider consuming
    capacity that healthy providers need.
    """

    code = "provider_circuit_open"
    category = ErrorCategory.PROVIDER_UNAVAILABLE

    def __init__(
        self,
        provider: str,
        *,
        retry_after_seconds: float | None = None,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            f"the circuit breaker for provider '{provider}' is open",
            provider=provider,
            retryable=True,
            retry_after_seconds=retry_after_seconds,
            correlation_id=correlation_id,
        )


class BulkheadRejectedError(ProviderError):
    """The provider's concurrency limit is saturated.

    Backpressure, not failure: the work is shed quickly so the caller can retry
    later rather than queueing without bound.
    """

    code = "provider_concurrency_exhausted"
    category = ErrorCategory.PROVIDER_UNAVAILABLE

    def __init__(self, provider: str, *, limit: int, correlation_id: str | None = None) -> None:
        super().__init__(
            f"provider '{provider}' has no free concurrency slots",
            provider=provider,
            retryable=True,
            correlation_id=correlation_id,
            metadata={"concurrency_limit": limit},
        )


# ---------------------------------------------------------------------------
# Webhook errors
# ---------------------------------------------------------------------------


class WebhookVerificationError(DomainError):
    """A webhook failed identity, signature, or freshness verification.

    The message is deliberately coarse. Telling an attacker exactly which check
    failed is a signature-forgery oracle, so the detailed reason is recorded on
    the persisted receipt and in audit, never returned to the caller.
    """

    code = "webhook_verification_failed"
    category = ErrorCategory.AUTHENTICATION

    def __init__(
        self,
        reason: str,
        *,
        provider: str,
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(
            "webhook verification failed",
            provider=provider,
            correlation_id=correlation_id,
            retryable=False,
            metadata={"verification_failure": reason},
        )
        self.reason = reason


class WebhookPayloadError(ValidationError):
    """A verified webhook body could not be normalized."""

    code = "webhook_payload_invalid"


# ---------------------------------------------------------------------------
# Infrastructure-adjacent errors that still need a normalized shape
# ---------------------------------------------------------------------------


class LockAcquisitionError(DomainError):
    """A distributed lock could not be acquired within its timeout."""

    code = "lock_unavailable"
    category = ErrorCategory.INTERNAL

    def __init__(self, resource: str, *, correlation_id: str | None = None) -> None:
        super().__init__(
            f"could not acquire the lock for '{resource}'",
            correlation_id=correlation_id,
            retryable=True,
            metadata={"resource": resource},
        )


class EventPublicationError(DomainError):
    """An outbox event could not be published to the message broker."""

    code = "event_publication_failed"
    category = ErrorCategory.INTERNAL

    def __init__(self, message: str, *, event_id: str | None = None) -> None:
        super().__init__(
            message,
            retryable=True,
            metadata={"event_id": event_id} if event_id else {},
        )
