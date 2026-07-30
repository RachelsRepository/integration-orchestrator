"""Domain enumerations.

Note what is deliberately *not* an enum: the provider identity. Providers are
represented by :class:`~integration_orchestrator.domain.value_objects.ProviderSlug`
so that onboarding a fourth provider never requires editing the domain layer.
"""

from __future__ import annotations

from enum import StrEnum


class RequestStatus(StrEnum):
    """Lifecycle state of an integration request."""

    RECEIVED = "received"
    VALIDATING = "validating"
    DISPATCHING = "dispatching"
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRY_SCHEDULED = "retry_scheduled"
    CANCELLED = "cancelled"
    MANUAL_REVIEW = "manual_review"

    @property
    def is_terminal(self) -> bool:
        """Terminal states never transition again without operator action."""
        return self in _TERMINAL_STATUSES

    @property
    def is_in_flight(self) -> bool:
        """States where the provider may still be working on the operation."""
        return self in (RequestStatus.DISPATCHING, RequestStatus.PENDING)


_TERMINAL_STATUSES: frozenset[RequestStatus] = frozenset(
    {RequestStatus.SUCCEEDED, RequestStatus.CANCELLED}
)


class OperationType(StrEnum):
    """The kind of work being requested from a provider.

    Operation types are normalized: every provider adapter is responsible for
    translating these into whatever vocabulary its provider uses, or rejecting
    the operation as unsupported.
    """

    RESOURCE_PROVISION = "resource_provision"
    RESOURCE_DEPROVISION = "resource_deprovision"
    RESOURCE_UPDATE = "resource_update"
    ACCESS_GRANT = "access_grant"
    ACCESS_REVOKE = "access_revoke"


class NormalizedStatus(StrEnum):
    """Provider-neutral outcome of a provider operation.

    Adapters map provider vocabularies onto this small set. ``UNKNOWN`` is a
    first-class value: it means the provider reported something the adapter does
    not recognise, which must never be silently coerced into success or failure.
    """

    ACCEPTED = "accepted"
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class WebhookProcessingStatus(StrEnum):
    """Processing state of a persisted webhook receipt.

    ``DEFERRED`` exists because a provider can deliver a completion webhook
    before our own dispatch call has committed the provider reference. Such a
    receipt is kept, not discarded, and resolved once the reference appears.
    """

    RECEIVED = "received"
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    DEFERRED = "deferred"
    REJECTED = "rejected"
    FAILED = "failed"
    ABANDONED = "abandoned"


class CircuitState(StrEnum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    @property
    def numeric(self) -> int:
        """Numeric encoding for the ``provider_circuit_state`` gauge."""
        return {
            CircuitState.CLOSED: 0,
            CircuitState.HALF_OPEN: 1,
            CircuitState.OPEN: 2,
        }[self]


class ErrorCategory(StrEnum):
    """Normalized error taxonomy.

    The category determines the HTTP status returned to internal callers, the
    default retryability, and which metric counter is incremented.
    """

    VALIDATION = "validation"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    PROVIDER_VALIDATION = "provider_validation"
    PROVIDER_AUTHENTICATION = "provider_authentication"
    PROVIDER_RATE_LIMIT = "provider_rate_limit"
    PROVIDER_TIMEOUT = "provider_timeout"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    CONFLICT = "conflict"
    NOT_FOUND = "not_found"
    UNSUPPORTED_OPERATION = "unsupported_operation"
    INTERNAL = "internal"


class AuditAction(StrEnum):
    """Auditable actions.

    Every meaningful decision the platform makes produces one of these, giving a
    complete reconstruction of why a request reached its current state.
    """

    REQUEST_RECEIVED = "request.received"
    REQUEST_VALIDATED = "request.validated"
    PROVIDER_SELECTED = "provider.selected"
    DISPATCH_ATTEMPTED = "dispatch.attempted"
    PROVIDER_ACCEPTED = "provider.accepted"
    PROVIDER_REJECTED = "provider.rejected"
    PROVIDER_FAILED = "provider.failed"
    RETRY_SCHEDULED = "retry.scheduled"
    RETRY_REQUESTED = "retry.requested"
    RETRIES_EXHAUSTED = "retry.exhausted"
    WEBHOOK_RECEIVED = "webhook.received"
    WEBHOOK_REJECTED = "webhook.rejected"
    WEBHOOK_DUPLICATE_IGNORED = "webhook.duplicate_ignored"
    WEBHOOK_DEFERRED = "webhook.deferred"
    WEBHOOK_APPLIED = "webhook.applied"
    WEBHOOK_ABANDONED = "webhook.abandoned"
    STATE_RECONCILED = "state.reconciled"
    RECONCILIATION_MISMATCH = "state.reconciliation_mismatch"
    MOVED_TO_MANUAL_REVIEW = "state.manual_review"
    REQUEST_CANCELLED = "request.cancelled"
    CANCELLATION_REJECTED = "request.cancellation_rejected"
    CIRCUIT_OPENED = "provider.circuit_opened"
    CIRCUIT_CLOSED = "provider.circuit_closed"
    INVALID_TRANSITION_ATTEMPTED = "state.invalid_transition"


class ActorType(StrEnum):
    """Who or what caused an audited action."""

    API_CLIENT = "api_client"
    WEBHOOK = "webhook"
    RETRY_WORKER = "retry_worker"
    RECONCILIATION_WORKER = "reconciliation_worker"
    OUTBOX_PUBLISHER = "outbox_publisher"
    SYSTEM = "system"
