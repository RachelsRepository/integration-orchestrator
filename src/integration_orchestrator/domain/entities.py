"""Domain entities.

:class:`IntegrationRequest` is the aggregate root. It has no public status
setter: every state change goes through a named method that consults the state
machine first, so an illegal transition is impossible to express. Mutators take
``now`` as an argument rather than reading a clock, keeping the entity pure and
its tests deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Self
from uuid import UUID

from integration_orchestrator.domain.enums import (
    NormalizedStatus,
    OperationType,
    RequestStatus,
    WebhookProcessingStatus,
)
from integration_orchestrator.domain.errors import (
    ConflictError,
    ValidationError,
)
from integration_orchestrator.domain.state_machine import (
    assert_transition_allowed,
    is_forward_progress,
    project_normalized_status,
)
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ExternalReference,
    IdempotencyKey,
    ProviderSlug,
    SignatureMetadata,
)


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    """Reject naive timestamps.

    Every timestamp in this system is timezone-aware. A naive datetime is
    ambiguous the moment it crosses a process or region boundary, and comparing
    one against an aware value raises at runtime in the worst possible place.
    """
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValidationError(f"{field_name} must be timezone-aware")
    return value


@dataclass(frozen=True, slots=True)
class StatusTransition:
    """A record of one applied status change.

    Returned by every mutator so the caller can write the matching audit and
    outbox rows without re-deriving what happened.
    """

    previous_status: RequestStatus
    new_status: RequestStatus
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class FailureDetail:
    """The normalized reason a request failed, stored for operators."""

    code: str
    message: str
    category: str
    retryable: bool
    provider_code: str | None = None


@dataclass(slots=True)
class IntegrationRequest:
    """The aggregate root tracking one externally-fulfilled operation."""

    id: UUID
    provider: ProviderSlug
    operation_type: OperationType
    external_reference: ExternalReference
    normalized_payload: dict[str, Any]
    correlation_id: CorrelationId
    created_at: datetime
    updated_at: datetime
    status: RequestStatus = RequestStatus.RECEIVED
    provider_payload: dict[str, Any] | None = None
    provider_reference: str | None = None
    idempotency_key: IdempotencyKey | None = None
    attempt_count: int = 0
    last_error_code: str | None = None
    last_error_message: str | None = None
    last_error_category: str | None = None
    next_retry_at: datetime | None = None
    completed_at: datetime | None = None
    manual_review_reason: str | None = None
    version: int = 0

    def __post_init__(self) -> None:
        _require_aware(self.created_at, field_name="created_at")
        _require_aware(self.updated_at, field_name="updated_at")
        if not isinstance(self.normalized_payload, dict):
            raise ValidationError("the normalized payload must be a JSON object")

    # -- construction -------------------------------------------------------

    @classmethod
    def create(
        cls,
        *,
        request_id: UUID,
        provider: ProviderSlug,
        operation_type: OperationType,
        external_reference: ExternalReference,
        normalized_payload: dict[str, Any],
        correlation_id: CorrelationId,
        now: datetime,
        idempotency_key: IdempotencyKey | None = None,
    ) -> Self:
        """Create a request in the ``received`` state."""
        _require_aware(now, field_name="now")
        return cls(
            id=request_id,
            provider=provider,
            operation_type=operation_type,
            external_reference=external_reference,
            normalized_payload=dict(normalized_payload),
            correlation_id=correlation_id,
            created_at=now,
            updated_at=now,
            status=RequestStatus.RECEIVED,
            idempotency_key=idempotency_key,
        )

    # -- transitions --------------------------------------------------------

    def begin_validation(self, *, now: datetime) -> StatusTransition:
        return self._transition(RequestStatus.VALIDATING, now=now)

    def begin_dispatch(self, *, now: datetime) -> StatusTransition:
        """Move into ``dispatching`` and count the attempt.

        The attempt is counted before the provider is called, not after. If the
        process dies mid-call the attempt must still be visible, otherwise a
        crash loop would retry without bound while ``attempt_count`` stayed at
        zero.
        """
        transition = self._transition(RequestStatus.DISPATCHING, now=now)
        self.attempt_count += 1
        self.next_retry_at = None
        return transition

    def record_provider_payload(self, payload: dict[str, Any]) -> None:
        """Store the redacted provider-shaped request the adapter produced.

        Kept for operator diagnosis of mapping problems. Adapters redact before
        handing it over; the entity has no way to know which provider fields are
        sensitive, so it does not attempt redaction itself.
        """
        self.provider_payload = dict(payload)

    def mark_accepted(
        self,
        *,
        provider_reference: str,
        now: datetime,
    ) -> StatusTransition:
        """The provider accepted the operation and will complete it later."""
        if not provider_reference:
            raise ValidationError("a provider reference is required to mark a request pending")
        transition = self._transition(RequestStatus.PENDING, now=now)
        self.provider_reference = provider_reference
        self._clear_error()
        return transition

    def mark_succeeded(
        self,
        *,
        now: datetime,
        provider_reference: str | None = None,
    ) -> StatusTransition:
        transition = self._transition(RequestStatus.SUCCEEDED, now=now)
        if provider_reference:
            self.provider_reference = provider_reference
        self.completed_at = now
        self.next_retry_at = None
        self._clear_error()
        return transition

    def mark_failed(self, *, failure: FailureDetail, now: datetime) -> StatusTransition:
        transition = self._transition(RequestStatus.FAILED, now=now)
        self._apply_failure(failure)
        self.completed_at = now
        self.next_retry_at = None
        return transition

    def schedule_retry(
        self,
        *,
        failure: FailureDetail,
        next_retry_at: datetime,
        now: datetime,
    ) -> StatusTransition:
        _require_aware(next_retry_at, field_name="next_retry_at")
        transition = self._transition(RequestStatus.RETRY_SCHEDULED, now=now)
        self._apply_failure(failure)
        self.next_retry_at = next_retry_at
        self.completed_at = None
        return transition

    def mark_cancelled(self, *, now: datetime, reason: str | None = None) -> StatusTransition:
        transition = self._transition(RequestStatus.CANCELLED, now=now)
        self.completed_at = now
        self.next_retry_at = None
        if reason:
            self.manual_review_reason = None
            self.last_error_message = reason
        return transition

    def mark_manual_review(self, *, reason: str, now: datetime) -> StatusTransition:
        """Escalate to a human.

        Reached when the platform can see that something is wrong but cannot
        safely decide what the correct state is. Guessing here is what produces
        silently wrong data, so the request stops moving instead.
        """
        transition = self._transition(RequestStatus.MANUAL_REVIEW, now=now)
        self.manual_review_reason = reason
        self.next_retry_at = None
        return transition

    def restore_for_retry(self, *, next_retry_at: datetime, now: datetime) -> StatusTransition:
        """Operator-requested retry from a failed or manually reviewed state."""
        _require_aware(next_retry_at, field_name="next_retry_at")
        transition = self._transition(RequestStatus.RETRY_SCHEDULED, now=now)
        self.next_retry_at = next_retry_at
        self.completed_at = None
        self.manual_review_reason = None
        return transition

    # -- externally driven updates -----------------------------------------

    def apply_normalized_status(
        self,
        status: NormalizedStatus,
        *,
        now: datetime,
        provider_reference: str | None = None,
        failure: FailureDetail | None = None,
    ) -> StatusTransition | None:
        """Apply a provider-reported outcome from a webhook or a status poll.

        Returns ``None`` when the update carries no forward progress, which is
        the normal outcome for a duplicate or out-of-order delivery. Providers do
        not guarantee webhook ordering, so a late ``pending`` notification must
        not drag a succeeded request backwards.
        """
        target = project_normalized_status(status)
        if target is None:
            return None
        if not is_forward_progress(self.status, target):
            return None

        if provider_reference and not self.provider_reference:
            self.provider_reference = provider_reference

        if target is RequestStatus.SUCCEEDED:
            return self.mark_succeeded(now=now, provider_reference=provider_reference)
        if target is RequestStatus.FAILED:
            detail = failure or FailureDetail(
                code="provider_reported_failure",
                message="the provider reported the operation as failed",
                category="provider_unavailable",
                retryable=False,
            )
            return self.mark_failed(failure=detail, now=now)
        if target is RequestStatus.CANCELLED:
            return self.mark_cancelled(now=now, reason="cancelled by the provider")
        if target is RequestStatus.PENDING:
            reference = provider_reference or self.provider_reference
            if not reference:
                return None
            return self.mark_accepted(provider_reference=reference, now=now)
        return None

    def attach_provider_reference(self, reference: str, *, now: datetime) -> None:
        """Record the provider's identifier without changing status.

        Used when a timeout hid the provider's response and reconciliation later
        discovered the operation really was created.
        """
        if not reference:
            raise ValidationError("a provider reference must not be empty")
        if self.provider_reference and self.provider_reference != reference:
            raise ConflictError(
                "the request is already linked to a different provider reference",
                metadata={
                    "existing_provider_reference": self.provider_reference,
                    "supplied_provider_reference": reference,
                },
            )
        self.provider_reference = reference
        self.touch(now=now)

    def touch(self, *, now: datetime) -> None:
        """Advance ``updated_at`` and the optimistic concurrency version."""
        self.updated_at = _require_aware(now, field_name="now")
        self.version += 1

    # -- queries ------------------------------------------------------------

    @property
    def is_terminal(self) -> bool:
        return self.status.is_terminal

    def is_due_for_retry(self, *, now: datetime) -> bool:
        if self.status is not RequestStatus.RETRY_SCHEDULED or self.next_retry_at is None:
            return False
        return self.next_retry_at <= now

    def age_seconds(self, *, now: datetime) -> float:
        return (now - self.created_at).total_seconds()

    def seconds_since_update(self, *, now: datetime) -> float:
        return (now - self.updated_at).total_seconds()

    # -- internals ----------------------------------------------------------

    def _transition(self, requested: RequestStatus, *, now: datetime) -> StatusTransition:
        _require_aware(now, field_name="now")
        assert_transition_allowed(
            self.status,
            requested,
            aggregate_id=str(self.id),
            correlation_id=self.correlation_id.value,
        )
        previous = self.status
        self.status = requested
        self.touch(now=now)
        return StatusTransition(previous_status=previous, new_status=requested, occurred_at=now)

    def _apply_failure(self, failure: FailureDetail) -> None:
        self.last_error_code = failure.code
        self.last_error_message = failure.message
        self.last_error_category = failure.category

    def _clear_error(self) -> None:
        self.last_error_code = None
        self.last_error_message = None
        self.last_error_category = None


@dataclass(slots=True)
class WebhookReceipt:
    """A persisted record of one inbound webhook delivery.

    Receipts are written before any correlation or state change is attempted.
    That ordering is what makes the pipeline debuggable: a webhook that fails
    verification, references an unknown operation, or crashes the processor still
    leaves evidence behind.
    """

    id: UUID
    provider: ProviderSlug
    event_id: str
    event_type: str
    payload: dict[str, Any]
    signature_metadata: SignatureMetadata
    received_at: datetime
    provider_reference: str | None = None
    correlation_id: CorrelationId | None = None
    processing_status: WebhookProcessingStatus = WebhookProcessingStatus.RECEIVED
    integration_request_id: UUID | None = None
    processed_at: datetime | None = None
    failure_reason: str | None = None
    attempt_count: int = 0
    next_attempt_at: datetime | None = None

    def __post_init__(self) -> None:
        _require_aware(self.received_at, field_name="received_at")
        if not self.event_id:
            raise ValidationError("a webhook receipt requires a provider event id")

    def mark_processed(
        self,
        *,
        now: datetime,
        integration_request_id: UUID | None = None,
    ) -> None:
        self.processing_status = WebhookProcessingStatus.PROCESSED
        self.processed_at = _require_aware(now, field_name="now")
        self.failure_reason = None
        self.next_attempt_at = None
        if integration_request_id is not None:
            self.integration_request_id = integration_request_id

    def mark_duplicate(self, *, now: datetime) -> None:
        """The provider redelivered an event we have already applied."""
        self.processing_status = WebhookProcessingStatus.DUPLICATE
        self.processed_at = _require_aware(now, field_name="now")
        self.next_attempt_at = None

    def mark_rejected(self, *, reason: str, now: datetime) -> None:
        """Verification failed. Retained as evidence, never processed."""
        self.processing_status = WebhookProcessingStatus.REJECTED
        self.processed_at = _require_aware(now, field_name="now")
        self.failure_reason = reason
        self.next_attempt_at = None

    def mark_deferred(self, *, reason: str, next_attempt_at: datetime, now: datetime) -> None:
        """Hold a verified webhook that cannot yet be correlated.

        This is the webhook-before-response race: the provider notified us of
        completion before our own dispatch call committed the provider
        reference. Discarding the webhook would strand the request until
        reconciliation noticed; holding it lets the resolver apply it as soon as
        the reference lands.
        """
        _require_aware(next_attempt_at, field_name="next_attempt_at")
        self.processing_status = WebhookProcessingStatus.DEFERRED
        self.failure_reason = reason
        self.attempt_count += 1
        self.next_attempt_at = next_attempt_at
        self.processed_at = None
        _require_aware(now, field_name="now")

    def mark_failed(self, *, reason: str, now: datetime) -> None:
        self.processing_status = WebhookProcessingStatus.FAILED
        self.failure_reason = reason
        self.attempt_count += 1
        self.processed_at = _require_aware(now, field_name="now")

    def mark_abandoned(self, *, reason: str, now: datetime) -> None:
        """Give up on a deferred receipt that never found its request."""
        self.processing_status = WebhookProcessingStatus.ABANDONED
        self.failure_reason = reason
        self.processed_at = _require_aware(now, field_name="now")
        self.next_attempt_at = None

    @property
    def is_settled(self) -> bool:
        return self.processing_status in (
            WebhookProcessingStatus.PROCESSED,
            WebhookProcessingStatus.DUPLICATE,
            WebhookProcessingStatus.REJECTED,
            WebhookProcessingStatus.ABANDONED,
        )


@dataclass(frozen=True, slots=True)
class ProviderDescriptor:
    """Static, publicly describable facts about a configured provider."""

    slug: ProviderSlug
    display_name: str
    authentication_type: str
    enabled: bool
    supported_operations: frozenset[OperationType]
    supports_cancellation: bool
    supports_status_lookup: bool
    supports_provider_idempotency: bool
    webhook_signature_scheme: str
    max_concurrency: int
    max_attempts: int
    total_timeout_seconds: float

    def supports(self, operation_type: OperationType) -> bool:
        return operation_type in self.supported_operations


@dataclass(frozen=True, slots=True)
class ProviderHealth:
    """A point-in-time health summary for one provider."""

    slug: ProviderSlug
    healthy: bool
    circuit_state: str
    checked_at: datetime
    detail: str | None = None
    consecutive_failures: int = 0
    in_flight: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
