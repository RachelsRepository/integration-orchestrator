"""Normalized domain events and their published envelope.

Event names are versioned in the type string itself
(``integration.request.succeeded.v1``) rather than in a header, because the type
is what consumers subscribe to and route on. A breaking change publishes a new
type alongside the old one so producers and consumers can migrate independently.

Schema evolution rules for these events are documented in ``docs/event-model.md``:
additive optional fields are compatible and stay on the current version; removing
a field, renaming a field, or narrowing its meaning requires a new version.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final
from uuid import UUID

from integration_orchestrator.domain.enums import RequestStatus
from integration_orchestrator.domain.errors import ValidationError
from integration_orchestrator.domain.value_objects import CorrelationId

CURRENT_EVENT_VERSION: Final[int] = 1
PRODUCER_NAME: Final[str] = "integration-orchestrator"


class EventType:
    """Canonical event type strings."""

    REQUEST_RECEIVED: Final[str] = "integration.request.received.v1"
    REQUEST_DISPATCHED: Final[str] = "integration.request.dispatched.v1"
    REQUEST_PENDING: Final[str] = "integration.request.pending.v1"
    REQUEST_SUCCEEDED: Final[str] = "integration.request.succeeded.v1"
    REQUEST_FAILED: Final[str] = "integration.request.failed.v1"
    REQUEST_RETRY_SCHEDULED: Final[str] = "integration.request.retry_scheduled.v1"
    REQUEST_CANCELLED: Final[str] = "integration.request.cancelled.v1"
    REQUEST_MANUAL_REVIEW: Final[str] = "integration.request.manual_review.v1"
    PROVIDER_CIRCUIT_OPENED: Final[str] = "provider.circuit_opened.v1"
    PROVIDER_CIRCUIT_CLOSED: Final[str] = "provider.circuit_closed.v1"


# Which event a status transition publishes. Keeping this as data rather than a
# chain of conditionals means adding a status cannot silently publish nothing.
_STATUS_EVENT_TYPES: dict[RequestStatus, str] = {
    RequestStatus.RECEIVED: EventType.REQUEST_RECEIVED,
    RequestStatus.VALIDATING: EventType.REQUEST_RECEIVED,
    RequestStatus.DISPATCHING: EventType.REQUEST_DISPATCHED,
    RequestStatus.PENDING: EventType.REQUEST_PENDING,
    RequestStatus.SUCCEEDED: EventType.REQUEST_SUCCEEDED,
    RequestStatus.FAILED: EventType.REQUEST_FAILED,
    RequestStatus.RETRY_SCHEDULED: EventType.REQUEST_RETRY_SCHEDULED,
    RequestStatus.CANCELLED: EventType.REQUEST_CANCELLED,
    RequestStatus.MANUAL_REVIEW: EventType.REQUEST_MANUAL_REVIEW,
}


def event_type_for_status(status: RequestStatus) -> str:
    """Return the event type published when a request reaches ``status``."""
    try:
        return _STATUS_EVENT_TYPES[status]
    except KeyError as exc:  # pragma: no cover - unreachable while the map is total
        raise ValidationError(f"no event type is mapped for status '{status.value}'") from exc


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """The wire format for every published event.

    Consumers depend on this envelope shape, not on any particular payload, so
    generic infrastructure such as dead-letter tooling and audit mirrors can be
    written once.
    """

    event_id: UUID
    event_type: str
    event_version: int
    aggregate_type: str
    aggregate_id: str
    occurred_at: datetime
    correlation_id: str
    producer: str
    payload: dict[str, Any]
    causation_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None:
            raise ValidationError("event timestamps must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": str(self.event_id),
            "event_type": self.event_type,
            "event_version": self.event_version,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "occurred_at": self.occurred_at.isoformat(),
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "producer": self.producer,
            "payload": self.payload,
            "metadata": self.metadata,
        }


def build_request_event_payload(
    *,
    request_id: UUID,
    provider: str,
    operation_type: str,
    external_reference: str,
    status: RequestStatus,
    previous_status: RequestStatus | None,
    occurred_at: datetime,
    provider_reference: str | None = None,
    attempt_count: int = 0,
    error_code: str | None = None,
    error_category: str | None = None,
    next_retry_at: datetime | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble the payload for a request lifecycle event.

    Deliberately excludes ``normalized_payload`` and ``provider_payload``.
    Business payloads can contain customer data, and a Kafka topic is a far
    wider blast radius than a database row, so events carry identifiers and
    state rather than content.
    """
    payload: dict[str, Any] = {
        "request_id": str(request_id),
        "provider": provider,
        "operation_type": operation_type,
        "external_reference": external_reference,
        "status": status.value,
        "previous_status": previous_status.value if previous_status else None,
        "provider_reference": provider_reference,
        "attempt_count": attempt_count,
        "occurred_at": occurred_at.isoformat(),
    }
    if error_code:
        payload["error_code"] = error_code
    if error_category:
        payload["error_category"] = error_category
    if next_retry_at:
        payload["next_retry_at"] = next_retry_at.isoformat()
    if extra:
        payload.update(extra)
    return payload


def build_circuit_event_payload(
    *,
    provider: str,
    previous_state: str,
    new_state: str,
    occurred_at: datetime,
    failure_count: int = 0,
    reason: str | None = None,
) -> dict[str, Any]:
    """Assemble the payload for a circuit breaker lifecycle event."""
    payload: dict[str, Any] = {
        "provider": provider,
        "previous_state": previous_state,
        "state": new_state,
        "failure_count": failure_count,
        "occurred_at": occurred_at.isoformat(),
    }
    if reason:
        payload["reason"] = reason
    return payload


def envelope_from_parts(
    *,
    event_id: UUID,
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    occurred_at: datetime,
    correlation_id: CorrelationId,
    payload: dict[str, Any],
    event_version: int = CURRENT_EVENT_VERSION,
    causation_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> EventEnvelope:
    """Construct an envelope with the standard producer identity."""
    return EventEnvelope(
        event_id=event_id,
        event_type=event_type,
        event_version=event_version,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        occurred_at=occurred_at,
        correlation_id=correlation_id.value,
        causation_id=causation_id,
        producer=PRODUCER_NAME,
        payload=payload,
        metadata=dict(metadata or {}),
    )
