"""Append-only supporting records: audit, outbox and idempotency.

These are written in the same database transaction as the aggregate change they
describe. That is the whole point of the outbox pattern: a state change and the
promise to tell the world about it either both commit or neither does.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Self
from uuid import UUID

from integration_orchestrator.domain.enums import ActorType, AuditAction, RequestStatus
from integration_orchestrator.domain.errors import ValidationError
from integration_orchestrator.domain.value_objects import CorrelationId


@dataclass(frozen=True, slots=True)
class AuditEvent:
    """An immutable record of one meaningful decision.

    Audit rows are never updated or deleted. Metadata is expected to have been
    redacted before it reaches this constructor; the entity does not attempt
    redaction itself because it cannot know which provider fields are sensitive.
    """

    id: UUID
    aggregate_type: str
    aggregate_id: str
    action: AuditAction
    actor: ActorType
    correlation_id: CorrelationId
    occurred_at: datetime
    previous_state: str | None = None
    new_state: str | None = None
    actor_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None:
            raise ValidationError("audit timestamps must be timezone-aware")
        if not self.aggregate_id:
            raise ValidationError("an audit event requires an aggregate id")

    @classmethod
    def for_request(
        cls,
        *,
        event_id: UUID,
        request_id: UUID,
        action: AuditAction,
        actor: ActorType,
        correlation_id: CorrelationId,
        occurred_at: datetime,
        previous_state: RequestStatus | None = None,
        new_state: RequestStatus | None = None,
        actor_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            id=event_id,
            aggregate_type="integration_request",
            aggregate_id=str(request_id),
            action=action,
            actor=actor,
            correlation_id=correlation_id,
            occurred_at=occurred_at,
            previous_state=previous_state.value if previous_state else None,
            new_state=new_state.value if new_state else None,
            actor_id=actor_id,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def for_provider(
        cls,
        *,
        event_id: UUID,
        provider: str,
        action: AuditAction,
        actor: ActorType,
        correlation_id: CorrelationId,
        occurred_at: datetime,
        previous_state: str | None = None,
        new_state: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            id=event_id,
            aggregate_type="provider",
            aggregate_id=provider,
            action=action,
            actor=actor,
            correlation_id=correlation_id,
            occurred_at=occurred_at,
            previous_state=previous_state,
            new_state=new_state,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def for_webhook(
        cls,
        *,
        event_id: UUID,
        receipt_id: UUID,
        action: AuditAction,
        correlation_id: CorrelationId,
        occurred_at: datetime,
        metadata: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            id=event_id,
            aggregate_type="webhook_receipt",
            aggregate_id=str(receipt_id),
            action=action,
            actor=ActorType.WEBHOOK,
            correlation_id=correlation_id,
            occurred_at=occurred_at,
            metadata=dict(metadata or {}),
        )


@dataclass(slots=True)
class OutboxEvent:
    """A durable promise to publish a domain event.

    ``event_id`` is stable and generated once, at the moment the state change
    committed. It is not regenerated on republication, which is what lets
    consumers deduplicate under at-least-once delivery.
    """

    id: UUID
    event_id: UUID
    event_type: str
    event_version: int
    aggregate_type: str
    aggregate_id: str
    payload: dict[str, Any]
    correlation_id: CorrelationId
    created_at: datetime
    causation_id: str | None = None
    partition_key: str | None = None
    published_at: datetime | None = None
    attempt_count: int = 0
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    dead_lettered_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None:
            raise ValidationError("outbox timestamps must be timezone-aware")
        if self.event_version < 1:
            raise ValidationError("an event version must be at least 1")

    @property
    def is_published(self) -> bool:
        return self.published_at is not None

    @property
    def is_dead_lettered(self) -> bool:
        return self.dead_lettered_at is not None

    @property
    def routing_key(self) -> str:
        """Partition key. Defaults to the aggregate id to preserve per-aggregate ordering."""
        return self.partition_key or self.aggregate_id

    def mark_published(self, *, now: datetime) -> None:
        self.published_at = now
        self.last_error = None
        self.next_attempt_at = None

    def mark_publish_failed(self, *, error: str, next_attempt_at: datetime, now: datetime) -> None:
        self.attempt_count += 1
        self.last_error = error[:1000]
        self.next_attempt_at = next_attempt_at
        del now  # retained for signature symmetry with the other mutators

    def mark_dead_lettered(self, *, error: str, now: datetime) -> None:
        """Stop retrying: the event needs an operator, not another attempt."""
        self.attempt_count += 1
        self.last_error = error[:1000]
        self.next_attempt_at = None
        self.dead_lettered_at = now


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    """The stored outcome of a previously handled creation request.

    Holding the fingerprint alongside the resulting request id is what lets the
    platform distinguish an honest client retry (same key, same body, return the
    original result) from a client bug (same key, different body, reject).
    """

    key: str
    fingerprint: str
    request_id: UUID
    response_status: int
    created_at: datetime
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None:
            raise ValidationError("idempotency timestamps must be timezone-aware")

    def matches(self, fingerprint: str) -> bool:
        return self.fingerprint == fingerprint
