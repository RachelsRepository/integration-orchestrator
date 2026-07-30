"""Application commands.

Commands carry already-parsed value objects. Parsing untrusted strings is the
API layer's job, so by the time a use case receives a command every field is
known to be well-formed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from integration_orchestrator.domain.contracts import InboundWebhook
from integration_orchestrator.domain.enums import ActorType, OperationType
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ExternalReference,
    IdempotencyKey,
    ProviderSlug,
)


@dataclass(frozen=True, slots=True)
class Actor:
    """Who initiated an operation, for audit attribution."""

    type: ActorType
    id: str | None = None

    @classmethod
    def system(cls) -> Actor:
        return cls(type=ActorType.SYSTEM)

    @classmethod
    def worker(cls, actor_type: ActorType) -> Actor:
        return cls(type=actor_type)


@dataclass(frozen=True, slots=True)
class CreateIntegrationRequestCommand:
    """Create a new integration request."""

    provider: ProviderSlug
    operation_type: OperationType
    external_reference: ExternalReference
    payload: dict[str, Any]
    correlation_id: CorrelationId
    actor: Actor
    idempotency_key: IdempotencyKey | None = None


@dataclass(frozen=True, slots=True)
class RetryRequestCommand:
    """Operator-initiated retry of an eligible request."""

    request_id: UUID
    correlation_id: CorrelationId
    actor: Actor
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class CancelRequestCommand:
    """Attempt cancellation of a request."""

    request_id: UUID
    correlation_id: CorrelationId
    actor: Actor
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class IngestWebhookCommand:
    """Handle one inbound provider webhook."""

    webhook: InboundWebhook
    correlation_id: CorrelationId
    metadata: dict[str, Any] = field(default_factory=dict)
