"""The workflow journal.

Every state change in the platform is written through this service, and it
always writes three things together: the aggregate, an audit row explaining what
happened, and an outbox row promising to tell the rest of the world. Because all
three go through the same unit of work, they share one transaction and cannot
drift apart.

The journal deliberately does not commit. Callers own the transaction boundary,
which lets a single use case record several transitions atomically — for example
a webhook that both settles a request and marks its receipt processed.
"""

from __future__ import annotations

from typing import Any

from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.application.ports.system import Clock, IdentifierGenerator
from integration_orchestrator.application.ports.unit_of_work import UnitOfWork
from integration_orchestrator.domain.entities import IntegrationRequest, StatusTransition
from integration_orchestrator.domain.enums import AuditAction
from integration_orchestrator.domain.events import (
    CURRENT_EVENT_VERSION,
    build_circuit_event_payload,
    build_request_event_payload,
    event_type_for_status,
)
from integration_orchestrator.domain.records import AuditEvent, OutboxEvent
from integration_orchestrator.domain.value_objects import CorrelationId, ProviderSlug


class WorkflowJournal:
    """Writes aggregate changes, audit history and outbox events atomically."""

    def __init__(self, *, clock: Clock, ids: IdentifierGenerator) -> None:
        self._clock = clock
        self._ids = ids

    async def record_transition(
        self,
        uow: UnitOfWork,
        request: IntegrationRequest,
        transition: StatusTransition,
        *,
        action: AuditAction,
        actor: Actor,
        metadata: dict[str, Any] | None = None,
        causation_id: str | None = None,
        publish_event: bool = True,
    ) -> None:
        """Persist a status change together with its audit and outbox rows."""
        await uow.requests.update(request)
        await uow.audit.append(
            AuditEvent.for_request(
                event_id=self._ids.new_id(),
                request_id=request.id,
                action=action,
                actor=actor.type,
                actor_id=actor.id,
                correlation_id=request.correlation_id,
                occurred_at=transition.occurred_at,
                previous_state=transition.previous_status,
                new_state=transition.new_status,
                metadata=dict(metadata or {}),
            )
        )
        if not publish_event:
            return

        event_type = event_type_for_status(transition.new_status)
        payload = build_request_event_payload(
            request_id=request.id,
            provider=request.provider.value,
            operation_type=request.operation_type.value,
            external_reference=request.external_reference.value,
            status=transition.new_status,
            previous_status=transition.previous_status,
            occurred_at=transition.occurred_at,
            provider_reference=request.provider_reference,
            attempt_count=request.attempt_count,
            error_code=request.last_error_code,
            error_category=request.last_error_category,
            next_retry_at=request.next_retry_at,
        )
        await uow.outbox.add(
            OutboxEvent(
                id=self._ids.new_id(),
                event_id=self._ids.new_id(),
                event_type=event_type,
                event_version=CURRENT_EVENT_VERSION,
                aggregate_type="integration_request",
                aggregate_id=str(request.id),
                payload=payload,
                correlation_id=request.correlation_id,
                causation_id=causation_id,
                partition_key=str(request.id),
                created_at=transition.occurred_at,
            )
        )

    async def record_creation(
        self,
        uow: UnitOfWork,
        request: IntegrationRequest,
        *,
        actor: Actor,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record the birth of a request.

        Separate from :meth:`record_transition` because the aggregate has just
        been inserted rather than updated, so re-issuing an update here would
        trip the optimistic concurrency check on a row that has no prior version.
        """
        await uow.audit.append(
            AuditEvent.for_request(
                event_id=self._ids.new_id(),
                request_id=request.id,
                action=AuditAction.REQUEST_RECEIVED,
                actor=actor.type,
                actor_id=actor.id,
                correlation_id=request.correlation_id,
                occurred_at=request.created_at,
                new_state=request.status,
                metadata=dict(metadata or {}),
            )
        )
        await uow.outbox.add(
            OutboxEvent(
                id=self._ids.new_id(),
                event_id=self._ids.new_id(),
                event_type=event_type_for_status(request.status),
                event_version=CURRENT_EVENT_VERSION,
                aggregate_type="integration_request",
                aggregate_id=str(request.id),
                payload=build_request_event_payload(
                    request_id=request.id,
                    provider=request.provider.value,
                    operation_type=request.operation_type.value,
                    external_reference=request.external_reference.value,
                    status=request.status,
                    previous_status=None,
                    occurred_at=request.created_at,
                    attempt_count=request.attempt_count,
                ),
                correlation_id=request.correlation_id,
                partition_key=str(request.id),
                created_at=request.created_at,
            )
        )

    async def record_request_action(
        self,
        uow: UnitOfWork,
        request: IntegrationRequest,
        *,
        action: AuditAction,
        actor: Actor,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record something noteworthy that did not change the request's status.

        Dispatch attempts, cancellation rejections and reconciliation matches all
        need to be visible in the audit trail even though the status stayed put.
        """
        await uow.audit.append(
            AuditEvent.for_request(
                event_id=self._ids.new_id(),
                request_id=request.id,
                action=action,
                actor=actor.type,
                actor_id=actor.id,
                correlation_id=request.correlation_id,
                occurred_at=self._clock.now(),
                previous_state=request.status,
                new_state=request.status,
                metadata=dict(metadata or {}),
            )
        )

    async def record_webhook_action(
        self,
        uow: UnitOfWork,
        *,
        receipt_id: Any,
        action: AuditAction,
        correlation_id: CorrelationId,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record a webhook lifecycle decision against the receipt aggregate."""
        await uow.audit.append(
            AuditEvent.for_webhook(
                event_id=self._ids.new_id(),
                receipt_id=receipt_id,
                action=action,
                correlation_id=correlation_id,
                occurred_at=self._clock.now(),
                metadata=dict(metadata or {}),
            )
        )

    async def record_circuit_transition(
        self,
        uow: UnitOfWork,
        *,
        provider: ProviderSlug,
        previous_state: str,
        new_state: str,
        event_type: str,
        action: AuditAction,
        correlation_id: CorrelationId,
        failure_count: int = 0,
        reason: str | None = None,
    ) -> None:
        """Record a circuit breaker state change for audit and publication.

        Circuit transitions are published as events because they are operational
        facts other systems care about: dashboards, alerting, and any service
        that wants to stop enqueueing work for a provider that is down.
        """
        now = self._clock.now()
        await uow.audit.append(
            AuditEvent.for_provider(
                event_id=self._ids.new_id(),
                provider=provider.value,
                action=action,
                actor=Actor.system().type,
                correlation_id=correlation_id,
                occurred_at=now,
                previous_state=previous_state,
                new_state=new_state,
                metadata={"failure_count": failure_count, "reason": reason},
            )
        )
        await uow.outbox.add(
            OutboxEvent(
                id=self._ids.new_id(),
                event_id=self._ids.new_id(),
                event_type=event_type,
                event_version=CURRENT_EVENT_VERSION,
                aggregate_type="provider",
                aggregate_id=provider.value,
                payload=build_circuit_event_payload(
                    provider=provider.value,
                    previous_state=previous_state,
                    new_state=new_state,
                    occurred_at=now,
                    failure_count=failure_count,
                    reason=reason,
                ),
                correlation_id=correlation_id,
                partition_key=provider.value,
                created_at=now,
            )
        )
