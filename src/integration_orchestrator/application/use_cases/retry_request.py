"""Operator-initiated retry.

A manual retry does not call the provider inline. It puts the request back into
``retry_scheduled`` with an immediate due time and lets the retry worker pick it
up, so manual retries flow through exactly the same claiming, concurrency limit
and circuit breaker path as automatic ones. An operator retrying a thousand
stuck requests therefore cannot bypass the protections that exist to stop the
platform overwhelming a recovering provider.
"""

from __future__ import annotations

import logging

from integration_orchestrator.application.dto.commands import RetryRequestCommand
from integration_orchestrator.application.ports.system import Clock
from integration_orchestrator.application.ports.unit_of_work import UnitOfWorkFactory
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.domain.entities import IntegrationRequest
from integration_orchestrator.domain.enums import AuditAction
from integration_orchestrator.domain.errors import ConflictError, NotFoundError
from integration_orchestrator.domain.state_machine import MANUALLY_RETRYABLE_STATUSES

logger = logging.getLogger(__name__)


class RetryIntegrationRequestUseCase:
    """Re-queues an eligible failed request."""

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        journal: WorkflowJournal,
        clock: Clock,
    ) -> None:
        self._uow_factory = uow_factory
        self._journal = journal
        self._clock = clock

    async def execute(self, command: RetryRequestCommand) -> IntegrationRequest:
        async with self._uow_factory() as uow:
            request = await uow.requests.get_for_update(command.request_id)
            if request is None:
                raise NotFoundError(
                    f"integration request '{command.request_id}' does not exist",
                    correlation_id=command.correlation_id.value,
                    metadata={"request_id": str(command.request_id)},
                )
            if request.status not in MANUALLY_RETRYABLE_STATUSES:
                raise ConflictError(
                    f"a request in status '{request.status.value}' cannot be retried",
                    correlation_id=command.correlation_id.value,
                    retryable=False,
                    metadata={
                        "status": request.status.value,
                        "retryable_statuses": sorted(
                            status.value for status in MANUALLY_RETRYABLE_STATUSES
                        ),
                    },
                )

            now = self._clock.now()
            transition = request.restore_for_retry(next_retry_at=now, now=now)
            await self._journal.record_transition(
                uow,
                request,
                transition,
                action=AuditAction.RETRY_REQUESTED,
                actor=command.actor,
                metadata={
                    "reason": command.reason,
                    "previous_attempt_count": request.attempt_count,
                    "manual": True,
                },
            )
            await uow.commit()

        logger.info(
            "manual retry scheduled",
            extra={
                "integration_request_id": str(request.id),
                "correlation_id": command.correlation_id.value,
                "provider": request.provider.value,
                "attempt": request.attempt_count,
            },
        )
        return request
