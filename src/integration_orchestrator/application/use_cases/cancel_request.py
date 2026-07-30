"""Cancellation.

Cancellation splits into two very different cases. A request that has not
reached the provider can simply be cancelled locally: nothing external exists to
undo. A request the provider has already accepted can only be cancelled if that
provider supports cancellation at all, and the attempt is a network call whose
outcome the provider decides.

Providers that do not support cancellation are rejected explicitly rather than
being silently cancelled locally. Marking a request cancelled while the provider
continues to fulfil it would make our record a lie.
"""

from __future__ import annotations

import logging

from integration_orchestrator.application.dto.commands import CancelRequestCommand
from integration_orchestrator.application.ports.provider_gateway import ProviderRegistry
from integration_orchestrator.application.ports.system import Clock
from integration_orchestrator.application.ports.unit_of_work import UnitOfWorkFactory
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.domain.contracts import (
    CancelProviderOperationCommand,
    ProviderErrorInfo,
    ProviderOperationResult,
)
from integration_orchestrator.domain.entities import IntegrationRequest
from integration_orchestrator.domain.enums import (
    AuditAction,
    NormalizedStatus,
    RequestStatus,
)
from integration_orchestrator.domain.errors import (
    ConflictError,
    NotFoundError,
    ProviderError,
    UnsupportedOperationError,
)
from integration_orchestrator.domain.state_machine import CANCELLABLE_STATUSES

logger = logging.getLogger(__name__)


class CancelIntegrationRequestUseCase:
    """Attempts to cancel a request, locally or at the provider."""

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        registry: ProviderRegistry,
        journal: WorkflowJournal,
        clock: Clock,
    ) -> None:
        self._uow_factory = uow_factory
        self._registry = registry
        self._journal = journal
        self._clock = clock

    async def execute(self, command: CancelRequestCommand) -> IntegrationRequest:
        async with self._uow_factory() as uow:
            request = await uow.requests.get_for_update(command.request_id)
            if request is None:
                raise NotFoundError(
                    f"integration request '{command.request_id}' does not exist",
                    correlation_id=command.correlation_id.value,
                )
            self._assert_cancellable(request, command)

            if request.provider_reference is None:
                # Nothing exists at the provider, so cancellation is purely local.
                transition = request.mark_cancelled(
                    now=self._clock.now(), reason=command.reason or "cancelled before dispatch"
                )
                await self._journal.record_transition(
                    uow,
                    request,
                    transition,
                    action=AuditAction.REQUEST_CANCELLED,
                    actor=command.actor,
                    metadata={"reason": command.reason, "provider_contacted": False},
                )
                await uow.commit()
                return request
            provider_reference = request.provider_reference

        result = await self._request_provider_cancellation(
            command, provider_reference=provider_reference
        )

        async with self._uow_factory() as uow:
            current = await uow.requests.get_for_update(command.request_id)
            if current is None:  # pragma: no cover - the row cannot vanish mid-flight
                raise NotFoundError(f"integration request '{command.request_id}' does not exist")

            if current.status.is_terminal:
                # The provider settled it while we were asking it to stop.
                await self._journal.record_request_action(
                    uow,
                    current,
                    action=AuditAction.CANCELLATION_REJECTED,
                    actor=command.actor,
                    metadata={"reason": "the request reached a terminal state first"},
                )
                await uow.commit()
                return current

            if result.accepted and result.normalized_status in (
                NormalizedStatus.CANCELLED,
                NormalizedStatus.ACCEPTED,
            ):
                transition = current.mark_cancelled(
                    now=self._clock.now(), reason=command.reason or "cancelled at the provider"
                )
                await self._journal.record_transition(
                    uow,
                    current,
                    transition,
                    action=AuditAction.REQUEST_CANCELLED,
                    actor=command.actor,
                    metadata={
                        "reason": command.reason,
                        "provider_contacted": True,
                        "provider_status": result.provider_status,
                    },
                )
                await uow.commit()
                return current

            error = result.error
            await self._journal.record_request_action(
                uow,
                current,
                action=AuditAction.CANCELLATION_REJECTED,
                actor=command.actor,
                metadata={
                    "provider_status": result.provider_status,
                    "error_code": error.code if error else None,
                    "error_category": error.category.value if error else None,
                },
            )
            await uow.commit()

        raise ConflictError(
            "the provider refused to cancel this operation",
            correlation_id=command.correlation_id.value,
            retryable=False,
            provider=current.provider.value,
            metadata={"provider_status": result.provider_status},
        )

    # -- helpers ------------------------------------------------------------

    def _assert_cancellable(
        self, request: IntegrationRequest, command: CancelRequestCommand
    ) -> None:
        if request.status not in CANCELLABLE_STATUSES:
            raise ConflictError(
                f"a request in status '{request.status.value}' cannot be cancelled",
                correlation_id=command.correlation_id.value,
                retryable=False,
                metadata={"status": request.status.value},
            )
        descriptor = self._registry.get(request.provider).descriptor()
        if request.status is RequestStatus.PENDING and not descriptor.supports_cancellation:
            raise UnsupportedOperationError(
                f"provider '{request.provider.value}' does not support cancelling an "
                "operation it has already accepted",
                provider=request.provider.value,
                correlation_id=command.correlation_id.value,
            )

    async def _request_provider_cancellation(
        self, command: CancelRequestCommand, *, provider_reference: str
    ) -> ProviderOperationResult:
        async with self._uow_factory() as uow:
            request = await uow.requests.get(command.request_id)
        if request is None:  # pragma: no cover - the row cannot vanish mid-flight
            raise NotFoundError(f"integration request '{command.request_id}' does not exist")

        gateway = self._registry.get(request.provider)
        try:
            return await gateway.cancel_operation(
                CancelProviderOperationCommand(
                    request_id=request.id,
                    provider=request.provider,
                    provider_reference=provider_reference,
                    correlation_id=command.correlation_id,
                    reason=command.reason,
                )
            )
        except ProviderError as exc:
            logger.warning(
                "provider cancellation failed",
                extra={
                    "integration_request_id": str(request.id),
                    "correlation_id": command.correlation_id.value,
                    "provider": request.provider.value,
                    "error_code": exc.code,
                },
            )
            return ProviderOperationResult.failure(error=ProviderErrorInfo.from_error(exc))
