"""Read-side use cases."""

from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from integration_orchestrator.application.dto.queries import IntegrationRequestFilter, Page
from integration_orchestrator.application.ports.unit_of_work import UnitOfWorkFactory
from integration_orchestrator.domain.entities import IntegrationRequest
from integration_orchestrator.domain.errors import NotFoundError
from integration_orchestrator.domain.records import AuditEvent

AUDIT_HISTORY_LIMIT = 500


class GetIntegrationRequestUseCase:
    """Fetch one request by id."""

    def __init__(self, *, uow_factory: UnitOfWorkFactory) -> None:
        self._uow_factory = uow_factory

    async def execute(self, request_id: UUID) -> IntegrationRequest:
        async with self._uow_factory() as uow:
            request = await uow.requests.get(request_id)
            if request is None:
                raise NotFoundError(
                    f"integration request '{request_id}' does not exist",
                    metadata={"request_id": str(request_id)},
                )
            return request


class ListIntegrationRequestsUseCase:
    """Fetch a filtered, cursor-paginated page of requests."""

    def __init__(self, *, uow_factory: UnitOfWorkFactory) -> None:
        self._uow_factory = uow_factory

    async def execute(self, criteria: IntegrationRequestFilter) -> Page[IntegrationRequest]:
        async with self._uow_factory() as uow:
            return await uow.requests.list(criteria)


class GetAuditHistoryUseCase:
    """Fetch the audit trail for one request."""

    def __init__(self, *, uow_factory: UnitOfWorkFactory) -> None:
        self._uow_factory = uow_factory

    async def execute(
        self, request_id: UUID, *, limit: int = AUDIT_HISTORY_LIMIT
    ) -> Sequence[AuditEvent]:
        async with self._uow_factory() as uow:
            request = await uow.requests.get(request_id)
            if request is None:
                raise NotFoundError(
                    f"integration request '{request_id}' does not exist",
                    metadata={"request_id": str(request_id)},
                )
            return await uow.audit.list_for_aggregate(
                aggregate_type="integration_request",
                aggregate_id=str(request_id),
                limit=limit,
            )
