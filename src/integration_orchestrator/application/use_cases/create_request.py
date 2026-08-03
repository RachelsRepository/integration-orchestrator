"""Create an integration request.

Two things make this use case more involved than a simple insert.

The first is idempotency. A client may send the same ``Idempotency-Key`` twice
because its own retry fired, and the platform must return the original result
rather than creating a second provider operation. It must also refuse the key
when the body differs, because silently returning the first result for a
different request would hide a client bug behind a success response.

The second is that duplicate protection has to hold under concurrency. Reading
"does this key exist?" and then inserting is a race: two simultaneous requests
both read nothing and both insert. The database's unique constraint is therefore
the arbiter, not the read. The loser of the insert race replays the winner's
result instead of failing.
"""

from __future__ import annotations

import logging

from integration_orchestrator.application.dto.commands import (
    Actor,
    CreateIntegrationRequestCommand,
)
from integration_orchestrator.application.dto.results import CreateIntegrationRequestResult
from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.application.ports.provider_gateway import ProviderRegistry
from integration_orchestrator.application.ports.system import Clock, IdentifierGenerator
from integration_orchestrator.application.ports.unit_of_work import (
    UnitOfWork,
    UnitOfWorkFactory,
)
from integration_orchestrator.application.services.dispatcher import RequestDispatcher
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.domain.entities import IntegrationRequest
from integration_orchestrator.domain.enums import AuditAction
from integration_orchestrator.domain.errors import (
    ConflictError,
    IdempotencyConflictError,
    NotFoundError,
    ProviderNotConfiguredError,
    UnsupportedOperationError,
)
from integration_orchestrator.domain.records import IdempotencyRecord
from integration_orchestrator.domain.value_objects import RequestFingerprint

logger = logging.getLogger(__name__)


class CreateIntegrationRequestUseCase:
    """Accepts, persists and dispatches a new integration request."""

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        registry: ProviderRegistry,
        journal: WorkflowJournal,
        dispatcher: RequestDispatcher,
        clock: Clock,
        ids: IdentifierGenerator,
        metrics: MetricsSink,
    ) -> None:
        self._uow_factory = uow_factory
        self._registry = registry
        self._journal = journal
        self._dispatcher = dispatcher
        self._clock = clock
        self._ids = ids
        self._metrics = metrics

    async def execute(
        self, command: CreateIntegrationRequestCommand
    ) -> CreateIntegrationRequestResult:
        self._validate_provider_support(command)

        fingerprint = RequestFingerprint.of(
            provider=command.provider,
            operation_type=command.operation_type.value,
            external_reference=command.external_reference,
            payload=command.payload,
        )

        async with self._uow_factory() as uow:
            if command.idempotency_key is not None:
                replay = await self._replay_if_seen(
                    uow, key=command.idempotency_key.value, fingerprint=fingerprint
                )
                if replay is not None:
                    return replay

            request = await self._persist_new_request(uow, command, fingerprint)
            if request is None:
                # Lost the insert race. The winner's record is committed, so a
                # fresh transaction can read it and replay the original result.
                return await self._replay_after_race(
                    key=command.idempotency_key.value if command.idempotency_key else "",
                    fingerprint=fingerprint,
                )
            await uow.commit()

        self._metrics.increment(
            "integration_requests_total",
            labels={
                "provider": command.provider.value,
                "operation_type": command.operation_type.value,
                "outcome": "created",
            },
        )

        # The request is durable before the provider is contacted. If dispatch
        # fails outright the caller still gets a tracked request id rather than
        # an error with no record behind it.
        dispatched = await self._dispatcher.dispatch(request.id, actor=command.actor)
        return CreateIntegrationRequestResult(request=dispatched, replayed=False)

    # -- validation ---------------------------------------------------------

    def _validate_provider_support(self, command: CreateIntegrationRequestCommand) -> None:
        if not self._registry.has(command.provider):
            raise ProviderNotConfiguredError(
                command.provider.value, correlation_id=command.correlation_id.value
            )
        descriptor = self._registry.get(command.provider).descriptor()
        if not descriptor.supports(command.operation_type):
            raise UnsupportedOperationError(
                f"provider '{command.provider.value}' does not support the operation "
                f"'{command.operation_type.value}'",
                provider=command.provider.value,
                correlation_id=command.correlation_id.value,
                metadata={
                    "supported_operations": sorted(
                        operation.value for operation in descriptor.supported_operations
                    )
                },
            )

    # -- idempotency --------------------------------------------------------

    async def _replay_if_seen(
        self,
        uow: UnitOfWork,
        *,
        key: str,
        fingerprint: RequestFingerprint,
    ) -> CreateIntegrationRequestResult | None:
        record = await uow.idempotency.get(key)
        if record is None:
            return None
        if not record.matches(fingerprint.value):
            self._metrics.increment("idempotency_conflicts_total")
            raise IdempotencyConflictError(
                "this idempotency key was already used with a different request body",
                metadata={"idempotency_key": key},
            )
        request = await uow.requests.get(record.request_id)
        if request is None:  # pragma: no cover - foreign key makes this unreachable
            raise NotFoundError(
                "the request referenced by this idempotency key no longer exists",
                metadata={"idempotency_key": key},
            )
        self._metrics.increment(
            "integration_requests_total",
            labels={
                "provider": request.provider.value,
                "operation_type": request.operation_type.value,
                "outcome": "replayed",
            },
        )
        logger.info(
            "replaying an idempotent request",
            extra={
                "integration_request_id": str(request.id),
                "correlation_id": request.correlation_id.value,
                "provider": request.provider.value,
            },
        )
        return CreateIntegrationRequestResult(request=request, replayed=True)

    async def _replay_after_race(
        self, *, key: str, fingerprint: RequestFingerprint
    ) -> CreateIntegrationRequestResult:
        async with self._uow_factory() as uow:
            replay = await self._replay_if_seen(uow, key=key, fingerprint=fingerprint)
            if replay is None:  # pragma: no cover - the winner has committed by now
                raise ConflictError(
                    "a concurrent request is still being created; retry shortly",
                    metadata={"idempotency_key": key},
                )
            return replay

    # -- persistence --------------------------------------------------------

    async def _persist_new_request(
        self,
        uow: UnitOfWork,
        command: CreateIntegrationRequestCommand,
        fingerprint: RequestFingerprint,
    ) -> IntegrationRequest | None:
        """Insert the request, its audit row and its idempotency claim.

        Returns ``None`` when a concurrent creation won the unique-constraint
        race, which the caller turns into a replay.
        """
        now = self._clock.now()
        request = IntegrationRequest.create(
            request_id=self._ids.new_id(),
            provider=command.provider,
            operation_type=command.operation_type,
            external_reference=command.external_reference,
            normalized_payload=command.payload,
            correlation_id=command.correlation_id,
            now=now,
            idempotency_key=command.idempotency_key,
            owner_subject=command.actor.id,
        )
        await uow.requests.add(request)

        # Flush the request before the idempotency row. The two models are not
        # linked by an ORM relationship, so SQLAlchemy otherwise inserts the
        # child first and the foreign key rejects it on an empty table — which
        # the race handler then misreads as a concurrent creation.
        if command.idempotency_key is not None:
            try:
                await uow.flush()
            except ConflictError:
                await uow.rollback()
                return None
            await uow.idempotency.add(
                IdempotencyRecord(
                    key=command.idempotency_key.value,
                    fingerprint=fingerprint.value,
                    request_id=request.id,
                    response_status=201,
                    created_at=now,
                )
            )

        try:
            await uow.flush()
        except ConflictError:
            await uow.rollback()
            if command.idempotency_key is None:
                raise
            return None

        await self._journal.record_creation(
            uow,
            request,
            actor=command.actor,
            metadata={
                "operation_type": request.operation_type.value,
                "external_reference": request.external_reference.value,
                "idempotent": command.idempotency_key is not None,
            },
        )
        await self._journal.record_request_action(
            uow,
            request,
            action=AuditAction.PROVIDER_SELECTED,
            actor=Actor.system(),
            metadata={"provider": request.provider.value},
        )
        return request
