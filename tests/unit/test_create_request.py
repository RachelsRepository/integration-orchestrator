"""Request creation, idempotency and the insert race."""

from __future__ import annotations

import pytest

from integration_orchestrator.application.dto.commands import (
    Actor,
    CreateIntegrationRequestCommand,
)
from integration_orchestrator.application.services.dispatcher import RequestDispatcher
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.application.use_cases.create_request import (
    CreateIntegrationRequestUseCase,
)
from integration_orchestrator.domain.enums import AuditAction, OperationType, RequestStatus
from integration_orchestrator.domain.errors import (
    IdempotencyConflictError,
    ProviderNotConfiguredError,
    UnsupportedOperationError,
)
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ExternalReference,
    IdempotencyKey,
    ProviderSlug,
)
from integration_orchestrator.infrastructure.system import (
    FrozenClock,
    SequentialIdentifierGenerator,
)
from tests.support.doubles import (
    FakeGateway,
    FakeRegistry,
    RecordingMetrics,
    accepted_result,
    descriptor_for,
)
from tests.support.memory_uow import MemoryStore, MemoryUnitOfWorkFactory

pytestmark = pytest.mark.unit

NORTHSTAR = ProviderSlug.parse("northstar")


def command(
    *,
    payload: dict[str, object] | None = None,
    idempotency_key: str | None = None,
    provider: ProviderSlug = NORTHSTAR,
    operation_type: OperationType = OperationType.RESOURCE_PROVISION,
    external_reference: str = "order-1001",
    actor: Actor | None = None,
) -> CreateIntegrationRequestCommand:
    return CreateIntegrationRequestCommand(
        provider=provider,
        operation_type=operation_type,
        external_reference=ExternalReference(external_reference),
        payload=payload if payload is not None else {"quantity": 2},
        correlation_id=CorrelationId("corr-1"),
        actor=actor or Actor.system(),
        idempotency_key=IdempotencyKey(idempotency_key) if idempotency_key else None,
    )


async def test_a_created_request_is_persisted_and_dispatched(
    create_use_case: CreateIntegrationRequestUseCase,
    gateway: FakeGateway,
    store: MemoryStore,
) -> None:
    gateway.queue_create(accepted_result("prv-1"))

    result = await create_use_case.execute(command())

    assert result.replayed is False
    assert result.request.status is RequestStatus.PENDING
    assert store.requests[result.request.id].provider_reference == "prv-1"


async def test_the_request_is_durable_before_the_provider_is_contacted(
    create_use_case: CreateIntegrationRequestUseCase,
    gateway: FakeGateway,
    store: MemoryStore,
) -> None:
    """A caller must never get an error with no tracked record behind it."""
    seen: list[int] = []

    async def _observe(*_: object, **__: object):
        seen.append(len(store.requests))
        return accepted_result("prv-1")

    gateway.create_operation = _observe  # type: ignore[method-assign]

    await create_use_case.execute(command())

    assert seen == [1]


async def test_creation_writes_the_audit_trail_and_the_outbox_event(
    create_use_case: CreateIntegrationRequestUseCase,
    gateway: FakeGateway,
    store: MemoryStore,
) -> None:
    gateway.queue_create(accepted_result())

    result = await create_use_case.execute(command())

    actions = store.audit_actions(result.request.id)
    assert actions[:2] == [
        AuditAction.REQUEST_RECEIVED.value,
        AuditAction.PROVIDER_SELECTED.value,
    ]
    assert "integration.request.received.v1" in store.outbox_types()


async def test_an_unknown_provider_is_rejected_before_anything_is_written(
    create_use_case: CreateIntegrationRequestUseCase, store: MemoryStore
) -> None:
    with pytest.raises(ProviderNotConfiguredError):
        await create_use_case.execute(command(provider=ProviderSlug("unknown-provider")))

    assert store.requests == {}


async def test_an_operation_the_provider_cannot_perform_is_rejected(
    uow_factory: MemoryUnitOfWorkFactory,
    journal: WorkflowJournal,
    dispatcher: RequestDispatcher,
    clock: FrozenClock,
    ids: SequentialIdentifierGenerator,
    metrics: RecordingMetrics,
) -> None:
    limited = FakeGateway(
        NORTHSTAR,
        descriptor=descriptor_for(
            NORTHSTAR,
            supported_operations=frozenset({OperationType.RESOURCE_PROVISION}),
        ),
    )
    use_case = CreateIntegrationRequestUseCase(
        uow_factory=uow_factory,
        registry=FakeRegistry(limited),
        journal=journal,
        dispatcher=dispatcher,
        clock=clock,
        ids=ids,
        metrics=metrics,
    )

    with pytest.raises(UnsupportedOperationError) as caught:
        await use_case.execute(command(operation_type=OperationType.ACCESS_REVOKE))

    assert caught.value.metadata["supported_operations"] == ["resource_provision"]


async def test_replaying_an_idempotency_key_returns_the_original_request(
    create_use_case: CreateIntegrationRequestUseCase,
    gateway: FakeGateway,
    store: MemoryStore,
) -> None:
    gateway.queue_create(accepted_result("prv-1"))
    first = await create_use_case.execute(command(idempotency_key="idem-key-0001"))

    second = await create_use_case.execute(command(idempotency_key="idem-key-0001"))

    assert second.replayed is True
    assert second.request.id == first.request.id
    assert len(store.requests) == 1
    assert len(gateway.create_calls) == 1


async def test_reusing_an_idempotency_key_with_a_different_body_is_refused(
    create_use_case: CreateIntegrationRequestUseCase, gateway: FakeGateway
) -> None:
    """Returning the first result for a different request would hide a client bug."""
    gateway.queue_create(accepted_result())
    await create_use_case.execute(command(idempotency_key="idem-key-0001", payload={"quantity": 1}))

    with pytest.raises(IdempotencyConflictError):
        await create_use_case.execute(
            command(idempotency_key="idem-key-0001", payload={"quantity": 999})
        )


async def test_a_reordered_body_is_treated_as_the_same_request(
    create_use_case: CreateIntegrationRequestUseCase, gateway: FakeGateway
) -> None:
    """The fingerprint is canonical, so key order cannot cause a false conflict."""
    gateway.queue_create(accepted_result())
    first = await create_use_case.execute(
        command(idempotency_key="idem-key-0002", payload={"a": 1, "b": 2})
    )

    second = await create_use_case.execute(
        command(idempotency_key="idem-key-0002", payload={"b": 2, "a": 1})
    )

    assert second.request.id == first.request.id


async def test_the_loser_of_an_insert_race_replays_the_winners_result(
    create_use_case: CreateIntegrationRequestUseCase,
    gateway: FakeGateway,
    store: MemoryStore,
    uow_factory: MemoryUnitOfWorkFactory,
) -> None:
    """The unique constraint arbitrates the race, not a read-then-write check."""
    gateway.queue_create(accepted_result("prv-1"))
    winner = await create_use_case.execute(command(idempotency_key="idem-key-0003"))

    # Rewind the store so the loser's read finds nothing, and land the winner's
    # row again just before the loser flushes. That is exactly the interleaving
    # PostgreSQL produces when two creations arrive at once.
    committed = dict(store.idempotency)
    store.idempotency.clear()
    uow_factory.before_flush = lambda: store.idempotency.update(committed)

    loser = await create_use_case.execute(command(idempotency_key="idem-key-0003"))

    assert loser.replayed is True
    assert loser.request.id == winner.request.id
    assert len(gateway.create_calls) == 1


async def test_creation_counts_both_new_and_replayed_requests(
    create_use_case: CreateIntegrationRequestUseCase,
    gateway: FakeGateway,
    metrics: RecordingMetrics,
) -> None:
    gateway.queue_create(accepted_result())
    await create_use_case.execute(command(idempotency_key="idem-key-0004"))
    await create_use_case.execute(command(idempotency_key="idem-key-0004"))

    outcomes = [
        labels["outcome"]
        for name, labels, _ in metrics.counters
        if name == "integration_requests_total"
    ]
    assert outcomes == ["created", "replayed"]
