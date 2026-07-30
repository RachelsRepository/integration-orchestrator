"""Operator-initiated retry and cancellation."""

from __future__ import annotations

import pytest

from integration_orchestrator.application.dto.commands import (
    Actor,
    CancelRequestCommand,
    RetryRequestCommand,
)
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.application.use_cases.cancel_request import (
    CancelIntegrationRequestUseCase,
)
from integration_orchestrator.application.use_cases.retry_request import (
    RetryIntegrationRequestUseCase,
)
from integration_orchestrator.domain.contracts import ProviderOperationResult
from integration_orchestrator.domain.enums import (
    AuditAction,
    NormalizedStatus,
    RequestStatus,
)
from integration_orchestrator.domain.errors import (
    ConflictError,
    NotFoundError,
    ProviderUnavailableError,
    UnsupportedOperationError,
)
from integration_orchestrator.domain.value_objects import CorrelationId
from integration_orchestrator.infrastructure.system import FrozenClock
from tests.support.builders import REFERENCE_TIME, make_request
from tests.support.doubles import FakeGateway, FakeRegistry, descriptor_for
from tests.support.memory_uow import MemoryStore, MemoryUnitOfWorkFactory

pytestmark = pytest.mark.unit

ACTOR = Actor.system()


def retry_command(request_id) -> RetryRequestCommand:  # type: ignore[no-untyped-def]
    return RetryRequestCommand(
        request_id=request_id,
        correlation_id=CorrelationId("corr-1"),
        actor=ACTOR,
        reason="operator investigation complete",
    )


def cancel_command(request_id, *, reason: str = "no longer needed") -> CancelRequestCommand:  # type: ignore[no-untyped-def]
    return CancelRequestCommand(
        request_id=request_id,
        correlation_id=CorrelationId("corr-1"),
        actor=ACTOR,
        reason=reason,
    )


# -- retry ------------------------------------------------------------------


async def test_a_manual_retry_queues_the_request_rather_than_calling_the_provider(
    retry_use_case: RetryIntegrationRequestUseCase,
    store: MemoryStore,
    gateway: FakeGateway,
) -> None:
    """Manual retries must not bypass the bulkhead and circuit breaker."""
    request = make_request(status=RequestStatus.FAILED, attempt_count=3)
    store.requests[request.id] = request

    retried = await retry_use_case.execute(retry_command(request.id))

    assert retried.status is RequestStatus.RETRY_SCHEDULED
    assert retried.next_retry_at == REFERENCE_TIME
    assert gateway.create_calls == []


async def test_a_manual_retry_is_audited_and_published(
    retry_use_case: RetryIntegrationRequestUseCase, store: MemoryStore
) -> None:
    request = make_request(status=RequestStatus.MANUAL_REVIEW)
    store.requests[request.id] = request

    await retry_use_case.execute(retry_command(request.id))

    assert AuditAction.RETRY_REQUESTED.value in store.audit_actions(request.id)
    assert any(
        name.startswith("integration.request.retry_scheduled") for name in store.outbox_types()
    )


@pytest.mark.parametrize(
    "status",
    [
        RequestStatus.RECEIVED,
        RequestStatus.PENDING,
        RequestStatus.DISPATCHING,
        RequestStatus.SUCCEEDED,
        RequestStatus.CANCELLED,
    ],
)
async def test_a_request_that_is_not_finished_failing_cannot_be_retried(
    retry_use_case: RetryIntegrationRequestUseCase,
    store: MemoryStore,
    status: RequestStatus,
) -> None:
    request = make_request(status=status)
    store.requests[request.id] = request

    with pytest.raises(ConflictError):
        await retry_use_case.execute(retry_command(request.id))


async def test_retrying_an_unknown_request_is_a_not_found_error(
    retry_use_case: RetryIntegrationRequestUseCase,
) -> None:
    with pytest.raises(NotFoundError):
        await retry_use_case.execute(retry_command(make_request().id))


# -- cancellation -----------------------------------------------------------


async def test_a_request_the_provider_never_saw_is_cancelled_locally(
    cancel_use_case: CancelIntegrationRequestUseCase,
    store: MemoryStore,
    gateway: FakeGateway,
) -> None:
    request = make_request(status=RequestStatus.RECEIVED)
    store.requests[request.id] = request

    cancelled = await cancel_use_case.execute(cancel_command(request.id))

    assert cancelled.status is RequestStatus.CANCELLED
    assert gateway.cancel_calls == []
    assert AuditAction.REQUEST_CANCELLED.value in store.audit_actions(request.id)


async def test_an_accepted_operation_is_cancelled_at_the_provider(
    cancel_use_case: CancelIntegrationRequestUseCase,
    store: MemoryStore,
    gateway: FakeGateway,
) -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.queue_cancel(
        ProviderOperationResult.success(
            normalized_status=NormalizedStatus.CANCELLED, provider_reference="prv-1"
        )
    )

    cancelled = await cancel_use_case.execute(cancel_command(request.id))

    assert cancelled.status is RequestStatus.CANCELLED
    assert [call.provider_reference for call in gateway.cancel_calls] == ["prv-1"]


async def test_a_provider_that_cannot_cancel_is_refused_rather_than_faked(
    uow_factory: MemoryUnitOfWorkFactory,
    journal: WorkflowJournal,
    clock: FrozenClock,
    store: MemoryStore,
) -> None:
    """Marking it cancelled while the provider fulfils it would make our record a lie."""
    stubborn = FakeGateway(
        descriptor=descriptor_for(FakeGateway().slug, supports_cancellation=False)
    )
    use_case = CancelIntegrationRequestUseCase(
        uow_factory=uow_factory,
        registry=FakeRegistry(stubborn),
        journal=journal,
        clock=clock,
    )
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request

    with pytest.raises(UnsupportedOperationError):
        await use_case.execute(cancel_command(request.id))

    assert store.requests[request.id].status is RequestStatus.PENDING


async def test_a_refused_cancellation_leaves_the_request_running_and_records_why(
    cancel_use_case: CancelIntegrationRequestUseCase,
    store: MemoryStore,
    gateway: FakeGateway,
) -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.queue_cancel(ProviderUnavailableError("cannot cancel now", provider="northstar"))

    with pytest.raises(ConflictError):
        await cancel_use_case.execute(cancel_command(request.id))

    assert store.requests[request.id].status is RequestStatus.PENDING
    assert AuditAction.CANCELLATION_REJECTED.value in store.audit_actions(request.id)


async def test_a_request_that_completes_mid_cancellation_is_left_settled(
    cancel_use_case: CancelIntegrationRequestUseCase,
    store: MemoryStore,
    gateway: FakeGateway,
) -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request

    settled = make_request(
        request_id=request.id, status=RequestStatus.SUCCEEDED, provider_reference="prv-1"
    )

    async def _settle_then_refuse(*_: object, **__: object) -> ProviderOperationResult:
        store.requests[request.id] = settled
        return ProviderOperationResult.success(normalized_status=NormalizedStatus.CANCELLED)

    gateway.cancel_operation = _settle_then_refuse  # type: ignore[method-assign]

    result = await cancel_use_case.execute(cancel_command(request.id))

    assert result.status is RequestStatus.SUCCEEDED
    assert AuditAction.CANCELLATION_REJECTED.value in store.audit_actions(request.id)


@pytest.mark.parametrize("status", [RequestStatus.SUCCEEDED, RequestStatus.CANCELLED])
async def test_a_terminal_request_cannot_be_cancelled(
    cancel_use_case: CancelIntegrationRequestUseCase,
    store: MemoryStore,
    status: RequestStatus,
) -> None:
    request = make_request(status=status)
    store.requests[request.id] = request

    with pytest.raises(ConflictError):
        await cancel_use_case.execute(cancel_command(request.id))


async def test_cancelling_an_unknown_request_is_a_not_found_error(
    cancel_use_case: CancelIntegrationRequestUseCase,
) -> None:
    with pytest.raises(NotFoundError):
        await cancel_use_case.execute(cancel_command(make_request().id))
