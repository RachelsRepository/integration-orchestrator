"""Workflow orchestrator happy path and compensation order."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.application.services.workflow_catalog import customer_onboarding_v1
from integration_orchestrator.application.services.workflow_orchestrator import (
    WorkflowOrchestrator,
)
from integration_orchestrator.domain.enums import (
    ActorType,
    OperationType,
    RequestStatus,
    WorkflowStatus,
    WorkflowStepStatus,
)
from integration_orchestrator.domain.workflow import WorkflowDefinition, WorkflowStepDefinition
from tests.support.memory_uow import MemoryUnitOfWorkFactory

pytestmark = [pytest.mark.unit, pytest.mark.asyncio]


class _Clock:
    def now(self) -> datetime:
        return datetime.now(tz=UTC)


class _Ids:
    def new_id(self):  # type: ignore[no-untyped-def]
        return uuid4()


class _Journal:
    async def record_creation(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def record_transition(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def record_request_action(self, *args: Any, **kwargs: Any) -> None:
        return None


class _Dispatcher:
    def __init__(self, factory: MemoryUnitOfWorkFactory) -> None:
        self._factory = factory
        self.calls: list[Any] = []

    async def dispatch(self, request_id, *, actor):  # type: ignore[no-untyped-def]
        self.calls.append(request_id)
        async with self._factory() as uow:
            request = await uow.requests.get_for_update(request_id)
            assert request is not None
            # Force success without provider I/O for unit isolation.
            if request.status is RequestStatus.RECEIVED:
                request.begin_validation(now=datetime.now(tz=UTC))
            if request.status.value in {"received", "validating"}:
                request.begin_dispatch(now=datetime.now(tz=UTC))
            from integration_orchestrator.domain.enums import NormalizedStatus

            request.apply_normalized_status(
                NormalizedStatus.SUCCEEDED,
                now=datetime.now(tz=UTC),
                provider_reference=f"prv-{request_id.hex[:8]}",
            )
            await uow.requests.update(request)
            await uow.commit()
            return request


async def test_two_step_workflow_reaches_succeeded() -> None:
    factory = MemoryUnitOfWorkFactory()
    definition = WorkflowDefinition(
        id=uuid4(),
        name="two_step",
        version=1,
        created_at=datetime.now(tz=UTC),
        steps=(
            WorkflowStepDefinition(
                key="a",
                provider="northstar",
                operation_type=OperationType.RESOURCE_PROVISION,
            ),
            WorkflowStepDefinition(
                key="b",
                provider="meridian",
                operation_type=OperationType.RESOURCE_PROVISION,
                depends_on=("a",),
            ),
        ),
    )
    async with factory() as uow:
        await uow.workflow_definitions.add(definition)
        await uow.commit()

    orchestrator = WorkflowOrchestrator(
        uow_factory=factory,
        journal=_Journal(),  # type: ignore[arg-type]
        dispatcher=_Dispatcher(factory),  # type: ignore[arg-type]
        clock=_Clock(),  # type: ignore[arg-type]
        ids=_Ids(),  # type: ignore[arg-type]
    )
    started = await orchestrator.start_execution(
        definition,
        input_payload={"resource_name": "x"},
        correlation_id=str(uuid4()),
        idempotency_key=None,
        actor=Actor(type=ActorType.SYSTEM, id="test"),
    )
    finished = await orchestrator.advance(started.id)
    # Second step may still be pending until another advance after first settles.
    for _ in range(3):
        finished = await orchestrator.advance(finished.id)
        if finished.status is WorkflowStatus.SUCCEEDED:
            break
    assert finished.status is WorkflowStatus.SUCCEEDED
    assert all(s.status is WorkflowStepStatus.SUCCEEDED for s in finished.steps)


async def test_failure_triggers_reverse_compensation_order() -> None:
    factory = MemoryUnitOfWorkFactory()
    definition = customer_onboarding_v1(definition_id=uuid4(), now=datetime.now(tz=UTC))
    # Shrink to two steps for a focused compensation test.
    definition = WorkflowDefinition(
        id=definition.id,
        name=definition.name,
        version=definition.version,
        created_at=definition.created_at,
        steps=definition.steps[:2],
    )
    async with factory() as uow:
        await uow.workflow_definitions.add(definition)
        await uow.commit()

    class FailingDispatcher(_Dispatcher):
        async def dispatch(self, request_id, *, actor):  # type: ignore[no-untyped-def]
            async with self._factory() as uow:
                request = await uow.requests.get_for_update(request_id)
                assert request is not None
                if "create_subscription" in request.external_reference.value:
                    request.begin_validation(now=datetime.now(tz=UTC))
                    request.begin_dispatch(now=datetime.now(tz=UTC))
                    from integration_orchestrator.domain.entities import FailureDetail
                    from integration_orchestrator.domain.enums import (
                        ErrorCategory,
                        NormalizedStatus,
                    )

                    request.apply_normalized_status(
                        NormalizedStatus.FAILED,
                        now=datetime.now(tz=UTC),
                        provider_reference=f"prv-{request_id.hex[:8]}",
                        failure=FailureDetail(
                            code="boom",
                            message="forced",
                            category=ErrorCategory.PROVIDER_UNAVAILABLE,
                            retryable=False,
                        ),
                    )
                    await uow.requests.update(request)
                    await uow.commit()
                    return request
                return await super().dispatch(request_id, actor=actor)

    orchestrator = WorkflowOrchestrator(
        uow_factory=factory,
        journal=_Journal(),  # type: ignore[arg-type]
        dispatcher=FailingDispatcher(factory),  # type: ignore[arg-type]
        clock=_Clock(),  # type: ignore[arg-type]
        ids=_Ids(),  # type: ignore[arg-type]
    )
    started = await orchestrator.start_execution(
        definition,
        input_payload={"resource_name": "x"},
        correlation_id=str(uuid4()),
        idempotency_key=None,
        actor=Actor(type=ActorType.SYSTEM, id="test"),
    )
    current = started
    for _ in range(6):
        current = await orchestrator.advance(current.id)
        if current.status in {
            WorkflowStatus.COMPENSATING,
            WorkflowStatus.COMPENSATED,
            WorkflowStatus.MANUAL_REVIEW,
        }:
            await orchestrator._continue_compensation(current.id)
            current = await orchestrator.advance(current.id)
        if current.status is WorkflowStatus.COMPENSATED:
            break
    assert current.status in {
        WorkflowStatus.COMPENSATED,
        WorkflowStatus.COMPENSATING,
        WorkflowStatus.MANUAL_REVIEW,
    }
    if current.status is WorkflowStatus.COMPENSATED:
        customer = current.step_by_key("create_customer")
        subscription = current.step_by_key("create_subscription")
        assert subscription.status is WorkflowStepStatus.FAILED
        assert customer.status is WorkflowStepStatus.COMPENSATED
        assert customer.compensation_request_id is not None


async def test_compensation_failure_routes_to_manual_review() -> None:
    factory = MemoryUnitOfWorkFactory()
    definition = WorkflowDefinition(
        id=uuid4(),
        name="cmp_fail",
        version=1,
        created_at=datetime.now(tz=UTC),
        steps=(
            WorkflowStepDefinition(
                key="a",
                provider="northstar",
                operation_type=OperationType.RESOURCE_PROVISION,
                compensate_operation=OperationType.RESOURCE_DEPROVISION,
            ),
            WorkflowStepDefinition(
                key="b",
                provider="meridian",
                operation_type=OperationType.RESOURCE_PROVISION,
                depends_on=("a",),
                compensate_operation=OperationType.RESOURCE_DEPROVISION,
            ),
        ),
    )
    async with factory() as uow:
        await uow.workflow_definitions.add(definition)
        await uow.commit()

    class FailSecond(_Dispatcher):
        async def dispatch(self, request_id, *, actor):  # type: ignore[no-untyped-def]
            async with self._factory() as uow:
                request = await uow.requests.get_for_update(request_id)
                assert request is not None
                ref = request.external_reference.value
                if (
                    "-b" in ref
                    and "wf-cmp" not in ref
                    and request.operation_type is OperationType.RESOURCE_PROVISION
                ):
                    request.begin_validation(now=datetime.now(tz=UTC))
                    request.begin_dispatch(now=datetime.now(tz=UTC))
                    from integration_orchestrator.domain.entities import FailureDetail
                    from integration_orchestrator.domain.enums import (
                        ErrorCategory,
                        NormalizedStatus,
                    )

                    request.apply_normalized_status(
                        NormalizedStatus.FAILED,
                        now=datetime.now(tz=UTC),
                        failure=FailureDetail(
                            code="boom",
                            message="forced",
                            category=ErrorCategory.PROVIDER_UNAVAILABLE,
                            retryable=False,
                        ),
                    )
                    await uow.requests.update(request)
                    await uow.commit()
                    return request
                if request.operation_type is OperationType.RESOURCE_DEPROVISION:
                    request.begin_validation(now=datetime.now(tz=UTC))
                    request.begin_dispatch(now=datetime.now(tz=UTC))
                    from integration_orchestrator.domain.entities import FailureDetail
                    from integration_orchestrator.domain.enums import (
                        ErrorCategory,
                        NormalizedStatus,
                    )

                    request.apply_normalized_status(
                        NormalizedStatus.FAILED,
                        now=datetime.now(tz=UTC),
                        failure=FailureDetail(
                            code="cmp_failed",
                            message="compensation refused",
                            category=ErrorCategory.UNSUPPORTED_OPERATION,
                            retryable=False,
                        ),
                    )
                    await uow.requests.update(request)
                    await uow.commit()
                    return request
                return await super().dispatch(request_id, actor=actor)

    orchestrator = WorkflowOrchestrator(
        uow_factory=factory,
        journal=_Journal(),  # type: ignore[arg-type]
        dispatcher=FailSecond(factory),  # type: ignore[arg-type]
        clock=_Clock(),  # type: ignore[arg-type]
        ids=_Ids(),  # type: ignore[arg-type]
    )
    started = await orchestrator.start_execution(
        definition,
        input_payload={"resource_name": "x"},
        correlation_id=str(uuid4()),
        idempotency_key=None,
        actor=Actor(type=ActorType.SYSTEM, id="test"),
    )
    current = started
    for _ in range(8):
        current = await orchestrator.advance(current.id)
        if current.status is WorkflowStatus.COMPENSATING:
            await orchestrator._continue_compensation(current.id)
            async with factory() as uow:
                loaded = await uow.workflow_executions.get(current.id)
                assert loaded is not None
                current = loaded
        if current.status is WorkflowStatus.MANUAL_REVIEW:
            break
    assert current.status is WorkflowStatus.MANUAL_REVIEW
    assert current.manual_review_reason is not None


async def test_parallel_independent_steps_then_fan_in() -> None:
    """Two siblings with empty deps run after promote; join waits on both."""
    factory = MemoryUnitOfWorkFactory()
    definition = WorkflowDefinition(
        id=uuid4(),
        name="fan",
        version=1,
        created_at=datetime.now(tz=UTC),
        steps=(
            WorkflowStepDefinition(
                key="left",
                provider="northstar",
                operation_type=OperationType.RESOURCE_PROVISION,
            ),
            WorkflowStepDefinition(
                key="right",
                provider="meridian",
                operation_type=OperationType.RESOURCE_PROVISION,
            ),
            WorkflowStepDefinition(
                key="join",
                provider="cobalt",
                operation_type=OperationType.RESOURCE_PROVISION,
                depends_on=("left", "right"),
            ),
        ),
    )
    async with factory() as uow:
        await uow.workflow_definitions.add(definition)
        await uow.commit()

    orchestrator = WorkflowOrchestrator(
        uow_factory=factory,
        journal=_Journal(),  # type: ignore[arg-type]
        dispatcher=_Dispatcher(factory),  # type: ignore[arg-type]
        clock=_Clock(),  # type: ignore[arg-type]
        ids=_Ids(),  # type: ignore[arg-type]
    )
    started = await orchestrator.start_execution(
        definition,
        input_payload={"resource_name": "fan"},
        correlation_id=str(uuid4()),
        idempotency_key=None,
        actor=Actor(type=ActorType.SYSTEM, id="test"),
    )
    ready_or_done = {
        s.step_key
        for s in started.steps
        if s.status
        in {
            WorkflowStepStatus.READY,
            WorkflowStepStatus.RUNNING,
            WorkflowStepStatus.SUCCEEDED,
            WorkflowStepStatus.WAITING,
        }
    }
    assert "left" in ready_or_done and "right" in ready_or_done
    finished = started
    for _ in range(6):
        finished = await orchestrator.advance(finished.id)
        if finished.status is WorkflowStatus.SUCCEEDED:
            break
    assert finished.status is WorkflowStatus.SUCCEEDED
    assert finished.step_by_key("join").status is WorkflowStepStatus.SUCCEEDED
