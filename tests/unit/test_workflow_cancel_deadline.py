"""Unit tests for workflow cancel and hard deadline enforcement."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.application.services.workflow_catalog import (
    customer_onboarding_v1,
    parallel_provisioning_v1,
)
from integration_orchestrator.application.services.workflow_orchestrator import (
    WorkflowOrchestrator,
)
from integration_orchestrator.domain.enums import (
    ActorType,
    OperationType,
    WorkflowStatus,
)
from integration_orchestrator.domain.errors import ConflictError
from integration_orchestrator.domain.workflow import WorkflowDefinition, WorkflowStepDefinition
from tests.support.memory_uow import MemoryUnitOfWorkFactory
from tests.unit.test_workflow_orchestrator import _Clock, _Dispatcher, _Ids, _Journal

pytestmark = [pytest.mark.unit, pytest.mark.asyncio]


def _orchestrator(factory: MemoryUnitOfWorkFactory) -> WorkflowOrchestrator:
    return WorkflowOrchestrator(
        uow_factory=factory,
        journal=_Journal(),  # type: ignore[arg-type]
        dispatcher=_Dispatcher(factory),  # type: ignore[arg-type]
        clock=_Clock(),  # type: ignore[arg-type]
        ids=_Ids(),  # type: ignore[arg-type]
    )


async def test_cancel_queued_workflow_is_idempotent() -> None:
    factory = MemoryUnitOfWorkFactory()
    definition = customer_onboarding_v1(definition_id=uuid4(), now=datetime.now(tz=UTC))
    async with factory() as uow:
        await uow.workflow_definitions.add(definition)
        await uow.commit()
    orch = _orchestrator(factory)
    started = await orch.start_execution(
        definition,
        input_payload={"resource_name": "x"},
        correlation_id=str(uuid4()),
        idempotency_key=None,
        actor=Actor(type=ActorType.SYSTEM, id="test"),
    )
    # Force back to queued-like by cancelling before advance completes heavily.
    cancelled = await orch.cancel(
        started.id, actor=Actor(type=ActorType.API_CLIENT, id="test"), reason="stop"
    )
    assert cancelled.status in {WorkflowStatus.CANCELLED, WorkflowStatus.COMPENSATED}
    again = await orch.cancel(started.id, actor=Actor(type=ActorType.API_CLIENT, id="test"))
    assert again.status is cancelled.status


async def test_cancel_after_success_raises_conflict() -> None:
    factory = MemoryUnitOfWorkFactory()
    definition = WorkflowDefinition(
        id=uuid4(),
        name="one",
        version=1,
        created_at=datetime.now(tz=UTC),
        steps=(
            WorkflowStepDefinition(
                key="a",
                provider="northstar",
                operation_type=OperationType.RESOURCE_PROVISION,
            ),
        ),
    )
    async with factory() as uow:
        await uow.workflow_definitions.add(definition)
        await uow.commit()
    orch = _orchestrator(factory)
    started = await orch.start_execution(
        definition,
        input_payload={},
        correlation_id=str(uuid4()),
        idempotency_key=None,
        actor=Actor(type=ActorType.SYSTEM, id="test"),
    )
    finished = await orch.advance(started.id)
    assert finished.status is WorkflowStatus.SUCCEEDED
    with pytest.raises(ConflictError):
        await orch.cancel(finished.id, actor=Actor(type=ActorType.API_CLIENT, id="test"))


async def test_deadline_cancels_before_steps_when_already_expired() -> None:
    factory = MemoryUnitOfWorkFactory()
    definition = parallel_provisioning_v1(definition_id=uuid4(), now=datetime.now(tz=UTC))
    async with factory() as uow:
        await uow.workflow_definitions.add(definition)
        await uow.commit()
    orch = _orchestrator(factory)
    past = datetime.now(tz=UTC) - timedelta(seconds=5)
    started = await orch.start_execution(
        definition,
        input_payload={"resource_name": "late"},
        correlation_id=str(uuid4()),
        idempotency_key=None,
        actor=Actor(type=ActorType.SYSTEM, id="test"),
        deadline_at=past,
    )
    advanced = await orch.advance(started.id)
    assert advanced.status in {
        WorkflowStatus.CANCELLED,
        WorkflowStatus.COMPENSATED,
        WorkflowStatus.TIMED_OUT,
    }
    assert advanced.deadline_processed_at is not None


async def test_duplicate_deadline_processing_is_idempotent() -> None:
    factory = MemoryUnitOfWorkFactory()
    definition = customer_onboarding_v1(definition_id=uuid4(), now=datetime.now(tz=UTC))
    async with factory() as uow:
        await uow.workflow_definitions.add(definition)
        await uow.commit()
    orch = _orchestrator(factory)
    past = datetime.now(tz=UTC) - timedelta(seconds=1)
    started = await orch.start_execution(
        definition,
        input_payload={"resource_name": "d"},
        correlation_id=str(uuid4()),
        idempotency_key=None,
        actor=Actor(type=ActorType.SYSTEM, id="test"),
        deadline_at=past,
    )
    first = await orch.enforce_deadline(started.id)
    second = await orch.enforce_deadline(started.id)
    assert first.deadline_processed_at == second.deadline_processed_at
    assert first.status == second.status
