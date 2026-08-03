"""Workflow persistence against real PostgreSQL."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from integration_orchestrator.application.services.workflow_catalog import (
    customer_onboarding_v1,
    parallel_provisioning_v1,
)
from integration_orchestrator.domain.enums import WorkflowStatus, WorkflowStepStatus
from integration_orchestrator.domain.workflow import WorkflowExecution, WorkflowStepExecution
from integration_orchestrator.infrastructure.db.unit_of_work import SqlUnitOfWorkFactory

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

NOW = datetime(2026, 8, 3, 12, 0, 0, tzinfo=UTC)


async def test_workflow_definition_and_execution_round_trip(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    definition = parallel_provisioning_v1(definition_id=uuid4(), now=NOW)
    execution_id = uuid4()
    steps = [
        WorkflowStepExecution(
            id=uuid4(),
            workflow_execution_id=execution_id,
            step_key=template.key,
            provider=template.provider,
            operation_type=template.operation_type,
            depends_on=template.depends_on,
            compensate_operation=template.compensate_operation,
            wait_for_webhook=template.wait_for_webhook,
            status=WorkflowStepStatus.PENDING,
            attempt_count=0,
            max_attempts=template.max_attempts,
            integration_request_id=None,
            compensation_request_id=None,
            input_payload={"resource_name": "integration"},
            output_payload=None,
            error_code=None,
            error_message=None,
            created_at=NOW,
            updated_at=NOW,
        )
        for template in definition.steps
    ]
    execution = WorkflowExecution(
        id=execution_id,
        definition_id=definition.id,
        definition_name=definition.name,
        definition_version=definition.version,
        status=WorkflowStatus.QUEUED,
        correlation_id=str(uuid4()),
        idempotency_key=f"int-wf-{uuid4()}",
        input_payload={"resource_name": "integration"},
        steps=steps,
        created_at=NOW,
        updated_at=NOW,
        owner_subject="integration-tester",
        deadline_at=NOW + timedelta(minutes=5),
        cancel_reason=None,
    )
    execution.promote_ready_steps(now=NOW)

    async with uow_factory() as uow:
        await uow.workflow_definitions.add(definition)
        await uow.workflow_executions.add(execution)
        await uow.commit()

    async with uow_factory() as uow:
        loaded_def = await uow.workflow_definitions.get_by_name_version(
            definition.name, definition.version
        )
        loaded = await uow.workflow_executions.get(execution_id)
    assert loaded_def is not None
    assert loaded is not None
    assert loaded.deadline_at == execution.deadline_at
    assert loaded.owner_subject == "integration-tester"
    ready = [s for s in loaded.steps if s.status is WorkflowStepStatus.READY]
    assert len(ready) == 1
    assert ready[0].step_key == "create_customer"


async def test_workflow_claim_lease_and_deadline_fields(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    definition = customer_onboarding_v1(definition_id=uuid4(), now=NOW)
    execution_id = uuid4()
    step = WorkflowStepExecution(
        id=uuid4(),
        workflow_execution_id=execution_id,
        step_key="create_customer",
        provider="northstar",
        operation_type=definition.steps[0].operation_type,
        depends_on=(),
        compensate_operation=definition.steps[0].compensate_operation,
        wait_for_webhook=False,
        status=WorkflowStepStatus.READY,
        attempt_count=0,
        max_attempts=3,
        integration_request_id=None,
        compensation_request_id=None,
        input_payload={},
        output_payload=None,
        error_code=None,
        error_message=None,
        created_at=NOW,
        updated_at=NOW,
    )
    execution = WorkflowExecution(
        id=execution_id,
        definition_id=definition.id,
        definition_name=definition.name,
        definition_version=1,
        status=WorkflowStatus.QUEUED,
        correlation_id=str(uuid4()),
        idempotency_key=None,
        input_payload={},
        steps=[step],
        created_at=NOW,
        updated_at=NOW,
        deadline_at=NOW - timedelta(seconds=1),
    )

    async with uow_factory() as uow:
        await uow.workflow_definitions.add(definition)
        await uow.workflow_executions.add(execution)
        await uow.commit()

    lease_until = NOW + timedelta(seconds=30)
    async with uow_factory() as uow:
        claimed = await uow.workflow_executions.claim_runnable(
            limit=5, now=NOW, lease_until=lease_until
        )
        await uow.commit()
    assert any(item.id == execution_id for item in claimed)

    async with uow_factory() as uow:
        loaded = await uow.workflow_executions.get(execution_id)
    assert loaded is not None
    assert loaded.claim_lease_until == lease_until
    assert loaded.is_past_deadline(now=NOW)
