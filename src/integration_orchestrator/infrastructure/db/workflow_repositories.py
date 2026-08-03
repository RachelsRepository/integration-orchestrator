"""SQLAlchemy repositories for workflow definitions and executions."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from integration_orchestrator.domain.enums import WorkflowStatus
from integration_orchestrator.domain.errors import ConcurrencyConflictError, ConflictError
from integration_orchestrator.domain.workflow import WorkflowDefinition, WorkflowExecution
from integration_orchestrator.infrastructure.db.mappers import (
    workflow_definition_to_domain,
    workflow_definition_to_row,
    workflow_execution_to_domain,
    workflow_execution_to_row,
    workflow_step_to_row,
)
from integration_orchestrator.infrastructure.db.models import (
    WorkflowDefinitionModel,
    WorkflowExecutionModel,
    WorkflowStepExecutionModel,
)

_RUNNABLE = (
    WorkflowStatus.QUEUED.value,
    WorkflowStatus.RUNNING.value,
    WorkflowStatus.WAITING.value,
    WorkflowStatus.COMPENSATING.value,
    WorkflowStatus.RETRY_SCHEDULED.value,
    WorkflowStatus.TIMED_OUT.value,
)


class SqlWorkflowDefinitionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, definition: WorkflowDefinition) -> None:
        self._session.add(workflow_definition_to_row(definition))

    async def get(self, definition_id: UUID) -> WorkflowDefinition | None:
        row = await self._session.get(WorkflowDefinitionModel, definition_id)
        return workflow_definition_to_domain(row) if row else None

    async def get_by_name_version(self, name: str, version: int) -> WorkflowDefinition | None:
        result = await self._session.execute(
            select(WorkflowDefinitionModel).where(
                WorkflowDefinitionModel.name == name,
                WorkflowDefinitionModel.version == version,
            )
        )
        row = result.scalar_one_or_none()
        return workflow_definition_to_domain(row) if row else None

    async def mark_immutable(self, definition: WorkflowDefinition) -> None:
        row = await self._session.get(WorkflowDefinitionModel, definition.id)
        if row is None:
            raise ConflictError("workflow definition not found")
        row.immutable = True


class SqlWorkflowExecutionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, execution: WorkflowExecution) -> None:
        self._session.add(workflow_execution_to_row(execution))
        for step in execution.steps:
            self._session.add(workflow_step_to_row(step))

    async def get(self, execution_id: UUID) -> WorkflowExecution | None:
        return await self._load(execution_id, for_update=False)

    async def get_for_update(self, execution_id: UUID) -> WorkflowExecution | None:
        return await self._load(execution_id, for_update=True)

    async def get_by_idempotency_key(self, key: str) -> WorkflowExecution | None:
        result = await self._session.execute(
            select(WorkflowExecutionModel).where(WorkflowExecutionModel.idempotency_key == key)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return await self._load(row.id, for_update=False)

    async def update(self, execution: WorkflowExecution) -> None:
        row = await self._session.get(WorkflowExecutionModel, execution.id, with_for_update=True)
        if row is None:
            raise ConflictError("workflow execution not found")
        if row.version > execution.version:
            raise ConcurrencyConflictError(str(execution.id))
        row.status = execution.status.value
        row.manual_review_reason = execution.manual_review_reason
        row.owner_subject = execution.owner_subject
        row.claim_lease_until = execution.claim_lease_until
        row.deadline_at = execution.deadline_at
        row.cancel_reason = execution.cancel_reason
        row.deadline_processed_at = execution.deadline_processed_at
        row.updated_at = execution.updated_at
        row.completed_at = execution.completed_at
        row.version = max(execution.version, row.version + 1)
        execution.version = row.version
        for step in execution.steps:
            step_row = await self._session.get(
                WorkflowStepExecutionModel, step.id, with_for_update=True
            )
            if step_row is None:
                self._session.add(workflow_step_to_row(step))
                continue
            if step_row.version > step.version:
                raise ConcurrencyConflictError(str(step.id))
            step_row.status = step.status.value
            step_row.attempt_count = step.attempt_count
            step_row.integration_request_id = step.integration_request_id
            step_row.compensation_request_id = step.compensation_request_id
            step_row.output_payload = step.output_payload
            step_row.error_code = step.error_code
            step_row.error_message = step.error_message
            step_row.updated_at = step.updated_at
            step_row.completed_at = step.completed_at
            step_row.version = max(step.version, step_row.version + 1)
            step.version = step_row.version

    async def claim_runnable(
        self, *, limit: int, now: datetime, lease_until: datetime
    ) -> Sequence[WorkflowExecution]:
        """Claim runnable executions under a time-bounded lease.

        ``FOR UPDATE SKIP LOCKED`` prevents two workers racing inside one
        transaction. Writing ``claim_lease_until`` before commit prevents a
        second replica from re-claiming the same row after the claim
        transaction ends but before the first worker finishes advancing it.
        """
        result = await self._session.execute(
            select(WorkflowExecutionModel.id)
            .where(
                WorkflowExecutionModel.status.in_(_RUNNABLE),
                (
                    WorkflowExecutionModel.claim_lease_until.is_(None)
                    | (WorkflowExecutionModel.claim_lease_until <= now)
                ),
            )
            .order_by(WorkflowExecutionModel.updated_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        ids = list(result.scalars().all())
        executions: list[WorkflowExecution] = []
        for execution_id in ids:
            loaded = await self._load(execution_id, for_update=True)
            if loaded is None:
                continue
            row = await self._session.get(WorkflowExecutionModel, execution_id)
            if row is not None:
                row.claim_lease_until = lease_until
                row.updated_at = now
                loaded.claim_lease_until = lease_until
                loaded.updated_at = now
            executions.append(loaded)
        return executions

    async def find_by_request_id(self, request_id: UUID) -> WorkflowExecution | None:
        result = await self._session.execute(
            select(WorkflowStepExecutionModel.workflow_execution_id).where(
                (WorkflowStepExecutionModel.integration_request_id == request_id)
                | (WorkflowStepExecutionModel.compensation_request_id == request_id)
            )
        )
        execution_id = result.scalar_one_or_none()
        if execution_id is None:
            return None
        return await self._load(execution_id, for_update=False)

    async def _load(self, execution_id: UUID, *, for_update: bool) -> WorkflowExecution | None:
        stmt = select(WorkflowExecutionModel).where(WorkflowExecutionModel.id == execution_id)
        if for_update:
            stmt = stmt.with_for_update()
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None
        steps_stmt = select(WorkflowStepExecutionModel).where(
            WorkflowStepExecutionModel.workflow_execution_id == execution_id
        )
        if for_update:
            steps_stmt = steps_stmt.with_for_update()
        steps_result = await self._session.execute(steps_stmt)
        steps = list(steps_result.scalars().all())
        return workflow_execution_to_domain(row, steps)
