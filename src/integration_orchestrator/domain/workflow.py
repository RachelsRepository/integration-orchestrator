"""Multi-step workflow aggregates.

Each forward step materialises one :class:`IntegrationRequest` (the durable
provider side-effect unit). Compensation walks succeeded steps in reverse and
creates compensating requests where the provider supports them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from integration_orchestrator.domain.enums import (
    OperationType,
    WorkflowStatus,
    WorkflowStepStatus,
)
from integration_orchestrator.domain.workflow_state_machine import (
    assert_step_transition,
    assert_workflow_transition,
)


@dataclass(frozen=True, slots=True)
class WorkflowStepDefinition:
    """Immutable step template inside a workflow definition version."""

    key: str
    provider: str
    operation_type: OperationType
    depends_on: tuple[str, ...] = ()
    compensate_operation: OperationType | None = None
    wait_for_webhook: bool = False
    max_attempts: int = 3
    timeout_seconds: int | None = None
    payload_template: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    """Versioned, immutable workflow template once executions reference it."""

    id: UUID
    name: str
    version: int
    steps: tuple[WorkflowStepDefinition, ...]
    created_at: datetime
    immutable: bool = False

    def step(self, key: str) -> WorkflowStepDefinition:
        for step in self.steps:
            if step.key == key:
                return step
        raise KeyError(key)

    def mark_immutable(self) -> WorkflowDefinition:
        return WorkflowDefinition(
            id=self.id,
            name=self.name,
            version=self.version,
            steps=self.steps,
            created_at=self.created_at,
            immutable=True,
        )


@dataclass(slots=True)
class WorkflowStepExecution:
    """One runtime step inside a workflow execution."""

    id: UUID
    workflow_execution_id: UUID
    step_key: str
    provider: str
    operation_type: OperationType
    depends_on: tuple[str, ...]
    compensate_operation: OperationType | None
    wait_for_webhook: bool
    status: WorkflowStepStatus
    attempt_count: int
    max_attempts: int
    integration_request_id: UUID | None
    compensation_request_id: UUID | None
    input_payload: dict[str, Any]
    output_payload: dict[str, Any] | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None
    version: int = 1

    def transition_to(self, new_status: WorkflowStepStatus, *, now: datetime) -> None:
        assert_step_transition(self.status, new_status)
        self.status = new_status
        self.updated_at = now
        if new_status.is_terminal or new_status is WorkflowStepStatus.COMPENSATED:
            self.completed_at = now
        self.version += 1

    def mark_ready(self, *, now: datetime) -> None:
        self.transition_to(WorkflowStepStatus.READY, now=now)

    def mark_running(self, *, now: datetime) -> None:
        self.attempt_count += 1
        self.transition_to(WorkflowStepStatus.RUNNING, now=now)

    def mark_waiting(self, *, now: datetime) -> None:
        self.transition_to(WorkflowStepStatus.WAITING, now=now)

    def mark_succeeded(
        self, *, now: datetime, request_id: UUID, output: dict[str, Any] | None = None
    ) -> None:
        self.integration_request_id = request_id
        self.output_payload = output
        self.transition_to(WorkflowStepStatus.SUCCEEDED, now=now)

    def mark_failed(self, *, now: datetime, code: str | None, message: str | None) -> None:
        self.error_code = code
        self.error_message = message
        self.transition_to(WorkflowStepStatus.FAILED, now=now)

    def begin_compensation(self, *, now: datetime) -> None:
        self.transition_to(WorkflowStepStatus.COMPENSATING, now=now)

    def mark_compensated(self, *, now: datetime, compensation_request_id: UUID | None) -> None:
        self.compensation_request_id = compensation_request_id
        self.transition_to(WorkflowStepStatus.COMPENSATED, now=now)


@dataclass(slots=True)
class WorkflowExecution:
    """A running instance of a workflow definition version."""

    id: UUID
    definition_id: UUID
    definition_name: str
    definition_version: int
    status: WorkflowStatus
    correlation_id: str
    idempotency_key: str | None
    input_payload: dict[str, Any]
    steps: list[WorkflowStepExecution]
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None
    manual_review_reason: str | None = None
    version: int = 1
    owner_subject: str | None = None
    claim_lease_until: datetime | None = None
    deadline_at: datetime | None = None
    cancel_reason: str | None = None
    deadline_processed_at: datetime | None = None

    def transition_to(self, new_status: WorkflowStatus, *, now: datetime) -> None:
        assert_workflow_transition(self.status, new_status)
        self.status = new_status
        self.updated_at = now
        if new_status.is_terminal:
            self.completed_at = now
        self.version += 1

    def step_by_key(self, key: str) -> WorkflowStepExecution:
        for step in self.steps:
            if step.step_key == key:
                return step
        raise KeyError(key)

    def dependencies_satisfied(self, step: WorkflowStepExecution) -> bool:
        for dep in step.depends_on:
            if self.step_by_key(dep).status is not WorkflowStepStatus.SUCCEEDED:
                return False
        return True

    def promote_ready_steps(self, *, now: datetime) -> list[WorkflowStepExecution]:
        promoted: list[WorkflowStepExecution] = []
        for step in self.steps:
            if step.status is WorkflowStepStatus.PENDING and self.dependencies_satisfied(step):
                step.mark_ready(now=now)
                promoted.append(step)
        return promoted

    def succeeded_in_reverse(self) -> list[WorkflowStepExecution]:
        """Return succeeded steps in reverse completion order (saga unwind)."""
        completed = [s for s in self.steps if s.status is WorkflowStepStatus.SUCCEEDED]
        return sorted(
            completed,
            key=lambda s: s.completed_at or s.updated_at,
            reverse=True,
        )

    def all_forward_succeeded(self) -> bool:
        return all(s.status is WorkflowStepStatus.SUCCEEDED for s in self.steps)

    def any_waiting(self) -> bool:
        return any(s.status is WorkflowStepStatus.WAITING for s in self.steps)

    def is_past_deadline(self, *, now: datetime) -> bool:
        return self.deadline_at is not None and now >= self.deadline_at

    def has_succeeded_steps(self) -> bool:
        return any(s.status is WorkflowStepStatus.SUCCEEDED for s in self.steps)
