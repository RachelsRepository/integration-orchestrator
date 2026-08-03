"""Workflow repository ports."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

from integration_orchestrator.domain.workflow import WorkflowDefinition, WorkflowExecution


@runtime_checkable
class WorkflowDefinitionRepository(Protocol):
    async def add(self, definition: WorkflowDefinition) -> None: ...

    async def get(self, definition_id: UUID) -> WorkflowDefinition | None: ...

    async def get_by_name_version(self, name: str, version: int) -> WorkflowDefinition | None: ...

    async def mark_immutable(self, definition: WorkflowDefinition) -> None: ...


@runtime_checkable
class WorkflowExecutionRepository(Protocol):
    async def add(self, execution: WorkflowExecution) -> None: ...

    async def get(self, execution_id: UUID) -> WorkflowExecution | None: ...

    async def get_for_update(self, execution_id: UUID) -> WorkflowExecution | None: ...

    async def get_by_idempotency_key(self, key: str) -> WorkflowExecution | None: ...

    async def update(self, execution: WorkflowExecution) -> None: ...

    async def claim_runnable(
        self, *, limit: int, now: datetime, lease_until: datetime
    ) -> Sequence[WorkflowExecution]:
        """Claim executions under a lease (SKIP LOCKED)."""
        ...

    async def find_by_request_id(self, request_id: UUID) -> WorkflowExecution | None:
        """Find the execution that owns a linked integration request."""
        ...
