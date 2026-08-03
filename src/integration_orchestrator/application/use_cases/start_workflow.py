"""Start, cancel, and inspect multi-step workflow executions."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.application.ports.system import Clock, IdentifierGenerator
from integration_orchestrator.application.ports.unit_of_work import UnitOfWorkFactory
from integration_orchestrator.application.services.workflow_catalog import (
    CUSTOMER_ONBOARDING,
    PARALLEL_PROVISIONING,
    customer_onboarding_v1,
    parallel_provisioning_v1,
)
from integration_orchestrator.application.services.workflow_orchestrator import (
    WorkflowOrchestrator,
)
from integration_orchestrator.domain.errors import NotFoundError
from integration_orchestrator.domain.workflow import WorkflowDefinition, WorkflowExecution


class RegisterWorkflowDefinitionUseCase:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        clock: Clock,
        ids: IdentifierGenerator,
    ) -> None:
        self._uow_factory = uow_factory
        self._clock = clock
        self._ids = ids

    async def ensure_customer_onboarding(self) -> WorkflowDefinition:
        async with self._uow_factory() as uow:
            existing = await uow.workflow_definitions.get_by_name_version(CUSTOMER_ONBOARDING, 1)
            if existing is not None:
                return existing
            definition = customer_onboarding_v1(
                definition_id=self._ids.new_id(), now=self._clock.now()
            )
            await uow.workflow_definitions.add(definition)
            await uow.commit()
            return definition

    async def ensure_parallel_provisioning(self) -> WorkflowDefinition:
        async with self._uow_factory() as uow:
            existing = await uow.workflow_definitions.get_by_name_version(PARALLEL_PROVISIONING, 1)
            if existing is not None:
                return existing
            definition = parallel_provisioning_v1(
                definition_id=self._ids.new_id(), now=self._clock.now()
            )
            await uow.workflow_definitions.add(definition)
            await uow.commit()
            return definition


class StartWorkflowUseCase:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        orchestrator: WorkflowOrchestrator,
        register: RegisterWorkflowDefinitionUseCase,
        clock: Clock,
    ) -> None:
        self._uow_factory = uow_factory
        self._orchestrator = orchestrator
        self._register = register
        self._clock = clock

    async def execute(
        self,
        *,
        definition_name: str,
        definition_version: int,
        input_payload: dict[str, Any],
        correlation_id: str,
        idempotency_key: str | None,
        actor: Actor,
        deadline_seconds: int | None = None,
    ) -> WorkflowExecution:
        if definition_name == CUSTOMER_ONBOARDING and definition_version == 1:
            definition = await self._register.ensure_customer_onboarding()
        elif definition_name == PARALLEL_PROVISIONING and definition_version == 1:
            definition = await self._register.ensure_parallel_provisioning()
        else:
            async with self._uow_factory() as uow:
                loaded = await uow.workflow_definitions.get_by_name_version(
                    definition_name, definition_version
                )
            if loaded is None:
                raise NotFoundError("workflow definition not found")
            definition = loaded
        deadline_at = None
        if deadline_seconds is not None:
            deadline_at = self._clock.now() + timedelta(seconds=deadline_seconds)
        execution = await self._orchestrator.start_execution(
            definition,
            input_payload=input_payload,
            correlation_id=correlation_id,
            idempotency_key=idempotency_key,
            actor=actor,
            deadline_at=deadline_at,
        )
        return await self._orchestrator.advance(execution.id)


class CancelWorkflowUseCase:
    def __init__(self, *, orchestrator: WorkflowOrchestrator) -> None:
        self._orchestrator = orchestrator

    async def execute(
        self,
        execution_id: UUID,
        *,
        actor: Actor,
        reason: str | None = None,
    ) -> WorkflowExecution:
        return await self._orchestrator.cancel(execution_id, actor=actor, reason=reason)


class GetWorkflowUseCase:
    def __init__(self, *, uow_factory: UnitOfWorkFactory) -> None:
        self._uow_factory = uow_factory

    async def execute(self, execution_id: UUID) -> WorkflowExecution:
        async with self._uow_factory() as uow:
            execution = await uow.workflow_executions.get(execution_id)
        if execution is None:
            raise NotFoundError("workflow execution not found")
        return execution
