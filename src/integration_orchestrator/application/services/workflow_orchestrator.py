"""Orchestrates multi-step workflow executions.

Each forward step creates one IntegrationRequest — the durable provider
side-effect unit — and waits for its terminal status. On downstream failure,
succeeded steps are compensated in reverse completion order.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.application.ports.system import Clock, IdentifierGenerator
from integration_orchestrator.application.ports.unit_of_work import UnitOfWorkFactory
from integration_orchestrator.application.services.dispatcher import RequestDispatcher
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.domain.entities import IntegrationRequest
from integration_orchestrator.domain.enums import (
    ActorType,
    AuditAction,
    RequestStatus,
    WorkflowStatus,
    WorkflowStepStatus,
)
from integration_orchestrator.domain.errors import ConflictError, NotFoundError
from integration_orchestrator.domain.records import AuditEvent, OutboxEvent
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ExternalReference,
    ProviderSlug,
)
from integration_orchestrator.domain.workflow import (
    WorkflowDefinition,
    WorkflowExecution,
    WorkflowStepExecution,
)

logger = logging.getLogger(__name__)

_TERMINAL_REQUESTS = frozenset(
    {
        RequestStatus.SUCCEEDED,
        RequestStatus.FAILED,
        RequestStatus.CANCELLED,
        RequestStatus.MANUAL_REVIEW,
    }
)


class WorkflowOrchestrator:
    """Advances and compensates durable multi-step workflow executions."""

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        journal: WorkflowJournal,
        dispatcher: RequestDispatcher,
        clock: Clock,
        ids: IdentifierGenerator,
    ) -> None:
        self._uow_factory = uow_factory
        self._journal = journal
        self._dispatcher = dispatcher
        self._clock = clock
        self._ids = ids

    async def start_execution(
        self,
        definition: WorkflowDefinition,
        *,
        input_payload: dict[str, Any],
        correlation_id: str,
        idempotency_key: str | None,
        actor: Actor,
        deadline_at: datetime | None = None,
    ) -> WorkflowExecution:
        if idempotency_key:
            async with self._uow_factory() as uow:
                existing = await uow.workflow_executions.get_by_idempotency_key(idempotency_key)
                if existing is not None:
                    return existing

        now = self._clock.now()
        execution_id = self._ids.new_id()
        steps: list[WorkflowStepExecution] = []
        for template in definition.steps:
            payload = {**template.payload_template, **input_payload.get(template.key, {})}
            if not payload:
                payload = dict(input_payload)
            steps.append(
                WorkflowStepExecution(
                    id=self._ids.new_id(),
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
                    input_payload=payload,
                    output_payload=None,
                    error_code=None,
                    error_message=None,
                    created_at=now,
                    updated_at=now,
                )
            )

        execution = WorkflowExecution(
            id=execution_id,
            definition_id=definition.id,
            definition_name=definition.name,
            definition_version=definition.version,
            status=WorkflowStatus.CREATED,
            correlation_id=correlation_id,
            idempotency_key=idempotency_key,
            input_payload=dict(input_payload),
            steps=steps,
            created_at=now,
            updated_at=now,
            owner_subject=actor.id,
            deadline_at=deadline_at,
        )
        # Deadline already elapsed at start → queue then immediately time out on advance.
        execution.transition_to(WorkflowStatus.QUEUED, now=now)
        execution.promote_ready_steps(now=now)

        async with self._uow_factory() as uow:
            if not definition.immutable:
                await uow.workflow_definitions.mark_immutable(definition)
            await uow.workflow_executions.add(execution)
            await self._record(
                uow,
                execution,
                action=AuditAction.WORKFLOW_STARTED,
                actor=actor,
                previous=WorkflowStatus.CREATED,
                new=execution.status,
            )
            await uow.commit()
        return execution

    async def cancel(
        self,
        execution_id: UUID,
        *,
        actor: Actor,
        reason: str | None = None,
    ) -> WorkflowExecution:
        """Cancel a workflow idempotently; compensate succeeded steps when needed."""
        async with self._uow_factory() as uow:
            execution = await uow.workflow_executions.get_for_update(execution_id)
            if execution is None:
                raise NotFoundError("workflow execution not found")
            now = self._clock.now()
            if execution.status is WorkflowStatus.CANCELLED:
                return execution
            if execution.status in {
                WorkflowStatus.SUCCEEDED,
                WorkflowStatus.COMPENSATED,
                WorkflowStatus.FAILED,
                WorkflowStatus.DEAD_LETTERED,
            }:
                raise ConflictError(
                    f"cannot cancel workflow in terminal status {execution.status.value}"
                )
            previous = execution.status
            execution.cancel_reason = reason or execution.cancel_reason or "client_cancelled"
            # During compensation: record intent; finish as CANCELLED when unwind completes.
            if execution.status is WorkflowStatus.COMPENSATING:
                await self._record(
                    uow,
                    execution,
                    action=AuditAction.WORKFLOW_CANCELLED,
                    actor=actor,
                    previous=previous,
                    new=previous,
                    metadata={"deferred": True, "reason": execution.cancel_reason},
                )
                await uow.workflow_executions.update(execution)
                await uow.commit()
                return execution

            for step in execution.steps:
                if step.status in {
                    WorkflowStepStatus.PENDING,
                    WorkflowStepStatus.READY,
                    WorkflowStepStatus.WAITING,
                    WorkflowStepStatus.RETRY_SCHEDULED,
                }:
                    step.transition_to(WorkflowStepStatus.CANCELLED, now=now)
                elif step.status is WorkflowStepStatus.RUNNING:
                    # Provider calls are not assumed cancellable mid-flight.
                    step.transition_to(WorkflowStepStatus.CANCELLED, now=now)

            needs_compensation = execution.has_succeeded_steps()
            if needs_compensation:
                execution.transition_to(WorkflowStatus.COMPENSATING, now=now)
                await self._record(
                    uow,
                    execution,
                    action=AuditAction.WORKFLOW_COMPENSATING,
                    actor=actor,
                    previous=previous,
                    new=WorkflowStatus.COMPENSATING,
                    metadata={"reason": "cancel", "cancel_reason": execution.cancel_reason},
                )
            else:
                execution.transition_to(WorkflowStatus.CANCELLED, now=now)
                await self._record(
                    uow,
                    execution,
                    action=AuditAction.WORKFLOW_CANCELLED,
                    actor=actor,
                    previous=previous,
                    new=WorkflowStatus.CANCELLED,
                    metadata={"reason": execution.cancel_reason},
                )
            await uow.workflow_executions.update(execution)
            await uow.commit()

        if needs_compensation:
            await self._continue_compensation(execution.id)
            async with self._uow_factory() as uow:
                loaded = await uow.workflow_executions.get(execution.id)
                assert loaded is not None
                return loaded
        return execution

    async def enforce_deadline(self, execution_id: UUID) -> WorkflowExecution:
        """Idempotently expire a workflow past its hard deadline."""
        async with self._uow_factory() as uow:
            execution = await uow.workflow_executions.get_for_update(execution_id)
            if execution is None:
                raise NotFoundError("workflow execution not found")
            now = self._clock.now()
            if execution.status.is_terminal or execution.status in {
                WorkflowStatus.COMPENSATING,
                WorkflowStatus.MANUAL_REVIEW,
                WorkflowStatus.TIMED_OUT,
            }:
                if execution.deadline_processed_at is not None:
                    return execution
            if not execution.is_past_deadline(now=now):
                return execution
            if execution.deadline_processed_at is not None:
                return execution
            previous = execution.status
            execution.deadline_processed_at = now
            for step in execution.steps:
                if step.status in {
                    WorkflowStepStatus.PENDING,
                    WorkflowStepStatus.READY,
                }:
                    step.transition_to(WorkflowStepStatus.CANCELLED, now=now)
                elif step.status in {
                    WorkflowStepStatus.WAITING,
                    WorkflowStepStatus.RETRY_SCHEDULED,
                    WorkflowStepStatus.RUNNING,
                }:
                    step.mark_failed(
                        now=now,
                        code="workflow_deadline_exceeded",
                        message="workflow hard deadline elapsed",
                    )
            execution.transition_to(WorkflowStatus.TIMED_OUT, now=now)
            await self._record(
                uow,
                execution,
                action=AuditAction.WORKFLOW_TIMED_OUT,
                actor=Actor(type=ActorType.WORKFLOW_WORKER, id="deadline"),
                previous=previous,
                new=WorkflowStatus.TIMED_OUT,
            )
            needs_compensation = execution.has_succeeded_steps()
            if needs_compensation:
                execution.transition_to(WorkflowStatus.COMPENSATING, now=now)
                await self._record(
                    uow,
                    execution,
                    action=AuditAction.WORKFLOW_COMPENSATING,
                    actor=Actor(type=ActorType.WORKFLOW_WORKER, id="deadline"),
                    previous=WorkflowStatus.TIMED_OUT,
                    new=WorkflowStatus.COMPENSATING,
                )
            else:
                execution.transition_to(WorkflowStatus.CANCELLED, now=now)
                execution.cancel_reason = execution.cancel_reason or "workflow_deadline_exceeded"
                await self._record(
                    uow,
                    execution,
                    action=AuditAction.WORKFLOW_CANCELLED,
                    actor=Actor(type=ActorType.WORKFLOW_WORKER, id="deadline"),
                    previous=WorkflowStatus.TIMED_OUT,
                    new=WorkflowStatus.CANCELLED,
                )
            await uow.workflow_executions.update(execution)
            await uow.commit()

        if needs_compensation:
            await self._continue_compensation(execution.id)
            async with self._uow_factory() as uow:
                loaded = await uow.workflow_executions.get(execution.id)
                assert loaded is not None
                return loaded
        return execution

    async def advance(self, execution_id: UUID) -> WorkflowExecution:
        async with self._uow_factory() as uow:
            execution = await uow.workflow_executions.get_for_update(execution_id)
            if execution is None:
                raise NotFoundError("workflow execution not found")
            if execution.status.is_terminal:
                return execution
            if execution.status is WorkflowStatus.MANUAL_REVIEW:
                return execution
            now = self._clock.now()
            if (
                execution.is_past_deadline(now=now)
                and execution.deadline_processed_at is None
                and execution.status
                not in {WorkflowStatus.COMPENSATING, WorkflowStatus.MANUAL_REVIEW}
            ):
                await uow.commit()
                return await self.enforce_deadline(execution_id)
            if execution.status is WorkflowStatus.CANCELLED:
                return execution
            if execution.status is WorkflowStatus.QUEUED:
                execution.transition_to(WorkflowStatus.RUNNING, now=now)
            # Resume WAITING steps whose linked requests already completed.
            for step in execution.steps:
                if (
                    step.status is WorkflowStepStatus.WAITING
                    and step.integration_request_id is not None
                ):
                    request = await uow.requests.get(step.integration_request_id)
                    if request is not None and request.status is RequestStatus.SUCCEEDED:
                        step.mark_succeeded(
                            now=now,
                            request_id=request.id,
                            output={"provider_reference": request.provider_reference},
                        )
            execution.promote_ready_steps(now=now)
            await self._refresh_execution_status(uow, execution)
            await uow.workflow_executions.update(execution)
            await uow.commit()

        if (
            execution.status
            in {
                WorkflowStatus.CANCELLED,
                WorkflowStatus.COMPENSATING,
                WorkflowStatus.MANUAL_REVIEW,
            }
            or execution.status.is_terminal
        ):
            return execution

        ready = [s for s in execution.steps if s.status is WorkflowStepStatus.READY]
        # Independent READY steps are promoted together (fan-out). Dispatch is
        # serialised per execution to avoid optimistic-lock races on the shared
        # workflow row; concurrency is across workers claiming different executions.
        for step in ready:
            await self._run_forward_step(execution.id, step.id)

        async with self._uow_factory() as uow:
            execution = await uow.workflow_executions.get_for_update(execution_id)
            assert execution is not None
            await self._refresh_execution_status(uow, execution)
            await uow.workflow_executions.update(execution)
            await uow.commit()
            return execution

    async def on_request_terminal(self, request_id: UUID) -> None:
        async with self._uow_factory() as uow:
            execution = await uow.workflow_executions.find_by_request_id(request_id)
            if execution is None:
                return
            request = await uow.requests.get(request_id)
            if request is None or request.status not in _TERMINAL_REQUESTS:
                return
            execution = await uow.workflow_executions.get_for_update(execution.id)
            assert execution is not None
            now = self._clock.now()
            step = self._step_for_request(execution, request_id)
            if step is None:
                return
            if step.status in {
                WorkflowStepStatus.RUNNING,
                WorkflowStepStatus.WAITING,
                WorkflowStepStatus.COMPENSATING,
            }:
                if request.status is RequestStatus.SUCCEEDED:
                    if step.status is WorkflowStepStatus.COMPENSATING:
                        step.mark_compensated(now=now, compensation_request_id=request_id)
                    else:
                        step.mark_succeeded(
                            now=now,
                            request_id=request_id,
                            output={"provider_reference": request.provider_reference},
                        )
                elif request.status in {RequestStatus.FAILED, RequestStatus.MANUAL_REVIEW}:
                    if step.status is WorkflowStepStatus.COMPENSATING:
                        execution.manual_review_reason = (
                            f"compensation failed for step {step.step_key}"
                        )
                        if execution.status is not WorkflowStatus.MANUAL_REVIEW:
                            if execution.status is WorkflowStatus.COMPENSATING:
                                execution.transition_to(WorkflowStatus.MANUAL_REVIEW, now=now)
                            await self._record(
                                uow,
                                execution,
                                action=AuditAction.WORKFLOW_MANUAL_REVIEW,
                                actor=Actor(type=ActorType.WORKFLOW_WORKER, id="workflow"),
                                previous=WorkflowStatus.COMPENSATING,
                                new=WorkflowStatus.MANUAL_REVIEW,
                            )
                    else:
                        step.mark_failed(
                            now=now,
                            code=request.last_error_code,
                            message=request.last_error_message,
                        )
                elif request.status is RequestStatus.CANCELLED:
                    step.transition_to(WorkflowStepStatus.CANCELLED, now=now)

            await self._refresh_execution_status(uow, execution)
            await uow.workflow_executions.update(execution)
            await uow.commit()

        if execution.status is WorkflowStatus.RUNNING:
            await self.advance(execution.id)
        elif execution.status is WorkflowStatus.COMPENSATING:
            await self._continue_compensation(execution.id)
        # CANCELLED / terminal: webhooks must not resume normal flow.

    async def _run_forward_step(self, execution_id: UUID, step_id: UUID) -> None:
        async with self._uow_factory() as uow:
            execution = await uow.workflow_executions.get_for_update(execution_id)
            assert execution is not None
            step = next(s for s in execution.steps if s.id == step_id)
            if step.status is not WorkflowStepStatus.READY:
                return
            now = self._clock.now()
            if (
                execution.status
                in {
                    WorkflowStatus.CANCELLED,
                    WorkflowStatus.COMPENSATING,
                    WorkflowStatus.MANUAL_REVIEW,
                    WorkflowStatus.TIMED_OUT,
                }
                or execution.status.is_terminal
            ):
                return
            if execution.is_past_deadline(now=now) and execution.deadline_processed_at is None:
                await uow.commit()
                await self.enforce_deadline(execution_id)
                return
            step.mark_running(now=now)
            # Optional Compose/demo fault injection: prefix the sandbox scenario
            # onto the step external reference when fail_at_step matches.
            fail_at = execution.input_payload.get("fail_at_step")
            scenario = execution.input_payload.get("fail_scenario", "scenario-reject")
            ref = f"wf-{execution.id.hex[:8]}-{step.step_key}"
            if isinstance(fail_at, str) and fail_at == step.step_key and isinstance(scenario, str):
                ref = f"{scenario}-{ref}"
            request = IntegrationRequest.create(
                request_id=self._ids.new_id(),
                provider=ProviderSlug.parse(step.provider),
                operation_type=step.operation_type,
                external_reference=ExternalReference(ref),
                normalized_payload=dict(step.input_payload),
                correlation_id=CorrelationId.parse(execution.correlation_id),
                now=now,
            )
            await uow.requests.add(request)
            step.integration_request_id = request.id
            await uow.workflow_executions.update(execution)
            await self._journal.record_creation(
                uow,
                request,
                actor=Actor(type=ActorType.WORKFLOW_WORKER, id="workflow"),
                metadata={"workflow_execution_id": str(execution.id), "step_key": step.step_key},
            )
            await uow.commit()
            request_id = request.id

        dispatched = await self._dispatcher.dispatch(
            request_id, actor=Actor(type=ActorType.WORKFLOW_WORKER, id="workflow")
        )

        async with self._uow_factory() as uow:
            execution = await uow.workflow_executions.get_for_update(execution_id)
            assert execution is not None
            step = next(s for s in execution.steps if s.id == step_id)
            now = self._clock.now()
            if dispatched.status is RequestStatus.SUCCEEDED:
                step.mark_succeeded(
                    now=now,
                    request_id=request_id,
                    output={"provider_reference": dispatched.provider_reference},
                )
            elif dispatched.status is RequestStatus.PENDING:
                if step.status in {
                    WorkflowStepStatus.RUNNING,
                    WorkflowStepStatus.WAITING,
                }:
                    step.mark_waiting(now=now)
            elif dispatched.status in {RequestStatus.FAILED, RequestStatus.MANUAL_REVIEW}:
                if step.status not in {
                    WorkflowStepStatus.FAILED,
                    WorkflowStepStatus.CANCELLED,
                    WorkflowStepStatus.SUCCEEDED,
                }:
                    step.mark_failed(
                        now=now,
                        code=dispatched.last_error_code,
                        message=dispatched.last_error_message,
                    )
            elif dispatched.status is RequestStatus.RETRY_SCHEDULED:
                if step.status is WorkflowStepStatus.RUNNING:
                    step.transition_to(WorkflowStepStatus.RETRY_SCHEDULED, now=now)
            await self._refresh_execution_status(uow, execution)
            await uow.workflow_executions.update(execution)
            await uow.commit()

        if execution.status is WorkflowStatus.COMPENSATING:
            await self._continue_compensation(execution.id)

    async def _continue_compensation(self, execution_id: UUID) -> None:
        async with self._uow_factory() as uow:
            execution = await uow.workflow_executions.get_for_update(execution_id)
            assert execution is not None
            if execution.status is not WorkflowStatus.COMPENSATING:
                return
            now = self._clock.now()
            for step in execution.succeeded_in_reverse():
                if step.status is WorkflowStepStatus.SUCCEEDED:
                    step.begin_compensation(now=now)
                    await uow.workflow_executions.update(execution)
                    await uow.commit()
                    await self._compensate_step(execution_id, step.id)
                    return
            # All compensations done
            execution = await uow.workflow_executions.get_for_update(execution_id)
            assert execution is not None
            if all(
                s.status
                in {
                    WorkflowStepStatus.COMPENSATED,
                    WorkflowStepStatus.FAILED,
                    WorkflowStepStatus.SKIPPED,
                    WorkflowStepStatus.CANCELLED,
                }
                or s.status is WorkflowStepStatus.PENDING
                for s in execution.steps
            ):
                # Pending never-run steps stay pending; succeeded ones compensated.
                if all(
                    s.status is not WorkflowStepStatus.SUCCEEDED
                    and s.status is not WorkflowStepStatus.COMPENSATING
                    for s in execution.steps
                ):
                    terminal = (
                        WorkflowStatus.CANCELLED
                        if execution.cancel_reason
                        else WorkflowStatus.COMPENSATED
                    )
                    action = (
                        AuditAction.WORKFLOW_CANCELLED
                        if terminal is WorkflowStatus.CANCELLED
                        else AuditAction.WORKFLOW_COMPENSATED
                    )
                    execution.transition_to(terminal, now=self._clock.now())
                    await self._record(
                        uow,
                        execution,
                        action=action,
                        actor=Actor(type=ActorType.WORKFLOW_WORKER, id="workflow"),
                        previous=WorkflowStatus.COMPENSATING,
                        new=terminal,
                    )
            await uow.workflow_executions.update(execution)
            await uow.commit()

    async def _compensate_step(self, execution_id: UUID, step_id: UUID) -> None:
        async with self._uow_factory() as uow:
            execution = await uow.workflow_executions.get_for_update(execution_id)
            assert execution is not None
            step = next(s for s in execution.steps if s.id == step_id)
            now = self._clock.now()
            if step.compensate_operation is None:
                step.mark_compensated(now=now, compensation_request_id=None)
                await uow.workflow_executions.update(execution)
                await uow.commit()
                await self._continue_compensation(execution_id)
                return
            request = IntegrationRequest.create(
                request_id=self._ids.new_id(),
                provider=ProviderSlug.parse(step.provider),
                operation_type=step.compensate_operation,
                external_reference=ExternalReference(
                    f"wf-cmp-{execution.id.hex[:8]}-{step.step_key}"
                ),
                normalized_payload=dict(step.input_payload),
                correlation_id=CorrelationId.parse(execution.correlation_id),
                now=now,
            )
            await uow.requests.add(request)
            step.compensation_request_id = request.id
            await uow.workflow_executions.update(execution)
            await self._journal.record_creation(
                uow,
                request,
                actor=Actor(type=ActorType.WORKFLOW_WORKER, id="workflow"),
                metadata={
                    "workflow_execution_id": str(execution.id),
                    "step_key": step.step_key,
                    "compensation": True,
                },
            )
            await uow.commit()
            request_id = request.id

        dispatched = await self._dispatcher.dispatch(
            request_id, actor=Actor(type=ActorType.WORKFLOW_WORKER, id="workflow")
        )
        async with self._uow_factory() as uow:
            execution = await uow.workflow_executions.get_for_update(execution_id)
            assert execution is not None
            step = next(s for s in execution.steps if s.id == step_id)
            now = self._clock.now()
            if dispatched.status is RequestStatus.SUCCEEDED:
                step.mark_compensated(now=now, compensation_request_id=request_id)
            elif dispatched.status is RequestStatus.PENDING:
                # Wait for webhook on compensation too.
                pass
            else:
                step.mark_failed(
                    now=now,
                    code=dispatched.last_error_code or "compensation_failed",
                    message=dispatched.last_error_message or "compensation failed",
                )
                execution.manual_review_reason = f"compensation failed for {step.step_key}"
                execution.transition_to(WorkflowStatus.MANUAL_REVIEW, now=now)
            await uow.workflow_executions.update(execution)
            await uow.commit()
        if execution.status is WorkflowStatus.COMPENSATING:
            await self._continue_compensation(execution_id)

    async def _refresh_execution_status(self, uow: Any, execution: WorkflowExecution) -> None:
        now = self._clock.now()
        actor = Actor(type=ActorType.WORKFLOW_WORKER, id="workflow")
        failed = [s for s in execution.steps if s.status is WorkflowStepStatus.FAILED]
        if execution.status in {
            WorkflowStatus.CANCELLED,
            WorkflowStatus.COMPENSATED,
            WorkflowStatus.SUCCEEDED,
            WorkflowStatus.DEAD_LETTERED,
        }:
            return
        if failed and execution.status not in {
            WorkflowStatus.COMPENSATING,
            WorkflowStatus.COMPENSATED,
            WorkflowStatus.MANUAL_REVIEW,
            WorkflowStatus.FAILED,
            WorkflowStatus.TIMED_OUT,
            WorkflowStatus.CANCELLED,
        }:
            previous = execution.status
            execution.transition_to(WorkflowStatus.COMPENSATING, now=now)
            await self._record(
                uow,
                execution,
                action=AuditAction.WORKFLOW_COMPENSATING,
                actor=actor,
                previous=previous,
                new=WorkflowStatus.COMPENSATING,
                metadata={"failed_step": failed[0].step_key},
            )
            return
        if execution.any_waiting() and execution.status is WorkflowStatus.RUNNING:
            previous = execution.status
            execution.transition_to(WorkflowStatus.WAITING, now=now)
            await self._record(
                uow,
                execution,
                action=AuditAction.WORKFLOW_WAITING,
                actor=actor,
                previous=previous,
                new=WorkflowStatus.WAITING,
            )
            return
        if (
            execution.status is WorkflowStatus.WAITING
            and not execution.any_waiting()
            and not failed
        ):
            execution.transition_to(WorkflowStatus.RUNNING, now=now)
            execution.promote_ready_steps(now=now)
        if execution.all_forward_succeeded() and execution.status is WorkflowStatus.RUNNING:
            previous = execution.status
            execution.transition_to(WorkflowStatus.SUCCEEDED, now=now)
            await self._record(
                uow,
                execution,
                action=AuditAction.WORKFLOW_SUCCEEDED,
                actor=actor,
                previous=previous,
                new=WorkflowStatus.SUCCEEDED,
            )

    def _step_for_request(
        self, execution: WorkflowExecution, request_id: UUID
    ) -> WorkflowStepExecution | None:
        for step in execution.steps:
            if (
                step.integration_request_id == request_id
                or step.compensation_request_id == request_id
            ):
                return step
        return None

    async def _record(
        self,
        uow: Any,
        execution: WorkflowExecution,
        *,
        action: AuditAction,
        actor: Actor,
        previous: WorkflowStatus | None,
        new: WorkflowStatus,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = self._clock.now()
        await uow.audit.append(
            AuditEvent(
                id=self._ids.new_id(),
                aggregate_type="workflow_execution",
                aggregate_id=str(execution.id),
                action=action,
                actor=actor.type,
                actor_id=actor.id,
                correlation_id=CorrelationId.parse(execution.correlation_id),
                occurred_at=now,
                previous_state=previous.value if previous else None,
                new_state=new.value,
                metadata=dict(metadata or {}),
            )
        )
        await uow.outbox.add(
            OutboxEvent(
                id=self._ids.new_id(),
                event_id=self._ids.new_id(),
                event_type=f"workflow.execution.{new.value}.v1",
                event_version=1,
                aggregate_type="workflow_execution",
                aggregate_id=str(execution.id),
                payload={
                    "workflow_execution_id": str(execution.id),
                    "status": new.value,
                    "definition": execution.definition_name,
                    "version": execution.definition_version,
                },
                correlation_id=CorrelationId.parse(execution.correlation_id),
                created_at=now,
                partition_key=str(execution.id),
            )
        )
