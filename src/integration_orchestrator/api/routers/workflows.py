"""HTTP API for multi-step workflow definitions and executions."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header
from pydantic import BaseModel, Field
from starlette.requests import Request

from integration_orchestrator.api.dependencies import get_container
from integration_orchestrator.api.ownership import assert_owner_access
from integration_orchestrator.api.security import RequireRequestsRead, RequireRequestsWrite
from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.domain.enums import ActorType

router = APIRouter(prefix="/api/v1/workflows", tags=["workflows"])


class StartWorkflowBody(BaseModel):
    definition_name: str = "customer_onboarding"
    definition_version: int = 1
    payload: dict[str, Any] = Field(default_factory=dict)
    deadline_seconds: int | None = Field(default=None, ge=1, le=86400)


class CancelWorkflowBody(BaseModel):
    reason: str | None = None


class WorkflowStepResponse(BaseModel):
    key: str
    provider: str
    status: str
    integration_request_id: UUID | None
    compensation_request_id: UUID | None
    attempt_count: int
    completed_at: str | None = None


class WorkflowExecutionResponse(BaseModel):
    id: UUID
    definition_name: str
    definition_version: int
    status: str
    correlation_id: str
    steps: list[WorkflowStepResponse]
    manual_review_reason: str | None = None
    cancel_reason: str | None = None
    deadline_at: str | None = None


def _to_response(execution: Any) -> WorkflowExecutionResponse:
    return WorkflowExecutionResponse(
        id=execution.id,
        definition_name=execution.definition_name,
        definition_version=execution.definition_version,
        status=execution.status.value,
        correlation_id=execution.correlation_id,
        manual_review_reason=execution.manual_review_reason,
        cancel_reason=execution.cancel_reason,
        deadline_at=execution.deadline_at.isoformat() if execution.deadline_at else None,
        steps=[
            WorkflowStepResponse(
                key=step.step_key,
                provider=step.provider,
                status=step.status.value,
                integration_request_id=step.integration_request_id,
                compensation_request_id=step.compensation_request_id,
                attempt_count=step.attempt_count,
                completed_at=step.completed_at.isoformat() if step.completed_at else None,
            )
            for step in execution.steps
        ],
    )


@router.post("/executions", response_model=WorkflowExecutionResponse, status_code=201)
async def start_workflow(
    body: StartWorkflowBody,
    request: Request,
    principal: RequireRequestsWrite,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    correlation_id: str | None = Header(default=None, alias="X-Correlation-ID"),
) -> WorkflowExecutionResponse:
    container = get_container(request)
    from uuid import uuid4

    execution = await container.use_cases.start_workflow.execute(
        definition_name=body.definition_name,
        definition_version=body.definition_version,
        input_payload=body.payload,
        correlation_id=correlation_id or str(uuid4()),
        idempotency_key=idempotency_key,
        actor=Actor(type=ActorType.API_CLIENT, id=principal.subject),
        deadline_seconds=body.deadline_seconds,
    )
    return _to_response(execution)


@router.get("/executions/{execution_id}", response_model=WorkflowExecutionResponse)
async def get_workflow(
    execution_id: UUID,
    request: Request,
    principal: RequireRequestsRead,
) -> WorkflowExecutionResponse:
    container = get_container(request)
    execution = await container.use_cases.get_workflow.execute(execution_id)
    assert_owner_access(
        principal=principal,
        owner_subject=execution.owner_subject,
        enforce=container.settings.security.enforce_subject_isolation,
        resource_label="workflow execution",
    )
    return _to_response(execution)


@router.post("/executions/{execution_id}/cancel", response_model=WorkflowExecutionResponse)
async def cancel_workflow(
    execution_id: UUID,
    request: Request,
    principal: RequireRequestsWrite,
    body: CancelWorkflowBody | None = None,
) -> WorkflowExecutionResponse:
    container = get_container(request)
    execution = await container.use_cases.get_workflow.execute(execution_id)
    assert_owner_access(
        principal=principal,
        owner_subject=execution.owner_subject,
        enforce=container.settings.security.enforce_subject_isolation,
        resource_label="workflow execution",
    )
    cancelled = await container.use_cases.cancel_workflow.execute(
        execution_id,
        actor=Actor(type=ActorType.API_CLIENT, id=principal.subject),
        reason=(body.reason if body else None),
    )
    return _to_response(cancelled)
