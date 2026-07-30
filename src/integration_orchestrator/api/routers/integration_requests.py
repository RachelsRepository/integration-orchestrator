"""Integration request endpoints.

Handlers here are thin on purpose. Each one parses untrusted input into value
objects, builds a command, calls exactly one use case, and maps the result onto a
response model. No orchestration decision, no provider knowledge, and no
persistence detail appears in this module.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, Query, Response, status

from integration_orchestrator.api.dependencies import (
    AuditHistoryDep,
    CancelRequestDep,
    CorrelationDep,
    CreateRequestDep,
    GetRequestDep,
    ListRequestsDep,
    RetryRequestDep,
)
from integration_orchestrator.api.schemas.common import ErrorResponse
from integration_orchestrator.api.schemas.requests import (
    CancelRequestBody,
    CreateIntegrationRequestBody,
    RetryRequestBody,
)
from integration_orchestrator.api.schemas.responses import (
    AuditHistoryResponse,
    IntegrationRequestPage,
    IntegrationRequestResponse,
)
from integration_orchestrator.api.security import (
    RequireRequestsCancel,
    RequireRequestsRead,
    RequireRequestsRetry,
    RequireRequestsWrite,
)
from integration_orchestrator.application.dto.commands import (
    Actor,
    CancelRequestCommand,
    CreateIntegrationRequestCommand,
    RetryRequestCommand,
)
from integration_orchestrator.application.dto.queries import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    Cursor,
    IntegrationRequestFilter,
)
from integration_orchestrator.domain.enums import ActorType, OperationType, RequestStatus
from integration_orchestrator.domain.value_objects import (
    ExternalReference,
    IdempotencyKey,
    ProviderSlug,
)
from integration_orchestrator.observability.correlation import set_integration_request_id

router = APIRouter(prefix="/api/v1/integration-requests", tags=["integration requests"])

COMMON_ERRORS: dict[int | str, dict[str, Any]] = {
    400: {"model": ErrorResponse, "description": "The request is not valid."},
    401: {"model": ErrorResponse, "description": "Authentication failed."},
    403: {"model": ErrorResponse, "description": "The token lacks the required scope."},
}


@router.post(
    "",
    response_model=IntegrationRequestResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create an integration request",
    responses={
        **COMMON_ERRORS,
        200: {
            "model": IntegrationRequestResponse,
            "description": "An idempotent replay of a previously created request.",
        },
        409: {"model": ErrorResponse, "description": "The idempotency key was reused."},
        422: {
            "model": ErrorResponse,
            "description": "The provider does not support the operation.",
        },
    },
)
async def create_integration_request(
    body: CreateIntegrationRequestBody,
    principal: RequireRequestsWrite,
    use_case: CreateRequestDep,
    correlation_id: CorrelationDep,
    response: Response,
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            description=(
                "Makes creation safely repeatable. Replaying a key with an "
                "identical body returns the original request; replaying it with a "
                "different body is rejected."
            ),
        ),
    ] = None,
) -> IntegrationRequestResponse:
    command = CreateIntegrationRequestCommand(
        provider=ProviderSlug.parse(body.provider),
        operation_type=body.operation_type,
        external_reference=ExternalReference(body.external_reference.strip()),
        payload=body.payload,
        correlation_id=correlation_id,
        actor=Actor(type=ActorType.API_CLIENT, id=principal.subject),
        idempotency_key=IdempotencyKey.parse(idempotency_key),
    )
    result = await use_case.execute(command)
    set_integration_request_id(str(result.request.id))

    # A replay is not a creation, so it answers 200. Clients that distinguish the
    # two can tell whether their retry actually produced new work.
    response.status_code = result.http_status
    response.headers["Location"] = f"/api/v1/integration-requests/{result.request.id}"
    return IntegrationRequestResponse.from_domain(result.request)


@router.get(
    "",
    response_model=IntegrationRequestPage,
    summary="List integration requests",
    responses=COMMON_ERRORS,
)
async def list_integration_requests(
    principal: RequireRequestsRead,
    use_case: ListRequestsDep,
    provider: Annotated[str | None, Query(description="Filter by provider slug.")] = None,
    request_status: Annotated[
        list[RequestStatus] | None,
        Query(alias="status", description="Filter by one or more lifecycle states."),
    ] = None,
    operation_type: Annotated[OperationType | None, Query()] = None,
    external_reference: Annotated[str | None, Query(max_length=255)] = None,
    created_after: Annotated[datetime | None, Query()] = None,
    created_before: Annotated[datetime | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    cursor: Annotated[
        str | None, Query(description="Opaque cursor returned by a previous page.")
    ] = None,
) -> IntegrationRequestPage:
    criteria = IntegrationRequestFilter(
        provider=ProviderSlug.parse(provider) if provider else None,
        statuses=frozenset(request_status or ()),
        operation_type=operation_type,
        external_reference=external_reference,
        created_after=created_after,
        created_before=created_before,
        limit=limit,
        cursor=Cursor.decode(cursor) if cursor else None,
    )
    page = await use_case.execute(criteria)
    return IntegrationRequestPage.from_domain(page)


@router.get(
    "/{request_id}",
    response_model=IntegrationRequestResponse,
    summary="Fetch one integration request",
    responses={**COMMON_ERRORS, 404: {"model": ErrorResponse, "description": "Unknown request."}},
)
async def get_integration_request(
    request_id: UUID,
    principal: RequireRequestsRead,
    use_case: GetRequestDep,
) -> IntegrationRequestResponse:
    set_integration_request_id(str(request_id))
    request = await use_case.execute(request_id)
    return IntegrationRequestResponse.from_domain(request)


@router.get(
    "/{request_id}/audit",
    response_model=AuditHistoryResponse,
    summary="Fetch the audit history for a request",
    responses={**COMMON_ERRORS, 404: {"model": ErrorResponse, "description": "Unknown request."}},
)
async def get_audit_history(
    request_id: UUID,
    principal: RequireRequestsRead,
    use_case: AuditHistoryDep,
) -> AuditHistoryResponse:
    set_integration_request_id(str(request_id))
    events = await use_case.execute(request_id)
    return AuditHistoryResponse.from_domain(request_id, events)


@router.post(
    "/{request_id}/retry",
    response_model=IntegrationRequestResponse,
    summary="Retry a failed request",
    responses={
        **COMMON_ERRORS,
        404: {"model": ErrorResponse, "description": "Unknown request."},
        409: {
            "model": ErrorResponse,
            "description": "The request is not in a state that can be retried.",
        },
    },
)
async def retry_integration_request(
    request_id: UUID,
    principal: RequireRequestsRetry,
    use_case: RetryRequestDep,
    correlation_id: CorrelationDep,
    body: RetryRequestBody | None = None,
) -> IntegrationRequestResponse:
    set_integration_request_id(str(request_id))
    # The retry is scheduled rather than performed inline: the retry worker owns
    # dispatch, so a burst of operator retries cannot saturate the API's
    # connection pool with provider calls.
    request = await use_case.execute(
        RetryRequestCommand(
            request_id=request_id,
            correlation_id=correlation_id,
            actor=Actor(type=ActorType.API_CLIENT, id=principal.subject),
            reason=body.reason if body else None,
        )
    )
    return IntegrationRequestResponse.from_domain(request)


@router.post(
    "/{request_id}/cancel",
    response_model=IntegrationRequestResponse,
    summary="Cancel a request",
    responses={
        **COMMON_ERRORS,
        404: {"model": ErrorResponse, "description": "Unknown request."},
        409: {
            "model": ErrorResponse,
            "description": "The request has already reached a terminal state.",
        },
        422: {
            "model": ErrorResponse,
            "description": "The provider does not support cancellation.",
        },
    },
)
async def cancel_integration_request(
    request_id: UUID,
    principal: RequireRequestsCancel,
    use_case: CancelRequestDep,
    correlation_id: CorrelationDep,
    body: CancelRequestBody | None = None,
) -> IntegrationRequestResponse:
    set_integration_request_id(str(request_id))
    request = await use_case.execute(
        CancelRequestCommand(
            request_id=request_id,
            correlation_id=correlation_id,
            actor=Actor(type=ActorType.API_CLIENT, id=principal.subject),
            reason=body.reason if body else None,
        )
    )
    return IntegrationRequestResponse.from_domain(request)
