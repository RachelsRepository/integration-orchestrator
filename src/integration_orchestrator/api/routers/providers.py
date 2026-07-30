"""Provider catalogue and health endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from integration_orchestrator.api.dependencies import ListProvidersDep
from integration_orchestrator.api.schemas.common import ErrorResponse
from integration_orchestrator.api.schemas.responses import (
    ProviderHealthResponse,
    ProviderListResponse,
)
from integration_orchestrator.api.security import RequireProvidersRead
from integration_orchestrator.domain.errors import NotFoundError

router = APIRouter(prefix="/api/v1/providers", tags=["providers"])


@router.get(
    "",
    response_model=ProviderListResponse,
    summary="List configured providers and their health",
)
async def list_providers(
    principal: RequireProvidersRead,
    use_case: ListProvidersDep,
    probe: Annotated[
        bool,
        Query(
            description=(
                "Probe each provider's health endpoint. Disable for a fast answer "
                "that reports circuit state only."
            )
        ),
    ] = True,
) -> ProviderListResponse:
    summaries = await use_case.execute(probe=probe)
    return ProviderListResponse(
        providers=[ProviderHealthResponse.from_summary(summary) for summary in summaries]
    )


@router.get(
    "/{provider}",
    response_model=ProviderHealthResponse,
    summary="Fetch one provider's capabilities and health",
    responses={404: {"model": ErrorResponse, "description": "Unknown provider."}},
)
async def get_provider(
    provider: str,
    principal: RequireProvidersRead,
    use_case: ListProvidersDep,
    probe: Annotated[bool, Query()] = True,
) -> ProviderHealthResponse:
    summaries = await use_case.execute(probe=probe)
    for summary in summaries:
        if summary.descriptor.slug.value == provider.lower():
            return ProviderHealthResponse.from_summary(summary)
    raise NotFoundError(f"provider '{provider}' is not configured", metadata={"provider": provider})
