"""Liveness, readiness and metrics endpoints.

Liveness and readiness answer genuinely different questions, and conflating them
causes outages. Liveness asks "is this process broken beyond recovery?" — if it
fails, the orchestrator kills the pod. Readiness asks "can this process serve
traffic right now?" — if it fails, the pod is removed from the load balancer but
left alone to recover.

So liveness must not check dependencies. A shared database blip would otherwise
make every replica fail liveness at once, and the orchestrator would restart the
entire fleet in response to a problem restarting cannot fix.

Providers are excluded from readiness too. A pod with an unreachable provider can
still serve reads, accept work for the other providers, and process webhooks; the
circuit breaker already handles that provider being down.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Response, status
from redis.exceptions import RedisError
from sqlalchemy import text

from integration_orchestrator.api.dependencies import ContainerDep
from integration_orchestrator.api.schemas.responses import (
    DependencyStatus,
    HealthResponse,
    ReadinessResponse,
)
from integration_orchestrator.composition import Container

logger = logging.getLogger(__name__)

router = APIRouter(tags=["operations"])

PROBE_TIMEOUT_SECONDS = 3.0


@router.get(
    "/health/live",
    response_model=HealthResponse,
    summary="Liveness probe",
    include_in_schema=False,
)
async def liveness(container: ContainerDep) -> HealthResponse:
    settings = container.settings
    return HealthResponse(
        status="alive",
        service=settings.service_name,
        version=settings.service_version,
        environment=settings.environment.value,
    )


@router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    summary="Readiness probe",
    include_in_schema=False,
)
async def readiness(container: ContainerDep, response: Response) -> ReadinessResponse:
    checks = await asyncio.gather(
        _check_database(container),
        _check_redis(container),
        _check_broker(container),
    )
    ready = all(check.healthy for check in checks)
    response.status_code = status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(status="ready" if ready else "not_ready", dependencies=list(checks))


async def _check_database(container: Container) -> DependencyStatus:
    try:
        async with asyncio.timeout(PROBE_TIMEOUT_SECONDS):
            async with container.engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
    except Exception as exc:
        return DependencyStatus(name="postgresql", healthy=False, detail=type(exc).__name__)
    return DependencyStatus(name="postgresql", healthy=True)


async def _check_redis(container: Container) -> DependencyStatus:
    try:
        async with asyncio.timeout(PROBE_TIMEOUT_SECONDS):
            await container.redis.ping()
    except (TimeoutError, RedisError) as exc:
        return DependencyStatus(name="redis", healthy=False, detail=type(exc).__name__)
    return DependencyStatus(name="redis", healthy=True)


async def _check_broker(container: Container) -> DependencyStatus:
    if not container.settings.kafka.enabled:
        return DependencyStatus(
            name="kafka", healthy=True, detail="disabled; events are published in process"
        )
    try:
        async with asyncio.timeout(PROBE_TIMEOUT_SECONDS):
            healthy = await container.publisher.healthy()
    except TimeoutError:
        return DependencyStatus(name="kafka", healthy=False, detail="the probe timed out")
    return DependencyStatus(
        name="kafka",
        healthy=healthy,
        detail=None if healthy else "the producer has no live broker connection",
    )


@router.get("/metrics", summary="Prometheus metrics", include_in_schema=False)
async def metrics(container: ContainerDep) -> Response:
    return Response(
        content=container.metrics.render(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
