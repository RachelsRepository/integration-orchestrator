"""FastAPI application assembly.

The application is assembled by a factory rather than at import time. That keeps
the import side-effect free — tests can import this module without opening a
database connection — and lets a test build an app around a container it
constructed itself.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager

from fastapi import FastAPI

from integration_orchestrator.api.errors import register_exception_handlers
from integration_orchestrator.api.middleware import (
    AccessLogMiddleware,
    BodySizeLimitMiddleware,
    CorrelationMiddleware,
)
from integration_orchestrator.api.routers import health, integration_requests, providers, webhooks
from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.composition import Container, build_container
from integration_orchestrator.config.settings import Settings, get_settings
from integration_orchestrator.observability.tracing import instrument_application

logger = logging.getLogger(__name__)

DESCRIPTION = """
A provider-agnostic orchestration layer for enterprise API integrations.

Callers submit a normalized request naming a provider and an operation. The
platform selects an adapter, authenticates to the provider, applies timeout,
retry, circuit breaker and concurrency controls, tracks the workflow through an
explicit state machine, ingests the provider's webhooks, publishes normalized
domain events, and reconciles anything that ends up ambiguous.

Every response uses one error envelope, every state change is audited, and every
call can be traced end to end through the `X-Correlation-ID` header.
"""

TAGS_METADATA = [
    {
        "name": "integration requests",
        "description": "Create, inspect, retry and cancel externally-fulfilled operations.",
    },
    {
        "name": "providers",
        "description": "Provider capabilities and live operational health.",
    },
    {
        "name": "webhooks",
        "description": (
            "Inbound provider callbacks. Authenticated by signature verification "
            "rather than by bearer token."
        ),
    },
    {"name": "operations", "description": "Health, readiness and metrics."},
]


def create_app(*, settings: Settings | None = None, container: Container | None = None) -> FastAPI:
    """Build the API application.

    ``container`` is accepted so tests can inject a container wired against test
    doubles. When it is omitted the container is built during startup, which is
    the production path.
    """
    settings = settings or (container.settings if container else get_settings())

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owns_container = container is None
        active = container or await build_container(settings)
        app.state.container = active
        if owns_container:
            await active.startup()
        instrument_application(app, engine=active.engine)
        logger.info(
            "api started",
            extra={
                "environment": settings.environment.value,
                "providers": active.registry.slugs(),
            },
        )
        try:
            yield
        finally:
            # Only the owner tears down: a test that supplied its own container
            # is responsible for its lifecycle, and closing it here would break
            # any subsequent assertion against it.
            if owns_container:
                await active.shutdown()

    app = FastAPI(
        title="Integration Orchestrator",
        description=DESCRIPTION,
        version=settings.service_version,
        openapi_tags=TAGS_METADATA,
        root_path=settings.api_root_path,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # Registration order is reversed at execution time, so correlation is added
    # last here in order to run first at request time.
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=settings.webhooks.max_body_bytes)
    app.add_middleware(
        AccessLogMiddleware,
        metrics=container.metrics if container else _lazy_metrics(app),
    )
    app.add_middleware(CorrelationMiddleware)

    register_exception_handlers(app)

    app.include_router(health.router)
    app.include_router(integration_requests.router)
    app.include_router(providers.router)
    app.include_router(webhooks.router)

    _mount_sandbox(app, settings)
    return app


def _lazy_metrics(app: FastAPI) -> MetricsSink:
    """Provide a metrics sink to middleware constructed before the container exists.

    Middleware is instantiated at application build time, but the container is
    only available once the lifespan has run. This proxy defers the lookup to the
    first metric call rather than forcing the container to be built eagerly.
    """
    from integration_orchestrator.observability.metrics import NullMetrics

    fallback: MetricsSink = NullMetrics()

    class _Proxy:
        def _sink(self) -> MetricsSink:
            container = getattr(app.state, "container", None)
            return container.metrics if container is not None else fallback

        def increment(
            self,
            name: str,
            *,
            labels: Mapping[str, str] | None = None,
            amount: float = 1.0,
        ) -> None:
            self._sink().increment(name, labels=labels, amount=amount)

        def observe(
            self, name: str, value: float, *, labels: Mapping[str, str] | None = None
        ) -> None:
            self._sink().observe(name, value, labels=labels)

        def set_gauge(
            self, name: str, value: float, *, labels: Mapping[str, str] | None = None
        ) -> None:
            self._sink().set_gauge(name, value, labels=labels)

    return _Proxy()


def _mount_sandbox(app: FastAPI, settings: Settings) -> None:
    """Mount the fake provider services when they are enabled.

    Guarded twice: the settings validator rejects a production-like environment
    with the sandbox enabled, and this function checks the flag again before
    importing anything from the sandbox package. Defence in depth for something
    that must never be reachable in a deployed environment.
    """
    if not (settings.provider_sandbox.enabled and settings.provider_sandbox.mount_in_app):
        return
    if settings.environment.is_production_like:  # pragma: no cover - settings reject this first
        logger.error("refusing to mount the provider sandbox in a production-like environment")
        return

    from integration_orchestrator.infrastructure.providers.sandbox.app import create_sandbox_app

    # The sandbox delivers webhooks back into this same process, which is what
    # makes the local stack a genuine end-to-end demonstration.
    callback_base_url = (
        f"{settings.provider_sandbox.callback_base_url.rstrip('/')}{settings.api_root_path}"
    )
    app.mount(
        settings.provider_sandbox.mount_path,
        create_sandbox_app(callback_base_url=callback_base_url),
        name="provider-sandbox",
    )
    logger.warning(
        "the provider sandbox is mounted; this must never happen in a deployed environment",
        extra={"mount_path": settings.provider_sandbox.mount_path},
    )
