"""Prove the API and worker entrypoints import and assemble cleanly.

These are compilation and wiring checks, not behavioural tests. A missing
export, a circular import, or a type-only symbol used at runtime fails here
before Compose ever starts.
"""

from __future__ import annotations

import importlib

import pytest

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "module_name",
    [
        "integration_orchestrator.api.app",
        "integration_orchestrator.composition",
        "integration_orchestrator.workers.runner",
        "integration_orchestrator.workers.outbox_publisher",
        "integration_orchestrator.workers.retry_worker",
        "integration_orchestrator.workers.webhook_processor",
        "integration_orchestrator.workers.reconciliation_worker",
        "integration_orchestrator.cli",
        "integration_orchestrator.infrastructure.providers.registry",
        "integration_orchestrator.infrastructure.redis.webhook_replay",
    ],
)
def test_runtime_modules_import(module_name: str) -> None:
    module = importlib.import_module(module_name)
    assert module is not None


def test_create_app_factory_builds_without_starting_lifespan() -> None:
    from integration_orchestrator.api.app import create_app
    from integration_orchestrator.config.settings import (
        Environment,
        KafkaSettings,
        ProviderSandboxSettings,
        Settings,
        reset_settings_cache,
    )

    reset_settings_cache()
    settings = Settings(
        environment=Environment.TEST,
        kafka=KafkaSettings(enabled=False),
        provider_sandbox=ProviderSandboxSettings(enabled=False, mount_in_app=False),
    )
    app = create_app(settings=settings)
    assert app.title == "Integration Orchestrator"

    def _collect_paths(routes: object) -> set[str]:
        found: set[str] = set()
        for route in routes:  # type: ignore[attr-defined]
            path = getattr(route, "path", None)
            if isinstance(path, str):
                found.add(path)
            nested = getattr(route, "routes", None)
            if nested is not None:
                found.update(_collect_paths(nested))
            original = getattr(route, "original_router", None)
            if original is not None and getattr(original, "routes", None) is not None:
                found.update(_collect_paths(original.routes))
        return found

    paths = _collect_paths(app.router.routes)
    assert "/health/live" in paths
    assert "/health/ready" in paths
    assert "/api/v1/integration-requests" in paths


def test_composition_always_binds_sqlalchemy_unit_of_work() -> None:
    """Memory UoW is a test double; production wiring must never select it."""
    from integration_orchestrator.composition import build_container
    from integration_orchestrator.config.settings import (
        Environment,
        KafkaSettings,
        ProviderSandboxSettings,
        Settings,
        reset_settings_cache,
    )
    from integration_orchestrator.infrastructure.db.unit_of_work import SqlUnitOfWorkFactory

    reset_settings_cache()
    settings = Settings(
        environment=Environment.TEST,
        kafka=KafkaSettings(enabled=False),
        provider_sandbox=ProviderSandboxSettings(enabled=False, mount_in_app=False),
    )

    async def _build() -> None:
        container = await build_container(settings)
        try:
            assert isinstance(container.uow_factory, SqlUnitOfWorkFactory)
        finally:
            await container.shutdown()

    import asyncio

    asyncio.run(_build())
