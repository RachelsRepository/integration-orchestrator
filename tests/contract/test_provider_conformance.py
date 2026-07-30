"""Rules every provider adapter must obey, whatever its provider does.

These are the guarantees the application layer is written against. If a fourth
provider were added tomorrow, this file is what would tell its author whether the
adapter is finished.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from integration_orchestrator.config.settings import Environment, Settings
from integration_orchestrator.domain.contracts import InboundWebhook
from integration_orchestrator.domain.enums import NormalizedStatus
from integration_orchestrator.domain.errors import WebhookPayloadError
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.providers.registry import (
    ADAPTER_FACTORIES,
    build_provider_registry,
)
from integration_orchestrator.infrastructure.providers.resilient import ResilientProviderGateway
from integration_orchestrator.infrastructure.resilience.bulkhead import ProviderBulkhead
from tests.contract.conftest import SandboxHarness, create_command, provider_settings
from tests.support.doubles import (
    AllowAllRateLimiter,
    MemoryCircuitBreaker,
    MemoryTokenCache,
    NullLockManager,
    RecordingMetrics,
)

pytestmark = [pytest.mark.contract, pytest.mark.asyncio]

PROVIDERS = ["northstar", "meridian", "cobalt"]

#: The only facts an adapter may publish about an exchange. Anything else risks
#: carrying a provider response body into audit rows and events.
ALLOWED_METADATA_KEYS = frozenset(
    {
        "http_status",
        "latency_ms",
        "provider_request_id",
        "provider_request",
        "deduplicated",
        "missing_provider_reference",
    }
)


@pytest.mark.parametrize("slug", PROVIDERS)
async def test_the_descriptor_describes_the_adapter_it_belongs_to(
    sandbox: SandboxHarness, slug: str
) -> None:
    descriptor = sandbox.adapter(slug).descriptor()

    assert descriptor.slug == ProviderSlug(slug)
    assert descriptor.display_name
    assert descriptor.supported_operations
    assert descriptor.max_attempts >= 1
    assert descriptor.total_timeout_seconds > 0


@pytest.mark.parametrize("slug", PROVIDERS)
async def test_an_accepted_creation_always_names_the_operation(
    sandbox: SandboxHarness, slug: str
) -> None:
    """Without a provider reference there is nothing to reconcile or cancel against."""
    adapter = sandbox.adapter(slug)
    operation_type = next(iter(sorted(adapter.supported_operations, key=lambda op: op.value)))

    result = await adapter.create_operation(
        create_command(f"{slug}-conformance-1", operation_type=operation_type)
    )

    assert result.accepted is True
    assert result.provider_reference


@pytest.mark.parametrize("slug", PROVIDERS)
async def test_only_audit_safe_facts_are_recorded_about_an_exchange(
    sandbox: SandboxHarness, slug: str
) -> None:
    adapter = sandbox.adapter(slug)
    operation_type = next(iter(sorted(adapter.supported_operations, key=lambda op: op.value)))

    result = await adapter.create_operation(
        create_command(f"{slug}-conformance-2", operation_type=operation_type)
    )

    assert set(result.raw_response_metadata) <= ALLOWED_METADATA_KEYS


@pytest.mark.parametrize("slug", PROVIDERS)
async def test_an_unsigned_delivery_is_never_accepted(sandbox: SandboxHarness, slug: str) -> None:
    adapter = sandbox.adapter(slug)
    webhook = InboundWebhook(
        provider=ProviderSlug(slug),
        headers={"content-type": "application/json"},
        body=b'{"event_id":"forged","event_type":"operation.completed"}',
        received_at=datetime.now(tz=UTC),
    )

    verification = adapter.validate_webhook(webhook)

    assert verification.verified is False
    assert verification.reason
    assert verification.signature_metadata.verified is False


@pytest.mark.parametrize("slug", PROVIDERS)
async def test_a_body_that_is_not_json_is_rejected_rather_than_guessed(
    sandbox: SandboxHarness, slug: str
) -> None:
    adapter = sandbox.adapter(slug)
    webhook = InboundWebhook(
        provider=ProviderSlug(slug),
        headers={},
        body=b"this is not json",
        received_at=datetime.now(tz=UTC),
    )

    with pytest.raises(WebhookPayloadError):
        adapter.normalize_webhook(webhook)


@pytest.mark.parametrize("slug", PROVIDERS)
async def test_a_body_missing_its_event_identity_is_rejected(
    sandbox: SandboxHarness, slug: str
) -> None:
    """Without an event id there is no way to deduplicate a replayed delivery."""
    adapter = sandbox.adapter(slug)
    webhook = InboundWebhook(
        provider=ProviderSlug(slug),
        headers={},
        body=b'{"status":"succeeded"}',
        received_at=datetime.now(tz=UTC),
    )

    with pytest.raises(WebhookPayloadError):
        adapter.normalize_webhook(webhook)


@pytest.mark.parametrize("slug", PROVIDERS)
async def test_a_normalized_event_uses_only_the_shared_vocabulary(
    sandbox: SandboxHarness, slug: str
) -> None:
    adapter = sandbox.adapter(slug)
    operation_type = next(iter(sorted(adapter.supported_operations, key=lambda op: op.value)))
    created = await adapter.create_operation(
        create_command(f"{slug}-conformance-3", operation_type=operation_type)
    )
    assert created.provider_reference is not None

    webhook = await sandbox.signed_webhook(slug, created.provider_reference)
    event = adapter.normalize_webhook(webhook)

    assert event.provider == ProviderSlug(slug)
    assert isinstance(event.normalized_status, NormalizedStatus)
    assert event.provider_event_id
    assert event.occurred_at.tzinfo is not None


@pytest.mark.parametrize("slug", PROVIDERS)
async def test_a_delivery_signed_for_one_provider_does_not_verify_at_another(
    sandbox: SandboxHarness, slug: str
) -> None:
    """Each provider's material must be scoped to that provider alone."""
    adapter = sandbox.adapter(slug)
    operation_type = next(iter(sorted(adapter.supported_operations, key=lambda op: op.value)))
    created = await adapter.create_operation(
        create_command(f"{slug}-conformance-4", operation_type=operation_type)
    )
    assert created.provider_reference is not None
    webhook = await sandbox.signed_webhook(slug, created.provider_reference)

    for other_slug in PROVIDERS:
        if other_slug == slug:
            continue
        other = sandbox.adapter(other_slug)
        assert other.validate_webhook(webhook).verified is False


async def test_every_configured_provider_is_registered_and_wrapped(
    sandbox: SandboxHarness,
) -> None:
    """Registration is where resilience is applied, so nothing can bypass it."""
    settings = Settings(
        environment=Environment.TEST,
        providers={slug: provider_settings(slug) for slug in PROVIDERS},
    )

    registry = build_provider_registry(
        settings,
        http_client=sandbox.client,
        token_cache=MemoryTokenCache(),
        locks=NullLockManager(),
        circuit_breaker=MemoryCircuitBreaker(),
        rate_limiter=AllowAllRateLimiter(),
        bulkhead=ProviderBulkhead(settings.enabled_providers(), metrics=RecordingMetrics()),
        metrics=RecordingMetrics(),
    )

    assert registry.slugs() == sorted(PROVIDERS)
    for gateway in registry.all():
        assert isinstance(gateway, ResilientProviderGateway)


async def test_the_registry_is_the_only_place_a_slug_is_bound_to_an_implementation() -> None:
    """Adding a provider must not require touching the domain or a use case."""
    assert set(ADAPTER_FACTORIES) == set(PROVIDERS)
