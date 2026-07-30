"""Meridian Services adapter against the Meridian sandbox service.

Meridian is the awkward provider on purpose: it ignores idempotency keys, names
the same field differently on different endpoints, and offers no cancellation.
These tests pin down that the adapter absorbs all of that rather than letting it
reach the application layer.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import SecretStr

from integration_orchestrator.domain.contracts import CancelProviderOperationCommand
from integration_orchestrator.domain.enums import NormalizedStatus, OperationType
from integration_orchestrator.domain.errors import (
    ProviderAuthenticationError,
    ProviderNotFoundError,
    UnsupportedOperationError,
)
from integration_orchestrator.domain.value_objects import CorrelationId
from integration_orchestrator.infrastructure.providers.sandbox.scenarios import Scenario
from tests.contract.conftest import SandboxHarness, create_command, provider_settings

pytestmark = [pytest.mark.contract, pytest.mark.asyncio]


async def test_a_created_request_is_accepted_with_a_provider_reference(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("meridian")

    result = await adapter.create_operation(create_command("mref-0001"))

    assert result.accepted is True
    assert result.normalized_status is NormalizedStatus.PENDING
    assert result.provider_reference is not None
    assert result.provider_reference.startswith("mrd-req-")


async def test_the_api_key_travels_in_meridians_own_header(
    sandbox: SandboxHarness,
) -> None:
    """A default ``X-API-Key`` would be silently rejected by this provider."""
    settings = provider_settings("meridian").model_copy(
        update={"api_key": SecretStr("the-wrong-key")}
    )
    adapter = sandbox.adapter("meridian", settings=settings)

    with pytest.raises(ProviderAuthenticationError):
        await adapter.create_operation(create_command("mref-0002"))


async def test_a_rejected_api_key_is_not_retried(sandbox: SandboxHarness) -> None:
    """There is no token to refresh, so a second attempt would fail identically."""
    settings = provider_settings("meridian").model_copy(
        update={"api_key": SecretStr("the-wrong-key")}
    )
    adapter = sandbox.adapter("meridian", settings=settings)

    with pytest.raises(ProviderAuthenticationError):
        await adapter.create_operation(create_command("mref-0003"))

    attempts = [
        outcome
        for name, labels, _ in sandbox.metrics.counters
        if name == "provider_http_requests_total"
        for outcome in [labels.get("outcome")]
    ]
    assert attempts.count("http_401") == 1


async def test_a_repeated_create_genuinely_creates_a_second_request(
    sandbox: SandboxHarness,
) -> None:
    """Meridian has no provider-side deduplication; the platform must supply it."""
    adapter = sandbox.adapter("meridian")
    command = create_command("mref-0004", idempotency_key="stable-key-0004")

    first = await adapter.create_operation(command)
    second = await adapter.create_operation(command)

    assert first.provider_reference != second.provider_reference
    assert len(await sandbox.operations("meridian")) == 2
    assert adapter.descriptor().supports_provider_idempotency is False


async def test_the_status_endpoint_is_read_through_its_other_spelling(
    sandbox: SandboxHarness,
) -> None:
    """Create returns ``requestId``/``status``; status returns ``request_id``/``state``."""
    adapter = sandbox.adapter("meridian")
    created = await adapter.create_operation(create_command("mref-0005"))
    assert created.provider_reference is not None

    status = await adapter.get_operation_status(created.provider_reference)

    assert status.accepted is True
    assert status.normalized_status is NormalizedStatus.SUCCEEDED
    assert status.provider_status == "fulfilled"


async def test_a_failed_request_reports_its_reason_through_the_status_endpoint(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("meridian")
    created = await adapter.create_operation(create_command(f"{Scenario.ASYNC_FAILURE.prefix}0006"))
    assert created.provider_reference is not None

    status = await adapter.get_operation_status(created.provider_reference)

    assert status.accepted is False
    assert status.normalized_status is NormalizedStatus.FAILED
    assert status.error is not None
    assert status.error.provider_code == "downstream_rejected"


async def test_an_unknown_reference_raises_a_not_found_error(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("meridian")

    with pytest.raises(ProviderNotFoundError) as caught:
        await adapter.get_operation_status("mrd-req-99999999")

    assert caught.value.retryable is False


async def test_meridian_does_not_support_cancellation(sandbox: SandboxHarness) -> None:
    adapter = sandbox.adapter("meridian")

    assert adapter.descriptor().supports_cancellation is False
    with pytest.raises(UnsupportedOperationError):
        await adapter.cancel_operation(
            CancelProviderOperationCommand(
                request_id=uuid4(),
                provider=adapter.slug,
                provider_reference="mrd-req-00000001",
                correlation_id=CorrelationId("contract-correlation"),
            )
        )


async def test_meridian_can_grant_and_revoke_access(sandbox: SandboxHarness) -> None:
    adapter = sandbox.adapter("meridian")

    granted = await adapter.create_operation(
        create_command("mref-0007", operation_type=OperationType.ACCESS_GRANT)
    )
    revoked = await adapter.create_operation(
        create_command("mref-0008", operation_type=OperationType.ACCESS_REVOKE)
    )

    assert granted.accepted is True
    assert revoked.accepted is True


async def test_an_operation_meridian_cannot_perform_never_reaches_the_network(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("meridian")

    with pytest.raises(UnsupportedOperationError):
        await adapter.create_operation(
            create_command("mref-0009", operation_type=OperationType.RESOURCE_DEPROVISION)
        )

    assert await sandbox.operations("meridian") == []


async def test_the_health_probe_reports_a_reachable_provider(
    sandbox: SandboxHarness,
) -> None:
    probe = await sandbox.adapter("meridian").health_check()

    assert probe.healthy is True
    assert probe.detail == "operational"


# -- webhooks ---------------------------------------------------------------


async def test_a_genuine_delivery_verifies_and_normalizes(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("meridian")
    created = await adapter.create_operation(create_command("mref-0010"))
    assert created.provider_reference is not None

    webhook = await sandbox.signed_webhook("meridian", created.provider_reference)
    verification = adapter.validate_webhook(webhook)
    event = adapter.normalize_webhook(webhook)

    assert verification.verified is True
    assert event.provider_reference == created.provider_reference
    assert event.external_reference == "mref-0010"
    assert event.normalized_status is NormalizedStatus.SUCCEEDED


async def test_the_signature_covers_the_body_alone(sandbox: SandboxHarness) -> None:
    """Meridian signs no timestamp, which is why event-id deduplication matters."""
    adapter = sandbox.adapter("meridian")
    created = await adapter.create_operation(create_command("mref-0011"))
    assert created.provider_reference is not None
    webhook = await sandbox.signed_webhook("meridian", created.provider_reference)

    verification = adapter.validate_webhook(webhook)

    assert verification.verified is True
    assert verification.signature_metadata.timestamp is None
    assert verification.signature_metadata.scheme == "hmac-sha256-body"


async def test_a_tampered_body_fails_verification(sandbox: SandboxHarness) -> None:
    adapter = sandbox.adapter("meridian")
    created = await adapter.create_operation(create_command("mref-0012"))
    assert created.provider_reference is not None
    webhook = await sandbox.signed_webhook("meridian", created.provider_reference)

    tampered = type(webhook)(
        provider=webhook.provider,
        headers=webhook.headers,
        body=webhook.body.replace(b"mref-0012", b"mref-9999"),
        received_at=webhook.received_at,
    )

    assert adapter.validate_webhook(tampered).verified is False


async def test_a_signature_from_another_secret_is_refused(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("meridian")
    created = await adapter.create_operation(create_command("mref-0013"))
    assert created.provider_reference is not None
    webhook = await sandbox.signed_webhook("meridian", created.provider_reference)

    other = sandbox.adapter(
        "meridian",
        settings=provider_settings("meridian").model_copy(
            update={"webhook_secret": SecretStr("a-different-shared-secret")}
        ),
    )

    assert adapter.validate_webhook(webhook).verified is True
    assert other.validate_webhook(webhook).verified is False
