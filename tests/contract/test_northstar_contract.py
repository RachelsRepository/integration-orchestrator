"""Northstar Connect adapter against the Northstar sandbox service."""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from integration_orchestrator.domain.enums import NormalizedStatus, OperationType
from integration_orchestrator.domain.errors import (
    ProviderRateLimitError,
    ProviderUnavailableError,
    ProviderValidationError,
    UnsupportedOperationError,
)
from integration_orchestrator.infrastructure.providers.sandbox.scenarios import (
    RATE_LIMIT_RETRY_AFTER_SECONDS,
    Scenario,
)
from integration_orchestrator.observability.redaction import REDACTED
from tests.contract.conftest import SandboxHarness, create_command, provider_settings

pytestmark = [pytest.mark.contract, pytest.mark.asyncio]


async def test_a_created_operation_is_accepted_with_a_provider_reference(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("northstar")

    result = await adapter.create_operation(create_command("ref-0001"))

    assert result.accepted is True
    assert result.normalized_status is NormalizedStatus.ACCEPTED
    assert result.provider_reference is not None
    assert result.provider_reference.startswith("ns-op-")
    assert result.provider_status == "queued"


async def test_the_oauth_token_is_obtained_once_and_reused(
    sandbox: SandboxHarness,
) -> None:
    """A token per request would multiply load on the provider's auth service."""
    adapter = sandbox.adapter("northstar")

    await adapter.create_operation(create_command("ref-0002", idempotency_key="key-aaaa1111"))
    await adapter.create_operation(create_command("ref-0003", idempotency_key="key-bbbb2222"))

    cached = await sandbox.token_cache.get(adapter.slug)
    assert cached is not None
    assert cached.value.startswith("sandbox.northstar.")


async def test_the_idempotency_key_collapses_a_repeated_create(
    sandbox: SandboxHarness,
) -> None:
    """Northstar deduplicates, so a retried attempt must not create a second operation."""
    adapter = sandbox.adapter("northstar")
    command = create_command("ref-0004", idempotency_key="stable-key-0004")

    first = await adapter.create_operation(command)
    second = await adapter.create_operation(command)

    assert second.provider_reference == first.provider_reference
    assert second.raw_response_metadata["deduplicated"] is True
    assert len(await sandbox.operations("northstar")) == 1


async def test_a_rejected_request_raises_a_non_retryable_error(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("northstar")

    with pytest.raises(ProviderValidationError) as caught:
        await adapter.create_operation(create_command(f"{Scenario.REJECT.prefix}0005"))

    assert caught.value.retryable is False
    assert caught.value.provider_code == "invalid_request"


async def test_rate_limiting_surfaces_the_providers_own_retry_after(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("northstar")

    with pytest.raises(ProviderRateLimitError) as caught:
        await adapter.create_operation(create_command(f"{Scenario.RATE_LIMIT.prefix}0006"))

    assert caught.value.retryable is True
    assert caught.value.retry_after_seconds == RATE_LIMIT_RETRY_AFTER_SECONDS


async def test_an_unavailable_provider_raises_a_retryable_error(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("northstar")

    with pytest.raises(ProviderUnavailableError) as caught:
        await adapter.create_operation(create_command(f"{Scenario.ALWAYS_UNAVAILABLE.prefix}0007"))

    assert caught.value.retryable is True


async def test_a_rejected_token_is_refreshed_and_the_call_retried_once(
    sandbox: SandboxHarness,
) -> None:
    """The usual cause of a 401 is a rotated token, not a wrong client."""
    adapter = sandbox.adapter("northstar")

    result = await adapter.create_operation(create_command(f"{Scenario.AUTH_CHALLENGE.prefix}0008"))

    assert result.accepted is True
    assert sandbox.token_cache.invalidations == ["northstar"]


async def test_an_acceptance_without_a_reference_is_reported_as_such(
    sandbox: SandboxHarness,
) -> None:
    """Calling this a failure would invite a retry that creates a second operation.

    The provider answered 2xx, so the operation probably exists. The adapter
    reports the acceptance honestly, without a reference, and the dispatcher
    escalates it rather than guessing either way.
    """
    adapter = sandbox.adapter("northstar")

    result = await adapter.create_operation(create_command(f"{Scenario.NO_REFERENCE.prefix}0009"))

    assert result.accepted is True
    assert result.provider_reference is None
    assert result.raw_response_metadata["missing_provider_reference"] is True


async def test_a_status_string_the_adapter_does_not_know_becomes_unknown(
    sandbox: SandboxHarness,
) -> None:
    """A new provider state must never be guessed into meaning success."""
    adapter = sandbox.adapter("northstar")

    result = await adapter.create_operation(create_command(f"{Scenario.UNKNOWN_STATUS.prefix}0010"))

    assert result.accepted is True
    assert result.normalized_status is NormalizedStatus.UNKNOWN
    assert result.provider_status == "awaiting_downstream_review"


async def test_an_operation_northstar_cannot_perform_never_reaches_the_network(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("northstar")

    with pytest.raises(UnsupportedOperationError):
        await adapter.create_operation(
            create_command("ref-0011", operation_type=OperationType.ACCESS_GRANT)
        )

    assert await sandbox.operations("northstar") == []


async def test_northstar_offers_no_status_lookup_or_cancellation(
    sandbox: SandboxHarness,
) -> None:
    """Reconciliation must escalate rather than guess for this provider."""
    adapter = sandbox.adapter("northstar")
    descriptor = adapter.descriptor()

    assert descriptor.supports_status_lookup is False
    assert descriptor.supports_cancellation is False
    assert descriptor.supports_provider_idempotency is True

    with pytest.raises(UnsupportedOperationError):
        await adapter.get_operation_status("ns-op-00000001")


async def test_credentials_never_appear_in_the_recorded_metadata(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("northstar")

    result = await adapter.create_operation(
        create_command("ref-0012", payload={"region": "eu-west-1", "api_key": "super-secret"})
    )

    recorded = result.raw_response_metadata["provider_request"]
    assert recorded["attributes"]["api_key"] == REDACTED


async def test_a_wrong_client_secret_fails_authentication_without_calling_the_api(
    sandbox: SandboxHarness,
) -> None:
    settings = provider_settings("northstar").model_copy(
        update={"client_secret": SecretStr("not-the-right-secret")}
    )
    adapter = sandbox.adapter("northstar", settings=settings)

    with pytest.raises(Exception) as caught:
        await adapter.create_operation(create_command("ref-0013"))

    assert "credentials" in str(caught.value)
    assert await sandbox.operations("northstar") == []


async def test_the_health_probe_reports_a_reachable_provider(
    sandbox: SandboxHarness,
) -> None:
    probe = await sandbox.adapter("northstar").health_check()

    assert probe.healthy is True
    assert probe.detail == "ok"


# -- webhooks ---------------------------------------------------------------


async def test_a_genuine_delivery_verifies_and_normalizes(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("northstar")
    result = await adapter.create_operation(create_command("ref-0014"))
    assert result.provider_reference is not None

    webhook = await sandbox.signed_webhook("northstar", result.provider_reference)
    verification = adapter.validate_webhook(webhook)
    event = adapter.normalize_webhook(webhook)

    assert verification.verified is True
    assert verification.signature_metadata.scheme == "hmac-sha256-timestamped"
    assert event.provider_reference == result.provider_reference
    assert event.external_reference == "ref-0014"
    assert event.normalized_status is NormalizedStatus.SUCCEEDED


async def test_a_tampered_body_fails_verification(sandbox: SandboxHarness) -> None:
    adapter = sandbox.adapter("northstar")
    result = await adapter.create_operation(create_command("ref-0015"))
    assert result.provider_reference is not None
    webhook = await sandbox.signed_webhook("northstar", result.provider_reference)

    tampered = webhook.body.replace(b'"state":"complete"', b'"state":"error"')
    assert tampered != webhook.body
    verification = adapter.validate_webhook(
        type(webhook)(
            provider=webhook.provider,
            headers=webhook.headers,
            body=tampered,
            received_at=webhook.received_at,
        )
    )

    assert verification.verified is False
    assert verification.reason == "the signature does not match"


async def test_a_delivery_without_a_signature_is_refused(sandbox: SandboxHarness) -> None:
    adapter = sandbox.adapter("northstar")
    result = await adapter.create_operation(create_command("ref-0016"))
    assert result.provider_reference is not None
    webhook = await sandbox.signed_webhook("northstar", result.provider_reference)

    stripped = {
        key: value for key, value in webhook.headers.items() if key != "x-northstar-signature"
    }
    verification = adapter.validate_webhook(
        type(webhook)(
            provider=webhook.provider,
            headers=stripped,
            body=webhook.body,
            received_at=webhook.received_at,
        )
    )

    assert verification.verified is False


async def test_a_failure_webhook_carries_the_providers_reason(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("northstar")
    result = await adapter.create_operation(create_command(f"{Scenario.ASYNC_FAILURE.prefix}0017"))
    assert result.provider_reference is not None

    webhook = await sandbox.signed_webhook(
        "northstar", result.provider_reference, event_type="operation.failed"
    )
    event = adapter.normalize_webhook(webhook)

    assert event.normalized_status is NormalizedStatus.FAILED
    assert event.error is not None
    assert event.error.provider_code == "downstream_rejected"
    assert event.error.retryable is False
