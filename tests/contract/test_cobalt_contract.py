"""Cobalt Network adapter against the Cobalt sandbox service.

Cobalt is the fully asynchronous provider: it accepts jobs, completes them later,
signs its webhooks asymmetrically, and is the only one that can cancel work in
flight.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest

from integration_orchestrator.domain.contracts import CancelProviderOperationCommand
from integration_orchestrator.domain.enums import NormalizedStatus, OperationType
from integration_orchestrator.domain.value_objects import CorrelationId, ProviderSlug
from integration_orchestrator.infrastructure.providers.sandbox.scenarios import Scenario
from integration_orchestrator.infrastructure.providers.sandbox.signing import COBALT_KEY_ID
from tests.contract.conftest import SandboxHarness, create_command, provider_settings

pytestmark = [pytest.mark.contract, pytest.mark.asyncio]


def cancel_command(reference: str, *, reason: str | None = None) -> CancelProviderOperationCommand:
    return CancelProviderOperationCommand(
        request_id=uuid4(),
        provider=ProviderSlug("cobalt"),
        provider_reference=reference,
        correlation_id=CorrelationId("contract-correlation"),
        reason=reason,
    )


async def test_a_created_job_is_accepted_never_completed(sandbox: SandboxHarness) -> None:
    """Cobalt has no synchronous success, so a request always passes through pending."""
    adapter = sandbox.adapter("cobalt")

    result = await adapter.create_operation(create_command("cref-0001"))

    assert result.accepted is True
    assert result.normalized_status is NormalizedStatus.ACCEPTED
    assert result.provider_reference is not None
    assert result.provider_reference.startswith("cbl-job-")


async def test_the_idempotency_key_collapses_a_repeated_create(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("cobalt")
    command = create_command("cref-0002", idempotency_key="stable-key-0002")

    first = await adapter.create_operation(command)
    second = await adapter.create_operation(command)

    assert first.provider_reference == second.provider_reference
    assert len(await sandbox.operations("cobalt")) == 1


async def test_cobalt_accepts_every_operation_type(sandbox: SandboxHarness) -> None:
    adapter = sandbox.adapter("cobalt")

    for index, operation_type in enumerate(OperationType):
        result = await adapter.create_operation(
            create_command(
                f"cref-op-{index}",
                operation_type=operation_type,
                idempotency_key=f"key-operation-{index:04d}",
            )
        )
        assert result.accepted is True


async def test_the_status_endpoint_reports_the_eventual_outcome(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("cobalt")
    created = await adapter.create_operation(create_command("cref-0003"))
    assert created.provider_reference is not None

    status = await adapter.get_operation_status(created.provider_reference)

    assert status.normalized_status is NormalizedStatus.SUCCEEDED
    assert status.provider_status == "succeeded"


async def test_a_failed_job_reports_its_failure_through_the_status_endpoint(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("cobalt")
    created = await adapter.create_operation(create_command(f"{Scenario.ASYNC_FAILURE.prefix}0004"))
    assert created.provider_reference is not None

    status = await adapter.get_operation_status(created.provider_reference)

    assert status.accepted is False
    assert status.normalized_status is NormalizedStatus.FAILED
    assert status.error is not None
    assert status.error.provider_code == "downstream_rejected"


async def test_a_terminal_status_on_creation_is_treated_as_unknown(
    sandbox: SandboxHarness,
) -> None:
    """When the adapter's model and the provider's behaviour disagree, trust neither."""
    adapter = sandbox.adapter("cobalt")

    result = await adapter.create_operation(create_command(f"{Scenario.UNKNOWN_STATUS.prefix}0005"))

    assert result.normalized_status is NormalizedStatus.UNKNOWN
    assert result.provider_status == "quarantined"


async def test_an_in_flight_job_can_be_cancelled(sandbox: SandboxHarness) -> None:
    adapter = sandbox.adapter("cobalt")
    created = await adapter.create_operation(
        create_command(f"{Scenario.UNKNOWN_STATUS.prefix}0006")
    )
    assert created.provider_reference is not None

    result = await adapter.cancel_operation(cancel_command(created.provider_reference))

    assert result.accepted is True
    assert result.normalized_status is NormalizedStatus.CANCELLED


async def test_cancelling_a_finished_job_is_refused_not_failed(
    sandbox: SandboxHarness,
) -> None:
    """The job ran. Reporting that as an error would misdescribe what happened."""
    adapter = sandbox.adapter("cobalt")
    created = await adapter.create_operation(create_command("cref-0007"))
    assert created.provider_reference is not None

    result = await adapter.cancel_operation(cancel_command(created.provider_reference))

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "provider_cancellation_refused"
    assert result.error.retryable is False


async def test_the_health_probe_reports_a_reachable_provider(
    sandbox: SandboxHarness,
) -> None:
    probe = await sandbox.adapter("cobalt").health_check()

    assert probe.healthy is True


# -- webhooks ---------------------------------------------------------------


async def test_a_genuine_delivery_verifies_against_the_public_key(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("cobalt")
    created = await adapter.create_operation(create_command("cref-0008"))
    assert created.provider_reference is not None

    webhook = await sandbox.signed_webhook("cobalt", created.provider_reference)
    verification = adapter.validate_webhook(webhook)
    event = adapter.normalize_webhook(webhook)

    assert verification.verified is True
    assert verification.signature_metadata.algorithm == "ed25519"
    assert verification.signature_metadata.key_id == COBALT_KEY_ID
    assert event.normalized_status is NormalizedStatus.SUCCEEDED
    assert event.external_reference == "cref-0008"


async def test_a_delivery_without_a_key_identifier_is_refused(
    sandbox: SandboxHarness,
) -> None:
    """The key id is part of the signed material, so rotation depends on it."""
    adapter = sandbox.adapter("cobalt")
    created = await adapter.create_operation(create_command("cref-0009"))
    assert created.provider_reference is not None
    webhook = await sandbox.signed_webhook("cobalt", created.provider_reference)

    stripped = type(webhook)(
        provider=webhook.provider,
        headers={k: v for k, v in webhook.headers.items() if k != "x-cobalt-key-id"},
        body=webhook.body,
        received_at=webhook.received_at,
    )

    assert adapter.validate_webhook(stripped).verified is False


async def test_a_substituted_key_identifier_breaks_the_signature(
    sandbox: SandboxHarness,
) -> None:
    adapter = sandbox.adapter("cobalt")
    created = await adapter.create_operation(create_command("cref-0010"))
    assert created.provider_reference is not None
    webhook = await sandbox.signed_webhook("cobalt", created.provider_reference)

    swapped = type(webhook)(
        provider=webhook.provider,
        headers={**webhook.headers, "x-cobalt-key-id": "cobalt-sandbox-2099-99"},
        body=webhook.body,
        received_at=webhook.received_at,
    )

    assert adapter.validate_webhook(swapped).verified is False


async def test_a_delivery_is_refused_when_no_public_key_is_configured(
    sandbox: SandboxHarness,
) -> None:
    """Unverifiable is not the same as trusted."""
    adapter = sandbox.adapter("cobalt")
    created = await adapter.create_operation(create_command("cref-0011"))
    assert created.provider_reference is not None
    webhook = await sandbox.signed_webhook("cobalt", created.provider_reference)

    unconfigured = sandbox.adapter(
        "cobalt",
        settings=provider_settings("cobalt").model_copy(update={"webhook_public_key": None}),
    )

    verification = unconfigured.validate_webhook(webhook)
    assert verification.verified is False
    assert verification.reason == "no webhook public key is configured"


async def test_a_stale_delivery_is_refused_on_freshness(sandbox: SandboxHarness) -> None:
    """The timestamp is signed, so a capture cannot be made to look recent."""
    adapter = sandbox.adapter("cobalt")
    created = await adapter.create_operation(create_command("cref-0012"))
    assert created.provider_reference is not None
    webhook = await sandbox.signed_webhook("cobalt", created.provider_reference)

    late = type(webhook)(
        provider=webhook.provider,
        headers=webhook.headers,
        body=webhook.body,
        received_at=webhook.received_at + timedelta(hours=2),
    )

    verification = adapter.validate_webhook(late)
    assert verification.verified is False
    assert verification.reason == "the timestamp is outside the replay window"
