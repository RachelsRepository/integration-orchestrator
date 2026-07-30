"""Webhook delivery, from the provider's signature to the completed request."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from integration_orchestrator.domain.enums import RequestStatus
from integration_orchestrator.workers.outbox_publisher import OutboxPublisherWorker
from integration_orchestrator.workers.webhook_processor import WebhookProcessorWorker
from tests.e2e.conftest import SANDBOX_ROOT, Harness
from tests.support.builders import make_request

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


async def test_a_verified_webhook_completes_the_request(harness: Harness) -> None:
    created = (await harness.create_request(external_reference="wh-0001")).json()

    delivery = await harness.deliver_webhook("northstar", created["provider_reference"])

    assert delivery.status_code == 200
    assert delivery.json()["status"] == "processed"
    assert (await harness.fetch(created["id"]))["status"] == "succeeded"


async def test_completion_is_audited_and_published(
    harness: Harness, outbox_worker: OutboxPublisherWorker
) -> None:
    created = (await harness.create_request(external_reference="wh-0002")).json()
    await harness.deliver_webhook("northstar", created["provider_reference"])

    await outbox_worker.run_once()

    assert "webhook.applied" in await harness.audit_actions(created["id"])
    assert "integration.request.succeeded.v1" in harness.published_event_types()


async def test_a_forged_signature_is_refused(harness: Harness) -> None:
    created = (await harness.create_request(external_reference="wh-0003")).json()
    signed = await harness.sandbox.post(
        f"{SANDBOX_ROOT}/_control/northstar/emit/{created['provider_reference']}"
    )
    delivery = signed.json()

    response = await harness.api.post(
        "/webhooks/northstar",
        content=delivery["body"].encode("utf-8"),
        headers={**delivery["headers"], "x-northstar-signature": "sha256=forged"},
    )

    assert response.status_code == 401
    assert (await harness.fetch(created["id"]))["status"] == "pending"


async def test_a_tampered_body_is_refused(harness: Harness) -> None:
    """The signature covers the body, so an edited payload cannot be applied."""
    created = (await harness.create_request(external_reference="wh-0004")).json()
    signed = (
        await harness.sandbox.post(
            f"{SANDBOX_ROOT}/_control/northstar/emit/{created['provider_reference']}"
        )
    ).json()

    response = await harness.api.post(
        "/webhooks/northstar",
        content=signed["body"].replace("complete", "error").encode("utf-8"),
        headers=signed["headers"],
    )

    assert response.status_code == 401
    assert (await harness.fetch(created["id"]))["status"] == "pending"


async def test_a_redelivered_webhook_is_recognised_as_a_duplicate(
    harness: Harness,
) -> None:
    """Providers redeliver. Applying the same completion twice must be impossible."""
    created = (await harness.create_request(external_reference="wh-0005")).json()
    signed = (
        await harness.sandbox.post(
            f"{SANDBOX_ROOT}/_control/northstar/emit/{created['provider_reference']}"
        )
    ).json()
    body = signed["body"].encode("utf-8")

    first = await harness.api.post("/webhooks/northstar", content=body, headers=signed["headers"])
    second = await harness.api.post("/webhooks/northstar", content=body, headers=signed["headers"])

    assert first.status_code == 200
    # 2xx on purpose: a non-2xx would make the provider redeliver an event that
    # was already handled correctly.
    assert second.status_code == 202
    assert second.json()["status"] == "duplicate"
    assert (await harness.fetch(created["id"]))["status"] == "succeeded"


async def test_a_webhook_for_an_unknown_operation_is_kept_not_discarded(
    harness: Harness,
) -> None:
    """A verified webhook may be the only notification the provider ever sends."""
    orphan = await harness.sandbox.post(
        f"{SANDBOX_ROOT}/northstar/operations",
        json={"reference": "wh-0006", "operation": "provision"},
        headers=await _northstar_bearer(harness),
    )
    operation_id = orphan.json()["operation_id"]

    response = await harness.deliver_webhook("northstar", operation_id)

    assert response.status_code == 202
    assert response.json()["status"] == "deferred"
    assert len(harness.store.receipts) == 1


async def test_a_deferred_webhook_is_applied_once_its_request_appears(
    harness: Harness, webhook_worker: WebhookProcessorWorker
) -> None:
    """The webhook-before-response race, resolved by the deferred processor."""
    orphan = await harness.sandbox.post(
        f"{SANDBOX_ROOT}/northstar/operations",
        json={"reference": "wh-0007", "operation": "provision"},
        headers=await _northstar_bearer(harness),
    )
    operation_id = orphan.json()["operation_id"]
    await harness.deliver_webhook("northstar", operation_id)

    # The dispatch that created this operation commits after the webhook landed.
    late = make_request(
        external_reference="wh-0007",
        status=RequestStatus.PENDING,
        provider_reference=operation_id,
        now=datetime.now(tz=UTC),
        attempt_count=1,
    )
    harness.store.requests[late.id] = late

    processed = await webhook_worker.run_once()

    assert processed == 1
    assert harness.store.requests[late.id].status is RequestStatus.SUCCEEDED


async def test_an_oversized_body_is_rejected_before_it_is_parsed(
    harness: Harness,
) -> None:
    response = await harness.api.post(
        "/webhooks/northstar",
        content=b"x" * (harness.settings.webhooks.max_body_bytes + 1),
        headers={"content-type": "application/json"},
    )

    assert response.status_code in (400, 413)


async def test_a_webhook_needs_no_bearer_token(harness: Harness) -> None:
    """Providers cannot present our tokens; the signature is the authentication."""
    created = (await harness.create_request(external_reference="wh-0008")).json()

    delivery = await harness.deliver_webhook("northstar", created["provider_reference"])

    assert "authorization" not in {key.lower() for key in delivery.request.headers}
    assert delivery.status_code == 200


async def test_a_failure_webhook_moves_the_request_to_failed(harness: Harness) -> None:
    created = (
        await harness.create_request(external_reference="scenario-async-failure-wh-0009")
    ).json()

    await harness.deliver_webhook(
        "northstar", created["provider_reference"], event_type="operation.failed"
    )

    fetched = await harness.fetch(created["id"])
    assert fetched["status"] == "failed"
    assert fetched["last_failure"]["code"] == "provider_reported_failure"


async def test_a_cobalt_job_completes_through_its_signed_webhook(
    harness: Harness,
) -> None:
    """Every provider's own signing scheme has to work end to end, not just one."""
    created = (await harness.create_request(provider="cobalt", external_reference="wh-0010")).json()

    delivery = await harness.deliver_webhook("cobalt", created["provider_reference"])

    assert delivery.status_code == 200
    assert (await harness.fetch(created["id"]))["status"] == "succeeded"


async def test_a_meridian_request_completes_through_its_signed_webhook(
    harness: Harness,
) -> None:
    created = (
        await harness.create_request(provider="meridian", external_reference="wh-0011")
    ).json()

    delivery = await harness.deliver_webhook("meridian", created["provider_reference"])

    assert delivery.status_code == 200
    assert (await harness.fetch(created["id"]))["status"] == "succeeded"


async def _northstar_bearer(harness: Harness) -> dict[str, str]:
    """Obtain a sandbox token so a test can create an operation behind the platform."""
    response = await harness.sandbox.post(
        f"{SANDBOX_ROOT}/northstar/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "northstar-local-client",
            "client_secret": "northstar-local-secret",
        },
    )
    response.raise_for_status()
    return {"Authorization": f"Bearer {response.json()['access_token']}"}
