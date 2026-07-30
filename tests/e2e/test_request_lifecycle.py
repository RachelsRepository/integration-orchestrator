"""The path a request takes from an API call to a published event."""

from __future__ import annotations

import pytest

from integration_orchestrator.workers.outbox_publisher import OutboxPublisherWorker
from tests.e2e.conftest import Harness

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


async def test_a_request_is_created_dispatched_and_tracked(harness: Harness) -> None:
    response = await harness.create_request(external_reference="e2e-0001")

    assert response.status_code == 201
    body = response.json()
    assert body["provider"] == "northstar"
    assert body["status"] == "pending"
    assert body["provider_reference"].startswith("ns-op-")
    assert response.headers["Location"] == f"/api/v1/integration-requests/{body['id']}"


async def test_the_request_can_be_read_back_and_listed(harness: Harness) -> None:
    created = (await harness.create_request(external_reference="e2e-0002")).json()

    fetched = await harness.fetch(created["id"])
    listed = await harness.api.get(
        "/api/v1/integration-requests",
        params={"provider": "northstar", "external_reference": "e2e-0002"},
        headers=harness.auth(),
    )

    assert fetched["id"] == created["id"]
    assert [item["id"] for item in listed.json()["items"]] == [created["id"]]


async def test_every_state_change_leaves_an_audit_trail(harness: Harness) -> None:
    created = (await harness.create_request(external_reference="e2e-0003")).json()

    actions = await harness.audit_actions(created["id"])

    assert actions[0] == "request.received"
    assert "dispatch.attempted" in actions
    assert "provider.accepted" in actions


async def test_the_correlation_id_survives_the_whole_call(harness: Harness) -> None:
    response = await harness.api.post(
        "/api/v1/integration-requests",
        json={
            "provider": "northstar",
            "operation_type": "resource_provision",
            "external_reference": "e2e-0004",
            "payload": {"resource_type": "database"},
        },
        headers={**harness.auth(), "X-Correlation-ID": "caller-supplied-correlation"},
    )

    body = response.json()
    assert body["correlation_id"] == "caller-supplied-correlation"
    assert response.headers["X-Correlation-ID"] == "caller-supplied-correlation"


async def test_a_response_correlation_id_is_generated_when_none_is_supplied(
    harness: Harness,
) -> None:
    response = await harness.create_request(external_reference="e2e-0005")

    assert response.headers["X-Correlation-ID"]


# -- idempotency ------------------------------------------------------------


async def test_replaying_an_idempotency_key_returns_the_original_request(
    harness: Harness,
) -> None:
    first = await harness.create_request(
        external_reference="e2e-0006", idempotency_key="idem-key-0006"
    )
    second = await harness.create_request(
        external_reference="e2e-0006", idempotency_key="idem-key-0006"
    )

    assert first.status_code == 201
    # A replay is not a creation, so the caller can tell whether their retry
    # actually produced new work.
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert len(harness.store.requests) == 1


async def test_reusing_an_idempotency_key_with_a_different_body_is_refused(
    harness: Harness,
) -> None:
    await harness.create_request(external_reference="e2e-0007", idempotency_key="idem-key-0007")

    conflict = await harness.create_request(
        external_reference="e2e-0007",
        payload={"resource_type": "cache"},
        idempotency_key="idem-key-0007",
    )

    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_key_reused"


async def test_the_same_reference_without_a_key_creates_separate_work(
    harness: Harness,
) -> None:
    """Idempotency is the caller's decision, not something inferred from a payload."""
    first = await harness.create_request(external_reference="e2e-0008")
    second = await harness.create_request(external_reference="e2e-0008")

    assert first.json()["id"] != second.json()["id"]


# -- events -----------------------------------------------------------------


async def test_events_reach_the_broker_only_through_the_outbox(
    harness: Harness, outbox_worker: OutboxPublisherWorker
) -> None:
    """Nothing is published during the request; the worker drains it afterwards."""
    created = (await harness.create_request(external_reference="e2e-0009")).json()
    assert harness.publisher.published == []

    await outbox_worker.run_once()

    event_types = harness.published_event_types()
    assert "integration.request.received.v1" in event_types
    assert any(envelope.aggregate_id == created["id"] for envelope in harness.publisher.published)


async def test_a_published_event_carries_the_correlation_id_and_a_stable_identity(
    harness: Harness, outbox_worker: OutboxPublisherWorker
) -> None:
    await harness.api.post(
        "/api/v1/integration-requests",
        json={
            "provider": "northstar",
            "operation_type": "resource_provision",
            "external_reference": "e2e-0010",
            "payload": {},
        },
        headers={**harness.auth(), "X-Correlation-ID": "traceable-correlation"},
    )

    await outbox_worker.run_once()

    envelope = harness.publisher.published[0]
    assert envelope.correlation_id == "traceable-correlation"
    assert envelope.event_id is not None
    assert envelope.producer == "integration-orchestrator"


async def test_an_event_is_not_published_twice(
    harness: Harness, outbox_worker: OutboxPublisherWorker
) -> None:
    await harness.create_request(external_reference="e2e-0011")

    await outbox_worker.run_once()
    first_count = len(harness.publisher.published)
    await outbox_worker.run_once()

    assert len(harness.publisher.published) == first_count


async def test_payloads_published_downstream_carry_no_secrets(
    harness: Harness, outbox_worker: OutboxPublisherWorker
) -> None:
    await harness.create_request(
        external_reference="e2e-0012",
        payload={"resource_type": "database", "admin_password": "hunter2"},
    )

    await outbox_worker.run_once()

    serialised = str([envelope.payload for envelope in harness.publisher.published])
    assert "hunter2" not in serialised
