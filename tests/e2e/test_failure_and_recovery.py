"""What happens when providers misbehave.

The sandbox picks its behaviour from the external reference, so every scenario
here is a repeatable consequence of its input rather than a lucky timing window.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from integration_orchestrator.domain.enums import RequestStatus
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.workers.outbox_publisher import OutboxPublisherWorker
from integration_orchestrator.workers.reconciliation_worker import ReconciliationWorker
from integration_orchestrator.workers.retry_worker import RetryWorker
from tests.e2e.conftest import Harness

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


async def test_a_transient_failure_schedules_a_durable_retry(harness: Harness) -> None:
    created = (
        await harness.create_request(
            provider="meridian", external_reference="scenario-unavailable-fr-0001"
        )
    ).json()

    assert created["status"] == "retry_scheduled"
    assert created["next_retry_at"] is not None
    assert created["last_failure"]["code"] == "provider_unavailable"


async def test_the_retry_worker_picks_up_and_completes_a_scheduled_retry(
    harness: Harness, retry_worker: RetryWorker
) -> None:
    """The sandbox refuses the first two attempts and lets the third through.

    Northstar deduplicates on an idempotency key, so the resilient gateway is
    allowed one immediate in-process retry; the third attempt is the durable one
    this worker performs.
    """
    created = (
        await harness.create_request(external_reference="scenario-unavailable-once-fr-0002")
    ).json()
    assert created["status"] == "retry_scheduled"

    _advance_retries(harness)
    processed = await retry_worker.run_once()

    assert processed == 1
    assert (await harness.fetch(created["id"]))["status"] in ("pending", "succeeded")


async def test_a_retry_reuses_the_same_request_rather_than_creating_another(
    harness: Harness, retry_worker: RetryWorker
) -> None:
    created = (
        await harness.create_request(external_reference="scenario-unavailable-once-fr-0003")
    ).json()

    _advance_retries(harness)
    await retry_worker.run_once()

    assert len(harness.store.requests) == 1
    assert (await harness.fetch(created["id"]))["attempt_count"] >= 2


async def test_retries_run_out_and_the_request_fails(
    harness: Harness, retry_worker: RetryWorker
) -> None:
    created = (
        await harness.create_request(
            provider="meridian", external_reference="scenario-unavailable-fr-0004"
        )
    ).json()

    _advance_retries(harness)
    await retry_worker.run_once()

    fetched = await harness.fetch(created["id"])
    assert fetched["status"] == "failed"
    assert "retry.exhausted" in await harness.audit_actions(created["id"])


async def test_a_rejected_request_is_never_retried(harness: Harness) -> None:
    """The provider considered the request and said no. Asking again is pointless."""
    created = (
        await harness.create_request(
            provider="meridian", external_reference="scenario-reject-fr-0005"
        )
    ).json()

    assert created["status"] == "failed"
    assert created["next_retry_at"] is None


async def test_an_operator_can_retry_a_failed_request(
    harness: Harness, retry_worker: RetryWorker
) -> None:
    created = (
        await harness.create_request(
            provider="meridian", external_reference="scenario-reject-fr-0006"
        )
    ).json()
    assert created["status"] == "failed"

    response = await harness.api.post(
        f"/api/v1/integration-requests/{created['id']}/retry",
        json={"reason": "the provider confirmed the rejection was their bug"},
        headers=harness.auth(),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "retry_scheduled"
    assert "retry.requested" in await harness.audit_actions(created["id"])


async def test_a_request_with_no_provider_reference_goes_to_manual_review(
    harness: Harness,
) -> None:
    """Accepted but unnameable: it cannot be polled, correlated or safely retried."""
    created = (
        await harness.create_request(external_reference="scenario-no-reference-fr-0007")
    ).json()

    assert created["status"] == "manual_review"
    assert created["manual_review_reason"]


async def test_an_uninterpretable_status_goes_to_manual_review(harness: Harness) -> None:
    created = (
        await harness.create_request(external_reference="scenario-unknown-status-fr-0008")
    ).json()

    assert created["status"] == "manual_review"
    assert "could not interpret" in created["manual_review_reason"]


async def test_an_open_circuit_rejects_without_calling_the_provider(
    harness: Harness,
) -> None:
    harness.circuit_breaker.open(ProviderSlug("meridian"))

    created = (
        await harness.create_request(provider="meridian", external_reference="fr-0009")
    ).json()

    assert created["status"] == "retry_scheduled"
    assert created["last_failure"]["code"] == "provider_circuit_open"


async def test_reconciliation_recovers_a_request_whose_webhook_never_arrived(
    harness: Harness, reconciliation_worker: ReconciliationWorker
) -> None:
    """The provider finished the work; the notification was lost in transit."""
    created = (await harness.create_request(provider="cobalt", external_reference="fr-0010")).json()
    _make_stale(harness, created["id"])

    await reconciliation_worker.run_once()

    fetched = await harness.fetch(created["id"])
    assert fetched["status"] == "succeeded"
    assert "state.reconciled" in await harness.audit_actions(created["id"])


async def test_reconciliation_escalates_a_provider_it_cannot_ask(
    harness: Harness, reconciliation_worker: ReconciliationWorker
) -> None:
    """Northstar has no status endpoint, so guessing is the only alternative."""
    created = (await harness.create_request(external_reference="fr-0011")).json()
    _make_stale(harness, created["id"], escalatable=True)

    await reconciliation_worker.run_once()

    assert (await harness.fetch(created["id"]))["status"] == "manual_review"


async def test_a_recovered_request_still_publishes_its_completion(
    harness: Harness,
    reconciliation_worker: ReconciliationWorker,
    outbox_worker: OutboxPublisherWorker,
) -> None:
    created = (await harness.create_request(provider="cobalt", external_reference="fr-0012")).json()
    _make_stale(harness, created["id"])
    await reconciliation_worker.run_once()

    await outbox_worker.run_once()

    assert "integration.request.succeeded.v1" in harness.published_event_types()
    assert created["id"] in {envelope.aggregate_id for envelope in harness.publisher.published}


def _advance_retries(harness: Harness) -> None:
    """Bring every scheduled retry due, without waiting for the backoff."""
    past = datetime.now(tz=UTC) - timedelta(seconds=1)
    for request in harness.store.requests.values():
        if request.status is RequestStatus.RETRY_SCHEDULED:
            request.next_retry_at = past


def _make_stale(harness: Harness, request_id: str, *, escalatable: bool = False) -> None:
    """Age a request past the reconciliation threshold.

    Reconciliation has two thresholds: one to look at a request at all, and a
    much longer one before it will escalate a request it cannot verify.
    """
    workers = harness.settings.workers
    seconds = (
        workers.reconciliation_manual_review_after_seconds
        if escalatable
        else workers.reconciliation_stale_after_seconds
    )
    request = harness.store.requests[UUID(request_id)]
    request.updated_at = datetime.now(tz=UTC) - timedelta(seconds=seconds * 2)
