"""Webhook ingestion: evidence first, then correlation, then state.

The ordering assertions here are the point. A webhook that fails verification,
arrives twice, or refers to an operation we have not committed yet must all leave
a durable record, and none of them may move a request backwards.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from integration_orchestrator.application.dto.commands import IngestWebhookCommand
from integration_orchestrator.application.use_cases.ingest_webhook import (
    IngestWebhookUseCase,
    rehydrate_event,
)
from integration_orchestrator.domain.contracts import InboundWebhook, WebhookVerification
from integration_orchestrator.domain.enums import (
    AuditAction,
    NormalizedStatus,
    RequestStatus,
    WebhookProcessingStatus,
)
from integration_orchestrator.domain.errors import ProviderNotConfiguredError
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ProviderSlug,
    SignatureMetadata,
)
from tests.support.builders import REFERENCE_TIME, make_event, make_request
from tests.support.doubles import FakeGateway, RecordingMetrics
from tests.support.memory_uow import MemoryStore

pytestmark = pytest.mark.unit

NORTHSTAR = ProviderSlug.parse("northstar")


def inbound(
    *, provider: ProviderSlug = NORTHSTAR, body: bytes = b'{"id":"evt-1"}'
) -> InboundWebhook:
    return InboundWebhook(
        provider=provider,
        headers={"x-signature": "sig", "content-type": "application/json"},
        body=body,
        received_at=REFERENCE_TIME,
        remote_address="203.0.113.10",
    )


def ingest_command(webhook: InboundWebhook | None = None) -> IngestWebhookCommand:
    return IngestWebhookCommand(
        webhook=webhook or inbound(),
        correlation_id=CorrelationId("corr-1"),
    )


async def test_a_completion_webhook_settles_its_request(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway, store: MemoryStore
) -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.normalized_event = make_event(normalized_status=NormalizedStatus.SUCCEEDED)

    result = await ingest_use_case.execute(ingest_command())

    assert result.status is WebhookProcessingStatus.PROCESSED
    assert result.integration_request_id == request.id
    assert store.requests[request.id].status is RequestStatus.SUCCEEDED
    assert store.receipts[result.receipt_id].processing_status is WebhookProcessingStatus.PROCESSED


async def test_applying_a_webhook_writes_audit_and_outbox_rows_atomically(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway, store: MemoryStore
) -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.normalized_event = make_event()

    await ingest_use_case.execute(ingest_command())

    assert AuditAction.WEBHOOK_APPLIED.value in store.audit_actions(request.id)
    assert any(name.startswith("integration.request.succeeded") for name in store.outbox_types())


async def test_the_causation_chain_points_back_at_the_provider_event(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway, store: MemoryStore
) -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.normalized_event = make_event(provider_event_id="evt-42")

    await ingest_use_case.execute(ingest_command())

    published = [event for event in store.outbox.values() if event.causation_id]
    assert [event.causation_id for event in published] == ["evt-42"]


async def test_an_unverifiable_webhook_is_recorded_as_evidence_and_rejected(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway, store: MemoryStore
) -> None:
    gateway.verification = WebhookVerification.rejected(
        SignatureMetadata(scheme="hmac_sha256"), "signature mismatch"
    )

    result = await ingest_use_case.execute(ingest_command())

    assert result.status is WebhookProcessingStatus.REJECTED
    receipt = store.receipts[result.receipt_id]
    assert receipt.processing_status is WebhookProcessingStatus.REJECTED
    assert receipt.failure_reason == "signature mismatch"
    assert AuditAction.WEBHOOK_REJECTED.value in store.audit_actions(receipt.id)


async def test_a_rejected_webhook_never_takes_its_deduplication_key_from_the_body(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway, store: MemoryStore
) -> None:
    """Otherwise an attacker could pre-register an id and suppress a real event."""
    gateway.verification = WebhookVerification.rejected(
        SignatureMetadata(scheme="hmac_sha256"), "signature mismatch"
    )

    result = await ingest_use_case.execute(ingest_command())

    assert store.receipts[result.receipt_id].event_id.startswith("unverified:")


async def test_a_rejected_webhook_body_is_not_persisted(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway, store: MemoryStore
) -> None:
    gateway.verification = WebhookVerification.rejected(
        SignatureMetadata(scheme="hmac_sha256"), "stale timestamp"
    )

    result = await ingest_use_case.execute(ingest_command(inbound(body=b'{"secret":"x"}')))

    assert store.receipts[result.receipt_id].payload == {"body_bytes": 14}


async def test_an_unknown_provider_is_refused(
    ingest_use_case: IngestWebhookUseCase,
) -> None:
    with pytest.raises(ProviderNotConfiguredError):
        await ingest_use_case.execute(ingest_command(inbound(provider=ProviderSlug("mystery"))))


async def test_a_redelivered_webhook_is_recognised_as_a_duplicate(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway, store: MemoryStore
) -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.normalized_event = make_event()

    first = await ingest_use_case.execute(ingest_command())
    second = await ingest_use_case.execute(ingest_command())

    assert second.status is WebhookProcessingStatus.DUPLICATE
    assert second.receipt_id == first.receipt_id
    assert len(store.receipts) == 1


async def test_a_duplicate_does_not_reapply_the_state_change(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway, store: MemoryStore
) -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.normalized_event = make_event()

    await ingest_use_case.execute(ingest_command())
    outbox_after_first = len(store.outbox)
    await ingest_use_case.execute(ingest_command())

    assert len(store.outbox) == outbox_after_first
    assert store.requests[request.id].version == 1


async def test_a_late_webhook_carrying_no_progress_is_settled_without_a_transition(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway, store: MemoryStore
) -> None:
    """A stale ``pending`` must not drag a succeeded request backwards."""
    request = make_request(status=RequestStatus.SUCCEEDED, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.normalized_event = make_event(
        normalized_status=NormalizedStatus.PENDING, provider_event_id="evt-late"
    )

    result = await ingest_use_case.execute(ingest_command())

    assert result.status is WebhookProcessingStatus.PROCESSED
    assert store.requests[request.id].status is RequestStatus.SUCCEEDED
    assert AuditAction.WEBHOOK_DUPLICATE_IGNORED.value in store.audit_actions(request.id)


async def test_a_webhook_arriving_before_the_dispatch_commits_is_deferred(
    ingest_use_case: IngestWebhookUseCase,
    gateway: FakeGateway,
    store: MemoryStore,
    metrics: RecordingMetrics,
) -> None:
    """The webhook-before-response race: verified, real, and not yet correlatable."""
    gateway.normalized_event = make_event(provider_reference="prv-not-yet-known")

    result = await ingest_use_case.execute(ingest_command())

    assert result.status is WebhookProcessingStatus.DEFERRED
    receipt = store.receipts[result.receipt_id]
    assert receipt.processing_status is WebhookProcessingStatus.DEFERRED
    assert receipt.next_attempt_at == REFERENCE_TIME + timedelta(seconds=5)
    assert receipt.attempt_count == 1
    assert metrics.count("webhook_deferred_total") == 1


async def test_a_deferred_webhook_is_applied_once_its_request_appears(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway, store: MemoryStore
) -> None:
    gateway.normalized_event = make_event(provider_reference="prv-1")
    deferred = await ingest_use_case.execute(ingest_command())

    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request

    receipt = store.receipts[deferred.receipt_id]
    result = await ingest_use_case.process_receipt(receipt, rehydrate_event(receipt))

    assert result.status is WebhookProcessingStatus.PROCESSED
    assert store.requests[request.id].status is RequestStatus.SUCCEEDED


async def test_a_webhook_without_a_provider_reference_cannot_be_correlated(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway
) -> None:
    """Correlating on the caller's own reference could hit an unrelated request."""
    gateway.normalized_event = make_event(provider_reference=None)

    result = await ingest_use_case.execute(ingest_command())

    assert result.status is WebhookProcessingStatus.DEFERRED


async def test_a_receipt_round_trips_through_its_normalized_summary(
    ingest_use_case: IngestWebhookUseCase, gateway: FakeGateway, store: MemoryStore
) -> None:
    """The raw body is not stored, so the summary has to be sufficient on its own."""
    event = make_event(normalized_status=NormalizedStatus.FAILED, provider_event_id="evt-7")
    gateway.normalized_event = event

    result = await ingest_use_case.execute(ingest_command())
    rehydrated = rehydrate_event(store.receipts[result.receipt_id])

    assert rehydrated.provider_event_id == event.provider_event_id
    assert rehydrated.normalized_status is event.normalized_status
    assert rehydrated.occurred_at == event.occurred_at
    assert rehydrated.provider_reference == event.provider_reference


async def test_receipts_are_counted_on_arrival_regardless_of_outcome(
    ingest_use_case: IngestWebhookUseCase,
    gateway: FakeGateway,
    metrics: RecordingMetrics,
) -> None:
    gateway.normalized_event = make_event()

    await ingest_use_case.execute(ingest_command())

    assert metrics.count("webhook_received_total") == 1
