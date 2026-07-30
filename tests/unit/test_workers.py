"""Worker behaviour: claiming, publishing, retrying and giving up."""

from __future__ import annotations

from datetime import timedelta

import pytest

from integration_orchestrator.application.services.dispatcher import RequestDispatcher
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.application.use_cases.ingest_webhook import IngestWebhookUseCase
from integration_orchestrator.config.settings import WorkerSettings
from integration_orchestrator.domain.enums import (
    AuditAction,
    RequestStatus,
    WebhookProcessingStatus,
)
from integration_orchestrator.domain.errors import (
    EventPublicationError,
    ProviderUnavailableError,
)
from integration_orchestrator.infrastructure.messaging.memory import InMemoryEventPublisher
from integration_orchestrator.infrastructure.system import FrozenClock
from integration_orchestrator.workers.outbox_publisher import OutboxPublisherWorker
from integration_orchestrator.workers.retry_worker import RetryWorker
from integration_orchestrator.workers.webhook_processor import WebhookProcessorWorker
from tests.support.builders import (
    REFERENCE_TIME,
    make_outbox_event,
    make_receipt,
    make_request,
)
from tests.support.doubles import FakeGateway, RecordingMetrics, accepted_result
from tests.support.memory_uow import MemoryStore, MemoryUnitOfWorkFactory

pytestmark = pytest.mark.unit

SETTINGS = WorkerSettings(
    outbox_batch_size=10,
    retry_batch_size=10,
    webhook_deferred_batch_size=10,
    webhook_deferred_abandon_after_seconds=3600,
)


# -- outbox publisher -------------------------------------------------------


@pytest.fixture
def publisher() -> InMemoryEventPublisher:
    return InMemoryEventPublisher()


@pytest.fixture
def outbox_worker(
    uow_factory: MemoryUnitOfWorkFactory,
    publisher: InMemoryEventPublisher,
    clock: FrozenClock,
    metrics: RecordingMetrics,
) -> OutboxPublisherWorker:
    return OutboxPublisherWorker(
        uow_factory=uow_factory,
        publisher=publisher,
        clock=clock,
        metrics=metrics,
        settings=SETTINGS,
    )


async def test_staged_events_are_published_and_marked(
    outbox_worker: OutboxPublisherWorker,
    publisher: InMemoryEventPublisher,
    store: MemoryStore,
) -> None:
    event = make_outbox_event()
    store.outbox[event.id] = event

    processed = await outbox_worker.run_once()

    assert processed == 1
    assert publisher.types() == ["integration.request.succeeded"]
    assert store.outbox[event.id].published_at == REFERENCE_TIME


async def test_a_published_event_is_not_published_again(
    outbox_worker: OutboxPublisherWorker,
    publisher: InMemoryEventPublisher,
    store: MemoryStore,
) -> None:
    """The row's own id identifies it, not the consumer-facing ``event_id``."""
    event = make_outbox_event()
    store.outbox[event.id] = event

    await outbox_worker.run_once()
    await outbox_worker.run_once()

    assert len(publisher.published) == 1


async def test_the_event_id_is_stable_so_consumers_can_deduplicate(
    outbox_worker: OutboxPublisherWorker,
    publisher: InMemoryEventPublisher,
    store: MemoryStore,
) -> None:
    event = make_outbox_event()
    store.outbox[event.id] = event

    await outbox_worker.run_once()

    assert publisher.published[0].event_id == event.event_id
    assert publisher.published[0].correlation_id == event.correlation_id.value


async def test_a_failed_batch_is_retried_one_event_at_a_time(
    uow_factory: MemoryUnitOfWorkFactory,
    clock: FrozenClock,
    metrics: RecordingMetrics,
    store: MemoryStore,
) -> None:
    """One poisoned event must not stall every other event in the batch."""

    class SelectivePublisher(InMemoryEventPublisher):
        async def publish_batch(self, envelopes):  # type: ignore[no-untyped-def]
            if len(envelopes) > 1:
                raise EventPublicationError("the batch was rejected")
            await super().publish_batch(envelopes)

        async def publish(self, envelope):  # type: ignore[no-untyped-def]
            if envelope.aggregate_id == "poison":
                raise EventPublicationError("this event cannot be routed")
            await super().publish_batch([envelope])

    good = make_outbox_event(aggregate_id="good")
    poison = make_outbox_event(aggregate_id="poison")
    store.outbox[good.id] = good
    store.outbox[poison.id] = poison

    worker = OutboxPublisherWorker(
        uow_factory=uow_factory,
        publisher=SelectivePublisher(),
        clock=clock,
        metrics=metrics,
        settings=SETTINGS,
    )
    await worker.run_once()

    assert store.outbox[good.id].published_at == REFERENCE_TIME
    assert store.outbox[poison.id].published_at is None
    assert store.outbox[poison.id].attempt_count == 1
    assert store.outbox[poison.id].last_error == "this event cannot be routed"


async def test_a_failed_event_is_not_retried_before_its_backoff_elapses(
    uow_factory: MemoryUnitOfWorkFactory,
    clock: FrozenClock,
    metrics: RecordingMetrics,
    store: MemoryStore,
) -> None:
    event = make_outbox_event()
    store.outbox[event.id] = event
    failing = InMemoryEventPublisher(fail_next=2)
    worker = OutboxPublisherWorker(
        uow_factory=uow_factory,
        publisher=failing,
        clock=clock,
        metrics=metrics,
        settings=SETTINGS,
    )

    await worker.run_once()
    scheduled = store.outbox[event.id].next_attempt_at
    assert scheduled == REFERENCE_TIME + timedelta(seconds=SETTINGS.outbox_retry_base_seconds)

    assert await worker.run_once() == 0

    clock.set(scheduled)
    assert await worker.run_once() == 1


async def test_the_backlog_gauge_reflects_unpublished_events(
    outbox_worker: OutboxPublisherWorker, store: MemoryStore, metrics: RecordingMetrics
) -> None:
    for _ in range(3):
        event = make_outbox_event()
        store.outbox[event.id] = event

    await outbox_worker.run_once()

    gauges = [value for name, value, _ in metrics.gauges if name == "outbox_pending_total"]
    assert gauges == [3.0]


# -- retry worker -----------------------------------------------------------


@pytest.fixture
def retry_worker(
    uow_factory: MemoryUnitOfWorkFactory,
    dispatcher: RequestDispatcher,
    clock: FrozenClock,
    metrics: RecordingMetrics,
) -> RetryWorker:
    return RetryWorker(
        uow_factory=uow_factory,
        dispatcher=dispatcher,
        clock=clock,
        metrics=metrics,
        settings=SETTINGS,
    )


async def test_a_due_retry_is_claimed_and_dispatched(
    retry_worker: RetryWorker,
    store: MemoryStore,
    gateway: FakeGateway,
) -> None:
    request = make_request(
        status=RequestStatus.RETRY_SCHEDULED, attempt_count=1, next_retry_at=REFERENCE_TIME
    )
    store.requests[request.id] = request
    gateway.queue_create(accepted_result("prv-2"))

    processed = await retry_worker.run_once()

    assert processed == 1
    assert store.requests[request.id].status is RequestStatus.PENDING
    assert store.requests[request.id].attempt_count == 2


async def test_a_retry_that_is_not_due_yet_is_left_alone(
    retry_worker: RetryWorker, store: MemoryStore
) -> None:
    request = make_request(
        status=RequestStatus.RETRY_SCHEDULED,
        attempt_count=1,
        next_retry_at=REFERENCE_TIME + timedelta(seconds=30),
    )
    store.requests[request.id] = request

    assert await retry_worker.run_once() == 0
    assert store.requests[request.id].status is RequestStatus.RETRY_SCHEDULED


async def test_claiming_moves_the_request_out_of_the_queue_in_the_same_transaction(
    retry_worker: RetryWorker, store: MemoryStore, gateway: FakeGateway
) -> None:
    """Otherwise a crash between claim and dispatch would strand the request."""
    request = make_request(
        status=RequestStatus.RETRY_SCHEDULED, attempt_count=1, next_retry_at=REFERENCE_TIME
    )
    store.requests[request.id] = request

    async def _observe(*_: object, **__: object):
        assert store.requests[request.id].status is RequestStatus.DISPATCHING
        return accepted_result()

    gateway.create_operation = _observe  # type: ignore[method-assign]

    await retry_worker.run_once()


async def test_a_failing_retry_is_rescheduled_rather_than_lost(
    retry_worker: RetryWorker, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request(
        status=RequestStatus.RETRY_SCHEDULED, attempt_count=1, next_retry_at=REFERENCE_TIME
    )
    store.requests[request.id] = request
    gateway.queue_create(ProviderUnavailableError("503", provider="northstar"))

    await retry_worker.run_once()

    assert store.requests[request.id].status is RequestStatus.RETRY_SCHEDULED
    assert store.requests[request.id].attempt_count == 2


async def test_one_failing_request_does_not_stop_the_rest_of_the_batch(
    retry_worker: RetryWorker, store: MemoryStore, gateway: FakeGateway
) -> None:
    first = make_request(
        status=RequestStatus.RETRY_SCHEDULED,
        attempt_count=1,
        next_retry_at=REFERENCE_TIME,
        external_reference="order-a",
    )
    second = make_request(
        status=RequestStatus.RETRY_SCHEDULED,
        attempt_count=1,
        next_retry_at=REFERENCE_TIME + timedelta(seconds=-1),
        external_reference="order-b",
    )
    store.requests[first.id] = first
    store.requests[second.id] = second

    async def _explode_once(command, *_: object, **__: object):  # type: ignore[no-untyped-def]
        if command.external_reference.value == "order-a":
            raise RuntimeError("something structural broke")
        return accepted_result("prv-b")

    gateway.create_operation = _explode_once  # type: ignore[method-assign]

    processed = await retry_worker.run_once()

    assert processed == 2
    assert store.requests[second.id].status is RequestStatus.PENDING


# -- deferred webhook processor --------------------------------------------


@pytest.fixture
def webhook_worker(
    uow_factory: MemoryUnitOfWorkFactory,
    ingest_use_case: IngestWebhookUseCase,
    journal: WorkflowJournal,
    clock: FrozenClock,
    metrics: RecordingMetrics,
) -> WebhookProcessorWorker:
    return WebhookProcessorWorker(
        uow_factory=uow_factory,
        ingest=ingest_use_case,
        journal=journal,
        clock=clock,
        metrics=metrics,
        settings=SETTINGS,
    )


def _deferred_receipt(store: MemoryStore, *, provider_reference: str = "prv-1"):  # type: ignore[no-untyped-def]
    receipt = make_receipt(
        payload={
            "_normalized": {
                "status": "succeeded",
                "occurred_at": REFERENCE_TIME.isoformat(),
                "provider_reference": provider_reference,
                "external_reference": "order-1001",
            }
        },
        provider_reference=provider_reference,
    )
    receipt.mark_deferred(
        reason="not correlated yet", next_attempt_at=REFERENCE_TIME, now=REFERENCE_TIME
    )
    store.receipts[receipt.id] = receipt
    return receipt


async def test_a_deferred_receipt_is_applied_once_its_request_exists(
    webhook_worker: WebhookProcessorWorker, store: MemoryStore
) -> None:
    receipt = _deferred_receipt(store)
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request

    processed = await webhook_worker.run_once()

    assert processed == 1
    assert store.requests[request.id].status is RequestStatus.SUCCEEDED
    assert store.receipts[receipt.id].processing_status is WebhookProcessingStatus.PROCESSED


async def test_a_receipt_still_uncorrelated_stays_deferred(
    webhook_worker: WebhookProcessorWorker, store: MemoryStore
) -> None:
    receipt = _deferred_receipt(store)

    await webhook_worker.run_once()

    assert store.receipts[receipt.id].processing_status is WebhookProcessingStatus.DEFERRED
    assert store.receipts[receipt.id].attempt_count == 2


async def test_a_receipt_that_ages_out_is_abandoned_with_its_evidence_kept(
    webhook_worker: WebhookProcessorWorker, store: MemoryStore, clock: FrozenClock
) -> None:
    """Almost always another environment pointed at the same webhook URL."""
    receipt = _deferred_receipt(store)
    clock.advance(SETTINGS.webhook_deferred_abandon_after_seconds + 1)

    await webhook_worker.run_once()

    stored = store.receipts[receipt.id]
    assert stored.processing_status is WebhookProcessingStatus.ABANDONED
    assert stored.failure_reason is not None
    assert AuditAction.WEBHOOK_ABANDONED.value in store.audit_actions(receipt.id)


async def test_a_receipt_without_a_normalized_summary_is_abandoned_not_retried_forever(
    webhook_worker: WebhookProcessorWorker, store: MemoryStore
) -> None:
    receipt = make_receipt(payload={"nothing": "useful"})
    receipt.mark_deferred(
        reason="not correlated yet", next_attempt_at=REFERENCE_TIME, now=REFERENCE_TIME
    )
    store.receipts[receipt.id] = receipt

    await webhook_worker.run_once()

    assert store.receipts[receipt.id].processing_status is WebhookProcessingStatus.ABANDONED
