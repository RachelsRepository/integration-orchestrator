"""Reconciliation: correcting what was lost, escalating what is unknowable.

The recurring assertion is that reconciliation never invents an outcome. It
corrects local state only when the provider answered unambiguously about an
operation we can positively identify.
"""

from __future__ import annotations

import pytest

from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.application.services.reconciliation import ReconciliationService
from integration_orchestrator.domain.contracts import ProviderOperationResult
from integration_orchestrator.domain.enums import (
    AuditAction,
    NormalizedStatus,
    RequestStatus,
)
from integration_orchestrator.domain.errors import (
    ProviderNotFoundError,
    ProviderUnavailableError,
)
from integration_orchestrator.infrastructure.system import FrozenClock
from tests.support.builders import make_request
from tests.support.doubles import (
    FakeGateway,
    FakeRegistry,
    RecordingMetrics,
    accepted_result,
    completed_result,
    descriptor_for,
)
from tests.support.memory_uow import MemoryStore, MemoryUnitOfWorkFactory

pytestmark = pytest.mark.unit

STALE_AFTER_SECONDS = 300
ESCALATE_AFTER_SECONDS = 3600


@pytest.fixture
def reconciler(
    uow_factory: MemoryUnitOfWorkFactory,
    registry: FakeRegistry,
    journal: WorkflowJournal,
    clock: FrozenClock,
    metrics: RecordingMetrics,
) -> ReconciliationService:
    return ReconciliationService(
        uow_factory=uow_factory,
        registry=registry,
        journal=journal,
        clock=clock,
        metrics=metrics,
        stale_after_seconds=STALE_AFTER_SECONDS,
        manual_review_after_seconds=ESCALATE_AFTER_SECONDS,
    )


async def test_only_stale_in_flight_requests_are_candidates(
    reconciler: ReconciliationService, store: MemoryStore, clock: FrozenClock
) -> None:
    fresh = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    stale = make_request(status=RequestStatus.PENDING, provider_reference="prv-2")
    settled = make_request(status=RequestStatus.SUCCEEDED, provider_reference="prv-3")
    for request in (fresh, stale, settled):
        store.requests[request.id] = request

    clock.advance(STALE_AFTER_SECONDS + 1)
    fresh.touch(now=clock.now())

    candidates = await reconciler.find_candidates(limit=10)

    assert [candidate.id for candidate in candidates] == [stale.id]


async def test_a_lost_completion_is_recovered_from_provider_state(
    reconciler: ReconciliationService,
    store: MemoryStore,
    gateway: FakeGateway,
    clock: FrozenClock,
) -> None:
    """The webhook never arrived, but the provider knows the operation finished."""
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.queue_status(completed_result("prv-1"))
    clock.advance(STALE_AFTER_SECONDS + 1)

    outcome = await reconciler.reconcile(request)

    assert outcome.action == "corrected"
    assert outcome.new_status is RequestStatus.SUCCEEDED
    assert store.requests[request.id].status is RequestStatus.SUCCEEDED
    assert AuditAction.STATE_RECONCILED.value in store.audit_actions(request.id)


async def test_matching_state_is_confirmed_without_a_write_to_the_aggregate(
    reconciler: ReconciliationService,
    store: MemoryStore,
    gateway: FakeGateway,
) -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.queue_status(accepted_result("prv-1"))

    outcome = await reconciler.reconcile(request)

    assert outcome.action == "confirmed"
    assert not outcome.changed
    assert store.requests[request.id].version == request.version


async def test_an_unreachable_provider_defers_rather_than_changing_state(
    reconciler: ReconciliationService, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.queue_status(ProviderUnavailableError("503", provider="northstar"))

    outcome = await reconciler.reconcile(request)

    assert outcome.action == "deferred"
    assert store.requests[request.id].status is RequestStatus.PENDING


async def test_a_reference_the_provider_does_not_recognise_is_escalated(
    reconciler: ReconciliationService,
    store: MemoryStore,
    gateway: FakeGateway,
    metrics: RecordingMetrics,
) -> None:
    """Not-found can also mean an aged-out reference, so failing it would be a guess."""
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-gone")
    store.requests[request.id] = request
    gateway.queue_status(ProviderNotFoundError("no such operation", provider="northstar"))

    outcome = await reconciler.reconcile(request)

    assert outcome.action == "escalated"
    assert store.requests[request.id].status is RequestStatus.MANUAL_REVIEW
    assert metrics.count("reconciliation_mismatches_total") == 1


async def test_an_uninterpretable_provider_status_is_escalated(
    reconciler: ReconciliationService, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request
    gateway.queue_status(
        ProviderOperationResult.success(
            normalized_status=NormalizedStatus.UNKNOWN, provider_status="in_limbo"
        )
    )

    outcome = await reconciler.reconcile(request)

    assert outcome.action == "escalated"
    assert "in_limbo" in (outcome.detail or "")


async def test_a_timed_out_dispatch_is_given_a_grace_period_before_escalation(
    reconciler: ReconciliationService, store: MemoryStore, clock: FrozenClock
) -> None:
    """A late provider response may still arrive and identify the operation."""
    request = make_request(status=RequestStatus.DISPATCHING, attempt_count=1)
    store.requests[request.id] = request
    clock.advance(STALE_AFTER_SECONDS + 1)

    outcome = await reconciler.reconcile(request)

    assert outcome.action == "waiting"
    assert store.requests[request.id].status is RequestStatus.DISPATCHING


async def test_a_request_with_no_reference_is_escalated_once_the_threshold_passes(
    reconciler: ReconciliationService, store: MemoryStore, clock: FrozenClock
) -> None:
    request = make_request(status=RequestStatus.DISPATCHING, attempt_count=1)
    store.requests[request.id] = request
    clock.advance(ESCALATE_AFTER_SECONDS + 1)

    outcome = await reconciler.reconcile(request)

    assert outcome.action == "escalated"
    assert store.requests[request.id].status is RequestStatus.MANUAL_REVIEW


async def test_a_provider_without_status_lookup_can_only_be_resolved_by_time(
    uow_factory: MemoryUnitOfWorkFactory,
    journal: WorkflowJournal,
    clock: FrozenClock,
    metrics: RecordingMetrics,
    store: MemoryStore,
) -> None:
    blind = FakeGateway(descriptor=descriptor_for(FakeGateway().slug, supports_status_lookup=False))
    reconciler = ReconciliationService(
        uow_factory=uow_factory,
        registry=FakeRegistry(blind),
        journal=journal,
        clock=clock,
        metrics=metrics,
        stale_after_seconds=STALE_AFTER_SECONDS,
        manual_review_after_seconds=ESCALATE_AFTER_SECONDS,
    )
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")
    store.requests[request.id] = request

    waiting = await reconciler.reconcile(request)
    clock.advance(ESCALATE_AFTER_SECONDS + 1)
    escalated = await reconciler.reconcile(request)

    assert waiting.action == "waiting"
    assert escalated.action == "escalated"
    assert blind.status_calls == []


async def test_escalating_a_request_already_in_manual_review_changes_nothing(
    reconciler: ReconciliationService, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request(status=RequestStatus.MANUAL_REVIEW, provider_reference="prv-gone")
    store.requests[request.id] = request
    gateway.queue_status(ProviderNotFoundError("no such operation", provider="northstar"))

    outcome = await reconciler.reconcile(request)

    assert outcome.action == "no_action"
    assert store.requests[request.id].version == request.version
