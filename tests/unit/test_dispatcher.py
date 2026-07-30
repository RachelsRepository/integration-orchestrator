"""Dispatch: how a provider's answer becomes a state change.

These tests are the reason the dispatcher exists as a separate service. Every
scenario below is a provider behaving in a way that would otherwise need a
conditional inside a use case.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.application.services.dispatcher import (
    PROVIDER_REQUEST_METADATA_KEY,
    RequestDispatcher,
)
from integration_orchestrator.domain.contracts import ProviderOperationResult
from integration_orchestrator.domain.enums import (
    ActorType,
    AuditAction,
    NormalizedStatus,
    RequestStatus,
)
from integration_orchestrator.domain.errors import (
    NotFoundError,
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderUnavailableError,
    ProviderValidationError,
)
from tests.support.builders import REFERENCE_TIME, make_request
from tests.support.doubles import (
    FakeGateway,
    RecordingMetrics,
    accepted_result,
    completed_result,
)
from tests.support.memory_uow import MemoryStore

pytestmark = pytest.mark.unit

ACTOR = Actor(type=ActorType.API_CLIENT, id="tester")


async def test_an_asynchronous_acceptance_moves_the_request_to_pending(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(accepted_result("prv-77"))

    settled = await dispatcher.dispatch(request.id, actor=ACTOR)

    assert settled.status is RequestStatus.PENDING
    assert settled.provider_reference == "prv-77"
    assert settled.attempt_count == 1
    assert store.requests[request.id].status is RequestStatus.PENDING


async def test_a_synchronous_completion_settles_the_request_immediately(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(completed_result("prv-1"))

    settled = await dispatcher.dispatch(request.id, actor=ACTOR)

    assert settled.status is RequestStatus.SUCCEEDED
    assert settled.completed_at == REFERENCE_TIME


async def test_dispatch_walks_the_request_through_validation_and_records_every_step(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(accepted_result())

    await dispatcher.dispatch(request.id, actor=ACTOR)

    actions = store.audit_actions(request.id)
    assert actions == [
        AuditAction.REQUEST_VALIDATED.value,
        AuditAction.DISPATCH_ATTEMPTED.value,
        AuditAction.PROVIDER_ACCEPTED.value,
    ]


async def test_validation_is_audited_but_not_published_as_an_event(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    """An internal step nobody outside the platform can act on stays internal."""
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(accepted_result())

    await dispatcher.dispatch(request.id, actor=ACTOR)

    assert not any(
        name.startswith("integration.request.validating") for name in store.outbox_types()
    )


async def test_every_state_change_writes_its_audit_and_outbox_rows_together(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(accepted_result())

    await dispatcher.dispatch(request.id, actor=ACTOR)

    # Both published transitions produced an outbox row in the same transaction.
    assert len(store.outbox) == 2
    assert all(event.aggregate_id == str(request.id) for event in store.outbox.values())


async def test_a_retryable_failure_schedules_a_retry_with_backoff(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(ProviderUnavailableError("503 from the provider", provider="northstar"))

    settled = await dispatcher.dispatch(request.id, actor=ACTOR)

    assert settled.status is RequestStatus.RETRY_SCHEDULED
    # base 2s, one attempt made, equal jitter at 0.5 -> 1s deterministic + 0.5s.
    assert settled.next_retry_at == REFERENCE_TIME + timedelta(seconds=1.5)
    assert settled.last_error_code == "provider_unavailable"


async def test_a_rate_limit_response_is_retried_when_the_provider_says_to_come_back(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(
        ProviderRateLimitError("429", provider="northstar", retry_after_seconds=20.0)
    )

    settled = await dispatcher.dispatch(request.id, actor=ACTOR)

    assert settled.status is RequestStatus.RETRY_SCHEDULED
    assert settled.next_retry_at is not None
    assert (settled.next_retry_at - REFERENCE_TIME).total_seconds() >= 20.0


async def test_a_rejected_payload_fails_terminally_without_a_retry(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    """Repeating a request the provider called invalid cannot help."""
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(ProviderValidationError("field 'sku' is unknown", provider="northstar"))

    settled = await dispatcher.dispatch(request.id, actor=ACTOR)

    assert settled.status is RequestStatus.FAILED
    assert settled.next_retry_at is None
    assert AuditAction.PROVIDER_FAILED.value in store.audit_actions(request.id)


async def test_an_exhausted_timeout_goes_to_manual_review_rather_than_a_guess(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    """A timeout is ambiguous: the operation may or may not exist at the provider."""
    request = make_request(status=RequestStatus.VALIDATING, attempt_count=2)
    store.requests[request.id] = request
    gateway.queue_create(ProviderTimeoutError("read timeout", provider="northstar"))

    settled = await dispatcher.dispatch(request.id, actor=ACTOR)

    assert settled.status is RequestStatus.MANUAL_REVIEW
    assert settled.manual_review_reason is not None
    assert AuditAction.MOVED_TO_MANUAL_REVIEW.value in store.audit_actions(request.id)


async def test_an_exhausted_non_ambiguous_failure_ends_as_failed(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request(status=RequestStatus.VALIDATING, attempt_count=2)
    store.requests[request.id] = request
    gateway.queue_create(ProviderUnavailableError("503", provider="northstar"))

    settled = await dispatcher.dispatch(request.id, actor=ACTOR)

    assert settled.status is RequestStatus.FAILED
    assert AuditAction.RETRIES_EXHAUSTED.value in store.audit_actions(request.id)


async def test_an_acceptance_without_a_reference_is_escalated(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    """Untrackable work is worse than failed work: it cannot even be polled."""
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(
        ProviderOperationResult.success(normalized_status=NormalizedStatus.ACCEPTED)
    )

    settled = await dispatcher.dispatch(request.id, actor=ACTOR)

    assert settled.status is RequestStatus.MANUAL_REVIEW


async def test_an_uninterpretable_provider_status_is_escalated(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(
        ProviderOperationResult.success(
            normalized_status=NormalizedStatus.UNKNOWN, provider_status="quantum_superposition"
        )
    )

    settled = await dispatcher.dispatch(request.id, actor=ACTOR)

    assert settled.status is RequestStatus.MANUAL_REVIEW
    assert "quantum_superposition" in (settled.manual_review_reason or "")


async def test_a_webhook_that_lands_mid_call_is_not_overwritten_by_the_dispatch_outcome(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    """The settle phase reloads, so newer state wins over a stale answer."""
    request = make_request(status=RequestStatus.DISPATCHING, attempt_count=1)
    store.requests[request.id] = request

    settled_by_webhook = make_request(
        request_id=request.id,
        status=RequestStatus.SUCCEEDED,
        provider_reference="prv-1",
        attempt_count=1,
    )
    settled_by_webhook.version = request.version

    async def _complete_via_webhook(*_: object, **__: object) -> ProviderOperationResult:
        store.requests[request.id] = settled_by_webhook
        return accepted_result("prv-1")

    gateway.create_operation = _complete_via_webhook  # type: ignore[method-assign]

    settled = await dispatcher.complete_attempt(request, actor=ACTOR)

    assert settled.status is RequestStatus.SUCCEEDED
    assert AuditAction.STATE_RECONCILED.value in store.audit_actions(request.id)


async def test_a_superseded_dispatch_still_records_the_provider_reference(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    """The reference is how reconciliation and later webhooks find the request."""
    request = make_request(status=RequestStatus.DISPATCHING, attempt_count=1)
    store.requests[request.id] = request

    settled_elsewhere = make_request(request_id=request.id, status=RequestStatus.SUCCEEDED)
    settled_elsewhere.version = request.version

    async def _settle_then_answer(*_: object, **__: object) -> ProviderOperationResult:
        store.requests[request.id] = settled_elsewhere
        return accepted_result("prv-late")

    gateway.create_operation = _settle_then_answer  # type: ignore[method-assign]

    settled = await dispatcher.complete_attempt(request, actor=ACTOR)

    assert settled.provider_reference == "prv-late"
    assert store.requests[request.id].provider_reference == "prv-late"


async def test_the_provider_shaped_request_is_kept_for_diagnosis(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(
        ProviderOperationResult.success(
            normalized_status=NormalizedStatus.ACCEPTED,
            provider_reference="prv-1",
            metadata={PROVIDER_REQUEST_METADATA_KEY: {"sku": "A-1", "token": "[redacted]"}},
        )
    )

    settled = await dispatcher.dispatch(request.id, actor=ACTOR)

    assert settled.provider_payload == {"sku": "A-1", "token": "[redacted]"}


async def test_the_provider_shaped_request_is_not_copied_into_audit_metadata(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(
        ProviderOperationResult.success(
            normalized_status=NormalizedStatus.ACCEPTED,
            provider_reference="prv-1",
            metadata={PROVIDER_REQUEST_METADATA_KEY: {"sku": "A-1"}},
        )
    )

    await dispatcher.dispatch(request.id, actor=ACTOR)

    for event in store.audit:
        assert PROVIDER_REQUEST_METADATA_KEY not in event.metadata


async def test_the_same_idempotency_key_is_sent_on_every_attempt(
    dispatcher: RequestDispatcher, store: MemoryStore, gateway: FakeGateway
) -> None:
    """That is what lets a provider collapse duplicate creates into one operation."""
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(
        ProviderUnavailableError("503", provider="northstar"),
    )

    await dispatcher.dispatch(request.id, actor=ACTOR)

    retried = store.requests[request.id]
    gateway.queue_create(accepted_result())
    await dispatcher.dispatch(retried.id, actor=ACTOR)

    assert [call.idempotency_key for call in gateway.create_calls] == [
        str(request.id),
        str(request.id),
    ]
    assert [call.attempt for call in gateway.create_calls] == [1, 2]


async def test_dispatching_an_unknown_request_is_a_not_found_error(
    dispatcher: RequestDispatcher,
) -> None:
    with pytest.raises(NotFoundError):
        await dispatcher.dispatch(make_request().id, actor=ACTOR)


async def test_failures_are_counted_by_normalized_category(
    dispatcher: RequestDispatcher,
    store: MemoryStore,
    gateway: FakeGateway,
    metrics: RecordingMetrics,
) -> None:
    request = make_request()
    store.requests[request.id] = request
    gateway.queue_create(ProviderTimeoutError("timeout", provider="northstar"))

    await dispatcher.dispatch(request.id, actor=ACTOR)

    assert metrics.count("provider_failures_total") == 1
    assert metrics.count("provider_timeouts_total") == 1
    assert metrics.count("retries_scheduled_total") == 1
