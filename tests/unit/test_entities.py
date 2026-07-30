"""Aggregate behaviour: transitions, versioning and out-of-order updates."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from integration_orchestrator.domain.entities import FailureDetail, IntegrationRequest
from integration_orchestrator.domain.enums import (
    NormalizedStatus,
    OperationType,
    RequestStatus,
    WebhookProcessingStatus,
)
from integration_orchestrator.domain.errors import (
    ConflictError,
    InvalidStateTransitionError,
    ValidationError,
)
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ExternalReference,
)
from tests.support.builders import REFERENCE_TIME, make_receipt, make_request

pytestmark = pytest.mark.unit

FAILURE = FailureDetail(
    code="provider_unavailable",
    message="the provider returned 503",
    category="provider_unavailable",
    retryable=True,
)


def test_a_new_request_starts_in_received() -> None:
    request = make_request()
    assert request.status is RequestStatus.RECEIVED
    assert request.attempt_count == 0
    assert request.version == 0


def test_naive_timestamps_are_rejected() -> None:
    """A naive datetime is ambiguous and breaks comparisons at runtime."""
    with pytest.raises(ValidationError):
        IntegrationRequest.create(
            request_id=make_request().id,
            provider=make_request().provider,
            operation_type=OperationType.RESOURCE_PROVISION,
            external_reference=ExternalReference("order-1"),
            normalized_payload={},
            correlation_id=CorrelationId("corr-1"),
            now=datetime(2026, 3, 1, 12, 0, 0),  # noqa: DTZ001 - the point of the test
        )


def test_beginning_dispatch_counts_the_attempt_before_the_call() -> None:
    """The attempt must be durable before the provider is contacted."""
    request = make_request(status=RequestStatus.VALIDATING)

    request.begin_dispatch(now=REFERENCE_TIME)

    assert request.status is RequestStatus.DISPATCHING
    assert request.attempt_count == 1
    assert request.next_retry_at is None


def test_every_transition_advances_the_optimistic_concurrency_version() -> None:
    request = make_request()

    request.begin_validation(now=REFERENCE_TIME)
    request.begin_dispatch(now=REFERENCE_TIME)

    assert request.version == 2


def test_acceptance_requires_a_provider_reference() -> None:
    """Without a reference the operation can neither be polled nor correlated."""
    request = make_request(status=RequestStatus.DISPATCHING)

    with pytest.raises(ValidationError):
        request.mark_accepted(provider_reference="", now=REFERENCE_TIME)


def test_success_clears_the_previous_error_and_stamps_completion() -> None:
    request = make_request(status=RequestStatus.DISPATCHING)
    request.schedule_retry(
        failure=FAILURE, next_retry_at=REFERENCE_TIME + timedelta(seconds=5), now=REFERENCE_TIME
    )
    request.begin_dispatch(now=REFERENCE_TIME)

    request.mark_succeeded(now=REFERENCE_TIME, provider_reference="prv-9")

    assert request.status is RequestStatus.SUCCEEDED
    assert request.completed_at == REFERENCE_TIME
    assert request.last_error_code is None
    assert request.provider_reference == "prv-9"


def test_scheduling_a_retry_records_the_failure_and_the_due_time() -> None:
    request = make_request(status=RequestStatus.DISPATCHING)
    due = REFERENCE_TIME + timedelta(seconds=30)

    request.schedule_retry(failure=FAILURE, next_retry_at=due, now=REFERENCE_TIME)

    assert request.status is RequestStatus.RETRY_SCHEDULED
    assert request.next_retry_at == due
    assert request.last_error_code == "provider_unavailable"
    assert request.completed_at is None
    assert request.is_due_for_retry(now=due)
    assert not request.is_due_for_retry(now=due - timedelta(seconds=1))


def test_a_terminal_request_refuses_further_transitions() -> None:
    request = make_request(status=RequestStatus.SUCCEEDED)

    with pytest.raises(InvalidStateTransitionError):
        request.mark_failed(failure=FAILURE, now=REFERENCE_TIME)


def test_an_out_of_order_pending_webhook_leaves_a_succeeded_request_alone() -> None:
    request = make_request(status=RequestStatus.SUCCEEDED, provider_reference="prv-1")

    transition = request.apply_normalized_status(
        NormalizedStatus.PENDING, now=REFERENCE_TIME, provider_reference="prv-1"
    )

    assert transition is None
    assert request.status is RequestStatus.SUCCEEDED


def test_an_unknown_provider_status_is_never_guessed() -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")

    transition = request.apply_normalized_status(NormalizedStatus.UNKNOWN, now=REFERENCE_TIME)

    assert transition is None
    assert request.status is RequestStatus.PENDING


def test_a_completion_webhook_settles_a_pending_request() -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")

    transition = request.apply_normalized_status(
        NormalizedStatus.SUCCEEDED, now=REFERENCE_TIME, provider_reference="prv-1"
    )

    assert transition is not None
    assert transition.previous_status is RequestStatus.PENDING
    assert request.status is RequestStatus.SUCCEEDED


def test_a_failure_webhook_without_detail_still_records_a_reason() -> None:
    request = make_request(status=RequestStatus.PENDING, provider_reference="prv-1")

    request.apply_normalized_status(NormalizedStatus.FAILED, now=REFERENCE_TIME)

    assert request.status is RequestStatus.FAILED
    assert request.last_error_code == "provider_reported_failure"


def test_an_acceptance_webhook_needs_a_reference_from_somewhere() -> None:
    """A pending projection with no reference anywhere cannot be applied."""
    request = make_request(status=RequestStatus.DISPATCHING)

    transition = request.apply_normalized_status(NormalizedStatus.ACCEPTED, now=REFERENCE_TIME)

    assert transition is None
    assert request.status is RequestStatus.DISPATCHING


def test_attaching_a_conflicting_provider_reference_is_refused() -> None:
    """Two references for one operation means we correlated something wrong."""
    request = make_request(status=RequestStatus.DISPATCHING, provider_reference="prv-1")

    with pytest.raises(ConflictError):
        request.attach_provider_reference("prv-2", now=REFERENCE_TIME)


def test_attaching_the_same_reference_again_is_harmless() -> None:
    request = make_request(status=RequestStatus.DISPATCHING, provider_reference="prv-1")

    request.attach_provider_reference("prv-1", now=REFERENCE_TIME)

    assert request.provider_reference == "prv-1"


def test_manual_review_records_why_a_human_is_needed() -> None:
    request = make_request(status=RequestStatus.DISPATCHING)

    request.mark_manual_review(reason="ambiguous timeout", now=REFERENCE_TIME)

    assert request.status is RequestStatus.MANUAL_REVIEW
    assert request.manual_review_reason == "ambiguous timeout"
    assert request.next_retry_at is None


def test_restoring_for_retry_clears_the_manual_review_reason() -> None:
    request = make_request(status=RequestStatus.MANUAL_REVIEW)

    request.restore_for_retry(next_retry_at=REFERENCE_TIME, now=REFERENCE_TIME)

    assert request.status is RequestStatus.RETRY_SCHEDULED
    assert request.manual_review_reason is None


def test_age_and_staleness_are_measured_from_the_right_timestamps() -> None:
    created = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
    request = make_request(now=created)
    request.touch(now=created + timedelta(seconds=60))

    assert request.age_seconds(now=created + timedelta(seconds=90)) == 90
    assert request.seconds_since_update(now=created + timedelta(seconds=90)) == 30


# -- webhook receipts -------------------------------------------------------


def test_a_receipt_requires_a_provider_event_id() -> None:
    with pytest.raises(ValidationError):
        make_receipt(event_id="")


def test_deferring_a_receipt_counts_the_attempt_and_schedules_a_retry() -> None:
    receipt = make_receipt()
    due = REFERENCE_TIME + timedelta(seconds=5)

    receipt.mark_deferred(reason="not correlated yet", next_attempt_at=due, now=REFERENCE_TIME)

    assert receipt.processing_status is WebhookProcessingStatus.DEFERRED
    assert receipt.attempt_count == 1
    assert receipt.next_attempt_at == due
    assert receipt.processed_at is None
    assert not receipt.is_settled


def test_settled_receipt_statuses_are_the_ones_that_stop_retrying() -> None:
    for mutate, settled in [
        (lambda r: r.mark_processed(now=REFERENCE_TIME), True),
        (lambda r: r.mark_duplicate(now=REFERENCE_TIME), True),
        (lambda r: r.mark_rejected(reason="bad signature", now=REFERENCE_TIME), True),
        (lambda r: r.mark_abandoned(reason="never correlated", now=REFERENCE_TIME), True),
        (lambda r: r.mark_failed(reason="boom", now=REFERENCE_TIME), False),
    ]:
        receipt = make_receipt()
        mutate(receipt)
        assert receipt.is_settled is settled
