"""The transition table and its consequences."""

from __future__ import annotations

import pytest

from integration_orchestrator.domain.enums import NormalizedStatus, RequestStatus
from integration_orchestrator.domain.errors import InvalidStateTransitionError
from integration_orchestrator.domain.state_machine import (
    allowed_transitions,
    assert_transition_allowed,
    can_transition,
    is_forward_progress,
    project_normalized_status,
)

pytestmark = pytest.mark.unit


def test_every_status_has_an_entry_in_the_transition_table() -> None:
    """A status missing from the table would raise a KeyError at runtime."""
    for status in RequestStatus:
        assert isinstance(allowed_transitions(status), frozenset)


def test_terminal_statuses_have_no_outgoing_transitions() -> None:
    for status in RequestStatus:
        if status.is_terminal:
            assert allowed_transitions(status) == frozenset()


def test_no_status_may_transition_to_itself() -> None:
    """Self-transitions are duplicates and must be handled explicitly."""
    for status in RequestStatus:
        assert not can_transition(status, status)


def test_transition_targets_are_all_real_statuses() -> None:
    for status in RequestStatus:
        assert allowed_transitions(status) <= set(RequestStatus)


@pytest.mark.parametrize(
    ("current", "requested"),
    [
        (RequestStatus.RECEIVED, RequestStatus.VALIDATING),
        (RequestStatus.VALIDATING, RequestStatus.DISPATCHING),
        (RequestStatus.DISPATCHING, RequestStatus.PENDING),
        (RequestStatus.DISPATCHING, RequestStatus.RETRY_SCHEDULED),
        (RequestStatus.RETRY_SCHEDULED, RequestStatus.DISPATCHING),
        (RequestStatus.PENDING, RequestStatus.SUCCEEDED),
        (RequestStatus.FAILED, RequestStatus.RETRY_SCHEDULED),
        (RequestStatus.MANUAL_REVIEW, RequestStatus.DISPATCHING),
    ],
)
def test_legal_transitions_are_permitted(current: RequestStatus, requested: RequestStatus) -> None:
    assert_transition_allowed(current, requested)


@pytest.mark.parametrize(
    ("current", "requested"),
    [
        (RequestStatus.RECEIVED, RequestStatus.SUCCEEDED),
        (RequestStatus.RECEIVED, RequestStatus.PENDING),
        (RequestStatus.SUCCEEDED, RequestStatus.FAILED),
        (RequestStatus.CANCELLED, RequestStatus.DISPATCHING),
        (RequestStatus.PENDING, RequestStatus.DISPATCHING),
        (RequestStatus.VALIDATING, RequestStatus.PENDING),
    ],
)
def test_illegal_transitions_raise_with_both_statuses_recorded(
    current: RequestStatus, requested: RequestStatus
) -> None:
    with pytest.raises(InvalidStateTransitionError) as caught:
        assert_transition_allowed(current, requested, aggregate_id="req-1")

    error = caught.value
    assert error.metadata["current_status"] == current.value
    assert error.metadata["requested_status"] == requested.value
    assert error.metadata["aggregate_id"] == "req-1"
    assert error.retryable is False


def test_unknown_provider_status_has_no_projection() -> None:
    """An unrecognised provider status must never be coerced into an outcome."""
    assert project_normalized_status(NormalizedStatus.UNKNOWN) is None


@pytest.mark.parametrize(
    ("normalized", "expected"),
    [
        (NormalizedStatus.ACCEPTED, RequestStatus.PENDING),
        (NormalizedStatus.PENDING, RequestStatus.PENDING),
        (NormalizedStatus.SUCCEEDED, RequestStatus.SUCCEEDED),
        (NormalizedStatus.FAILED, RequestStatus.FAILED),
        (NormalizedStatus.CANCELLED, RequestStatus.CANCELLED),
    ],
)
def test_normalized_statuses_project_onto_request_statuses(
    normalized: NormalizedStatus, expected: RequestStatus
) -> None:
    assert project_normalized_status(normalized) is expected


def test_a_late_pending_webhook_is_not_forward_progress_for_a_succeeded_request() -> None:
    """Providers do not order webhooks; a stale one must not undo completion."""
    assert not is_forward_progress(RequestStatus.SUCCEEDED, RequestStatus.PENDING)


def test_repeating_the_current_status_is_not_forward_progress() -> None:
    assert not is_forward_progress(RequestStatus.PENDING, RequestStatus.PENDING)


def test_completion_of_a_pending_request_is_forward_progress() -> None:
    assert is_forward_progress(RequestStatus.PENDING, RequestStatus.SUCCEEDED)
