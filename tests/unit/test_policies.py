"""Backoff arithmetic and circuit breaker decisions."""

from __future__ import annotations

from datetime import timedelta

import pytest

from integration_orchestrator.domain.enums import CircuitState
from integration_orchestrator.domain.errors import ValidationError
from integration_orchestrator.domain.policies import (
    CircuitBreakerPolicy,
    CircuitSnapshot,
    RetryPolicy,
    admits_traffic,
    evaluate_circuit,
    next_state_after_failure,
    next_state_after_success,
)
from tests.support.builders import REFERENCE_TIME

pytestmark = pytest.mark.unit

POLICY = RetryPolicy(max_attempts=4, base_seconds=2.0, multiplier=2.0, max_seconds=30.0)
CIRCUIT = CircuitBreakerPolicy(failure_threshold=3, open_seconds=30.0, success_threshold=2)


def test_retries_are_bounded_by_max_attempts() -> None:
    assert POLICY.has_attempts_remaining(3)
    assert not POLICY.has_attempts_remaining(4)
    assert not POLICY.has_attempts_remaining(9)


def test_a_non_retryable_error_is_never_retried_however_many_attempts_remain() -> None:
    assert not POLICY.should_retry(attempt_count=1, retryable=False)


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(1, 2.0), (2, 4.0), (3, 8.0), (4, 16.0)],
)
def test_backoff_grows_exponentially_with_equal_jitter(attempt: int, expected: float) -> None:
    """Equal jitter: half the delay is deterministic, half is spread."""
    lower = POLICY.compute_delay_seconds(attempt_count=attempt, jitter=0.0)
    upper = POLICY.compute_delay_seconds(attempt_count=attempt, jitter=0.999)

    assert lower == pytest.approx(expected / 2)
    assert upper == pytest.approx(expected, rel=1e-3)


def test_jitter_never_schedules_a_retry_immediately() -> None:
    """Full jitter could return ~0; equal jitter guarantees a growing floor."""
    assert POLICY.compute_delay_seconds(attempt_count=1, jitter=0.0) > 0


def test_backoff_is_capped() -> None:
    delay = POLICY.compute_delay_seconds(attempt_count=10, jitter=0.999)
    assert delay <= POLICY.max_seconds


def test_a_provider_retry_after_is_honoured_rather_than_the_exponential_term() -> None:
    delay = POLICY.compute_delay_seconds(attempt_count=1, jitter=0.0, retry_after_seconds=45.0)
    assert delay == pytest.approx(45.0)


def test_a_retry_after_is_capped_so_a_hostile_header_cannot_park_a_request() -> None:
    policy = RetryPolicy(max_attempts=3, retry_after_cap_seconds=120.0)
    delay = policy.compute_delay_seconds(attempt_count=1, jitter=0.0, retry_after_seconds=86_400.0)
    assert delay == pytest.approx(120.0)


def test_retry_after_jitter_stays_small() -> None:
    """The provider told us when to return; the spread only breaks up the herd."""
    zero = POLICY.compute_delay_seconds(attempt_count=1, jitter=0.0, retry_after_seconds=10.0)
    most = POLICY.compute_delay_seconds(attempt_count=1, jitter=0.999, retry_after_seconds=10.0)
    assert most - zero <= 1.0


def test_next_retry_at_is_the_delay_applied_to_now() -> None:
    due = POLICY.next_retry_at(now=REFERENCE_TIME, attempt_count=1, jitter=0.0)
    assert due == REFERENCE_TIME + timedelta(seconds=1.0)


@pytest.mark.parametrize("jitter", [-0.1, 1.0, 5.0])
def test_out_of_range_jitter_is_rejected(jitter: float) -> None:
    with pytest.raises(ValidationError):
        POLICY.compute_delay_seconds(attempt_count=1, jitter=jitter)


def test_an_attempt_count_below_one_cannot_produce_a_delay() -> None:
    with pytest.raises(ValidationError):
        POLICY.compute_delay_seconds(attempt_count=0, jitter=0.0)


def test_invalid_policies_are_rejected_at_construction() -> None:
    with pytest.raises(ValidationError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValidationError):
        RetryPolicy(max_attempts=1, base_seconds=0)
    with pytest.raises(ValidationError):
        RetryPolicy(max_attempts=1, multiplier=0.5)
    with pytest.raises(ValidationError):
        CircuitBreakerPolicy(failure_threshold=0, open_seconds=1.0)
    with pytest.raises(ValidationError):
        CircuitBreakerPolicy(failure_threshold=1, open_seconds=0)


# -- circuit breaker --------------------------------------------------------


def test_an_open_breaker_becomes_half_open_once_its_window_elapses() -> None:
    """Derived on read, so the breaker recovers even with no traffic."""
    snapshot = CircuitSnapshot(state=CircuitState.OPEN, opened_at=REFERENCE_TIME)

    still_open = evaluate_circuit(
        snapshot, now=REFERENCE_TIME + timedelta(seconds=29), policy=CIRCUIT
    )
    elapsed = evaluate_circuit(snapshot, now=REFERENCE_TIME + timedelta(seconds=30), policy=CIRCUIT)

    assert still_open is CircuitState.OPEN
    assert elapsed is CircuitState.HALF_OPEN


def test_reaching_the_failure_threshold_opens_the_breaker() -> None:
    below = CircuitSnapshot(state=CircuitState.CLOSED, failure_count=1)
    at = CircuitSnapshot(state=CircuitState.CLOSED, failure_count=2)

    assert (
        next_state_after_failure(below, now=REFERENCE_TIME, policy=CIRCUIT) is CircuitState.CLOSED
    )
    assert next_state_after_failure(at, now=REFERENCE_TIME, policy=CIRCUIT) is CircuitState.OPEN


def test_a_failed_probe_reopens_the_breaker_immediately() -> None:
    snapshot = CircuitSnapshot(state=CircuitState.HALF_OPEN)
    assert (
        next_state_after_failure(snapshot, now=REFERENCE_TIME, policy=CIRCUIT) is CircuitState.OPEN
    )


def test_a_half_open_breaker_needs_several_successes_to_close() -> None:
    first = CircuitSnapshot(state=CircuitState.HALF_OPEN, success_count=0)
    second = CircuitSnapshot(state=CircuitState.HALF_OPEN, success_count=1)

    assert (
        next_state_after_success(first, now=REFERENCE_TIME, policy=CIRCUIT)
        is CircuitState.HALF_OPEN
    )
    assert (
        next_state_after_success(second, now=REFERENCE_TIME, policy=CIRCUIT) is CircuitState.CLOSED
    )


def test_only_one_probe_is_admitted_while_half_open() -> None:
    """Otherwise every waiting caller stampedes a provider that just came back."""
    free = CircuitSnapshot(state=CircuitState.HALF_OPEN, half_open_probes=0)
    taken = CircuitSnapshot(state=CircuitState.HALF_OPEN, half_open_probes=1)

    assert admits_traffic(free, now=REFERENCE_TIME, policy=CIRCUIT)
    assert not admits_traffic(taken, now=REFERENCE_TIME, policy=CIRCUIT)


def test_an_open_breaker_reports_how_long_until_it_reopens() -> None:
    snapshot = CircuitSnapshot(state=CircuitState.OPEN, opened_at=REFERENCE_TIME)

    remaining = snapshot.retry_after_seconds(
        now=REFERENCE_TIME + timedelta(seconds=10), policy=CIRCUIT
    )

    assert remaining == pytest.approx(20.0)


def test_a_closed_breaker_has_no_retry_after() -> None:
    snapshot = CircuitSnapshot(state=CircuitState.CLOSED)
    assert snapshot.retry_after_seconds(now=REFERENCE_TIME, policy=CIRCUIT) is None
