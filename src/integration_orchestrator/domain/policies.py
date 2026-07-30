"""Pure policy calculations.

Backoff and circuit breaker decisions live in the domain as side-effect-free
functions so they can be exhaustively unit tested without a clock, a network, or
a Redis instance. Randomness is injected as a caller-supplied ``jitter`` value in
``[0, 1)`` rather than drawn internally, which keeps every test deterministic
while still producing spread-out retries in production.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from integration_orchestrator.domain.enums import CircuitState
from integration_orchestrator.domain.errors import ValidationError

# Proportional jitter applied on top of a provider-supplied Retry-After. The
# provider told us when to come back, so the delay is honoured; the small spread
# only prevents every waiting caller returning on the same millisecond.
_RETRY_AFTER_JITTER_FRACTION = 0.1
_RETRY_AFTER_JITTER_CAP_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded exponential backoff with equal jitter.

    Equal jitter is used rather than full jitter: half the delay is the
    deterministic exponential term and half is random. Full jitter can schedule a
    retry almost immediately after a failure, which defeats the purpose of
    backing off when a provider is degraded. Equal jitter still breaks up
    synchronised retry storms while guaranteeing a growing minimum delay.
    """

    max_attempts: int
    base_seconds: float = 0.5
    multiplier: float = 2.0
    max_seconds: float = 60.0
    retry_after_cap_seconds: float = 300.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValidationError("max_attempts must be at least 1")
        if self.base_seconds <= 0 or self.max_seconds <= 0:
            raise ValidationError("backoff durations must be positive")
        if self.multiplier < 1.0:
            raise ValidationError("the backoff multiplier must be at least 1.0")

    def has_attempts_remaining(self, attempt_count: int) -> bool:
        """Report whether another attempt is within the bound.

        ``attempt_count`` is the number of attempts already made. Retries are
        always bounded; there is no configuration that produces infinite retries.
        """
        return attempt_count < self.max_attempts

    def should_retry(self, *, attempt_count: int, retryable: bool) -> bool:
        """Combine error classification with the attempt bound."""
        return retryable and self.has_attempts_remaining(attempt_count)

    def compute_delay_seconds(
        self,
        *,
        attempt_count: int,
        jitter: float,
        retry_after_seconds: float | None = None,
    ) -> float:
        """Return the delay before the next attempt.

        ``attempt_count`` is the number of attempts already made, so the first
        retry (after one failed attempt) uses the base delay.
        """
        if not 0.0 <= jitter < 1.0:
            raise ValidationError("jitter must be in the interval [0, 1)")
        if attempt_count < 1:
            raise ValidationError("attempt_count must be at least 1 to compute a retry delay")

        if retry_after_seconds is not None and retry_after_seconds > 0:
            honoured = min(retry_after_seconds, self.retry_after_cap_seconds)
            spread = min(honoured * _RETRY_AFTER_JITTER_FRACTION, _RETRY_AFTER_JITTER_CAP_SECONDS)
            return honoured + (jitter * spread)

        exponential = self.base_seconds * (self.multiplier ** (attempt_count - 1))
        capped = min(exponential, self.max_seconds)
        half = capped / 2.0
        return half + (jitter * half)

    def next_retry_at(
        self,
        *,
        now: datetime,
        attempt_count: int,
        jitter: float,
        retry_after_seconds: float | None = None,
    ) -> datetime:
        """Return the absolute timestamp of the next attempt."""
        delay = self.compute_delay_seconds(
            attempt_count=attempt_count,
            jitter=jitter,
            retry_after_seconds=retry_after_seconds,
        )
        return now + timedelta(seconds=delay)


@dataclass(frozen=True, slots=True)
class CircuitBreakerPolicy:
    """Thresholds governing provider-scoped circuit breaking."""

    failure_threshold: int
    open_seconds: float
    success_threshold: int = 2
    half_open_max_probes: int = 1

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValidationError("failure_threshold must be at least 1")
        if self.open_seconds <= 0:
            raise ValidationError("open_seconds must be positive")
        if self.success_threshold < 1:
            raise ValidationError("success_threshold must be at least 1")
        if self.half_open_max_probes < 1:
            raise ValidationError("half_open_max_probes must be at least 1")


@dataclass(frozen=True, slots=True)
class CircuitSnapshot:
    """The observable state of one provider's circuit breaker."""

    state: CircuitState
    failure_count: int = 0
    success_count: int = 0
    opened_at: datetime | None = None
    last_transition_at: datetime | None = None
    half_open_probes: int = 0

    def retry_after_seconds(self, *, now: datetime, policy: CircuitBreakerPolicy) -> float | None:
        """Seconds until the open window elapses, if currently open."""
        if self.state is not CircuitState.OPEN or self.opened_at is None:
            return None
        elapsed = (now - self.opened_at).total_seconds()
        return max(0.0, policy.open_seconds - elapsed)


def evaluate_circuit(
    snapshot: CircuitSnapshot,
    *,
    now: datetime,
    policy: CircuitBreakerPolicy,
) -> CircuitState:
    """Return the effective state, accounting for open-window expiry.

    Stored state is not the same as effective state: a breaker recorded as OPEN
    becomes HALF_OPEN once its cooldown elapses, without anyone having written to
    the store. Deriving this on read keeps the breaker correct even when no
    traffic arrived during the open window.
    """
    if snapshot.state is not CircuitState.OPEN:
        return snapshot.state
    if snapshot.opened_at is None:
        return CircuitState.OPEN
    if (now - snapshot.opened_at).total_seconds() >= policy.open_seconds:
        return CircuitState.HALF_OPEN
    return CircuitState.OPEN


def next_state_after_failure(
    snapshot: CircuitSnapshot,
    *,
    now: datetime,
    policy: CircuitBreakerPolicy,
) -> CircuitState:
    """Decide the state following a failed call."""
    effective = evaluate_circuit(snapshot, now=now, policy=policy)
    if effective is CircuitState.HALF_OPEN:
        # A failed probe immediately re-opens: the provider is still unhealthy.
        return CircuitState.OPEN
    if effective is CircuitState.OPEN:
        return CircuitState.OPEN
    if snapshot.failure_count + 1 >= policy.failure_threshold:
        return CircuitState.OPEN
    return CircuitState.CLOSED


def next_state_after_success(
    snapshot: CircuitSnapshot,
    *,
    now: datetime,
    policy: CircuitBreakerPolicy,
) -> CircuitState:
    """Decide the state following a successful call."""
    effective = evaluate_circuit(snapshot, now=now, policy=policy)
    if effective is CircuitState.HALF_OPEN:
        if snapshot.success_count + 1 >= policy.success_threshold:
            return CircuitState.CLOSED
        return CircuitState.HALF_OPEN
    return CircuitState.CLOSED


def admits_traffic(
    snapshot: CircuitSnapshot,
    *,
    now: datetime,
    policy: CircuitBreakerPolicy,
) -> bool:
    """Report whether a call may proceed under the current breaker state."""
    effective = evaluate_circuit(snapshot, now=now, policy=policy)
    if effective is CircuitState.CLOSED:
        return True
    if effective is CircuitState.OPEN:
        return False
    return snapshot.half_open_probes < policy.half_open_max_probes
