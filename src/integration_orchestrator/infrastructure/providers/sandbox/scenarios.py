"""Deterministic failure scenarios for the provider sandbox.

Randomised fault injection makes tests flaky and makes demonstrations
unrepeatable: a reviewer who sees a circuit breaker trip once cannot make it
happen again. Every fault here is instead selected by the caller's external
reference, so the behaviour of a request is a pure function of its input.

Prefix an external reference with a scenario name to trigger it, for example
``scenario-rate-limit-0001``. References without a known prefix behave normally.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Scenario(StrEnum):
    """Deterministic provider behaviours selectable from the external reference."""

    HEALTHY = "healthy"
    #: Never responds within the adapter's timeout budget.
    TIMEOUT = "scenario-timeout"
    #: Responds slowly but inside the budget, for latency observation.
    SLOW = "scenario-slow"
    #: Always returns 429 with a Retry-After header.
    RATE_LIMIT = "scenario-rate-limit"
    #: Returns 503 for the first two attempts, then succeeds.
    UNAVAILABLE_THEN_OK = "scenario-unavailable-once"
    #: Always returns 503, so retries exhaust and the circuit opens.
    ALWAYS_UNAVAILABLE = "scenario-unavailable"
    #: Returns 400. Never retried.
    REJECT = "scenario-reject"
    #: Returns 401 once, exercising the credential-refresh retry.
    AUTH_CHALLENGE = "scenario-auth-challenge"
    #: Accepts, then completes with a failure webhook.
    ASYNC_FAILURE = "scenario-async-failure"
    #: Accepts without returning any reference, exercising the escalation path.
    NO_REFERENCE = "scenario-no-reference"
    #: Reports a status string no adapter knows, exercising the unknown path.
    UNKNOWN_STATUS = "scenario-unknown-status"
    #: Emits its completion webhook before the create response is returned.
    WEBHOOK_FIRST = "scenario-webhook-first"

    @property
    def prefix(self) -> str:
        """The external-reference prefix that selects this scenario."""
        return "" if self is Scenario.HEALTHY else f"{self.value}-"

    @classmethod
    def detect(cls, external_reference: str | None) -> Scenario:
        """Select the scenario encoded in an external reference."""
        if not external_reference:
            return cls.HEALTHY
        lowered = external_reference.strip().lower()
        # Longest match first so "scenario-unavailable-once" is not shadowed by
        # "scenario-unavailable".
        for scenario in sorted(cls, key=lambda item: len(item.value), reverse=True):
            if scenario is cls.HEALTHY:
                continue
            if lowered.startswith(scenario.value):
                return scenario
        return cls.HEALTHY


@dataclass(slots=True)
class AttemptCounter:
    """Counts attempts per reference so "fail the first N times" is repeatable."""

    counts: dict[str, int]

    def __init__(self) -> None:
        self.counts = {}

    def record(self, key: str) -> int:
        self.counts[key] = self.counts.get(key, 0) + 1
        return self.counts[key]

    def reset(self) -> None:
        self.counts.clear()


#: How long the SLOW scenario waits. Comfortably inside every configured budget.
SLOW_RESPONSE_SECONDS = 0.4

#: How long the TIMEOUT scenario waits. Longer than every configured budget.
TIMEOUT_RESPONSE_SECONDS = 30.0

#: Retry-After returned by the RATE_LIMIT scenario.
RATE_LIMIT_RETRY_AFTER_SECONDS = 2

#: Attempts the UNAVAILABLE_THEN_OK scenario fails before succeeding.
UNAVAILABLE_ATTEMPTS = 2
