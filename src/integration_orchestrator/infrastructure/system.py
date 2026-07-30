"""Adapters for ambient system capabilities.

Thin by design: the value is not in the implementations but in the fact that
every consumer takes them as constructor arguments, so a test can freeze time or
fix identifiers without patching module globals.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime
from uuid import UUID, uuid4


class SystemClock:
    """Wall-clock time in UTC."""

    def now(self) -> datetime:
        return datetime.now(tz=UTC)


class FrozenClock:
    """A clock that only moves when told to.

    Lives in the production tree rather than the test tree because the workers'
    integration tests construct real components and need to advance time through
    the same interface those components hold.
    """

    def __init__(self, instant: datetime) -> None:
        if instant.tzinfo is None:
            raise ValueError("a frozen clock requires a timezone-aware instant")
        self._instant = instant.astimezone(UTC)

    def now(self) -> datetime:
        return self._instant

    def set(self, instant: datetime) -> None:
        self._instant = instant.astimezone(UTC)

    def advance(self, seconds: float) -> datetime:
        from datetime import timedelta

        self._instant = self._instant + timedelta(seconds=seconds)
        return self._instant


class UuidGenerator:
    """Random (version 4) identifiers."""

    def new_id(self) -> UUID:
        return uuid4()


class SequentialIdentifierGenerator:
    """Deterministic identifiers derived from a counter.

    Produces valid UUIDs so that database columns and event payloads behave
    exactly as they do in production, while remaining predictable.
    """

    def __init__(self, *, namespace: int = 0) -> None:
        self._namespace = namespace
        self._counter = 0

    def new_id(self) -> UUID:
        self._counter += 1
        return UUID(int=(self._namespace << 64) | self._counter)


class RandomJitter:
    """Uniform jitter in ``[0, 1)``.

    ``random`` rather than ``secrets``: this value spreads retry timing and is
    never used for anything security-sensitive, so the faster generator is the
    right choice.
    """

    def jitter(self) -> float:
        return random.random()  # noqa: S311


class FixedJitter:
    """A constant jitter value, so backoff arithmetic can be asserted exactly."""

    def __init__(self, value: float = 0.5) -> None:
        if not 0.0 <= value < 1.0:
            raise ValueError("jitter must fall in [0, 1)")
        self._value = value

    def jitter(self) -> float:
        return self._value
