"""Ports for ambient system capabilities.

Time, identity and randomness are injected rather than read from module-level
functions. Without this, half the behaviour in the platform (retry scheduling,
staleness detection, replay windows) would only be testable by sleeping, and
tests that sleep are slow and flaky.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class Clock(Protocol):
    """Supplies the current time as a timezone-aware UTC value."""

    def now(self) -> datetime:
        """Return the current instant in UTC."""
        ...


@runtime_checkable
class IdentifierGenerator(Protocol):
    """Supplies new identifiers.

    Injected so tests can produce stable ids, which makes assertions on audit
    trails and event payloads exact rather than approximate.
    """

    def new_id(self) -> UUID:
        """Return a fresh unique identifier."""
        ...


@runtime_checkable
class JitterSource(Protocol):
    """Supplies randomness in ``[0, 1)`` for backoff spreading."""

    def jitter(self) -> float:
        """Return a value in the half-open interval ``[0, 1)``."""
        ...
