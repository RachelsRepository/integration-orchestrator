"""Ports for resilience primitives.

These are ports rather than concrete helpers because their state is shared
across processes. A circuit breaker that lives in one worker's memory tells you
nothing about what the other twenty workers are experiencing, so the interface
is async and the production implementation is backed by Redis.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import Protocol, runtime_checkable

from integration_orchestrator.domain.enums import CircuitState
from integration_orchestrator.domain.policies import (
    CircuitBreakerPolicy,
    CircuitSnapshot,
    RetryPolicy,
)
from integration_orchestrator.domain.value_objects import ProviderSlug


@runtime_checkable
class PolicyProvider(Protocol):
    """Supplies per-provider policies to the application layer.

    Policies originate in configuration, but the application layer must not
    import the settings module: doing so would drag a serialisation framework
    into code that is supposed to depend on nothing but the domain. This port
    hands over plain domain policy objects instead.
    """

    def retry_policy(self, provider: ProviderSlug) -> RetryPolicy:
        """Return the retry bounds and backoff shape for a provider."""
        ...

    def circuit_policy(self, provider: ProviderSlug) -> CircuitBreakerPolicy:
        """Return the circuit breaker thresholds for a provider."""
        ...


@runtime_checkable
class CircuitBreaker(Protocol):
    """Provider-scoped circuit breaker."""

    async def state(self, provider: ProviderSlug) -> CircuitSnapshot:
        """Return the current breaker state for a provider."""
        ...

    async def allow(self, provider: ProviderSlug) -> bool:
        """Ask permission to make a call.

        In half-open state this also reserves the probe slot, so exactly one
        caller gets to test the provider rather than every caller stampeding the
        moment the open window elapses.
        """
        ...

    async def record_success(self, provider: ProviderSlug) -> CircuitState:
        """Record a successful call and return the resulting state."""
        ...

    async def record_failure(self, provider: ProviderSlug) -> CircuitState:
        """Record a failed call and return the resulting state."""
        ...

    async def reset(self, provider: ProviderSlug) -> None:
        """Force the breaker closed. Intended for operator intervention."""
        ...


@runtime_checkable
class RateLimiter(Protocol):
    """Client-side rate limiting, so we throttle ourselves before a provider does."""

    async def acquire(self, provider: ProviderSlug) -> bool:
        """Try to consume one token without waiting."""
        ...

    async def retry_after(self, provider: ProviderSlug) -> float:
        """Seconds until a token is expected to be available."""
        ...


@runtime_checkable
class ConcurrencyLimiter(Protocol):
    """Bulkhead limiting simultaneous in-flight calls per provider."""

    def slot(self, provider: ProviderSlug) -> AbstractAsyncContextManager[None]:
        """Acquire a concurrency slot for the duration of the context.

        Raises
        :class:`~integration_orchestrator.domain.errors.BulkheadRejectedError`
        when no slot becomes free within the configured acquisition timeout.
        Shedding load is deliberate: an unbounded wait queue converts a slow
        provider into a slow platform.
        """
        ...

    def in_flight(self, provider: ProviderSlug) -> int:
        """Current number of occupied slots, for the queue depth gauge."""
        ...

    def capacity(self, provider: ProviderSlug) -> int:
        """Configured slot count for a provider."""
        ...


@runtime_checkable
class DistributedLock(Protocol):
    """A mutual exclusion primitive shared across processes."""

    async def __aenter__(self) -> bool: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


@runtime_checkable
class LockManager(Protocol):
    """Creates named distributed locks."""

    def lock(
        self,
        resource: str,
        *,
        ttl_seconds: float = 30.0,
        wait_seconds: float = 5.0,
    ) -> DistributedLock:
        """Return a lock for ``resource``.

        ``ttl_seconds`` bounds how long a crashed holder can block others;
        ``wait_seconds`` bounds how long this caller waits before giving up.
        """
        ...
