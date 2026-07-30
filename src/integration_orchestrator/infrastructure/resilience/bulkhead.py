"""Provider bulkheads.

Each provider gets a fixed number of concurrency slots in each process. A slow
provider can occupy all of *its* slots and no more, so it cannot consume the
worker pool and starve the providers that are healthy. This is the difference
between one degraded integration and a degraded platform.

The limit is per process, not global. That is a deliberate simplification: a
distributed semaphore would need a lease, a renewal loop, and a recovery path for
crashed holders, and it would put a Redis round trip in front of every provider
call. Sizing is therefore ``limit x replicas``, which is documented in
``docs/resilience.md`` so capacity planning accounts for it.

Waiting is bounded. When no slot frees within the acquisition timeout the call is
rejected outright rather than queued. Unbounded queueing is how a slow dependency
turns into unbounded memory growth and a latency profile where every request
times out having achieved nothing.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.config.settings import ProviderSettings
from integration_orchestrator.domain.errors import BulkheadRejectedError
from integration_orchestrator.domain.value_objects import ProviderSlug

logger = logging.getLogger(__name__)


class ProviderBulkhead:
    """Per-provider concurrency limiting with bounded waiting."""

    def __init__(
        self,
        provider_settings: dict[str, ProviderSettings],
        *,
        metrics: MetricsSink,
    ) -> None:
        self._settings = provider_settings
        self._metrics = metrics
        self._semaphores: dict[str, asyncio.Semaphore] = {
            slug: asyncio.Semaphore(config.max_concurrency)
            for slug, config in provider_settings.items()
        }
        self._in_flight: dict[str, int] = dict.fromkeys(provider_settings, 0)

    @asynccontextmanager
    async def slot(self, provider: ProviderSlug) -> AsyncIterator[None]:
        semaphore = self._semaphores.get(provider.value)
        if semaphore is None:
            # An unregistered provider has no configured limit. Letting the call
            # through is safe because the registry has already rejected unknown
            # providers before this point.
            yield
            return

        config = self._settings[provider.value]
        try:
            await asyncio.wait_for(semaphore.acquire(), timeout=config.acquire_timeout_seconds)
        except TimeoutError as exc:
            self._metrics.increment(
                "provider_bulkhead_rejections_total", labels={"provider": provider.value}
            )
            logger.warning(
                "rejected a provider call because the bulkhead is saturated",
                extra={
                    "provider": provider.value,
                    "concurrency_limit": config.max_concurrency,
                    "in_flight": self._in_flight.get(provider.value, 0),
                },
            )
            raise BulkheadRejectedError(provider.value, limit=config.max_concurrency) from exc

        self._in_flight[provider.value] += 1
        self._metrics.set_gauge(
            "provider_in_flight_requests",
            self._in_flight[provider.value],
            labels={"provider": provider.value},
        )
        try:
            yield
        finally:
            self._in_flight[provider.value] -= 1
            self._metrics.set_gauge(
                "provider_in_flight_requests",
                self._in_flight[provider.value],
                labels={"provider": provider.value},
            )
            semaphore.release()

    def in_flight(self, provider: ProviderSlug) -> int:
        return self._in_flight.get(provider.value, 0)

    def capacity(self, provider: ProviderSlug) -> int:
        config = self._settings.get(provider.value)
        return config.max_concurrency if config else 0

    def available(self, provider: ProviderSlug) -> int:
        return max(0, self.capacity(provider) - self.in_flight(provider))
