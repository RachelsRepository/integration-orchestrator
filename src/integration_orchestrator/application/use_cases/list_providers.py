"""Provider catalogue and health summary.

Health is reported as two independent signals. The circuit breaker state is what
the platform is currently *doing* about a provider, derived from real traffic.
The probe is what the provider says about itself right now. They can disagree —
a breaker can still be open while the provider has recovered — and showing both
is what lets an operator tell "we are still backing off" from "it is still down".
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from integration_orchestrator.application.ports.provider_gateway import (
    ProviderGateway,
    ProviderRegistry,
)
from integration_orchestrator.application.ports.resilience import (
    CircuitBreaker,
    ConcurrencyLimiter,
)
from integration_orchestrator.application.ports.system import Clock
from integration_orchestrator.domain.entities import ProviderDescriptor
from integration_orchestrator.domain.enums import CircuitState
from integration_orchestrator.domain.errors import ProviderError

logger = logging.getLogger(__name__)

PROBE_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class ProviderSummary:
    """A provider's capabilities plus its current operational state."""

    descriptor: ProviderDescriptor
    circuit_state: CircuitState
    failure_count: int
    in_flight: int
    capacity: int
    reachable: bool
    detail: str | None = None

    @property
    def healthy(self) -> bool:
        """A provider is healthy when it is reachable and not being backed off."""
        return self.reachable and self.circuit_state is CircuitState.CLOSED


class ListProvidersUseCase:
    """Builds the provider catalogue with live health information."""

    def __init__(
        self,
        *,
        registry: ProviderRegistry,
        circuit_breaker: CircuitBreaker,
        concurrency: ConcurrencyLimiter,
        clock: Clock,
        probe_timeout_seconds: float = PROBE_TIMEOUT_SECONDS,
    ) -> None:
        self._registry = registry
        self._circuit_breaker = circuit_breaker
        self._concurrency = concurrency
        self._clock = clock
        self._probe_timeout_seconds = probe_timeout_seconds

    async def execute(self, *, probe: bool = True) -> list[ProviderSummary]:
        gateways = list(self._registry.all())
        summaries = await asyncio.gather(
            *(self._summarize(gateway, probe=probe) for gateway in gateways)
        )
        return sorted(summaries, key=lambda summary: summary.descriptor.slug.value)

    async def _summarize(self, gateway: ProviderGateway, *, probe: bool) -> ProviderSummary:
        descriptor = gateway.descriptor()
        snapshot = await self._circuit_breaker.state(gateway.slug)

        reachable = True
        detail: str | None = None
        if probe:
            reachable, detail = await self._probe(gateway)

        return ProviderSummary(
            descriptor=descriptor,
            circuit_state=snapshot.state,
            failure_count=snapshot.failure_count,
            in_flight=self._concurrency.in_flight(gateway.slug),
            capacity=self._concurrency.capacity(gateway.slug),
            reachable=reachable,
            detail=detail,
        )

    async def _probe(self, gateway: ProviderGateway) -> tuple[bool, str | None]:
        """Probe a provider without letting a hung provider stall the endpoint."""
        try:
            async with asyncio.timeout(self._probe_timeout_seconds):
                result = await gateway.health_check()
        except TimeoutError:
            return False, "the health probe exceeded its timeout"
        except ProviderError as exc:
            return False, exc.code
        except Exception:
            logger.exception(
                "provider health probe raised an unexpected error",
                extra={"provider": gateway.slug.value},
            )
            return False, "the health probe failed unexpectedly"
        return result.healthy, result.detail
