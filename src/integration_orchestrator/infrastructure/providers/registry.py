"""Provider registry and adapter construction.

Adapters are looked up by slug against a map built at startup. This is the
mechanism that makes "add a provider without changing core logic" true rather
than aspirational: onboarding a fourth provider means writing an adapter class,
adding one entry to :data:`ADAPTER_FACTORIES`, and supplying configuration. No
use case, no dispatcher branch, and no domain type changes.

Registration is also where the resilience decorator is applied, so it is
impossible to register an adapter that accidentally bypasses the circuit
breaker, the rate limiter or the bulkhead.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable

import httpx

from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.application.ports.provider_gateway import ProviderGateway
from integration_orchestrator.application.ports.resilience import (
    CircuitBreaker,
    LockManager,
    RateLimiter,
)
from integration_orchestrator.application.ports.security import TokenCache
from integration_orchestrator.config.settings import ProviderSettings, Settings
from integration_orchestrator.domain.entities import ProviderDescriptor
from integration_orchestrator.domain.errors import ProviderNotConfiguredError
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.providers.auth import build_authenticator
from integration_orchestrator.infrastructure.providers.base import BaseProviderAdapter
from integration_orchestrator.infrastructure.providers.cobalt import CobaltAdapter
from integration_orchestrator.infrastructure.providers.http import ProviderHttpClient
from integration_orchestrator.infrastructure.providers.meridian import (
    API_KEY_HEADER as MERIDIAN_API_KEY_HEADER,
)
from integration_orchestrator.infrastructure.providers.meridian import (
    MeridianAdapter,
)
from integration_orchestrator.infrastructure.providers.northstar import NorthstarAdapter
from integration_orchestrator.infrastructure.providers.resilient import (
    CircuitChangeCallback,
    ResilientProviderGateway,
)
from integration_orchestrator.infrastructure.resilience.bulkhead import ProviderBulkhead

logger = logging.getLogger(__name__)

AdapterFactory = Callable[..., BaseProviderAdapter]

#: The single place where a provider slug is bound to an implementation.
ADAPTER_FACTORIES: dict[str, type[BaseProviderAdapter]] = {
    "northstar": NorthstarAdapter,
    "meridian": MeridianAdapter,
    "cobalt": CobaltAdapter,
}

#: Providers that expect their API key in a non-default header.
API_KEY_HEADERS: dict[str, str] = {
    "meridian": MERIDIAN_API_KEY_HEADER,
}


class ProviderRegistry:
    """Immutable map of provider slug to gateway."""

    def __init__(self, gateways: dict[str, ProviderGateway]) -> None:
        self._gateways = dict(gateways)

    def get(self, slug: ProviderSlug) -> ProviderGateway:
        gateway = self._gateways.get(slug.value)
        if gateway is None:
            raise ProviderNotConfiguredError(slug.value)
        return gateway

    def has(self, slug: ProviderSlug) -> bool:
        return slug.value in self._gateways

    def all(self) -> Iterable[ProviderGateway]:
        return list(self._gateways.values())

    def descriptors(self) -> Iterable[ProviderDescriptor]:
        return [gateway.descriptor() for gateway in self._gateways.values()]

    def slugs(self) -> list[str]:
        return sorted(self._gateways)


def build_provider_registry(
    settings: Settings,
    *,
    http_client: httpx.AsyncClient,
    token_cache: TokenCache,
    locks: LockManager,
    circuit_breaker: CircuitBreaker,
    rate_limiter: RateLimiter,
    bulkhead: ProviderBulkhead,
    metrics: MetricsSink,
    on_circuit_change: CircuitChangeCallback | None = None,
) -> ProviderRegistry:
    """Construct every enabled provider gateway."""
    gateways: dict[str, ProviderGateway] = {}

    for slug_text, config in settings.enabled_providers().items():
        adapter_class = ADAPTER_FACTORIES.get(slug_text)
        if adapter_class is None:
            # Configuration referenced a provider with no implementation. Failing
            # startup would take the whole service down for one misconfigured
            # provider, so it is skipped loudly and every other provider works.
            logger.error(
                "no adapter is implemented for this configured provider; skipping it",
                extra={"provider": slug_text},
            )
            continue

        slug = ProviderSlug(slug_text)
        adapter = _build_adapter(
            adapter_class,
            slug=slug,
            config=config,
            settings=settings,
            http_client=http_client,
            token_cache=token_cache,
            locks=locks,
            metrics=metrics,
        )
        gateways[slug_text] = ResilientProviderGateway(
            adapter,
            circuit_breaker=circuit_breaker,
            rate_limiter=rate_limiter,
            bulkhead=bulkhead,
            metrics=metrics,
            on_circuit_change=on_circuit_change,
        )
        logger.info(
            "registered a provider adapter",
            extra={
                "provider": slug_text,
                "authentication_type": config.authentication_type.value,
                "base_url": config.base_url,
            },
        )

    if not gateways:
        logger.error("no provider adapters were registered; the platform cannot dispatch")
    return ProviderRegistry(gateways)


def _build_adapter(
    adapter_class: type[BaseProviderAdapter],
    *,
    slug: ProviderSlug,
    config: ProviderSettings,
    settings: Settings,
    http_client: httpx.AsyncClient,
    token_cache: TokenCache,
    locks: LockManager,
    metrics: MetricsSink,
) -> BaseProviderAdapter:
    authenticator = build_authenticator(
        slug=slug,
        config=config,
        client=http_client,
        token_cache=token_cache,
        locks=locks,
        api_key_header=API_KEY_HEADERS.get(slug.value, "X-API-Key"),
    )
    http = ProviderHttpClient(
        slug=slug,
        config=config,
        client=http_client,
        authenticator=authenticator,
        metrics=metrics,
    )
    return adapter_class(
        slug=slug,
        config=config,
        http=http,
        webhook_replay_window_seconds=settings.webhooks.replay_window_seconds,
    )
