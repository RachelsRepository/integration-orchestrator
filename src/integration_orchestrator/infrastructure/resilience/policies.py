"""Configuration-driven policy provider.

Bridges typed settings to the plain domain policy objects the application layer
consumes, so nothing above infrastructure has to know that a settings module
exists.
"""

from __future__ import annotations

from integration_orchestrator.config.settings import ProviderSettings
from integration_orchestrator.domain.errors import ProviderNotConfiguredError
from integration_orchestrator.domain.policies import CircuitBreakerPolicy, RetryPolicy
from integration_orchestrator.domain.value_objects import ProviderSlug


class ConfiguredPolicyProvider:
    """Builds retry and circuit breaker policies from provider settings."""

    def __init__(self, provider_settings: dict[str, ProviderSettings]) -> None:
        self._settings = provider_settings
        self._retry: dict[str, RetryPolicy] = {
            slug: RetryPolicy(
                max_attempts=config.max_attempts,
                base_seconds=config.backoff_base_seconds,
                multiplier=config.backoff_multiplier,
                max_seconds=config.backoff_max_seconds,
                retry_after_cap_seconds=config.retry_after_cap_seconds,
            )
            for slug, config in provider_settings.items()
        }
        self._circuit: dict[str, CircuitBreakerPolicy] = {
            slug: CircuitBreakerPolicy(
                failure_threshold=config.circuit_failure_threshold,
                open_seconds=config.circuit_open_seconds,
                success_threshold=config.circuit_success_threshold,
                half_open_max_probes=config.circuit_half_open_max_probes,
            )
            for slug, config in provider_settings.items()
        }

    def retry_policy(self, provider: ProviderSlug) -> RetryPolicy:
        try:
            return self._retry[provider.value]
        except KeyError as exc:
            raise ProviderNotConfiguredError(provider.value) from exc

    def circuit_policy(self, provider: ProviderSlug) -> CircuitBreakerPolicy:
        try:
            return self._circuit[provider.value]
        except KeyError as exc:
            raise ProviderNotConfiguredError(provider.value) from exc
