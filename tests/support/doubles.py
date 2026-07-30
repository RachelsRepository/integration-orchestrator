"""Test doubles for the infrastructure ports.

Each one implements the same protocol as its production counterpart, so a test
can build a full object graph without Redis, Kafka or a provider being reachable
while still exercising the real orchestration code.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import TracebackType
from typing import Any

from integration_orchestrator.application.ports.security import CachedToken
from integration_orchestrator.domain.contracts import (
    CancelProviderOperationCommand,
    CreateProviderOperationCommand,
    InboundWebhook,
    NormalizedWebhookEvent,
    ProviderHealthProbe,
    ProviderOperationResult,
    WebhookVerification,
)
from integration_orchestrator.domain.entities import ProviderDescriptor
from integration_orchestrator.domain.enums import (
    CircuitState,
    NormalizedStatus,
    OperationType,
)
from integration_orchestrator.domain.errors import (
    ProviderNotConfiguredError,
    UnsupportedOperationError,
)
from integration_orchestrator.domain.policies import CircuitSnapshot
from integration_orchestrator.domain.value_objects import ProviderSlug, SignatureMetadata

NORTHSTAR = ProviderSlug.parse("northstar")


def descriptor_for(
    slug: ProviderSlug,
    *,
    supports_cancellation: bool = True,
    supports_status_lookup: bool = True,
    supports_provider_idempotency: bool = True,
    supported_operations: frozenset[OperationType] | None = None,
) -> ProviderDescriptor:
    return ProviderDescriptor(
        slug=slug,
        display_name=slug.value.title(),
        authentication_type="oauth2_client_credentials",
        enabled=True,
        supported_operations=supported_operations or frozenset(OperationType),
        supports_cancellation=supports_cancellation,
        supports_status_lookup=supports_status_lookup,
        supports_provider_idempotency=supports_provider_idempotency,
        webhook_signature_scheme="hmac_sha256",
        max_concurrency=4,
        max_attempts=3,
        total_timeout_seconds=10.0,
    )


class FakeGateway:
    """A scripted provider adapter.

    Results are queued and consumed in order; exceptions in the queue are raised
    instead of returned, which is how transport failures are modelled.
    """

    def __init__(
        self,
        slug: ProviderSlug = NORTHSTAR,
        *,
        descriptor: ProviderDescriptor | None = None,
    ) -> None:
        self._slug = slug
        self._descriptor = descriptor or descriptor_for(slug)
        self.create_results: list[ProviderOperationResult | Exception] = []
        self.status_results: list[ProviderOperationResult | Exception] = []
        self.cancel_results: list[ProviderOperationResult | Exception] = []
        self.verification: WebhookVerification | None = None
        self.normalized_event: NormalizedWebhookEvent | None = None
        self.healthy = True
        self.create_calls: list[CreateProviderOperationCommand] = []
        self.status_calls: list[str] = []
        self.cancel_calls: list[CancelProviderOperationCommand] = []

    @property
    def slug(self) -> ProviderSlug:
        return self._slug

    def descriptor(self) -> ProviderDescriptor:
        return self._descriptor

    def queue_create(self, *results: ProviderOperationResult | Exception) -> FakeGateway:
        self.create_results.extend(results)
        return self

    def queue_status(self, *results: ProviderOperationResult | Exception) -> FakeGateway:
        self.status_results.extend(results)
        return self

    def queue_cancel(self, *results: ProviderOperationResult | Exception) -> FakeGateway:
        self.cancel_results.extend(results)
        return self

    async def create_operation(
        self, command: CreateProviderOperationCommand
    ) -> ProviderOperationResult:
        self.create_calls.append(command)
        return _next(self.create_results, "create_operation")

    async def get_operation_status(self, provider_reference: str) -> ProviderOperationResult:
        self.status_calls.append(provider_reference)
        if not self._descriptor.supports_status_lookup:
            raise UnsupportedOperationError(
                "this provider does not support status lookup", provider=self._slug.value
            )
        return _next(self.status_results, "get_operation_status")

    async def cancel_operation(
        self, command: CancelProviderOperationCommand
    ) -> ProviderOperationResult:
        self.cancel_calls.append(command)
        if not self._descriptor.supports_cancellation:
            raise UnsupportedOperationError(
                "this provider does not support cancellation", provider=self._slug.value
            )
        return _next(self.cancel_results, "cancel_operation")

    def validate_webhook(self, webhook: InboundWebhook) -> WebhookVerification:
        del webhook
        if self.verification is not None:
            return self.verification
        return WebhookVerification.accepted(
            SignatureMetadata(scheme="hmac_sha256", key_id="test", verified=True)
        )

    def normalize_webhook(self, webhook: InboundWebhook) -> NormalizedWebhookEvent:
        del webhook
        if self.normalized_event is None:
            raise AssertionError("no normalized webhook event was scripted")
        return self.normalized_event

    async def health_check(self) -> ProviderHealthProbe:
        return ProviderHealthProbe(healthy=self.healthy, checked_at=_now(), latency_ms=1.0)


def _next(
    queue: list[ProviderOperationResult | Exception], operation: str
) -> ProviderOperationResult:
    if not queue:
        raise AssertionError(f"no result was scripted for {operation}")
    item = queue.pop(0)
    if isinstance(item, Exception):
        raise item
    return item


def _now() -> datetime:
    return datetime.now(tz=UTC)


class FakeRegistry:
    """A registry over a fixed set of fake gateways."""

    def __init__(self, *gateways: Any) -> None:
        self._gateways = {gateway.slug: gateway for gateway in gateways}

    def get(self, slug: ProviderSlug) -> Any:
        gateway = self._gateways.get(slug)
        if gateway is None:
            raise ProviderNotConfiguredError(slug.value)
        return gateway

    def has(self, slug: ProviderSlug) -> bool:
        return slug in self._gateways

    def all(self) -> Iterable[Any]:
        return list(self._gateways.values())

    def descriptors(self) -> Iterable[ProviderDescriptor]:
        return [gateway.descriptor() for gateway in self._gateways.values()]


class MemoryTokenCache:
    """Process-local stand-in for the Redis token cache."""

    def __init__(self) -> None:
        self._tokens: dict[str, CachedToken] = {}
        self.invalidations: list[str] = []

    async def get(self, provider: ProviderSlug) -> CachedToken | None:
        return self._tokens.get(provider.value)

    async def set(self, provider: ProviderSlug, token: CachedToken) -> None:
        self._tokens[provider.value] = token

    async def invalidate(self, provider: ProviderSlug) -> None:
        self.invalidations.append(provider.value)
        self._tokens.pop(provider.value, None)


class _AlwaysAcquiredLock:
    async def __aenter__(self) -> bool:
        return True

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


class NullLockManager:
    """Grants every lock immediately.

    Correct for single-process tests: the lock exists to stop several replicas
    refreshing a token at once, and there is only one replica here.
    """

    def lock(
        self, resource: str, *, ttl_seconds: float = 30.0, wait_seconds: float = 5.0
    ) -> _AlwaysAcquiredLock:
        del resource, ttl_seconds, wait_seconds
        return _AlwaysAcquiredLock()


class MemoryCircuitBreaker:
    """An in-process circuit breaker with the same interface as the Redis one."""

    def __init__(self, *, allow: bool = True) -> None:
        self._allow = allow
        self._state: dict[str, CircuitState] = {}
        self.failures: list[str] = []
        self.successes: list[str] = []

    async def state(self, provider: ProviderSlug) -> CircuitSnapshot:
        return CircuitSnapshot(state=self._state.get(provider.value, CircuitState.CLOSED))

    async def allow(self, provider: ProviderSlug) -> bool:
        if self._state.get(provider.value) is CircuitState.OPEN:
            return False
        return self._allow

    async def record_success(self, provider: ProviderSlug) -> CircuitState:
        self.successes.append(provider.value)
        self._state[provider.value] = CircuitState.CLOSED
        return CircuitState.CLOSED

    async def record_failure(self, provider: ProviderSlug) -> CircuitState:
        self.failures.append(provider.value)
        return self._state.get(provider.value, CircuitState.CLOSED)

    async def reset(self, provider: ProviderSlug) -> None:
        self._state.pop(provider.value, None)

    def open(self, provider: ProviderSlug) -> None:
        self._state[provider.value] = CircuitState.OPEN


class AllowAllRateLimiter:
    async def acquire(self, provider: ProviderSlug) -> bool:
        del provider
        return True

    async def retry_after(self, provider: ProviderSlug) -> float:
        del provider
        return 0.0


class DenyAllRateLimiter:
    def __init__(self, retry_after_seconds: float = 1.5) -> None:
        self._retry_after = retry_after_seconds

    async def acquire(self, provider: ProviderSlug) -> bool:
        del provider
        return False

    async def retry_after(self, provider: ProviderSlug) -> float:
        del provider
        return self._retry_after


class UnlimitedBulkhead:
    """A concurrency limiter that never rejects."""

    @asynccontextmanager
    async def slot(self, provider: ProviderSlug) -> AsyncIterator[None]:
        del provider
        yield

    def in_flight(self, provider: ProviderSlug) -> int:
        del provider
        return 0

    def capacity(self, provider: ProviderSlug) -> int:
        del provider
        return 1


class RecordingMetrics:
    """Records metric calls so tests can assert on instrumentation."""

    def __init__(self) -> None:
        self.counters: list[tuple[str, dict[str, str], float]] = []
        self.observations: list[tuple[str, float, dict[str, str]]] = []
        self.gauges: list[tuple[str, float, dict[str, str]]] = []

    def increment(
        self, name: str, *, labels: dict[str, str] | None = None, amount: float = 1.0
    ) -> None:
        self.counters.append((name, dict(labels or {}), amount))

    def observe(self, name: str, value: float, *, labels: dict[str, str] | None = None) -> None:
        self.observations.append((name, value, dict(labels or {})))

    def set_gauge(self, name: str, value: float, *, labels: dict[str, str] | None = None) -> None:
        self.gauges.append((name, value, dict(labels or {})))

    def counter_names(self) -> list[str]:
        return [name for name, _, _ in self.counters]

    def count(self, name: str) -> float:
        return sum(amount for metric, _, amount in self.counters if metric == name)


class RecordingPublisher:
    """An event publisher that records envelopes and can be told to fail."""

    def __init__(self, *, fail_times: int = 0) -> None:
        self.published: list[Any] = []
        self._fail_times = fail_times

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish(self, envelope: Any) -> None:
        await self.publish_batch([envelope])

    async def publish_batch(self, envelopes: Sequence[Any]) -> None:
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("publication failed")
        self.published.extend(envelopes)

    async def healthy(self) -> bool:
        return True


def accepted_result(reference: str = "prv-1") -> ProviderOperationResult:
    return ProviderOperationResult.success(
        normalized_status=NormalizedStatus.ACCEPTED,
        provider_reference=reference,
        provider_status="accepted",
    )


def completed_result(reference: str = "prv-1") -> ProviderOperationResult:
    return ProviderOperationResult.success(
        normalized_status=NormalizedStatus.SUCCEEDED,
        provider_reference=reference,
        provider_status="completed",
    )
