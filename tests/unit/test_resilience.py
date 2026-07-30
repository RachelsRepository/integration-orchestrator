"""Rate limiting, circuit breaking, bulkheading and in-process retry."""

from __future__ import annotations

import asyncio

import pytest

from integration_orchestrator.config.settings import ProviderSettings
from integration_orchestrator.domain.enums import CircuitState, ErrorCategory
from integration_orchestrator.domain.errors import (
    BulkheadRejectedError,
    CircuitOpenError,
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderUnavailableError,
    ProviderValidationError,
)
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.providers.resilient import (
    IN_PROCESS_MAX_ATTEMPTS,
    ResilientProviderGateway,
)
from integration_orchestrator.infrastructure.resilience.bulkhead import ProviderBulkhead
from tests.support.doubles import (
    AllowAllRateLimiter,
    DenyAllRateLimiter,
    FakeGateway,
    MemoryCircuitBreaker,
    RecordingMetrics,
    accepted_result,
    descriptor_for,
)

pytestmark = pytest.mark.unit

NORTHSTAR = ProviderSlug.parse("northstar")


def provider_settings(
    *, max_concurrency: int = 2, acquire_timeout: float = 0.05
) -> dict[str, ProviderSettings]:
    return {
        NORTHSTAR.value: ProviderSettings(
            display_name="Northstar Connect",
            base_url="http://sandbox.invalid/northstar",
            max_concurrency=max_concurrency,
            acquire_timeout_seconds=acquire_timeout,
        )
    }


def build(
    inner: FakeGateway,
    *,
    metrics: RecordingMetrics,
    breaker: MemoryCircuitBreaker | None = None,
    limiter: AllowAllRateLimiter | DenyAllRateLimiter | None = None,
    bulkhead: ProviderBulkhead | None = None,
) -> ResilientProviderGateway:
    return ResilientProviderGateway(
        inner,
        circuit_breaker=breaker or MemoryCircuitBreaker(),
        rate_limiter=limiter or AllowAllRateLimiter(),
        bulkhead=bulkhead or ProviderBulkhead(provider_settings(), metrics=metrics),
        metrics=metrics,
    )


# -- bulkhead ---------------------------------------------------------------


async def test_a_bulkhead_bounds_concurrent_calls_per_provider(
    metrics: RecordingMetrics,
) -> None:
    bulkhead = ProviderBulkhead(provider_settings(max_concurrency=2), metrics=metrics)
    release = asyncio.Event()
    peak = 0

    async def _occupy() -> None:
        nonlocal peak
        async with bulkhead.slot(NORTHSTAR):
            peak = max(peak, bulkhead.in_flight(NORTHSTAR))
            await release.wait()

    holders = [asyncio.create_task(_occupy()) for _ in range(2)]
    await asyncio.sleep(0)
    assert bulkhead.in_flight(NORTHSTAR) == 2
    assert bulkhead.available(NORTHSTAR) == 0

    release.set()
    await asyncio.gather(*holders)

    assert peak == 2
    assert bulkhead.in_flight(NORTHSTAR) == 0


async def test_a_saturated_bulkhead_sheds_load_instead_of_queueing_forever(
    metrics: RecordingMetrics,
) -> None:
    """Unbounded queueing turns a slow dependency into unbounded memory growth."""
    bulkhead = ProviderBulkhead(
        provider_settings(max_concurrency=1, acquire_timeout=0.01), metrics=metrics
    )
    release = asyncio.Event()

    async def _hold() -> None:
        async with bulkhead.slot(NORTHSTAR):
            await release.wait()

    holder = asyncio.create_task(_hold())
    await asyncio.sleep(0)

    with pytest.raises(BulkheadRejectedError) as caught:
        async with bulkhead.slot(NORTHSTAR):
            pass

    release.set()
    await holder

    assert caught.value.metadata["concurrency_limit"] == 1
    assert caught.value.retryable is True
    assert metrics.count("provider_bulkhead_rejections_total") == 1


async def test_a_slot_is_released_even_when_the_call_raises(
    metrics: RecordingMetrics,
) -> None:
    bulkhead = ProviderBulkhead(provider_settings(max_concurrency=1), metrics=metrics)

    with pytest.raises(RuntimeError):
        async with bulkhead.slot(NORTHSTAR):
            raise RuntimeError("the provider call blew up")

    assert bulkhead.in_flight(NORTHSTAR) == 0


async def test_an_unconfigured_provider_is_not_blocked_by_the_bulkhead(
    metrics: RecordingMetrics,
) -> None:
    bulkhead = ProviderBulkhead(provider_settings(), metrics=metrics)

    async with bulkhead.slot(ProviderSlug("unregistered")):
        pass

    assert bulkhead.capacity(ProviderSlug("unregistered")) == 0


# -- rate limiting ----------------------------------------------------------


async def test_the_client_side_rate_limit_is_checked_before_the_provider_is_called(
    metrics: RecordingMetrics,
) -> None:
    inner = FakeGateway(NORTHSTAR)
    gateway = build(inner, metrics=metrics, limiter=DenyAllRateLimiter(2.5))

    with pytest.raises(ProviderRateLimitError) as caught:
        await gateway.get_operation_status("prv-1")

    assert caught.value.retry_after_seconds == 2.5
    assert caught.value.metadata["source"] == "client_rate_limiter"
    assert inner.status_calls == []


# -- circuit breaking -------------------------------------------------------


async def test_an_open_circuit_rejects_without_touching_the_provider(
    metrics: RecordingMetrics,
) -> None:
    inner = FakeGateway(NORTHSTAR)
    breaker = MemoryCircuitBreaker()
    breaker.open(NORTHSTAR)
    gateway = build(inner, metrics=metrics, breaker=breaker)

    with pytest.raises(CircuitOpenError):
        await gateway.get_operation_status("prv-1")

    assert inner.status_calls == []


async def test_only_availability_failures_count_toward_opening_the_circuit(
    metrics: RecordingMetrics,
) -> None:
    """One caller sending a malformed payload must not trip the breaker for all."""
    inner = FakeGateway(NORTHSTAR)
    breaker = MemoryCircuitBreaker()
    gateway = build(inner, metrics=metrics, breaker=breaker)

    inner.queue_status(ProviderValidationError("bad field", provider=NORTHSTAR.value))
    with pytest.raises(ProviderValidationError):
        await gateway.get_operation_status("prv-1")
    assert breaker.failures == []

    inner.queue_status(
        ProviderUnavailableError("503", provider=NORTHSTAR.value),
        ProviderUnavailableError("503", provider=NORTHSTAR.value),
    )
    with pytest.raises(ProviderUnavailableError):
        await gateway.get_operation_status("prv-1")
    assert breaker.failures == [NORTHSTAR.value, NORTHSTAR.value]


async def test_a_health_probe_still_runs_while_the_circuit_is_open(
    metrics: RecordingMetrics,
) -> None:
    """The probe is how an operator learns the provider came back."""
    inner = FakeGateway(NORTHSTAR)
    breaker = MemoryCircuitBreaker()
    breaker.open(NORTHSTAR)
    gateway = build(inner, metrics=metrics, breaker=breaker)

    probe = await gateway.health_check()

    assert probe.healthy is True


async def test_a_circuit_transition_is_reported_once(metrics: RecordingMetrics) -> None:
    seen: list[tuple[CircuitState, CircuitState]] = []

    async def _record(provider: ProviderSlug, previous: CircuitState, new: CircuitState) -> None:
        del provider
        seen.append((previous, new))

    inner = FakeGateway(NORTHSTAR)
    inner.queue_status(accepted_result(), accepted_result())
    breaker = MemoryCircuitBreaker()
    gateway = ResilientProviderGateway(
        inner,
        circuit_breaker=breaker,
        rate_limiter=AllowAllRateLimiter(),
        bulkhead=ProviderBulkhead(provider_settings(), metrics=metrics),
        metrics=metrics,
        on_circuit_change=_record,
    )

    await gateway.get_operation_status("prv-1")
    await gateway.get_operation_status("prv-1")

    assert seen == []  # stayed closed throughout


# -- in-process retry -------------------------------------------------------


async def test_a_transient_failure_is_retried_once_in_process(
    metrics: RecordingMetrics,
) -> None:
    inner = FakeGateway(NORTHSTAR)
    inner.queue_status(
        ProviderUnavailableError("503", provider=NORTHSTAR.value), accepted_result("prv-1")
    )
    gateway = build(inner, metrics=metrics)

    result = await gateway.get_operation_status("prv-1")

    assert result.accepted
    assert len(inner.status_calls) == IN_PROCESS_MAX_ATTEMPTS


async def test_rate_limiting_is_never_retried_in_process(
    metrics: RecordingMetrics,
) -> None:
    """The provider asked us to wait; a quarter-second retry does not honour that."""
    inner = FakeGateway(NORTHSTAR)
    inner.queue_status(
        ProviderRateLimitError("429", provider=NORTHSTAR.value, retry_after_seconds=30.0)
    )
    gateway = build(inner, metrics=metrics)

    with pytest.raises(ProviderRateLimitError):
        await gateway.get_operation_status("prv-1")

    assert len(inner.status_calls) == 1


async def test_a_create_is_only_retried_in_process_when_the_provider_deduplicates(
    metrics: RecordingMetrics,
) -> None:
    """Otherwise the retry creates a second real operation."""
    from uuid import uuid4

    from integration_orchestrator.domain.contracts import CreateProviderOperationCommand
    from integration_orchestrator.domain.enums import OperationType
    from integration_orchestrator.domain.value_objects import (
        CorrelationId,
        ExternalReference,
    )

    command = CreateProviderOperationCommand(
        request_id=uuid4(),
        provider=NORTHSTAR,
        operation_type=OperationType.RESOURCE_PROVISION,
        external_reference=ExternalReference("order-1"),
        payload={},
        correlation_id=CorrelationId("corr-1"),
        idempotency_key="idem-key-0001",
    )

    unsafe = FakeGateway(
        NORTHSTAR, descriptor=descriptor_for(NORTHSTAR, supports_provider_idempotency=False)
    )
    unsafe.queue_create(ProviderTimeoutError("timeout", provider=NORTHSTAR.value))
    with pytest.raises(ProviderTimeoutError):
        await build(unsafe, metrics=metrics).create_operation(command)
    assert len(unsafe.create_calls) == 1

    safe = FakeGateway(
        NORTHSTAR, descriptor=descriptor_for(NORTHSTAR, supports_provider_idempotency=True)
    )
    safe.queue_create(
        ProviderTimeoutError("timeout", provider=NORTHSTAR.value), accepted_result("prv-1")
    )
    result = await build(safe, metrics=metrics).create_operation(command)
    assert result.accepted
    assert len(safe.create_calls) == 2


def test_the_circuit_failure_categories_exclude_client_errors() -> None:
    from integration_orchestrator.infrastructure.providers.resilient import (
        _CIRCUIT_FAILURE_CATEGORIES,
    )

    assert ErrorCategory.PROVIDER_VALIDATION not in _CIRCUIT_FAILURE_CATEGORIES
    assert ErrorCategory.PROVIDER_AUTHENTICATION not in _CIRCUIT_FAILURE_CATEGORIES
    assert ErrorCategory.PROVIDER_UNAVAILABLE in _CIRCUIT_FAILURE_CATEGORIES
