"""Resilience decorator for provider gateways.

Wrapping the adapter rather than putting these controls inside it keeps every
provider's protection identical and keeps the adapters free of policy. The order
of the controls matters:

1. **Rate limiter.** Cheapest check. Shedding here costs one Redis call instead
   of a round trip and a bulkhead slot.
2. **Circuit breaker.** Rejects fast while a provider is known to be failing,
   which is what stops a dead provider consuming capacity healthy ones need.
3. **Bulkhead.** Bounds concurrent calls so one slow provider cannot occupy the
   whole worker pool.
4. **The call itself**, already bounded by the adapter's timeout budget.

Two kinds of retry exist and they are not interchangeable:

*In-process retry* handles a blip — a dropped connection, a single 503 — within
the same call, and is bounded to a small number of attempts over a short window.
It is only applied where repeating the request cannot create a duplicate: reads,
cancellations, and creates for providers that honour an idempotency key.

*Durable retry* handles everything else. The dispatcher records
``retry_scheduled`` with a due time and the retry worker picks it up later. It
survives process death, respects long ``Retry-After`` values, and is the only
safe option for a provider such as Meridian that does not deduplicate creates.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.application.ports.provider_gateway import ProviderGateway
from integration_orchestrator.application.ports.resilience import (
    CircuitBreaker,
    RateLimiter,
)
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
from integration_orchestrator.domain.enums import CircuitState, ErrorCategory
from integration_orchestrator.domain.errors import (
    CircuitOpenError,
    ProviderError,
    ProviderRateLimitError,
)
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.resilience.bulkhead import ProviderBulkhead

logger = logging.getLogger(__name__)

T = TypeVar("T")

CircuitChangeCallback = Callable[[ProviderSlug, CircuitState, CircuitState], Awaitable[None]]

# In-process retries are deliberately small. Anything that needs to wait longer
# than this belongs in the durable retry path, where it survives a restart.
IN_PROCESS_MAX_ATTEMPTS = 2
IN_PROCESS_BACKOFF_SECONDS = 0.25

# Only these categories indicate the provider itself is unhealthy. A rejected
# payload or a bad credential says nothing about provider availability, and
# counting those toward the failure threshold would trip the breaker for every
# caller because one caller sent malformed input.
_CIRCUIT_FAILURE_CATEGORIES: frozenset[ErrorCategory] = frozenset(
    {
        ErrorCategory.PROVIDER_TIMEOUT,
        ErrorCategory.PROVIDER_UNAVAILABLE,
        ErrorCategory.PROVIDER_RATE_LIMIT,
    }
)


class ResilientProviderGateway:
    """Applies rate limiting, circuit breaking and bulkheading to an adapter."""

    def __init__(
        self,
        inner: ProviderGateway,
        *,
        circuit_breaker: CircuitBreaker,
        rate_limiter: RateLimiter,
        bulkhead: ProviderBulkhead,
        metrics: MetricsSink,
        on_circuit_change: CircuitChangeCallback | None = None,
    ) -> None:
        self._inner = inner
        self._circuit_breaker = circuit_breaker
        self._rate_limiter = rate_limiter
        self._bulkhead = bulkhead
        self._metrics = metrics
        self._on_circuit_change = on_circuit_change
        # Last state observed in this process, used to detect transitions without
        # an extra read before every call. Several replicas may therefore each
        # report the same transition once; consumers of circuit events are
        # required to be idempotent, which they must be regardless.
        self._last_state = CircuitState.CLOSED

    @property
    def slug(self) -> ProviderSlug:
        return self._inner.slug

    def descriptor(self) -> ProviderDescriptor:
        return self._inner.descriptor()

    # -- guarded operations -------------------------------------------------

    async def create_operation(
        self, command: CreateProviderOperationCommand
    ) -> ProviderOperationResult:
        # A create may only be repeated in-process when the provider guarantees
        # it will collapse the duplicate. Otherwise a retry after an ambiguous
        # failure creates a second real operation.
        retryable = self.descriptor().supports_provider_idempotency
        return await self._guarded(
            lambda: self._inner.create_operation(command),
            operation="create_operation",
            allow_in_process_retry=retryable,
        )

    async def get_operation_status(self, provider_reference: str) -> ProviderOperationResult:
        return await self._guarded(
            lambda: self._inner.get_operation_status(provider_reference),
            operation="get_operation_status",
            allow_in_process_retry=True,
        )

    async def cancel_operation(
        self, command: CancelProviderOperationCommand
    ) -> ProviderOperationResult:
        # Cancelling an already-cancelled job is a no-op at every provider that
        # supports cancellation at all, so repeating it is safe.
        return await self._guarded(
            lambda: self._inner.cancel_operation(command),
            operation="cancel_operation",
            allow_in_process_retry=True,
        )

    async def health_check(self) -> ProviderHealthProbe:
        # Deliberately bypasses the circuit breaker. The probe is how an operator
        # finds out whether a provider has recovered, so refusing to run it while
        # the breaker is open would hide exactly the information they need.
        async with self._bulkhead.slot(self.slug):
            return await self._inner.health_check()

    # -- pass-through -------------------------------------------------------

    def validate_webhook(self, webhook: InboundWebhook) -> WebhookVerification:
        return self._inner.validate_webhook(webhook)

    def normalize_webhook(self, webhook: InboundWebhook) -> NormalizedWebhookEvent:
        return self._inner.normalize_webhook(webhook)

    # -- internals ----------------------------------------------------------

    async def _guarded(
        self,
        call: Callable[[], Awaitable[T]],
        *,
        operation: str,
        allow_in_process_retry: bool,
    ) -> T:
        await self._check_rate_limit()
        await self._check_circuit()

        attempts = IN_PROCESS_MAX_ATTEMPTS if allow_in_process_retry else 1
        last_error: ProviderError | None = None

        for attempt in range(1, attempts + 1):
            async with self._bulkhead.slot(self.slug):
                try:
                    result = await call()
                except ProviderError as exc:
                    last_error = exc
                    await self._record_outcome(success=False, error=exc)
                    if attempt >= attempts or not _is_retryable_in_process(exc):
                        raise
                    logger.info(
                        "retrying a provider call in process after a transient failure",
                        extra={
                            "provider": self.slug.value,
                            "operation": operation,
                            "attempt": attempt,
                            "error_code": exc.code,
                        },
                    )
                else:
                    await self._record_outcome(success=True, error=None)
                    return result

            await asyncio.sleep(IN_PROCESS_BACKOFF_SECONDS * attempt)

        # Unreachable while attempts >= 1, but keeps the type checker honest and
        # guarantees a ProviderError rather than a None escapes.
        raise last_error if last_error else RuntimeError("the provider call produced no result")

    async def _check_rate_limit(self) -> None:
        if await self._rate_limiter.acquire(self.slug):
            return
        retry_after = await self._rate_limiter.retry_after(self.slug)
        self._metrics.increment("provider_rate_limits_total", labels={"provider": self.slug.value})
        raise ProviderRateLimitError(
            "the client-side rate limit for this provider is exhausted",
            provider=self.slug.value,
            retry_after_seconds=retry_after,
            metadata={"source": "client_rate_limiter"},
        )

    async def _check_circuit(self) -> None:
        if await self._circuit_breaker.allow(self.slug):
            return
        snapshot = await self._circuit_breaker.state(self.slug)
        await self._note_state(snapshot.state)
        raise CircuitOpenError(self.slug.value)

    async def _record_outcome(self, *, success: bool, error: ProviderError | None) -> None:
        if success:
            state = await self._circuit_breaker.record_success(self.slug)
        elif error is not None and error.category in _CIRCUIT_FAILURE_CATEGORIES:
            state = await self._circuit_breaker.record_failure(self.slug)
        else:
            # The provider answered coherently; it is healthy even though the
            # answer was "no".
            return
        await self._note_state(state)

    async def _note_state(self, state: CircuitState) -> None:
        self._metrics.set_gauge(
            "provider_circuit_state", state.numeric, labels={"provider": self.slug.value}
        )
        if state is self._last_state:
            return
        previous, self._last_state = self._last_state, state
        logger.warning(
            "provider circuit breaker changed state",
            extra={
                "provider": self.slug.value,
                "previous_state": previous.value,
                "state": state.value,
            },
        )
        if self._on_circuit_change is not None:
            await self._on_circuit_change(self.slug, previous, state)


def _is_retryable_in_process(error: ProviderError) -> bool:
    """Only genuinely transient transport problems are worth an immediate retry.

    Rate limiting is excluded: the provider asked us to wait, and waiting a
    quarter of a second is not honouring that. It goes to the durable path where
    ``Retry-After`` can actually be respected.
    """
    return error.retryable and error.category in (
        ErrorCategory.PROVIDER_TIMEOUT,
        ErrorCategory.PROVIDER_UNAVAILABLE,
    )
