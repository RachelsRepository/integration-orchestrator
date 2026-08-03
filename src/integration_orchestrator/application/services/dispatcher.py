"""Provider dispatch orchestration.

This service owns the decision of what a provider's answer means for a request's
lifecycle. It contains no provider-specific logic whatsoever: it selects an
adapter from the registry by slug, hands it a normalized command, and interprets
a normalized result. Adding a fourth provider changes nothing here.

Dispatch runs in three phases, each with its own transaction boundary:

1. **Claim.** The request moves to ``dispatching`` and the attempt is counted,
   then the transaction commits. Committing *before* the call is what makes
   crash recovery possible: a process that dies mid-call leaves a durable
   ``dispatching`` row for reconciliation to investigate, rather than a
   ``retry_scheduled`` row that another worker will happily dispatch a second
   time.
2. **Call.** The provider is invoked with no database transaction held. Holding
   one across a multi-second network call would pin a connection, block vacuum,
   and turn a slow provider into database pressure.
3. **Settle.** The request is reloaded under a row lock and the outcome applied.
   Reloading matters because a webhook may have completed the request while the
   call was in flight; the settle phase detects that and leaves the newer state
   alone.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.application.ports.provider_gateway import ProviderRegistry
from integration_orchestrator.application.ports.resilience import PolicyProvider
from integration_orchestrator.application.ports.system import Clock, JitterSource
from integration_orchestrator.application.ports.unit_of_work import (
    UnitOfWork,
    UnitOfWorkFactory,
)
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.domain.contracts import (
    CreateProviderOperationCommand,
    ProviderErrorInfo,
    ProviderOperationResult,
)
from integration_orchestrator.domain.entities import IntegrationRequest
from integration_orchestrator.domain.enums import (
    AuditAction,
    ErrorCategory,
    NormalizedStatus,
    RequestStatus,
)
from integration_orchestrator.domain.errors import (
    NotFoundError,
    ProviderError,
    UnsupportedOperationError,
)

logger = logging.getLogger(__name__)

# Metadata key adapters use to hand back the redacted provider-shaped request
# they generated, so operators can diagnose mapping problems without the gateway
# interface needing an extra method.
PROVIDER_REQUEST_METADATA_KEY = "provider_request"


class RequestDispatcher:
    """Dispatches a request to its provider and records the outcome."""

    def __init__(
        self,
        *,
        registry: ProviderRegistry,
        journal: WorkflowJournal,
        policies: PolicyProvider,
        uow_factory: UnitOfWorkFactory,
        clock: Clock,
        jitter: JitterSource,
        metrics: MetricsSink,
        on_terminal: Any | None = None,
    ) -> None:
        self._registry = registry
        self._journal = journal
        self._policies = policies
        self._uow_factory = uow_factory
        self._clock = clock
        self._jitter = jitter
        self._metrics = metrics
        self._on_terminal = on_terminal

    # -- phase 1 ------------------------------------------------------------

    async def claim_for_dispatch(
        self,
        uow: UnitOfWork,
        request: IntegrationRequest,
        *,
        actor: Actor,
        causation_id: str | None = None,
    ) -> None:
        """Move a request into ``dispatching`` within the caller's transaction.

        Exposed separately so the retry worker can fold the claim into the same
        transaction that locked the row, making the claim and the state change
        indivisible.
        """
        transition = request.begin_dispatch(now=self._clock.now())
        await self._journal.record_transition(
            uow,
            request,
            transition,
            action=AuditAction.DISPATCH_ATTEMPTED,
            actor=actor,
            metadata={"attempt": request.attempt_count},
            causation_id=causation_id,
        )

    # -- full flow ----------------------------------------------------------

    async def dispatch(
        self,
        request_id: UUID,
        *,
        actor: Actor,
        causation_id: str | None = None,
    ) -> IntegrationRequest:
        """Run all three phases for a request that is ready to be dispatched."""
        async with self._uow_factory() as uow:
            request = await uow.requests.get_for_update(request_id)
            if request is None:
                raise NotFoundError(f"integration request '{request_id}' does not exist")
            if request.status is RequestStatus.RECEIVED:
                validating = request.begin_validation(now=self._clock.now())
                await self._journal.record_transition(
                    uow,
                    request,
                    validating,
                    action=AuditAction.REQUEST_VALIDATED,
                    actor=actor,
                    publish_event=False,
                )
            await self.claim_for_dispatch(uow, request, actor=actor, causation_id=causation_id)
            await uow.commit()

        settled = await self.complete_attempt(request, actor=actor, causation_id=causation_id)
        await self._notify_terminal(settled)
        return settled

    # -- phases 2 and 3 -----------------------------------------------------

    async def complete_attempt(
        self,
        request: IntegrationRequest,
        *,
        actor: Actor,
        causation_id: str | None = None,
    ) -> IntegrationRequest:
        """Call the provider and settle the outcome.

        ``request`` must already be in ``dispatching``. The returned entity is
        the freshly reloaded one, which may reflect a webhook that landed while
        the provider call was in flight.
        """
        result = await self._call_provider(request)

        self._metrics.increment(
            "provider_requests_total",
            labels={
                "provider": request.provider.value,
                "operation": "create_operation",
                "outcome": "accepted" if result.accepted else "rejected",
            },
        )

        async with self._uow_factory() as uow:
            current = await uow.requests.get_for_update(request.id)
            if current is None:  # pragma: no cover - the row cannot vanish mid-flight
                raise NotFoundError(f"integration request '{request.id}' does not exist")

            if current.status is not RequestStatus.DISPATCHING:
                # A webhook (or an operator) settled the request while we were
                # waiting on the provider. Its information is newer than ours, so
                # the dispatch outcome is recorded for audit and discarded.
                await self._record_superseded(
                    uow, current, result, actor=actor, causation_id=causation_id
                )
                await uow.commit()
                return current

            provider_request = result.raw_response_metadata.get(PROVIDER_REQUEST_METADATA_KEY)
            if isinstance(provider_request, dict):
                current.record_provider_payload(provider_request)

            if result.accepted:
                await self._apply_acceptance(
                    uow, current, result, actor=actor, causation_id=causation_id
                )
            else:
                await self._apply_failure(
                    uow, current, result, actor=actor, causation_id=causation_id
                )
            await uow.commit()
            return current

    async def _notify_terminal(self, request: IntegrationRequest) -> None:
        if self._on_terminal is None:
            return
        if request.status not in {
            RequestStatus.SUCCEEDED,
            RequestStatus.FAILED,
            RequestStatus.CANCELLED,
            RequestStatus.MANUAL_REVIEW,
            RequestStatus.PENDING,
        }:
            return
        try:
            await self._on_terminal(request.id)
        except Exception:
            logger.exception(
                "workflow terminal hook failed",
                extra={"integration_request_id": str(request.id)},
            )

    async def _call_provider(self, request: IntegrationRequest) -> ProviderOperationResult:
        gateway = self._registry.get(request.provider)
        command = CreateProviderOperationCommand(
            request_id=request.id,
            provider=request.provider,
            operation_type=request.operation_type,
            external_reference=request.external_reference,
            payload=dict(request.normalized_payload),
            correlation_id=request.correlation_id,
            # The request id is a stable, unique, caller-owned idempotency key.
            # Reusing it across every attempt is what lets providers that support
            # idempotency collapse duplicate creates into a single operation.
            idempotency_key=str(request.id),
            attempt=request.attempt_count,
        )
        try:
            return await gateway.create_operation(command)
        except UnsupportedOperationError as exc:
            logger.warning(
                "provider rejected unsupported operation",
                extra={
                    "integration_request_id": str(request.id),
                    "provider": request.provider.value,
                    "operation_type": request.operation_type.value,
                    "error_code": exc.code,
                },
            )
            return ProviderOperationResult.failure(
                error=ProviderErrorInfo(
                    code=exc.code,
                    message=exc.message,
                    category=exc.category,
                    retryable=False,
                )
            )
        except ProviderError as exc:
            logger.warning(
                "provider dispatch failed",
                extra={
                    "integration_request_id": str(request.id),
                    "correlation_id": request.correlation_id.value,
                    "provider": request.provider.value,
                    "operation_type": request.operation_type.value,
                    "attempt": request.attempt_count,
                    "error_code": exc.code,
                    "error_category": exc.category.value,
                    "retryable": exc.retryable,
                },
            )
            return ProviderOperationResult.failure(error=ProviderErrorInfo.from_error(exc))

    # -- outcome handling ---------------------------------------------------

    async def _apply_acceptance(
        self,
        uow: UnitOfWork,
        request: IntegrationRequest,
        result: ProviderOperationResult,
        *,
        actor: Actor,
        causation_id: str | None,
    ) -> None:
        now = self._clock.now()
        metadata = _safe_result_metadata(result)

        if result.normalized_status is NormalizedStatus.SUCCEEDED:
            transition = request.mark_succeeded(
                now=now, provider_reference=result.provider_reference
            )
            await self._journal.record_transition(
                uow,
                request,
                transition,
                action=AuditAction.PROVIDER_ACCEPTED,
                actor=actor,
                metadata=metadata,
                causation_id=causation_id,
            )
            return

        if result.normalized_status in (NormalizedStatus.ACCEPTED, NormalizedStatus.PENDING):
            if not result.provider_reference:
                # The provider said yes but gave us nothing to track it by. It
                # cannot be polled, its webhook cannot be correlated, and it
                # cannot safely be retried in case the operation really exists.
                await self._escalate(
                    uow,
                    request,
                    reason="the provider accepted the operation without returning a reference",
                    actor=actor,
                    metadata=metadata,
                    causation_id=causation_id,
                )
                return
            transition = request.mark_accepted(
                provider_reference=result.provider_reference, now=now
            )
            await self._journal.record_transition(
                uow,
                request,
                transition,
                action=AuditAction.PROVIDER_ACCEPTED,
                actor=actor,
                metadata=metadata,
                causation_id=causation_id,
            )
            return

        if result.normalized_status is NormalizedStatus.CANCELLED:
            transition = request.mark_cancelled(now=now, reason="cancelled by the provider")
            await self._journal.record_transition(
                uow,
                request,
                transition,
                action=AuditAction.REQUEST_CANCELLED,
                actor=actor,
                metadata=metadata,
                causation_id=causation_id,
            )
            return

        await self._escalate(
            uow,
            request,
            reason=(
                "the provider reported a status the adapter could not interpret: "
                f"{result.provider_status or 'unspecified'}"
            ),
            actor=actor,
            metadata=metadata,
            causation_id=causation_id,
        )

    async def _apply_failure(
        self,
        uow: UnitOfWork,
        request: IntegrationRequest,
        result: ProviderOperationResult,
        *,
        actor: Actor,
        causation_id: str | None,
    ) -> None:
        error = result.error
        if error is None:  # pragma: no cover - guarded by the result invariant
            error = ProviderErrorInfo(
                code="provider_error",
                message="the provider call failed without a reported reason",
                category=ErrorCategory.INTERNAL,
                retryable=False,
            )

        now = self._clock.now()
        policy = self._policies.retry_policy(request.provider)
        metadata: dict[str, Any] = {
            **_safe_result_metadata(result),
            "error_code": error.code,
            "error_category": error.category.value,
            "retryable": error.retryable,
            "provider_code": error.provider_code,
        }

        self._metrics.increment(
            "provider_failures_total",
            labels={"provider": request.provider.value, "error_category": error.category.value},
        )
        if error.category is ErrorCategory.PROVIDER_TIMEOUT:
            self._metrics.increment(
                "provider_timeouts_total", labels={"provider": request.provider.value}
            )
        elif error.category is ErrorCategory.PROVIDER_RATE_LIMIT:
            self._metrics.increment(
                "provider_rate_limits_total", labels={"provider": request.provider.value}
            )

        if policy.should_retry(attempt_count=request.attempt_count, retryable=error.retryable):
            next_retry_at = policy.next_retry_at(
                now=now,
                attempt_count=request.attempt_count,
                jitter=self._jitter.jitter(),
                retry_after_seconds=error.retry_after_seconds,
            )
            transition = request.schedule_retry(
                failure=error.to_failure_detail(),
                next_retry_at=next_retry_at,
                now=now,
            )
            await self._journal.record_transition(
                uow,
                request,
                transition,
                action=AuditAction.RETRY_SCHEDULED,
                actor=actor,
                metadata={**metadata, "next_retry_at": next_retry_at.isoformat()},
                causation_id=causation_id,
            )
            self._metrics.increment(
                "retries_scheduled_total",
                labels={"provider": request.provider.value, "error_category": error.category.value},
            )
            return

        exhausted = error.retryable and not policy.has_attempts_remaining(request.attempt_count)
        if exhausted:
            self._metrics.increment(
                "retries_exhausted_total", labels={"provider": request.provider.value}
            )

        # A timeout that has run out of retries is genuinely ambiguous: the
        # provider may have created the operation before the connection dropped.
        # Declaring it failed would be a guess, so it goes to manual review and
        # reconciliation gets a chance to find out what really happened.
        if exhausted and error.category is ErrorCategory.PROVIDER_TIMEOUT:
            await self._escalate(
                uow,
                request,
                reason="retries were exhausted on an ambiguous provider timeout",
                actor=actor,
                metadata=metadata,
                causation_id=causation_id,
            )
            return

        transition = request.mark_failed(failure=error.to_failure_detail(), now=now)
        await self._journal.record_transition(
            uow,
            request,
            transition,
            action=AuditAction.RETRIES_EXHAUSTED if exhausted else AuditAction.PROVIDER_FAILED,
            actor=actor,
            metadata={**metadata, "attempts_exhausted": exhausted},
            causation_id=causation_id,
        )

    async def _escalate(
        self,
        uow: UnitOfWork,
        request: IntegrationRequest,
        *,
        reason: str,
        actor: Actor,
        metadata: dict[str, Any],
        causation_id: str | None,
    ) -> None:
        transition = request.mark_manual_review(reason=reason, now=self._clock.now())
        await self._journal.record_transition(
            uow,
            request,
            transition,
            action=AuditAction.MOVED_TO_MANUAL_REVIEW,
            actor=actor,
            metadata={**metadata, "reason": reason},
            causation_id=causation_id,
        )
        logger.warning(
            "request escalated to manual review",
            extra={
                "integration_request_id": str(request.id),
                "correlation_id": request.correlation_id.value,
                "provider": request.provider.value,
                "reason": reason,
            },
        )

    async def _record_superseded(
        self,
        uow: UnitOfWork,
        request: IntegrationRequest,
        result: ProviderOperationResult,
        *,
        actor: Actor,
        causation_id: str | None,
    ) -> None:
        """Note that a dispatch outcome arrived after the request already moved on."""
        if result.provider_reference and not request.provider_reference:
            # Still worth keeping: the reference is how reconciliation and any
            # later webhook will find this request.
            request.attach_provider_reference(result.provider_reference, now=self._clock.now())
            await uow.requests.update(request)

        await self._journal.record_request_action(
            uow,
            request,
            action=AuditAction.STATE_RECONCILED,
            actor=actor,
            metadata={
                **_safe_result_metadata(result),
                "note": "the dispatch outcome was superseded by newer state",
                "causation_id": causation_id,
            },
        )
        logger.info(
            "dispatch outcome superseded by newer state",
            extra={
                "integration_request_id": str(request.id),
                "correlation_id": request.correlation_id.value,
                "provider": request.provider.value,
                "status": request.status.value,
            },
        )


def _safe_result_metadata(result: ProviderOperationResult) -> dict[str, Any]:
    """Extract audit-safe facts from a provider result.

    The provider-shaped request body is excluded even though the adapter already
    redacted it: the audit row records the outcome, and repeating payloads on
    every entry bloats the table without adding diagnostic value.
    """
    metadata: dict[str, Any] = {
        key: value
        for key, value in result.raw_response_metadata.items()
        if key != PROVIDER_REQUEST_METADATA_KEY
    }
    metadata["normalized_status"] = result.normalized_status.value
    if result.provider_status:
        metadata["provider_status"] = result.provider_status
    if result.provider_reference:
        metadata["provider_reference"] = result.provider_reference
    return metadata
