"""Reconciliation.

Distributed systems lose messages. A provider response can be lost to a timeout
after the operation was created, a webhook can be dropped or misrouted, and a
worker can crash between accepting a provider's answer and committing it.
Reconciliation exists to notice those cases and, where it is safe, fix them.

The governing rule is that reconciliation never guesses. It only rewrites local
state when the provider can be asked directly and answers unambiguously about an
operation we can positively identify. Everything else goes to manual review,
because silently inventing an outcome is worse than stopping and asking a human.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta

from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.application.ports.provider_gateway import ProviderRegistry
from integration_orchestrator.application.ports.system import Clock
from integration_orchestrator.application.ports.unit_of_work import UnitOfWorkFactory
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.domain.contracts import ProviderOperationResult
from integration_orchestrator.domain.entities import IntegrationRequest
from integration_orchestrator.domain.enums import (
    ActorType,
    AuditAction,
    NormalizedStatus,
    RequestStatus,
)
from integration_orchestrator.domain.errors import (
    NotFoundError,
    ProviderError,
    ProviderNotFoundError,
    UnsupportedOperationError,
)

logger = logging.getLogger(__name__)

RECONCILIATION_ACTOR = Actor(type=ActorType.RECONCILIATION_WORKER)


@dataclass(frozen=True, slots=True)
class ReconciliationOutcome:
    """What reconciliation decided about one request."""

    request_id: str
    action: str
    previous_status: RequestStatus
    new_status: RequestStatus
    detail: str | None = None

    @property
    def changed(self) -> bool:
        return self.previous_status is not self.new_status


class ReconciliationService:
    """Compares local workflow state against provider state."""

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        registry: ProviderRegistry,
        journal: WorkflowJournal,
        clock: Clock,
        metrics: MetricsSink,
        stale_after_seconds: int,
        manual_review_after_seconds: int,
    ) -> None:
        self._uow_factory = uow_factory
        self._registry = registry
        self._journal = journal
        self._clock = clock
        self._metrics = metrics
        self._stale_after_seconds = stale_after_seconds
        self._manual_review_after_seconds = manual_review_after_seconds

    async def find_candidates(self, *, limit: int) -> list[IntegrationRequest]:
        """Return in-flight requests that have stopped making progress."""
        cutoff = self._clock.now() - timedelta(seconds=self._stale_after_seconds)
        async with self._uow_factory() as uow:
            return list(await uow.requests.find_stale_in_flight(older_than=cutoff, limit=limit))

    async def reconcile(self, request: IntegrationRequest) -> ReconciliationOutcome:
        """Reconcile one request against its provider."""
        if request.provider_reference is None:
            return await self._handle_unidentifiable(request)

        descriptor = self._registry.get(request.provider).descriptor()
        if not descriptor.supports_status_lookup:
            return await self._handle_unverifiable(
                request, reason="the provider does not expose a status lookup"
            )

        try:
            result = await self._registry.get(request.provider).get_operation_status(
                request.provider_reference
            )
        except ProviderNotFoundError:
            # The provider has no record of an operation we believe it accepted.
            # That is a real mismatch, but "not found" can also mean a reference
            # that has aged out of the provider's retention window, so it is not
            # safe to declare the request failed.
            self._metrics.increment(
                "reconciliation_mismatches_total",
                labels={"provider": request.provider.value, "kind": "unknown_reference"},
            )
            return await self._escalate(
                request,
                reason="the provider does not recognise the reference it previously returned",
            )
        except UnsupportedOperationError:
            return await self._handle_unverifiable(
                request, reason="the provider does not support status lookup for this operation"
            )
        except ProviderError as exc:
            # Transient: leave the request alone and try again next cycle.
            logger.info(
                "reconciliation could not reach the provider",
                extra={
                    "integration_request_id": str(request.id),
                    "correlation_id": request.correlation_id.value,
                    "provider": request.provider.value,
                    "error_code": exc.code,
                },
            )
            return ReconciliationOutcome(
                request_id=str(request.id),
                action="deferred",
                previous_status=request.status,
                new_status=request.status,
                detail=f"provider unreachable: {exc.code}",
            )

        return await self._apply_provider_state(request, result)

    # -- decision branches --------------------------------------------------

    async def _apply_provider_state(
        self, request: IntegrationRequest, result: ProviderOperationResult
    ) -> ReconciliationOutcome:
        if result.normalized_status is NormalizedStatus.UNKNOWN:
            self._metrics.increment(
                "reconciliation_mismatches_total",
                labels={"provider": request.provider.value, "kind": "unmapped_status"},
            )
            return await self._escalate(
                request,
                reason=(
                    "the provider reported a status the adapter cannot interpret: "
                    f"{result.provider_status or 'unspecified'}"
                ),
            )

        async with self._uow_factory() as uow:
            current = await uow.requests.get_for_update(request.id)
            if current is None:
                raise NotFoundError(f"integration request '{request.id}' does not exist")

            previous_status = current.status
            transition = current.apply_normalized_status(
                result.normalized_status,
                now=self._clock.now(),
                provider_reference=result.provider_reference,
                failure=result.error.to_failure_detail() if result.error else None,
            )

            if transition is None:
                # Local state already matches or leads the provider's view. This
                # is the common case and means nothing was lost.
                await self._journal.record_request_action(
                    uow,
                    current,
                    action=AuditAction.STATE_RECONCILED,
                    actor=RECONCILIATION_ACTOR,
                    metadata={
                        "provider_status": result.provider_status,
                        "normalized_status": result.normalized_status.value,
                        "changed": False,
                    },
                )
                await uow.commit()
                return ReconciliationOutcome(
                    request_id=str(current.id),
                    action="confirmed",
                    previous_status=previous_status,
                    new_status=current.status,
                )

            self._metrics.increment(
                "reconciliation_mismatches_total",
                labels={"provider": request.provider.value, "kind": "stale_local_state"},
            )
            await self._journal.record_transition(
                uow,
                current,
                transition,
                action=AuditAction.STATE_RECONCILED,
                actor=RECONCILIATION_ACTOR,
                metadata={
                    "provider_status": result.provider_status,
                    "normalized_status": result.normalized_status.value,
                    "reason": "local state was behind the provider",
                },
            )
            await uow.commit()

        logger.info(
            "reconciled a request against provider state",
            extra={
                "integration_request_id": str(current.id),
                "correlation_id": current.correlation_id.value,
                "provider": current.provider.value,
                "previous_status": previous_status.value,
                "status": current.status.value,
            },
        )
        return ReconciliationOutcome(
            request_id=str(current.id),
            action="corrected",
            previous_status=previous_status,
            new_status=current.status,
            detail=result.provider_status,
        )

    async def _handle_unidentifiable(self, request: IntegrationRequest) -> ReconciliationOutcome:
        """A stale request with no provider reference.

        Typically a dispatch that timed out. The provider may or may not have
        created the operation, and without a reference there is no way to ask.
        The request is held until the escalation threshold rather than being
        failed immediately, because a late response may still arrive.
        """
        age = request.seconds_since_update(now=self._clock.now())
        if age < self._manual_review_after_seconds:
            return ReconciliationOutcome(
                request_id=str(request.id),
                action="waiting",
                previous_status=request.status,
                new_status=request.status,
                detail="no provider reference yet; still inside the grace period",
            )
        self._metrics.increment(
            "reconciliation_mismatches_total",
            labels={"provider": request.provider.value, "kind": "no_provider_reference"},
        )
        return await self._escalate(
            request,
            reason=(
                "the request has been in flight without a provider reference for longer "
                "than the escalation threshold"
            ),
        )

    async def _handle_unverifiable(
        self, request: IntegrationRequest, *, reason: str
    ) -> ReconciliationOutcome:
        """The provider cannot be polled, so only time can decide."""
        age = request.seconds_since_update(now=self._clock.now())
        if age < self._manual_review_after_seconds:
            return ReconciliationOutcome(
                request_id=str(request.id),
                action="waiting",
                previous_status=request.status,
                new_status=request.status,
                detail=reason,
            )
        return await self._escalate(request, reason=reason)

    async def _escalate(self, request: IntegrationRequest, *, reason: str) -> ReconciliationOutcome:
        async with self._uow_factory() as uow:
            current = await uow.requests.get_for_update(request.id)
            if current is None:
                raise NotFoundError(f"integration request '{request.id}' does not exist")
            previous_status = current.status
            if current.status is RequestStatus.MANUAL_REVIEW or current.status.is_terminal:
                await uow.rollback()
                return ReconciliationOutcome(
                    request_id=str(current.id),
                    action="no_action",
                    previous_status=previous_status,
                    new_status=current.status,
                    detail=reason,
                )
            transition = current.mark_manual_review(reason=reason, now=self._clock.now())
            await self._journal.record_transition(
                uow,
                current,
                transition,
                action=AuditAction.MOVED_TO_MANUAL_REVIEW,
                actor=RECONCILIATION_ACTOR,
                metadata={"reason": reason, "source": "reconciliation"},
            )
            await uow.commit()

        logger.warning(
            "reconciliation escalated a request to manual review",
            extra={
                "integration_request_id": str(current.id),
                "correlation_id": current.correlation_id.value,
                "provider": current.provider.value,
                "reason": reason,
            },
        )
        return ReconciliationOutcome(
            request_id=str(current.id),
            action="escalated",
            previous_status=previous_status,
            new_status=current.status,
            detail=reason,
        )
