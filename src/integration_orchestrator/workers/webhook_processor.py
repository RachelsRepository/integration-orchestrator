"""The deferred-webhook processor.

Solves the webhook-before-response race. A provider can deliver a completion
webhook before our own dispatch call has committed the provider reference, so the
event arrives referring to an operation the database does not know about yet.

Discarding it would be wrong: the webhook is verified, genuine, and may be the
only notification the provider ever sends. Blocking on it would be worse, because
the HTTP handler would hold a connection waiting for a write it does not control.
So the receipt is stored in a ``deferred`` state, and this worker retries the
correlation until it succeeds or the receipt ages out.

Abandonment is deliberate rather than infinite. A receipt that never finds its
request after the configured window is almost certainly for an operation created
by a different environment pointed at the same webhook URL — a common accident
with shared provider sandboxes. It is marked abandoned, which keeps the evidence
and stops the retry.
"""

from __future__ import annotations

import logging

from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.application.ports.system import Clock
from integration_orchestrator.application.ports.unit_of_work import UnitOfWorkFactory
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.application.use_cases.ingest_webhook import (
    IngestWebhookUseCase,
    rehydrate_event,
)
from integration_orchestrator.config.settings import WorkerSettings
from integration_orchestrator.domain.entities import WebhookReceipt
from integration_orchestrator.domain.enums import AuditAction
from integration_orchestrator.domain.errors import DomainError, ValidationError
from integration_orchestrator.domain.value_objects import CorrelationId
from integration_orchestrator.observability.correlation import correlation_scope
from integration_orchestrator.workers.base import Worker

logger = logging.getLogger(__name__)


class WebhookProcessorWorker(Worker):
    """Retries webhooks that could not be correlated when they arrived."""

    name = "webhook_processor"

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        ingest: IngestWebhookUseCase,
        journal: WorkflowJournal,
        clock: Clock,
        metrics: MetricsSink,
        settings: WorkerSettings,
    ) -> None:
        super().__init__(
            poll_interval_seconds=settings.webhook_deferred_retry_seconds, metrics=metrics
        )
        self._uow_factory = uow_factory
        self._ingest = ingest
        self._journal = journal
        self._clock = clock
        self._settings = settings

    async def run_once(self) -> int:
        now = self._clock.now()
        async with self._uow_factory() as uow:
            receipts = list(
                await uow.webhooks.claim_deferred(
                    now=now, limit=self._settings.webhook_deferred_batch_size
                )
            )
            await uow.commit()

        if not receipts:
            return 0

        for receipt in receipts:
            await self._process(receipt)
        return len(receipts)

    async def _process(self, receipt: WebhookReceipt) -> None:
        correlation_id = receipt.correlation_id or CorrelationId.generate()
        with correlation_scope(correlation_id=correlation_id.value):
            age_seconds = (self._clock.now() - receipt.received_at).total_seconds()
            if age_seconds > self._settings.webhook_deferred_abandon_after_seconds:
                await self._abandon(receipt, correlation_id, age_seconds)
                return

            try:
                event = rehydrate_event(receipt)
            except ValidationError:
                await self._abandon(
                    receipt,
                    correlation_id,
                    age_seconds,
                    reason="the receipt cannot be reprocessed",
                )
                return

            try:
                result = await self._ingest.process_receipt(receipt, event)
            except DomainError as exc:
                logger.warning(
                    "a deferred webhook could not be applied",
                    extra={
                        "worker": self.name,
                        "webhook_receipt_id": str(receipt.id),
                        "provider": receipt.provider.value,
                        "error_code": exc.code,
                    },
                )
                return

            logger.info(
                "reprocessed a deferred webhook",
                extra={
                    "worker": self.name,
                    "webhook_receipt_id": str(receipt.id),
                    "provider": receipt.provider.value,
                    "outcome": result.status.value,
                    "attempt": receipt.attempt_count,
                },
            )

    async def _abandon(
        self,
        receipt: WebhookReceipt,
        correlation_id: CorrelationId,
        age_seconds: float,
        *,
        reason: str = "no matching integration request appeared within the retention window",
    ) -> None:
        receipt.mark_abandoned(reason=reason, now=self._clock.now())
        async with self._uow_factory() as uow:
            await uow.webhooks.update(receipt)
            await self._journal.record_webhook_action(
                uow,
                receipt_id=receipt.id,
                action=AuditAction.WEBHOOK_ABANDONED,
                correlation_id=correlation_id,
                metadata={
                    "provider": receipt.provider.value,
                    "provider_reference": receipt.provider_reference,
                    "attempts": receipt.attempt_count,
                    "age_seconds": round(age_seconds, 1),
                    "reason": reason,
                },
            )
            await uow.commit()

        logger.warning(
            "abandoned a webhook that never found its request",
            extra={
                "worker": self.name,
                "webhook_receipt_id": str(receipt.id),
                "provider": receipt.provider.value,
                "age_seconds": round(age_seconds, 1),
            },
        )
