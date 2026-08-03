"""Webhook ingestion.

The pipeline is deliberately ordered:

1. Identify the provider. An unknown provider is rejected before anything else.
2. Verify the signature, timestamp and replay window inside the adapter.
3. Persist a receipt and commit it, *before* any processing is attempted.
4. Deduplicate on ``(provider, provider_event_id)``.
5. Correlate to an integration request, deferring when the reference is not known yet.
6. Apply the normalized status through the state machine.
7. Write audit and outbox rows in the same transaction as the state change.

Step 3 is the one that is easy to get wrong. Processing first and persisting
afterwards means a crash, a bug, or an unparseable body leaves no trace that the
provider ever called, and providers do not let you ask them to redeliver
yesterday's webhook. Committing the receipt first costs one extra transaction and
buys a complete record of everything that arrived.

Step 5 handles the webhook-before-response race. A provider can legitimately
deliver a completion webhook before our own dispatch call has committed the
provider reference. Such a webhook is verified and real, so it is held in a
``deferred`` state rather than discarded, and applied as soon as the reference
appears.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from integration_orchestrator.application.dto.commands import Actor, IngestWebhookCommand
from integration_orchestrator.application.dto.results import WebhookIngestionResult
from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.application.ports.provider_gateway import ProviderRegistry
from integration_orchestrator.application.ports.security import (
    NullWebhookReplayGuard,
    WebhookReplayGuard,
    extract_signature_header,
)
from integration_orchestrator.application.ports.system import Clock, IdentifierGenerator
from integration_orchestrator.application.ports.unit_of_work import (
    UnitOfWork,
    UnitOfWorkFactory,
)
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.domain.contracts import (
    InboundWebhook,
    NormalizedWebhookEvent,
    ProviderErrorInfo,
)
from integration_orchestrator.domain.entities import IntegrationRequest, WebhookReceipt
from integration_orchestrator.domain.enums import (
    ActorType,
    AuditAction,
    ErrorCategory,
    NormalizedStatus,
    WebhookProcessingStatus,
)
from integration_orchestrator.domain.errors import (
    ConflictError,
    ProviderNotConfiguredError,
    ValidationError,
)
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ProviderSlug,
    SignatureMetadata,
)

logger = logging.getLogger(__name__)

WEBHOOK_ACTOR = Actor(type=ActorType.WEBHOOK)

#: Reserved key under which the normalized interpretation of a webhook is stored
#: on its receipt. A deferred receipt has to be reprocessed later, and re-running
#: the adapter would need the raw body — which is deliberately not persisted,
#: because provider bodies are the most likely place for customer data to appear.
#: Storing the small normalized summary instead keeps the retry path exact
#: without retaining a payload nobody should be reading.
NORMALIZED_METADATA_KEY = "_normalized"


class IngestWebhookUseCase:
    """Verifies, records and applies one inbound provider webhook."""

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        registry: ProviderRegistry,
        journal: WorkflowJournal,
        clock: Clock,
        ids: IdentifierGenerator,
        metrics: MetricsSink,
        deferred_retry_seconds: float,
        replay_guard: WebhookReplayGuard | None = None,
        on_request_updated: Any | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._registry = registry
        self._journal = journal
        self._clock = clock
        self._ids = ids
        self._metrics = metrics
        self._deferred_retry_seconds = deferred_retry_seconds
        self._replay_guard = replay_guard or NullWebhookReplayGuard()
        self._on_request_updated = on_request_updated

    async def execute(self, command: IngestWebhookCommand) -> WebhookIngestionResult:
        webhook = command.webhook
        provider = webhook.provider
        self._metrics.increment("webhook_received_total", labels={"provider": provider.value})

        if not self._registry.has(provider):
            raise ProviderNotConfiguredError(
                provider.value, correlation_id=command.correlation_id.value
            )
        gateway = self._registry.get(provider)

        verification = gateway.validate_webhook(webhook)
        if not verification.verified:
            return await self._record_rejection(
                webhook,
                signature_metadata=verification.signature_metadata,
                reason=verification.reason or "signature verification failed",
                correlation_id=command.correlation_id,
            )

        signature = extract_signature_header(webhook.headers)
        if signature is not None and not await self._replay_guard.claim(provider, signature):
            self._metrics.increment(
                "webhook_signature_replay_total", labels={"provider": provider.value}
            )
            return await self._record_rejection(
                webhook,
                signature_metadata=verification.signature_metadata,
                reason="signature replay detected within the dedupe window",
                correlation_id=command.correlation_id,
            )

        try:
            event = gateway.normalize_webhook(webhook)
        except ValidationError as exc:
            return await self._record_rejection(
                webhook,
                signature_metadata=verification.signature_metadata,
                reason=f"normalization failed: {exc.code}",
                correlation_id=command.correlation_id,
            )

        receipt, duplicate = await self._persist_receipt(
            webhook,
            event,
            signature_metadata=verification.signature_metadata,
            correlation_id=command.correlation_id,
        )
        if duplicate:
            self._metrics.increment("webhook_duplicate_total", labels={"provider": provider.value})
            return WebhookIngestionResult(
                receipt_id=receipt.id,
                status=WebhookProcessingStatus.DUPLICATE,
                integration_request_id=receipt.integration_request_id,
                detail="this event was already recorded",
            )

        return await self.process_receipt(receipt, event)

    # -- receipt persistence ------------------------------------------------

    async def _persist_receipt(
        self,
        webhook: InboundWebhook,
        event: NormalizedWebhookEvent,
        *,
        signature_metadata: SignatureMetadata,
        correlation_id: CorrelationId,
    ) -> tuple[WebhookReceipt, bool]:
        """Insert the receipt and commit it before any processing happens.

        Returns the receipt and whether it was a duplicate delivery.
        """
        async with self._uow_factory() as uow:
            existing = await uow.webhooks.find_by_event_id(
                webhook.provider, event.provider_event_id
            )
            if existing is not None:
                if existing.is_settled:
                    await self._journal.record_webhook_action(
                        uow,
                        receipt_id=existing.id,
                        action=AuditAction.WEBHOOK_DUPLICATE_IGNORED,
                        correlation_id=correlation_id,
                        metadata={
                            "provider": webhook.provider.value,
                            "provider_event_id": event.provider_event_id,
                            "previous_status": existing.processing_status.value,
                        },
                    )
                    await uow.commit()
                    return existing, True
                # An unsettled receipt means an earlier attempt did not finish.
                # Reprocessing it is safe because every downstream step is
                # idempotent, and abandoning it would strand the event.
                await uow.commit()
                return existing, False

            receipt = WebhookReceipt(
                id=self._ids.new_id(),
                provider=webhook.provider,
                event_id=event.provider_event_id,
                event_type=event.event_type,
                payload=_receipt_payload(event),
                signature_metadata=signature_metadata,
                received_at=webhook.received_at,
                provider_reference=event.provider_reference,
                correlation_id=event.correlation_id or correlation_id,
            )
            await uow.webhooks.add(receipt)
            try:
                await uow.flush()
            except ConflictError:
                # A concurrent delivery of the same event won the race.
                await uow.rollback()
                return await self._reread_duplicate(
                    webhook.provider, event.provider_event_id, correlation_id
                )

            await self._journal.record_webhook_action(
                uow,
                receipt_id=receipt.id,
                action=AuditAction.WEBHOOK_RECEIVED,
                correlation_id=correlation_id,
                metadata={
                    "provider": webhook.provider.value,
                    "provider_event_id": event.provider_event_id,
                    "event_type": event.event_type,
                    "normalized_status": event.normalized_status.value,
                    "signature": signature_metadata.to_dict(),
                },
            )
            await uow.commit()
            return receipt, False

    async def _reread_duplicate(
        self, provider: ProviderSlug, event_id: str, correlation_id: CorrelationId
    ) -> tuple[WebhookReceipt, bool]:
        async with self._uow_factory() as uow:
            existing = await uow.webhooks.find_by_event_id(provider, event_id)
            if existing is None:  # pragma: no cover - the winner has committed by now
                raise ConflictError(
                    "a concurrent delivery of this event is still being recorded",
                    correlation_id=correlation_id.value,
                )
            return existing, True

    async def _record_rejection(
        self,
        webhook: InboundWebhook,
        *,
        signature_metadata: SignatureMetadata,
        reason: str,
        correlation_id: CorrelationId,
    ) -> WebhookIngestionResult:
        """Persist evidence of a webhook that failed verification.

        The event id is synthesised rather than read from the body: an
        unverified payload is attacker-controlled, and letting it choose the
        deduplication key would allow an attacker to suppress a genuine event by
        pre-registering its id.
        """
        self._metrics.increment(
            "webhook_rejected_total",
            labels={"provider": webhook.provider.value, "reason": reason[:48]},
        )
        receipt_id = self._ids.new_id()
        receipt = WebhookReceipt(
            id=receipt_id,
            provider=webhook.provider,
            event_id=f"unverified:{receipt_id}",
            event_type="unverified",
            payload={"body_bytes": len(webhook.body)},
            signature_metadata=signature_metadata,
            received_at=webhook.received_at,
            correlation_id=correlation_id,
        )
        receipt.mark_rejected(reason=reason, now=self._clock.now())

        async with self._uow_factory() as uow:
            await uow.webhooks.add(receipt)
            await self._journal.record_webhook_action(
                uow,
                receipt_id=receipt.id,
                action=AuditAction.WEBHOOK_REJECTED,
                correlation_id=correlation_id,
                metadata={
                    "provider": webhook.provider.value,
                    "reason": reason,
                    "remote_address": webhook.remote_address,
                },
            )
            await uow.commit()

        logger.warning(
            "rejected an inbound webhook",
            extra={
                "provider": webhook.provider.value,
                "correlation_id": correlation_id.value,
                "webhook_receipt_id": str(receipt.id),
                "reason": reason,
            },
        )
        return WebhookIngestionResult(
            receipt_id=receipt.id,
            status=WebhookProcessingStatus.REJECTED,
            detail="the webhook could not be verified",
        )

    # -- processing ---------------------------------------------------------

    async def process_receipt(
        self, receipt: WebhookReceipt, event: NormalizedWebhookEvent
    ) -> WebhookIngestionResult:
        """Correlate a recorded webhook and apply it to its request.

        Reused verbatim by the deferred-webhook worker, so a webhook that could
        not be correlated on arrival takes exactly the same path later.
        """
        async with self._uow_factory() as uow:
            request = await self._resolve_request(uow, event)
            if request is None:
                return await self._defer(uow, receipt, event)

            correlated_id = request.id
            transition = request.apply_normalized_status(
                event.normalized_status,
                now=self._clock.now(),
                provider_reference=event.provider_reference,
                failure=event.error.to_failure_detail() if event.error else None,
            )

            if transition is None:
                # Nothing to apply: the request already reflects this outcome or
                # has moved past it. Recording the receipt as processed keeps the
                # provider from redelivering forever.
                receipt.mark_processed(now=self._clock.now(), integration_request_id=correlated_id)
                await uow.webhooks.update(receipt)
                await self._journal.record_request_action(
                    uow,
                    request,
                    action=AuditAction.WEBHOOK_DUPLICATE_IGNORED,
                    actor=WEBHOOK_ACTOR,
                    metadata={
                        "provider_event_id": event.provider_event_id,
                        "reported_status": event.normalized_status.value,
                        "current_status": request.status.value,
                    },
                )
                await uow.commit()
                self._metrics.increment(
                    "webhook_duplicate_total", labels={"provider": event.provider.value}
                )
                return WebhookIngestionResult(
                    receipt_id=receipt.id,
                    status=WebhookProcessingStatus.PROCESSED,
                    integration_request_id=correlated_id,
                    detail="the reported status carried no forward progress",
                )

            await self._journal.record_transition(
                uow,
                request,
                transition,
                action=AuditAction.WEBHOOK_APPLIED,
                actor=WEBHOOK_ACTOR,
                metadata={
                    "provider_event_id": event.provider_event_id,
                    "event_type": event.event_type,
                    "reported_status": event.normalized_status.value,
                },
                causation_id=event.provider_event_id,
            )
            receipt.mark_processed(now=self._clock.now(), integration_request_id=correlated_id)
            await uow.webhooks.update(receipt)
            await uow.commit()

        logger.info(
            "applied a provider webhook",
            extra={
                "integration_request_id": str(correlated_id),
                "correlation_id": request.correlation_id.value,
                "provider": event.provider.value,
                "event_id": event.provider_event_id,
                "status": transition.new_status.value,
            },
        )
        if self._on_request_updated is not None:
            try:
                await self._on_request_updated(correlated_id)
            except Exception:
                logger.exception(
                    "workflow webhook hook failed",
                    extra={"integration_request_id": str(correlated_id)},
                )
        return WebhookIngestionResult(
            receipt_id=receipt.id,
            status=WebhookProcessingStatus.PROCESSED,
            integration_request_id=correlated_id,
        )

    async def _resolve_request(
        self, uow: UnitOfWork, event: NormalizedWebhookEvent
    ) -> IntegrationRequest | None:
        """Find the request a webhook refers to.

        Correlation is by provider reference only. The external reference is
        caller-supplied and not unique, so matching on it could apply a provider
        outcome to an unrelated request.
        """
        if not event.provider_reference:
            return None
        return await uow.requests.find_by_provider_reference(
            event.provider, event.provider_reference
        )

    async def _defer(
        self,
        uow: UnitOfWork,
        receipt: WebhookReceipt,
        event: NormalizedWebhookEvent,
    ) -> WebhookIngestionResult:
        now = self._clock.now()
        next_attempt = now + timedelta(seconds=self._deferred_retry_seconds)
        receipt.mark_deferred(
            reason="no integration request matches this provider reference yet",
            next_attempt_at=next_attempt,
            now=now,
        )
        await uow.webhooks.update(receipt)
        await self._journal.record_webhook_action(
            uow,
            receipt_id=receipt.id,
            action=AuditAction.WEBHOOK_DEFERRED,
            correlation_id=receipt.correlation_id
            or event.correlation_id
            or CorrelationId.generate(),
            metadata={
                "provider": event.provider.value,
                "provider_reference": event.provider_reference,
                "attempt": receipt.attempt_count,
                "next_attempt_at": next_attempt.isoformat(),
            },
        )
        await uow.commit()

        logger.info(
            "deferred a webhook that could not be correlated yet",
            extra={
                "provider": event.provider.value,
                "webhook_receipt_id": str(receipt.id),
                "event_id": event.provider_event_id,
                "attempt": receipt.attempt_count,
            },
        )
        self._metrics.increment("webhook_deferred_total", labels={"provider": event.provider.value})
        return WebhookIngestionResult(
            receipt_id=receipt.id,
            status=WebhookProcessingStatus.DEFERRED,
            detail="the referenced operation is not known yet; the event is held for retry",
        )


def normalized_status_is_terminal(status: NormalizedStatus) -> bool:
    """Report whether a provider-reported status ends the workflow."""
    return status in (
        NormalizedStatus.SUCCEEDED,
        NormalizedStatus.FAILED,
        NormalizedStatus.CANCELLED,
    )


def receipt_request_id(receipt: WebhookReceipt) -> UUID | None:
    """Convenience accessor used by the deferred webhook worker."""
    return receipt.integration_request_id


def _receipt_payload(event: NormalizedWebhookEvent) -> dict[str, Any]:
    """Build the receipt payload: adapter metadata plus the normalized summary."""
    normalized: dict[str, Any] = {
        "status": event.normalized_status.value,
        "occurred_at": event.occurred_at.isoformat(),
        "provider_reference": event.provider_reference,
        "external_reference": event.external_reference,
    }
    if event.error is not None:
        normalized["error"] = {
            "code": event.error.code,
            "message": event.error.message,
            "category": event.error.category.value,
            "retryable": event.error.retryable,
            "provider_code": event.error.provider_code,
        }
    return {**dict(event.payload_metadata), NORMALIZED_METADATA_KEY: normalized}


def rehydrate_event(receipt: WebhookReceipt) -> NormalizedWebhookEvent:
    """Rebuild the normalized event a receipt was created from.

    Used by the deferred-webhook worker so a held event is applied through
    exactly the same path as one applied on arrival.
    """
    normalized = receipt.payload.get(NORMALIZED_METADATA_KEY)
    if not isinstance(normalized, dict):
        raise ValidationError(
            "this receipt has no normalized summary and cannot be reprocessed",
            metadata={"webhook_receipt_id": str(receipt.id)},
        )

    error_data = normalized.get("error")
    error = None
    if isinstance(error_data, dict):
        error = ProviderErrorInfo(
            code=str(error_data.get("code", "provider_error")),
            message=str(error_data.get("message", "")),
            category=ErrorCategory(error_data.get("category", ErrorCategory.INTERNAL.value)),
            retryable=bool(error_data.get("retryable", False)),
            provider_code=error_data.get("provider_code"),
        )

    return NormalizedWebhookEvent(
        provider=receipt.provider,
        provider_event_id=receipt.event_id,
        event_type=receipt.event_type,
        normalized_status=NormalizedStatus(normalized["status"]),
        occurred_at=datetime.fromisoformat(normalized["occurred_at"]),
        provider_reference=normalized.get("provider_reference") or receipt.provider_reference,
        external_reference=normalized.get("external_reference"),
        correlation_id=receipt.correlation_id,
        payload_metadata={
            key: value for key, value in receipt.payload.items() if key != NORMALIZED_METADATA_KEY
        },
        error=error,
    )
