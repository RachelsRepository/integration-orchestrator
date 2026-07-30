"""Translation between ORM rows and domain objects.

Explicit mappers are used rather than mapping the domain classes directly onto
tables. The domain entities enforce invariants through their constructors and
have no public status setter; letting an ORM populate them by attribute
assignment would route around exactly the protection they exist to provide. The
cost is this module.
"""

from __future__ import annotations

from datetime import UTC, datetime

from integration_orchestrator.domain.entities import IntegrationRequest, WebhookReceipt
from integration_orchestrator.domain.records import (
    AuditEvent,
    IdempotencyRecord,
    OutboxEvent,
)
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ExternalReference,
    IdempotencyKey,
    ProviderSlug,
    SignatureMetadata,
)
from integration_orchestrator.infrastructure.db.models import (
    AuditEventModel,
    IdempotencyRecordModel,
    IntegrationRequestModel,
    OutboxEventModel,
    WebhookReceiptModel,
)


def as_utc(value: datetime) -> datetime:
    """Coerce a database timestamp to an aware UTC value.

    asyncpg returns aware datetimes for ``timestamptz``, but SQLite and some
    driver configurations do not. Normalising here means the domain never sees a
    naive timestamp regardless of which driver produced it.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def as_utc_optional(value: datetime | None) -> datetime | None:
    return None if value is None else as_utc(value)


# ---------------------------------------------------------------------------
# Integration requests
# ---------------------------------------------------------------------------


def request_to_domain(row: IntegrationRequestModel) -> IntegrationRequest:
    return IntegrationRequest(
        id=row.id,
        provider=ProviderSlug(row.provider),
        operation_type=row.operation_type,
        external_reference=ExternalReference(row.external_reference),
        normalized_payload=dict(row.normalized_payload),
        correlation_id=CorrelationId(row.correlation_id),
        created_at=as_utc(row.created_at),
        updated_at=as_utc(row.updated_at),
        status=row.status,
        provider_payload=dict(row.provider_payload) if row.provider_payload is not None else None,
        provider_reference=row.provider_reference,
        idempotency_key=IdempotencyKey(row.idempotency_key) if row.idempotency_key else None,
        attempt_count=row.attempt_count,
        last_error_code=row.last_error_code,
        last_error_message=row.last_error_message,
        last_error_category=row.last_error_category,
        next_retry_at=as_utc_optional(row.next_retry_at),
        completed_at=as_utc_optional(row.completed_at),
        manual_review_reason=row.manual_review_reason,
        version=row.version,
    )


def request_to_row(request: IntegrationRequest) -> IntegrationRequestModel:
    return IntegrationRequestModel(
        id=request.id,
        provider=request.provider.value,
        operation_type=request.operation_type,
        external_reference=request.external_reference.value,
        normalized_payload=dict(request.normalized_payload),
        provider_payload=dict(request.provider_payload)
        if request.provider_payload is not None
        else None,
        provider_reference=request.provider_reference,
        status=request.status,
        idempotency_key=request.idempotency_key.value if request.idempotency_key else None,
        attempt_count=request.attempt_count,
        correlation_id=request.correlation_id.value,
        last_error_code=request.last_error_code,
        last_error_message=request.last_error_message,
        last_error_category=request.last_error_category,
        manual_review_reason=request.manual_review_reason,
        next_retry_at=request.next_retry_at,
        created_at=request.created_at,
        updated_at=request.updated_at,
        completed_at=request.completed_at,
        version=request.version,
    )


def request_update_values(request: IntegrationRequest) -> dict[str, object]:
    """Column values for an optimistic-concurrency UPDATE.

    Deliberately excludes immutable columns: id, provider, operation type,
    external reference, correlation id, idempotency key and creation time never
    change after insert, so including them would allow a mapping bug to rewrite
    the identity of a request.
    """
    return {
        "normalized_payload": dict(request.normalized_payload),
        "provider_payload": dict(request.provider_payload)
        if request.provider_payload is not None
        else None,
        "provider_reference": request.provider_reference,
        "status": request.status,
        "attempt_count": request.attempt_count,
        "last_error_code": request.last_error_code,
        "last_error_message": request.last_error_message,
        "last_error_category": request.last_error_category,
        "manual_review_reason": request.manual_review_reason,
        "next_retry_at": request.next_retry_at,
        "updated_at": request.updated_at,
        "completed_at": request.completed_at,
        "version": request.version,
    }


# ---------------------------------------------------------------------------
# Webhook receipts
# ---------------------------------------------------------------------------


def receipt_to_domain(row: WebhookReceiptModel) -> WebhookReceipt:
    metadata = dict(row.signature_metadata or {})
    return WebhookReceipt(
        id=row.id,
        provider=ProviderSlug(row.provider),
        event_id=row.event_id,
        event_type=row.event_type,
        payload=dict(row.payload),
        signature_metadata=SignatureMetadata(
            scheme=str(metadata.get("scheme", "unknown")),
            key_id=metadata.get("key_id"),
            timestamp=metadata.get("timestamp"),
            algorithm=metadata.get("algorithm"),
            verified=bool(metadata.get("verified", False)),
        ),
        received_at=as_utc(row.received_at),
        provider_reference=row.provider_reference,
        correlation_id=CorrelationId(row.correlation_id) if row.correlation_id else None,
        processing_status=row.processing_status,
        integration_request_id=row.integration_request_id,
        processed_at=as_utc_optional(row.processed_at),
        failure_reason=row.failure_reason,
        attempt_count=row.attempt_count,
        next_attempt_at=as_utc_optional(row.next_attempt_at),
    )


def receipt_to_row(receipt: WebhookReceipt) -> WebhookReceiptModel:
    return WebhookReceiptModel(
        id=receipt.id,
        provider=receipt.provider.value,
        event_id=receipt.event_id,
        event_type=receipt.event_type,
        provider_reference=receipt.provider_reference,
        signature_metadata=receipt.signature_metadata.to_dict(),
        payload=dict(receipt.payload),
        processing_status=receipt.processing_status,
        integration_request_id=receipt.integration_request_id,
        correlation_id=receipt.correlation_id.value if receipt.correlation_id else None,
        failure_reason=receipt.failure_reason,
        attempt_count=receipt.attempt_count,
        received_at=receipt.received_at,
        processed_at=receipt.processed_at,
        next_attempt_at=receipt.next_attempt_at,
    )


def receipt_update_values(receipt: WebhookReceipt) -> dict[str, object]:
    return {
        "processing_status": receipt.processing_status,
        "integration_request_id": receipt.integration_request_id,
        "provider_reference": receipt.provider_reference,
        "failure_reason": receipt.failure_reason,
        "attempt_count": receipt.attempt_count,
        "processed_at": receipt.processed_at,
        "next_attempt_at": receipt.next_attempt_at,
    }


# ---------------------------------------------------------------------------
# Audit and outbox
# ---------------------------------------------------------------------------


def audit_to_domain(row: AuditEventModel) -> AuditEvent:
    return AuditEvent(
        id=row.id,
        aggregate_type=row.aggregate_type,
        aggregate_id=row.aggregate_id,
        action=row.action,
        actor=row.actor,
        correlation_id=CorrelationId(row.correlation_id),
        occurred_at=as_utc(row.occurred_at),
        previous_state=row.previous_state,
        new_state=row.new_state,
        actor_id=row.actor_id,
        metadata=dict(row.event_metadata or {}),
    )


def audit_to_row(event: AuditEvent) -> AuditEventModel:
    return AuditEventModel(
        id=event.id,
        aggregate_type=event.aggregate_type,
        aggregate_id=event.aggregate_id,
        action=event.action,
        actor=event.actor,
        actor_id=event.actor_id,
        previous_state=event.previous_state,
        new_state=event.new_state,
        correlation_id=event.correlation_id.value,
        event_metadata=dict(event.metadata),
        occurred_at=event.occurred_at,
    )


def outbox_to_domain(row: OutboxEventModel) -> OutboxEvent:
    return OutboxEvent(
        id=row.id,
        event_id=row.event_id,
        event_type=row.event_type,
        event_version=row.event_version,
        aggregate_type=row.aggregate_type,
        aggregate_id=row.aggregate_id,
        payload=dict(row.payload),
        correlation_id=CorrelationId(row.correlation_id),
        created_at=as_utc(row.created_at),
        causation_id=row.causation_id,
        partition_key=row.partition_key,
        published_at=as_utc_optional(row.published_at),
        attempt_count=row.attempt_count,
        next_attempt_at=as_utc_optional(row.next_attempt_at),
        last_error=row.last_error,
    )


def outbox_to_row(event: OutboxEvent) -> OutboxEventModel:
    return OutboxEventModel(
        id=event.id,
        event_id=event.event_id,
        event_type=event.event_type,
        event_version=event.event_version,
        aggregate_type=event.aggregate_type,
        aggregate_id=event.aggregate_id,
        payload=dict(event.payload),
        correlation_id=event.correlation_id.value,
        causation_id=event.causation_id,
        partition_key=event.partition_key,
        created_at=event.created_at,
        published_at=event.published_at,
        attempt_count=event.attempt_count,
        next_attempt_at=event.next_attempt_at,
        last_error=event.last_error,
    )


def idempotency_to_domain(row: IdempotencyRecordModel) -> IdempotencyRecord:
    return IdempotencyRecord(
        key=row.key,
        fingerprint=row.fingerprint,
        request_id=row.request_id,
        response_status=row.response_status,
        created_at=as_utc(row.created_at),
        expires_at=as_utc_optional(row.expires_at),
    )


def idempotency_to_row(record: IdempotencyRecord) -> IdempotencyRecordModel:
    return IdempotencyRecordModel(
        key=record.key,
        fingerprint=record.fingerprint,
        request_id=record.request_id,
        response_status=record.response_status,
        created_at=record.created_at,
        expires_at=record.expires_at,
    )
