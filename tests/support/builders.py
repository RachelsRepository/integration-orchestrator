"""Builders for domain objects used across the test suite.

Every builder takes an explicit ``now`` where a timestamp matters, so tests read
as statements about time rather than about whatever the wall clock happened to
be when they ran.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from integration_orchestrator.domain.contracts import NormalizedWebhookEvent
from integration_orchestrator.domain.entities import IntegrationRequest, WebhookReceipt
from integration_orchestrator.domain.enums import (
    NormalizedStatus,
    OperationType,
    RequestStatus,
)
from integration_orchestrator.domain.records import OutboxEvent
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ExternalReference,
    IdempotencyKey,
    ProviderSlug,
    SignatureMetadata,
)

REFERENCE_TIME = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
NORTHSTAR = ProviderSlug.parse("northstar")
MERIDIAN = ProviderSlug.parse("meridian")
COBALT = ProviderSlug.parse("cobalt")


def make_request(
    *,
    request_id: UUID | None = None,
    provider: ProviderSlug = NORTHSTAR,
    operation_type: OperationType = OperationType.RESOURCE_PROVISION,
    external_reference: str = "order-1001",
    payload: dict[str, Any] | None = None,
    correlation_id: str = "corr-1",
    now: datetime = REFERENCE_TIME,
    status: RequestStatus = RequestStatus.RECEIVED,
    provider_reference: str | None = None,
    attempt_count: int = 0,
    idempotency_key: str | None = None,
    next_retry_at: datetime | None = None,
) -> IntegrationRequest:
    """Build a request already in ``status``.

    The status is applied directly rather than by walking the state machine,
    because a test that needs a pending request should not have to restate the
    whole lifecycle to get one.
    """
    request = IntegrationRequest.create(
        request_id=request_id or uuid4(),
        provider=provider,
        operation_type=operation_type,
        external_reference=ExternalReference(external_reference),
        normalized_payload=payload if payload is not None else {"quantity": 1},
        correlation_id=CorrelationId(correlation_id),
        now=now,
        idempotency_key=IdempotencyKey(idempotency_key) if idempotency_key else None,
    )
    request.status = status
    request.provider_reference = provider_reference
    request.attempt_count = attempt_count
    request.next_retry_at = next_retry_at
    return request


def make_receipt(
    *,
    receipt_id: UUID | None = None,
    provider: ProviderSlug = NORTHSTAR,
    event_id: str = "evt-1",
    event_type: str = "operation.completed",
    payload: dict[str, Any] | None = None,
    provider_reference: str | None = "prv-1",
    received_at: datetime = REFERENCE_TIME,
    correlation_id: str = "corr-1",
) -> WebhookReceipt:
    return WebhookReceipt(
        id=receipt_id or uuid4(),
        provider=provider,
        event_id=event_id,
        event_type=event_type,
        payload=payload if payload is not None else {},
        signature_metadata=SignatureMetadata(scheme="hmac_sha256", verified=True),
        received_at=received_at,
        provider_reference=provider_reference,
        correlation_id=CorrelationId(correlation_id),
    )


def make_event(
    *,
    provider: ProviderSlug = NORTHSTAR,
    provider_event_id: str = "evt-1",
    event_type: str = "operation.completed",
    normalized_status: NormalizedStatus = NormalizedStatus.SUCCEEDED,
    occurred_at: datetime = REFERENCE_TIME,
    provider_reference: str | None = "prv-1",
    external_reference: str | None = "order-1001",
    correlation_id: str | None = "corr-1",
) -> NormalizedWebhookEvent:
    return NormalizedWebhookEvent(
        provider=provider,
        provider_event_id=provider_event_id,
        event_type=event_type,
        normalized_status=normalized_status,
        occurred_at=occurred_at,
        provider_reference=provider_reference,
        external_reference=external_reference,
        correlation_id=CorrelationId(correlation_id) if correlation_id else None,
    )


def make_outbox_event(
    *,
    outbox_id: UUID | None = None,
    event_type: str = "integration.request.succeeded",
    aggregate_id: str | None = None,
    created_at: datetime = REFERENCE_TIME,
    next_attempt_at: datetime | None = None,
    payload: dict[str, Any] | None = None,
) -> OutboxEvent:
    aggregate = aggregate_id or str(uuid4())
    return OutboxEvent(
        id=outbox_id or uuid4(),
        event_id=uuid4(),
        event_type=event_type,
        event_version=1,
        aggregate_type="integration_request",
        aggregate_id=aggregate,
        payload=payload if payload is not None else {"request_id": aggregate},
        correlation_id=CorrelationId("corr-1"),
        partition_key=aggregate,
        created_at=created_at,
        next_attempt_at=next_attempt_at,
    )


def seconds_after(base: datetime, seconds: float) -> datetime:
    return base + timedelta(seconds=seconds)
