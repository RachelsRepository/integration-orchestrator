"""Outbound response bodies.

Responses are built by explicit ``from_domain`` constructors rather than by
serialising entities directly. The mapping is therefore a decision that has to be
made on purpose: adding a field to the aggregate does not silently publish it,
which is what stops internal state such as provider payloads leaking into a
public contract.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import Field

from integration_orchestrator.api.schemas.common import ApiModel, PageMeta
from integration_orchestrator.application.dto.queries import Page
from integration_orchestrator.application.use_cases.list_providers import ProviderSummary
from integration_orchestrator.domain.entities import IntegrationRequest
from integration_orchestrator.domain.enums import OperationType, RequestStatus
from integration_orchestrator.domain.records import AuditEvent


class FailureSummary(ApiModel):
    """The most recent failure recorded against a request."""

    code: str
    message: str
    category: str | None = None


class IntegrationRequestResponse(ApiModel):
    """The canonical representation of an integration request."""

    id: UUID
    provider: str
    operation_type: OperationType
    external_reference: str
    status: RequestStatus
    correlation_id: str
    payload: dict[str, Any]
    provider_reference: str | None = None
    attempt_count: int
    next_retry_at: datetime | None = None
    completed_at: datetime | None = None
    manual_review_reason: str | None = None
    last_failure: FailureSummary | None = None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_domain(cls, request: IntegrationRequest) -> IntegrationRequestResponse:
        failure = None
        if request.last_error_code:
            failure = FailureSummary(
                code=request.last_error_code,
                message=request.last_error_message or "",
                category=request.last_error_category,
            )
        return cls(
            id=request.id,
            provider=request.provider.value,
            operation_type=request.operation_type,
            external_reference=request.external_reference.value,
            status=request.status,
            correlation_id=request.correlation_id.value,
            payload=request.normalized_payload,
            provider_reference=request.provider_reference,
            attempt_count=request.attempt_count,
            next_retry_at=request.next_retry_at,
            completed_at=request.completed_at,
            manual_review_reason=request.manual_review_reason,
            last_failure=failure,
            created_at=request.created_at,
            updated_at=request.updated_at,
        )


class IntegrationRequestPage(ApiModel):
    """A page of integration requests."""

    items: list[IntegrationRequestResponse]
    page: PageMeta

    @classmethod
    def from_domain(cls, page: Page[IntegrationRequest]) -> IntegrationRequestPage:
        return cls(
            items=[IntegrationRequestResponse.from_domain(item) for item in page.items],
            page=PageMeta(next_cursor=page.next_cursor, has_more=page.has_more),
        )


class AuditEventResponse(ApiModel):
    """One entry in a request's audit history."""

    id: UUID
    action: str
    actor_type: str
    actor_id: str | None = None
    previous_state: str | None = None
    new_state: str | None = None
    correlation_id: str
    occurred_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_domain(cls, event: AuditEvent) -> AuditEventResponse:
        return cls(
            id=event.id,
            action=event.action.value,
            actor_type=event.actor.value,
            actor_id=event.actor_id,
            previous_state=event.previous_state,
            new_state=event.new_state,
            correlation_id=event.correlation_id.value,
            occurred_at=event.occurred_at,
            metadata=event.metadata,
        )


class AuditHistoryResponse(ApiModel):
    """A request's full audit trail, oldest first."""

    request_id: UUID
    events: list[AuditEventResponse]

    @classmethod
    def from_domain(cls, request_id: UUID, events: Sequence[AuditEvent]) -> AuditHistoryResponse:
        return cls(
            request_id=request_id,
            events=[AuditEventResponse.from_domain(event) for event in events],
        )


class ProviderCapabilities(ApiModel):
    """What a provider can be asked to do."""

    supported_operations: list[OperationType]
    supports_cancellation: bool
    supports_status_lookup: bool
    supports_provider_idempotency: bool
    webhook_signature_scheme: str


class ProviderHealthResponse(ApiModel):
    """A provider's capabilities alongside its current operational state."""

    provider: str
    display_name: str
    enabled: bool
    healthy: bool
    reachable: bool
    circuit_state: str
    consecutive_failures: int
    in_flight: int
    capacity: int
    authentication_type: str
    max_attempts: int
    total_timeout_seconds: float
    capabilities: ProviderCapabilities
    detail: str | None = None

    @classmethod
    def from_summary(cls, summary: ProviderSummary) -> ProviderHealthResponse:
        descriptor = summary.descriptor
        return cls(
            provider=descriptor.slug.value,
            display_name=descriptor.display_name,
            enabled=descriptor.enabled,
            healthy=summary.healthy,
            reachable=summary.reachable,
            circuit_state=summary.circuit_state.value,
            consecutive_failures=summary.failure_count,
            in_flight=summary.in_flight,
            capacity=summary.capacity,
            authentication_type=descriptor.authentication_type,
            max_attempts=descriptor.max_attempts,
            total_timeout_seconds=descriptor.total_timeout_seconds,
            capabilities=ProviderCapabilities(
                supported_operations=sorted(
                    descriptor.supported_operations, key=lambda item: item.value
                ),
                supports_cancellation=descriptor.supports_cancellation,
                supports_status_lookup=descriptor.supports_status_lookup,
                supports_provider_idempotency=descriptor.supports_provider_idempotency,
                webhook_signature_scheme=descriptor.webhook_signature_scheme,
            ),
            detail=summary.detail,
        )


class ProviderListResponse(ApiModel):
    """The provider catalogue."""

    providers: list[ProviderHealthResponse]


class WebhookAcknowledgement(ApiModel):
    """The response returned to a provider after a webhook delivery.

    Deliberately minimal. Providers should learn only that the delivery was
    accepted; echoing internal identifiers or processing detail back to an
    external system gives away more than it needs to.
    """

    status: str = Field(description="Processing outcome from the platform's point of view.")
    receipt_id: UUID


class DependencyStatus(ApiModel):
    """The state of one external dependency."""

    name: str
    healthy: bool
    detail: str | None = None


class HealthResponse(ApiModel):
    """Liveness response."""

    status: str
    service: str
    version: str
    environment: str


class ReadinessResponse(ApiModel):
    """Readiness response, including per-dependency detail."""

    status: str
    dependencies: list[DependencyStatus]
