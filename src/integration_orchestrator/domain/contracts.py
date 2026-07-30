"""Provider-neutral commands and results.

This module is the contract between orchestration and provider adapters. The
application layer only ever constructs the command types here and only ever
reads the result types here; it never sees a provider's own vocabulary, field
names, status strings, or error codes. That is what makes provider-specific
branching in use cases unnecessary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from integration_orchestrator.domain.entities import FailureDetail
from integration_orchestrator.domain.enums import (
    ErrorCategory,
    NormalizedStatus,
    OperationType,
)
from integration_orchestrator.domain.errors import ProviderError, ValidationError
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ExternalReference,
    ProviderSlug,
    SignatureMetadata,
)


@dataclass(frozen=True, slots=True)
class CreateProviderOperationCommand:
    """Instruction to create one operation at a provider."""

    request_id: UUID
    provider: ProviderSlug
    operation_type: OperationType
    external_reference: ExternalReference
    payload: dict[str, Any]
    correlation_id: CorrelationId
    idempotency_key: str
    attempt: int = 1

    def __post_init__(self) -> None:
        if not self.idempotency_key:
            raise ValidationError("a provider operation command requires an idempotency key")
        if self.attempt < 1:
            raise ValidationError("attempt must be at least 1")


@dataclass(frozen=True, slots=True)
class CancelProviderOperationCommand:
    """Instruction to cancel a previously created provider operation."""

    request_id: UUID
    provider: ProviderSlug
    provider_reference: str
    correlation_id: CorrelationId
    reason: str | None = None

    def __post_init__(self) -> None:
        if not self.provider_reference:
            raise ValidationError("cancellation requires a provider reference")


@dataclass(frozen=True, slots=True)
class ProviderErrorInfo:
    """Normalized description of a provider-side failure."""

    code: str
    message: str
    category: ErrorCategory
    retryable: bool
    provider_code: str | None = None
    retry_after_seconds: float | None = None

    @classmethod
    def from_error(cls, error: ProviderError) -> ProviderErrorInfo:
        """Convert a raised provider error into result-shaped information.

        Transport failures arrive as exceptions while business rejections arrive
        as results. Collapsing both into this one shape means the orchestration
        code has a single path to reason about.
        """
        return cls(
            code=error.code,
            message=error.message,
            category=error.category,
            retryable=error.retryable,
            provider_code=error.provider_code,
            retry_after_seconds=error.retry_after_seconds,
        )

    def to_failure_detail(self) -> FailureDetail:
        return FailureDetail(
            code=self.code,
            message=self.message,
            category=self.category.value,
            retryable=self.retryable,
            provider_code=self.provider_code,
        )


@dataclass(frozen=True, slots=True)
class ProviderOperationResult:
    """Outcome of a create or status call, expressed in neutral terms.

    ``raw_response_metadata`` intentionally holds only non-sensitive facts about
    the exchange, such as the HTTP status and the provider's request id. Raw
    provider bodies never travel further than the adapter, so they cannot leak
    into events, audit rows, or API responses.
    """

    accepted: bool
    normalized_status: NormalizedStatus
    provider_reference: str | None = None
    provider_status: str | None = None
    raw_response_metadata: dict[str, Any] = field(default_factory=dict)
    error: ProviderErrorInfo | None = None
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.accepted and self.error is not None:
            raise ValidationError("an accepted provider result cannot carry an error")
        if not self.accepted and self.error is None:
            raise ValidationError("a rejected provider result must carry an error")

    @property
    def retryable(self) -> bool:
        return bool(self.error and self.error.retryable)

    @property
    def error_code(self) -> str | None:
        return self.error.code if self.error else None

    @property
    def error_message(self) -> str | None:
        return self.error.message if self.error else None

    @classmethod
    def success(
        cls,
        *,
        normalized_status: NormalizedStatus,
        provider_reference: str | None = None,
        provider_status: str | None = None,
        metadata: dict[str, Any] | None = None,
        occurred_at: datetime | None = None,
    ) -> ProviderOperationResult:
        return cls(
            accepted=True,
            normalized_status=normalized_status,
            provider_reference=provider_reference,
            provider_status=provider_status,
            raw_response_metadata=dict(metadata or {}),
            occurred_at=occurred_at,
        )

    @classmethod
    def failure(
        cls,
        *,
        error: ProviderErrorInfo,
        provider_reference: str | None = None,
        provider_status: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ProviderOperationResult:
        return cls(
            accepted=False,
            normalized_status=NormalizedStatus.FAILED,
            provider_reference=provider_reference,
            provider_status=provider_status,
            raw_response_metadata=dict(metadata or {}),
            error=error,
        )


@dataclass(frozen=True, slots=True)
class NormalizedWebhookEvent:
    """A verified provider webhook translated into neutral terms."""

    provider: ProviderSlug
    provider_event_id: str
    event_type: str
    normalized_status: NormalizedStatus
    occurred_at: datetime
    provider_reference: str | None = None
    external_reference: str | None = None
    correlation_id: CorrelationId | None = None
    payload_metadata: dict[str, Any] = field(default_factory=dict)
    error: ProviderErrorInfo | None = None

    def __post_init__(self) -> None:
        if not self.provider_event_id:
            raise ValidationError("a normalized webhook event requires a provider event id")
        if self.occurred_at.tzinfo is None:
            raise ValidationError("webhook timestamps must be timezone-aware")


@dataclass(frozen=True, slots=True)
class InboundWebhook:
    """The raw material of an inbound webhook, before verification.

    Headers arrive lowercased so adapters never have to worry about the casing a
    particular provider chose, and the body is kept as bytes because signature
    verification must run over exactly the bytes that were transmitted, not over
    a re-serialised parse of them.
    """

    provider: ProviderSlug
    headers: dict[str, str]
    body: bytes
    received_at: datetime
    remote_address: str | None = None

    def header(self, name: str) -> str | None:
        return self.headers.get(name.lower())

    def require_header(self, name: str) -> str:
        value = self.header(name)
        if value is None:
            raise ValidationError(f"the '{name}' header is required")
        return value


@dataclass(frozen=True, slots=True)
class WebhookVerification:
    """The result of verifying an inbound webhook's authenticity."""

    verified: bool
    signature_metadata: SignatureMetadata
    reason: str | None = None

    @classmethod
    def accepted(cls, metadata: SignatureMetadata) -> WebhookVerification:
        return cls(verified=True, signature_metadata=metadata)

    @classmethod
    def rejected(cls, metadata: SignatureMetadata, reason: str) -> WebhookVerification:
        return cls(verified=False, signature_metadata=metadata, reason=reason)


@dataclass(frozen=True, slots=True)
class ProviderHealthProbe:
    """The outcome of a provider health check."""

    healthy: bool
    checked_at: datetime
    latency_ms: float | None = None
    detail: str | None = None
