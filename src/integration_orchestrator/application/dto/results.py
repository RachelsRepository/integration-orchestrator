"""Application results."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from integration_orchestrator.domain.entities import IntegrationRequest
from integration_orchestrator.domain.enums import WebhookProcessingStatus


@dataclass(frozen=True, slots=True)
class CreateIntegrationRequestResult:
    """The outcome of a creation attempt.

    ``replayed`` distinguishes a fresh creation from an idempotent replay so the
    API can return 200 instead of 201 and operators can see, in metrics, how much
    duplicate traffic clients are sending.
    """

    request: IntegrationRequest
    replayed: bool = False

    @property
    def http_status(self) -> int:
        return 200 if self.replayed else 201


@dataclass(frozen=True, slots=True)
class WebhookIngestionResult:
    """The outcome of ingesting one webhook.

    Every outcome except a verification failure is a success from the provider's
    point of view. Providers retry on non-2xx responses, so returning an error
    for a duplicate would guarantee an endless redelivery loop.
    """

    receipt_id: UUID
    status: WebhookProcessingStatus
    integration_request_id: UUID | None = None
    detail: str | None = None

    @property
    def accepted(self) -> bool:
        return self.status is not WebhookProcessingStatus.REJECTED
