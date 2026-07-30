"""Shared behaviour for HTTP provider adapters.

Adapters inherit the parts that are genuinely identical — health probing,
descriptor construction, unsupported-operation rejection — and implement only the
translation that makes their provider different. Anything that starts to look
like policy rather than translation belongs one layer up, in the resilience
decorator or the dispatcher, not here.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

from integration_orchestrator.config.settings import ProviderSettings
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
from integration_orchestrator.domain.enums import NormalizedStatus, OperationType
from integration_orchestrator.domain.errors import (
    ProviderError,
    UnsupportedOperationError,
)
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.providers.http import ProviderHttpClient
from integration_orchestrator.observability.redaction import redact

logger = logging.getLogger(__name__)


class BaseProviderAdapter(ABC):
    """Common scaffolding for a provider gateway implementation."""

    #: Operations this provider can perform.
    supported_operations: frozenset[OperationType] = frozenset()
    #: Whether the provider deduplicates on a client-supplied idempotency key.
    supports_provider_idempotency: bool = False
    #: Whether an accepted operation can later be cancelled.
    supports_cancellation: bool = False
    #: Whether the provider exposes a status endpoint reconciliation can poll.
    supports_status_lookup: bool = False
    #: How this provider signs its webhooks, for documentation and the API.
    webhook_signature_scheme: str = "none"
    #: Path used by the health probe.
    health_path: str = "/health"

    def __init__(
        self,
        *,
        slug: ProviderSlug,
        config: ProviderSettings,
        http: ProviderHttpClient,
        webhook_replay_window_seconds: int,
    ) -> None:
        self._slug = slug
        self._config = config
        self._http = http
        self._replay_window_seconds = webhook_replay_window_seconds

    @property
    def slug(self) -> ProviderSlug:
        return self._slug

    def descriptor(self) -> ProviderDescriptor:
        return ProviderDescriptor(
            slug=self._slug,
            display_name=self._config.display_name or self._slug.value,
            authentication_type=self._config.authentication_type.value,
            enabled=self._config.enabled,
            supported_operations=self.supported_operations,
            supports_cancellation=self.supports_cancellation,
            supports_status_lookup=self.supports_status_lookup,
            supports_provider_idempotency=self.supports_provider_idempotency,
            webhook_signature_scheme=self.webhook_signature_scheme,
            max_concurrency=self._config.max_concurrency,
            max_attempts=self._config.max_attempts,
            total_timeout_seconds=self._config.total_timeout_seconds,
        )

    # -- gateway surface ----------------------------------------------------

    @abstractmethod
    async def create_operation(
        self, command: CreateProviderOperationCommand
    ) -> ProviderOperationResult: ...

    async def get_operation_status(self, provider_reference: str) -> ProviderOperationResult:
        raise UnsupportedOperationError(
            f"provider '{self._slug.value}' does not expose an operation status endpoint",
            provider=self._slug.value,
            metadata={"provider_reference": provider_reference},
        )

    async def cancel_operation(
        self, command: CancelProviderOperationCommand
    ) -> ProviderOperationResult:
        raise UnsupportedOperationError(
            f"provider '{self._slug.value}' does not support cancellation",
            provider=self._slug.value,
            metadata={"provider_reference": command.provider_reference},
        )

    @abstractmethod
    def validate_webhook(self, webhook: InboundWebhook) -> WebhookVerification: ...

    @abstractmethod
    def normalize_webhook(self, webhook: InboundWebhook) -> NormalizedWebhookEvent: ...

    async def health_check(self) -> ProviderHealthProbe:
        """Probe the provider's health endpoint."""
        started = datetime.now(tz=UTC)
        try:
            response = await self._http.request("GET", self.health_path, operation="health_check")
        except ProviderError as exc:
            return ProviderHealthProbe(
                healthy=False,
                checked_at=datetime.now(tz=UTC),
                detail=exc.code,
            )
        return ProviderHealthProbe(
            healthy=True,
            checked_at=datetime.now(tz=UTC),
            latency_ms=(datetime.now(tz=UTC) - started).total_seconds() * 1000,
            detail=str(response.body.get("status", "ok")),
        )

    # -- helpers ------------------------------------------------------------

    def _assert_supported(self, operation_type: OperationType) -> None:
        if operation_type not in self.supported_operations:
            raise UnsupportedOperationError(
                f"provider '{self._slug.value}' does not support the operation "
                f"'{operation_type.value}'",
                provider=self._slug.value,
                metadata={
                    "supported_operations": sorted(
                        operation.value for operation in self.supported_operations
                    )
                },
            )

    @staticmethod
    def _redacted(payload: dict[str, Any]) -> dict[str, Any]:
        """Redact a provider-shaped body before it is persisted for diagnosis."""
        result = redact(payload)
        return result if isinstance(result, dict) else {}

    @staticmethod
    def _first_present(body: dict[str, Any], *keys: str) -> Any:
        """Return the first present key from a set of alternative spellings.

        Some providers are inconsistent about naming between endpoints and across
        releases. Reading through a tolerant accessor is far less brittle than
        picking one spelling and breaking the day the provider changes it.
        """
        for key in keys:
            if key in body and body[key] is not None:
                return body[key]
        return None

    @staticmethod
    def _map_status(raw: str | None, mapping: dict[str, NormalizedStatus]) -> NormalizedStatus:
        """Map a provider status string onto the normalized vocabulary.

        Unrecognised statuses become ``UNKNOWN`` rather than a guessed value. A
        provider adding a new state must not silently be interpreted as success.
        """
        if raw is None:
            return NormalizedStatus.UNKNOWN
        return mapping.get(raw.strip().lower(), NormalizedStatus.UNKNOWN)
