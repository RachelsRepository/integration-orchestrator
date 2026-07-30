"""Meridian Services adapter.

Characteristics of this fictional provider:

* Static API key authentication. There is no token to refresh, so a rejected key
  is a configuration problem rather than something to retry.
* Synchronous creation *and* a status endpoint, which makes Meridian requests
  fully reconcilable.
* **No provider-side idempotency.** A retried create genuinely creates a second
  operation. This is why the platform's own workflow idempotency matters: the
  dispatcher only retries after a durable state change, and the local
  idempotency record stops a duplicate HTTP request ever becoming a duplicate
  workflow.
* Inconsistent field naming between endpoints — ``requestId`` on create,
  ``request_id`` on status, ``status`` in one place and ``state`` in another.
  The adapter reads through tolerant accessors rather than committing to one
  spelling.
* Times out under load, which is the ambiguous failure reconciliation exists for.

Webhook security: HMAC-SHA256 over the raw body with a shared secret, and no
timestamp. Replay protection therefore rests entirely on event-id deduplication,
which is weaker than a signed timestamp and is called out as such in
``docs/webhook-security.md``.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from integration_orchestrator.domain.contracts import (
    CreateProviderOperationCommand,
    InboundWebhook,
    NormalizedWebhookEvent,
    ProviderErrorInfo,
    ProviderOperationResult,
    WebhookVerification,
)
from integration_orchestrator.domain.enums import (
    ErrorCategory,
    NormalizedStatus,
    OperationType,
)
from integration_orchestrator.domain.errors import WebhookPayloadError
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ProviderSlug,
    SignatureMetadata,
)
from integration_orchestrator.infrastructure.providers.base import BaseProviderAdapter
from integration_orchestrator.infrastructure.providers.signatures import (
    parse_timestamp,
    verify_hmac_sha256,
)

logger = logging.getLogger(__name__)

SLUG = ProviderSlug("meridian")

SIGNATURE_HEADER = "x-meridian-signature"
API_KEY_HEADER = "X-Meridian-Key"

_SERVICE_CODES: dict[OperationType, str] = {
    OperationType.RESOURCE_PROVISION: "SVC_PROVISION",
    OperationType.ACCESS_GRANT: "SVC_ACCESS_GRANT",
    OperationType.ACCESS_REVOKE: "SVC_ACCESS_REVOKE",
}

_STATUS_MAP: dict[str, NormalizedStatus] = {
    "received": NormalizedStatus.ACCEPTED,
    "processing": NormalizedStatus.PENDING,
    "pending": NormalizedStatus.PENDING,
    "fulfilled": NormalizedStatus.SUCCEEDED,
    "success": NormalizedStatus.SUCCEEDED,
    "rejected": NormalizedStatus.FAILED,
    "failure": NormalizedStatus.FAILED,
    "cancelled": NormalizedStatus.CANCELLED,
}


class MeridianAdapter(BaseProviderAdapter):
    """Translates between the normalized contract and Meridian Services."""

    supported_operations = frozenset(
        {
            OperationType.RESOURCE_PROVISION,
            OperationType.ACCESS_GRANT,
            OperationType.ACCESS_REVOKE,
        }
    )
    supports_provider_idempotency = False
    supports_cancellation = False
    supports_status_lookup = True
    webhook_signature_scheme = "hmac-sha256-body"
    health_path = "/status"

    async def create_operation(
        self, command: CreateProviderOperationCommand
    ) -> ProviderOperationResult:
        self._assert_supported(command.operation_type)
        body = self._build_body(command)

        # No idempotency key is sent: Meridian ignores it, and sending a header
        # the provider does not honour would imply a guarantee that does not
        # exist. Duplicate protection for this provider is entirely local.
        response = await self._http.request(
            "POST",
            "/service-requests",
            json_body=body,
            operation="create_operation",
        )

        reference = self._first_present(
            response.body, "requestId", "request_id", "id", "serviceRequestId"
        )
        provider_status = self._first_present(response.body, "status", "state", "requestStatus")

        if not reference:
            # Meridian answered 2xx and does not deduplicate, so a retry would
            # create a genuine second service request. The acceptance is
            # reported without a reference and escalated instead.
            logger.error(
                "the provider accepted a request without returning an identifier",
                extra={"provider": self._slug.value, "http_status": response.status_code},
            )
            return ProviderOperationResult.success(
                normalized_status=NormalizedStatus.ACCEPTED,
                provider_reference=None,
                provider_status=_as_str(provider_status),
                metadata={**response.metadata(), "missing_provider_reference": True},
                occurred_at=datetime.now(tz=UTC),
            )

        return ProviderOperationResult.success(
            normalized_status=self._map_status(_as_str(provider_status), _STATUS_MAP),
            provider_reference=str(reference),
            provider_status=_as_str(provider_status),
            metadata={**response.metadata(), "provider_request": self._redacted(body)},
            occurred_at=datetime.now(tz=UTC),
        )

    async def get_operation_status(self, provider_reference: str) -> ProviderOperationResult:
        response = await self._http.request(
            "GET",
            f"/service-requests/{provider_reference}",
            operation="get_operation_status",
        )
        provider_status = self._first_present(
            response.body, "status", "state", "requestStatus", "currentState"
        )
        normalized = self._map_status(_as_str(provider_status), _STATUS_MAP)

        error: ProviderErrorInfo | None = None
        if normalized is NormalizedStatus.FAILED:
            error = ProviderErrorInfo(
                code="provider_reported_failure",
                message=str(
                    self._first_present(response.body, "reason", "failureReason", "message")
                    or "Meridian reported the request was rejected"
                ),
                category=ErrorCategory.PROVIDER_VALIDATION,
                retryable=False,
                provider_code=_as_str(
                    self._first_present(response.body, "reasonCode", "errorCode")
                ),
            )

        return ProviderOperationResult(
            accepted=error is None,
            normalized_status=normalized,
            provider_reference=provider_reference,
            provider_status=_as_str(provider_status),
            raw_response_metadata=response.metadata(),
            error=error,
        )

    def _build_body(self, command: CreateProviderOperationCommand) -> dict[str, Any]:
        payload = dict(command.payload)
        return {
            "serviceCode": _SERVICE_CODES[command.operation_type],
            "customerRef": command.external_reference.value,
            "parameters": payload,
            "meta": {"correlationId": command.correlation_id.value},
        }

    # -- webhooks -----------------------------------------------------------

    def validate_webhook(self, webhook: InboundWebhook) -> WebhookVerification:
        signature = webhook.header(SIGNATURE_HEADER)
        metadata = SignatureMetadata(scheme=self.webhook_signature_scheme, algorithm="hmac-sha256")

        if not signature:
            return WebhookVerification.rejected(metadata, "missing signature header")

        secret = self._config.webhook_secret
        if secret is None:
            logger.error(
                "no webhook secret is configured; rejecting the delivery",
                extra={"provider": self._slug.value},
            )
            return WebhookVerification.rejected(metadata, "no webhook secret is configured")

        if not verify_hmac_sha256(
            secret=secret.get_secret_value(), message=webhook.body, signature=signature
        ):
            return WebhookVerification.rejected(metadata, "the signature does not match")

        # Meridian signs the body only, so a captured delivery stays
        # cryptographically valid forever. Deduplication on the event id is the
        # only thing preventing replay for this provider.
        return WebhookVerification.accepted(
            SignatureMetadata(scheme=metadata.scheme, algorithm=metadata.algorithm, verified=True)
        )

    def normalize_webhook(self, webhook: InboundWebhook) -> NormalizedWebhookEvent:
        body = _decode_body(webhook.body)

        event_id = self._first_present(body, "notificationId", "notification_id", "eventId")
        event_type = self._first_present(body, "notificationType", "type", "eventType")
        reference = self._first_present(body, "requestId", "request_id", "serviceRequestId")
        status = self._first_present(body, "status", "state", "requestStatus")

        if not event_id or not event_type:
            raise WebhookPayloadError(
                "the Meridian webhook body is missing its notification identity",
                provider=self._slug.value,
            )

        normalized = self._map_status(_as_str(status), _STATUS_MAP)
        error: ProviderErrorInfo | None = None
        if normalized is NormalizedStatus.FAILED:
            error = ProviderErrorInfo(
                code="provider_reported_failure",
                message=str(
                    self._first_present(body, "reason", "failureReason", "message")
                    or "Meridian reported the request was rejected"
                ),
                category=ErrorCategory.PROVIDER_VALIDATION,
                retryable=False,
                provider_code=_as_str(self._first_present(body, "reasonCode", "errorCode")),
            )

        correlation = self._first_present(body, "correlationId", "correlation_id")
        occurred = self._first_present(body, "occurredAt", "occurred_at", "timestamp")
        return NormalizedWebhookEvent(
            provider=self._slug,
            provider_event_id=str(event_id),
            event_type=str(event_type),
            normalized_status=normalized,
            occurred_at=parse_timestamp(_as_str(occurred)) or webhook.received_at,
            provider_reference=_as_str(reference),
            external_reference=_as_str(self._first_present(body, "customerRef", "customer_ref")),
            correlation_id=CorrelationId(str(correlation)) if correlation else None,
            payload_metadata={
                "notification_type": str(event_type),
                "status": _as_str(status),
            },
            error=error,
        )


def _decode_body(raw: bytes) -> dict[str, Any]:
    try:
        body = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise WebhookPayloadError("the webhook body is not valid JSON") from exc
    if not isinstance(body, dict):
        raise WebhookPayloadError("the webhook body is not a JSON object")
    return body


def _as_str(value: Any) -> str | None:
    return None if value is None else str(value)
