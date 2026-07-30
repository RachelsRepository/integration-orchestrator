"""Northstar Connect adapter.

Characteristics of this fictional provider:

* OAuth2 client credentials.
* Synchronous creation that returns an operation id immediately, with completion
  delivered later by webhook.
* Honours a client-supplied ``Idempotency-Key``, so a retried create collapses
  onto the original operation instead of creating a second one.
* No status endpoint. Reconciliation therefore cannot confirm what happened to a
  Northstar operation and must escalate rather than guess.
* Applies rate limiting under load, with a ``Retry-After`` header.

Webhook security: HMAC-SHA256 over ``timestamp.body`` with a timestamp header and
a replay window.
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
    within_replay_window,
)

logger = logging.getLogger(__name__)

SLUG = ProviderSlug("northstar")

SIGNATURE_HEADER = "x-northstar-signature"
TIMESTAMP_HEADER = "x-northstar-timestamp"

# Northstar's own operation vocabulary.
_OPERATION_NAMES: dict[OperationType, str] = {
    OperationType.RESOURCE_PROVISION: "provision",
    OperationType.RESOURCE_DEPROVISION: "decommission",
    OperationType.RESOURCE_UPDATE: "modify",
}

_STATUS_MAP: dict[str, NormalizedStatus] = {
    "queued": NormalizedStatus.ACCEPTED,
    "accepted": NormalizedStatus.ACCEPTED,
    "running": NormalizedStatus.PENDING,
    "in_progress": NormalizedStatus.PENDING,
    "complete": NormalizedStatus.SUCCEEDED,
    "completed": NormalizedStatus.SUCCEEDED,
    "error": NormalizedStatus.FAILED,
    "failed": NormalizedStatus.FAILED,
}

_WEBHOOK_EVENT_STATUS: dict[str, NormalizedStatus] = {
    "operation.completed": NormalizedStatus.SUCCEEDED,
    "operation.failed": NormalizedStatus.FAILED,
    "operation.progressed": NormalizedStatus.PENDING,
}


class NorthstarAdapter(BaseProviderAdapter):
    """Translates between the normalized contract and Northstar Connect."""

    supported_operations = frozenset(
        {
            OperationType.RESOURCE_PROVISION,
            OperationType.RESOURCE_DEPROVISION,
            OperationType.RESOURCE_UPDATE,
        }
    )
    supports_provider_idempotency = True
    supports_cancellation = False
    supports_status_lookup = False
    webhook_signature_scheme = "hmac-sha256-timestamped"
    health_path = "/health"

    async def create_operation(
        self, command: CreateProviderOperationCommand
    ) -> ProviderOperationResult:
        self._assert_supported(command.operation_type)
        body = self._build_body(command)

        response = await self._http.request(
            "POST",
            "/operations",
            json_body=body,
            # Northstar deduplicates on this header, so an attempt that timed out
            # after the operation was created will not create a second one.
            idempotency_key=command.idempotency_key,
            operation="create_operation",
        )

        provider_status = response.body.get("state")

        reference = response.body.get("operation_id")
        if not reference:
            # Northstar answered 2xx, so the operation probably exists — it just
            # cannot be named. Reporting failure would invite a retry that
            # creates a second one, so the acceptance is passed up without a
            # reference and the dispatcher escalates it for a human.
            logger.error(
                "the provider accepted an operation without returning an identifier",
                extra={"provider": self._slug.value, "http_status": response.status_code},
            )
            return ProviderOperationResult.success(
                normalized_status=NormalizedStatus.ACCEPTED,
                provider_reference=None,
                provider_status=_optional_str(provider_status),
                metadata={**response.metadata(), "missing_provider_reference": True},
                occurred_at=datetime.now(tz=UTC),
            )

        return ProviderOperationResult.success(
            normalized_status=self._map_status(provider_status, _STATUS_MAP),
            provider_reference=str(reference),
            provider_status=provider_status,
            metadata={
                **response.metadata(),
                "provider_request": self._redacted(body),
                "deduplicated": bool(response.body.get("deduplicated", False)),
            },
            occurred_at=datetime.now(tz=UTC),
        )

    def _build_body(self, command: CreateProviderOperationCommand) -> dict[str, Any]:
        """Translate the normalized payload into Northstar's shape."""
        payload = dict(command.payload)
        return {
            "operation": _OPERATION_NAMES[command.operation_type],
            "reference": command.external_reference.value,
            "attributes": {
                "resource_type": payload.get("resource_type"),
                "region": payload.get("region"),
                **{
                    key: value
                    for key, value in payload.items()
                    if key not in ("resource_type", "region")
                },
            },
            "client_context": {
                "correlation_id": command.correlation_id.value,
                "attempt": command.attempt,
            },
        }

    # -- webhooks -----------------------------------------------------------

    def validate_webhook(self, webhook: InboundWebhook) -> WebhookVerification:
        timestamp_raw = webhook.header(TIMESTAMP_HEADER)
        signature = webhook.header(SIGNATURE_HEADER)
        metadata = SignatureMetadata(
            scheme=self.webhook_signature_scheme,
            algorithm="hmac-sha256",
            timestamp=timestamp_raw,
        )

        if not signature or not timestamp_raw:
            return WebhookVerification.rejected(metadata, "missing signature or timestamp header")

        secret = self._config.webhook_secret
        if secret is None:
            logger.error(
                "no webhook secret is configured; rejecting the delivery",
                extra={"provider": self._slug.value},
            )
            return WebhookVerification.rejected(metadata, "no webhook secret is configured")

        timestamp = parse_timestamp(timestamp_raw)
        if not within_replay_window(
            timestamp, now=webhook.received_at, window_seconds=self._replay_window_seconds
        ):
            # Checked before the signature so a captured-and-replayed delivery is
            # rejected on freshness, which is the cheaper check.
            return WebhookVerification.rejected(
                metadata, "the timestamp is outside the replay window"
            )

        # The timestamp is inside the signed material, so it cannot be altered to
        # make an old capture look fresh.
        signed_payload = timestamp_raw.encode("utf-8") + b"." + webhook.body
        if not verify_hmac_sha256(
            secret=secret.get_secret_value(), message=signed_payload, signature=signature
        ):
            return WebhookVerification.rejected(metadata, "the signature does not match")

        return WebhookVerification.accepted(
            SignatureMetadata(
                scheme=metadata.scheme,
                algorithm=metadata.algorithm,
                timestamp=timestamp_raw,
                verified=True,
            )
        )

    def normalize_webhook(self, webhook: InboundWebhook) -> NormalizedWebhookEvent:
        body = _decode_body(webhook.body)
        event_id = body.get("event_id")
        event_type = body.get("event_type")
        data = body.get("data")

        if not event_id or not event_type or not isinstance(data, dict):
            raise WebhookPayloadError(
                "the Northstar webhook body is missing required fields",
                provider=self._slug.value,
            )

        reference = data.get("operation_id")
        normalized_status = _WEBHOOK_EVENT_STATUS.get(
            str(event_type), self._map_status(data.get("state"), _STATUS_MAP)
        )

        error: ProviderErrorInfo | None = None
        if normalized_status is NormalizedStatus.FAILED:
            error = ProviderErrorInfo(
                code="provider_reported_failure",
                message=str(data.get("error_message", "Northstar reported the operation failed")),
                category=ErrorCategory.PROVIDER_VALIDATION,
                retryable=False,
                provider_code=_optional_str(data.get("error_code")),
            )

        correlation = data.get("correlation_id") or body.get("correlation_id")
        return NormalizedWebhookEvent(
            provider=self._slug,
            provider_event_id=str(event_id),
            event_type=str(event_type),
            normalized_status=normalized_status,
            occurred_at=parse_timestamp(_optional_str(body.get("occurred_at")))
            or webhook.received_at,
            provider_reference=_optional_str(reference),
            external_reference=_optional_str(data.get("reference")),
            correlation_id=CorrelationId(str(correlation)) if correlation else None,
            payload_metadata={
                "event_type": str(event_type),
                "state": _optional_str(data.get("state")),
                "reference": _optional_str(data.get("reference")),
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


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)
