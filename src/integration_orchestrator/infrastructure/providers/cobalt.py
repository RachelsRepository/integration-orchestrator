"""Cobalt Network adapter.

Characteristics of this fictional provider:

* OAuth2 client credentials.
* Purely asynchronous: creation returns ``202 Accepted`` with a job reference and
  nothing else. There is never a synchronous success, so a Cobalt request always
  passes through ``pending``.
* Supports cancellation of an accepted job, which is the only provider here that
  does.
* Exposes a status endpoint, so reconciliation can verify Cobalt jobs.
* Returns temporary service errors under load.

Webhook security: Ed25519 signatures over ``key_id.timestamp.body`` with a key
identifier header. Asymmetric verification means this service holds only a public
key, so compromising it does not let an attacker forge Cobalt webhooks to anyone
else. The key id supports rotation: the provider can publish a new key and sign
with it while old deliveries still verify.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from integration_orchestrator.domain.contracts import (
    CancelProviderOperationCommand,
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
    verify_ed25519,
    within_replay_window,
)

logger = logging.getLogger(__name__)

SLUG = ProviderSlug("cobalt")

SIGNATURE_HEADER = "x-cobalt-signature"
TIMESTAMP_HEADER = "x-cobalt-timestamp"
KEY_ID_HEADER = "x-cobalt-key-id"

_JOB_KINDS: dict[OperationType, str] = {
    OperationType.RESOURCE_PROVISION: "resource.create",
    OperationType.RESOURCE_DEPROVISION: "resource.destroy",
    OperationType.RESOURCE_UPDATE: "resource.update",
    OperationType.ACCESS_GRANT: "access.grant",
    OperationType.ACCESS_REVOKE: "access.revoke",
}

_STATUS_MAP: dict[str, NormalizedStatus] = {
    "accepted": NormalizedStatus.ACCEPTED,
    "scheduled": NormalizedStatus.ACCEPTED,
    "executing": NormalizedStatus.PENDING,
    "succeeded": NormalizedStatus.SUCCEEDED,
    "failed": NormalizedStatus.FAILED,
    "cancelled": NormalizedStatus.CANCELLED,
    "canceled": NormalizedStatus.CANCELLED,
}

_WEBHOOK_EVENT_STATUS: dict[str, NormalizedStatus] = {
    "job.succeeded": NormalizedStatus.SUCCEEDED,
    "job.failed": NormalizedStatus.FAILED,
    "job.cancelled": NormalizedStatus.CANCELLED,
    "job.started": NormalizedStatus.PENDING,
}


class CobaltAdapter(BaseProviderAdapter):
    """Translates between the normalized contract and Cobalt Network."""

    supported_operations = frozenset(OperationType)
    supports_provider_idempotency = True
    supports_cancellation = True
    supports_status_lookup = True
    webhook_signature_scheme = "ed25519-timestamped"
    health_path = "/healthz"

    async def create_operation(
        self, command: CreateProviderOperationCommand
    ) -> ProviderOperationResult:
        self._assert_supported(command.operation_type)
        body = self._build_body(command)

        response = await self._http.request(
            "POST",
            "/jobs",
            json_body=body,
            idempotency_key=command.idempotency_key,
            operation="create_operation",
        )

        reference = self._first_present(response.body, "job_id", "jobId", "id")
        provider_status = _as_str(self._first_present(response.body, "status", "state"))

        if not reference:
            # Cobalt answered 2xx, so a job very likely exists. It cannot be
            # polled, cancelled or correlated to a webhook without its id, which
            # is a situation only a human can resolve.
            logger.error(
                "the provider accepted a job without returning an identifier",
                extra={"provider": self._slug.value, "http_status": response.status_code},
            )
            return ProviderOperationResult.success(
                normalized_status=NormalizedStatus.ACCEPTED,
                provider_reference=None,
                provider_status=provider_status,
                metadata={**response.metadata(), "missing_provider_reference": True},
                occurred_at=datetime.now(tz=UTC),
            )

        # Cobalt never completes synchronously. A create response that claims
        # terminal success is treated as unknown rather than trusted, because the
        # adapter's model of the provider and the provider's behaviour disagree.
        normalized = self._map_status(provider_status, _STATUS_MAP)
        if normalized in (NormalizedStatus.SUCCEEDED, NormalizedStatus.FAILED):
            logger.warning(
                "Cobalt returned a terminal status on job creation, which it should not",
                extra={"provider": self._slug.value, "provider_status": provider_status},
            )
            normalized = NormalizedStatus.UNKNOWN

        return ProviderOperationResult.success(
            normalized_status=normalized,
            provider_reference=str(reference),
            provider_status=provider_status,
            metadata={**response.metadata(), "provider_request": self._redacted(body)},
            occurred_at=datetime.now(tz=UTC),
        )

    async def get_operation_status(self, provider_reference: str) -> ProviderOperationResult:
        response = await self._http.request(
            "GET", f"/jobs/{provider_reference}", operation="get_operation_status"
        )
        provider_status = _as_str(self._first_present(response.body, "status", "state"))
        normalized = self._map_status(provider_status, _STATUS_MAP)

        error: ProviderErrorInfo | None = None
        if normalized is NormalizedStatus.FAILED:
            failure = response.body.get("failure")
            failure = failure if isinstance(failure, dict) else {}
            error = ProviderErrorInfo(
                code="provider_reported_failure",
                message=str(failure.get("message", "Cobalt reported the job failed")),
                category=ErrorCategory.PROVIDER_VALIDATION,
                retryable=False,
                provider_code=_as_str(failure.get("code")),
            )

        return ProviderOperationResult(
            accepted=error is None,
            normalized_status=normalized,
            provider_reference=provider_reference,
            provider_status=provider_status,
            raw_response_metadata=response.metadata(),
            error=error,
        )

    async def cancel_operation(
        self, command: CancelProviderOperationCommand
    ) -> ProviderOperationResult:
        response = await self._http.request(
            "POST",
            f"/jobs/{command.provider_reference}/cancel",
            json_body={"reason": command.reason or "cancelled by the requesting system"},
            operation="cancel_operation",
        )
        provider_status = _as_str(self._first_present(response.body, "status", "state"))
        normalized = self._map_status(provider_status, _STATUS_MAP)

        if normalized is NormalizedStatus.CANCELLED:
            return ProviderOperationResult.success(
                normalized_status=NormalizedStatus.CANCELLED,
                provider_reference=command.provider_reference,
                provider_status=provider_status,
                metadata=response.metadata(),
            )

        # Cobalt accepted the request but the job has already moved past the
        # point where it can be stopped. That is a refusal, not a failure.
        return ProviderOperationResult.failure(
            error=ProviderErrorInfo(
                code="provider_cancellation_refused",
                message="Cobalt could not cancel the job in its current state",
                category=ErrorCategory.CONFLICT,
                retryable=False,
                provider_code=provider_status,
            ),
            provider_reference=command.provider_reference,
            provider_status=provider_status,
            metadata=response.metadata(),
        )

    def _build_body(self, command: CreateProviderOperationCommand) -> dict[str, Any]:
        payload = dict(command.payload)
        return {
            "kind": _JOB_KINDS[command.operation_type],
            "subject": {"external_id": command.external_reference.value},
            "spec": payload,
            "trace": {
                "correlation_id": command.correlation_id.value,
                "attempt": command.attempt,
            },
        }

    # -- webhooks -----------------------------------------------------------

    def validate_webhook(self, webhook: InboundWebhook) -> WebhookVerification:
        signature = webhook.header(SIGNATURE_HEADER)
        timestamp_raw = webhook.header(TIMESTAMP_HEADER)
        key_id = webhook.header(KEY_ID_HEADER)
        metadata = SignatureMetadata(
            scheme=self.webhook_signature_scheme,
            algorithm="ed25519",
            key_id=key_id,
            timestamp=timestamp_raw,
        )

        if not signature or not timestamp_raw or not key_id:
            return WebhookVerification.rejected(
                metadata, "missing signature, timestamp, or key identifier header"
            )

        public_key = self._config.webhook_public_key
        if not public_key:
            logger.error(
                "no webhook public key is configured; rejecting the delivery",
                extra={"provider": self._slug.value},
            )
            return WebhookVerification.rejected(metadata, "no webhook public key is configured")

        timestamp = parse_timestamp(timestamp_raw)
        if not within_replay_window(
            timestamp, now=webhook.received_at, window_seconds=self._replay_window_seconds
        ):
            return WebhookVerification.rejected(
                metadata, "the timestamp is outside the replay window"
            )

        signed_payload = b".".join(
            [key_id.encode("utf-8"), timestamp_raw.encode("utf-8"), webhook.body]
        )
        if not verify_ed25519(
            public_key_b64=public_key, message=signed_payload, signature_b64=signature
        ):
            return WebhookVerification.rejected(metadata, "the signature does not match")

        return WebhookVerification.accepted(
            SignatureMetadata(
                scheme=metadata.scheme,
                algorithm=metadata.algorithm,
                key_id=key_id,
                timestamp=timestamp_raw,
                verified=True,
            )
        )

    def normalize_webhook(self, webhook: InboundWebhook) -> NormalizedWebhookEvent:
        body = _decode_body(webhook.body)
        event_id = self._first_present(body, "id", "event_id")
        event_type = self._first_present(body, "type", "event_type")
        job = body.get("job")
        job = job if isinstance(job, dict) else {}

        if not event_id or not event_type:
            raise WebhookPayloadError(
                "the Cobalt webhook body is missing its event identity",
                provider=self._slug.value,
            )

        normalized = _WEBHOOK_EVENT_STATUS.get(
            str(event_type), self._map_status(_as_str(job.get("status")), _STATUS_MAP)
        )

        error: ProviderErrorInfo | None = None
        if normalized is NormalizedStatus.FAILED:
            failure = job.get("failure")
            failure = failure if isinstance(failure, dict) else {}
            error = ProviderErrorInfo(
                code="provider_reported_failure",
                message=str(failure.get("message", "Cobalt reported the job failed")),
                category=ErrorCategory.PROVIDER_VALIDATION,
                retryable=False,
                provider_code=_as_str(failure.get("code")),
            )

        correlation = (job.get("trace") or {}).get("correlation_id") if job else None
        return NormalizedWebhookEvent(
            provider=self._slug,
            provider_event_id=str(event_id),
            event_type=str(event_type),
            normalized_status=normalized,
            occurred_at=parse_timestamp(_as_str(body.get("created_at"))) or webhook.received_at,
            provider_reference=_as_str(self._first_present(job, "job_id", "jobId", "id")),
            external_reference=_as_str((job.get("subject") or {}).get("external_id"))
            if isinstance(job.get("subject"), dict)
            else None,
            correlation_id=CorrelationId(str(correlation)) if correlation else None,
            payload_metadata={
                "event_type": str(event_type),
                "job_status": _as_str(job.get("status")),
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
