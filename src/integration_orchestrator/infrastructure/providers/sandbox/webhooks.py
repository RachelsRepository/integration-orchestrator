"""Signed webhook construction for the provider sandbox.

Each fake provider emits webhooks in exactly the shape and with exactly the
signature scheme its real counterpart uses, so the adapters are exercised against
realistic input rather than against a body shaped to make them pass.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from integration_orchestrator.infrastructure.providers.sandbox.signing import (
    COBALT_KEY_ID,
    MERIDIAN_WEBHOOK_SECRET,
    NORTHSTAR_WEBHOOK_SECRET,
    cobalt_sign,
)
from integration_orchestrator.infrastructure.providers.sandbox.store import SandboxOperation
from integration_orchestrator.infrastructure.providers.signatures import compute_hmac_sha256


@dataclass(frozen=True, slots=True)
class SignedWebhook:
    """A ready-to-deliver webhook: raw body plus the headers that sign it."""

    path: str
    headers: dict[str, str]
    body: bytes

    def as_dict(self) -> dict[str, Any]:
        """Render for the sandbox control API, so tests can replay it verbatim."""
        return {
            "path": self.path,
            "headers": self.headers,
            "body": self.body.decode("utf-8"),
        }


def _canonical(body: dict[str, Any]) -> bytes:
    """Serialise once, sign and send the same bytes.

    Re-serialising between signing and sending is the classic way to produce a
    signature that never verifies.
    """
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")


def northstar_webhook(operation: SandboxOperation, *, event_type: str) -> SignedWebhook:
    body = _canonical(
        {
            "event_id": f"nsevt-{operation.id}-{len(operation.emitted_events) + 1}",
            "event_type": event_type,
            "occurred_at": datetime.now(tz=UTC).isoformat(),
            "correlation_id": operation.correlation_id,
            "data": {
                "operation_id": operation.id,
                "reference": operation.external_reference,
                "state": operation.status,
                "error_code": operation.failure_code,
                "error_message": operation.failure_message,
            },
        }
    )
    timestamp = str(int(datetime.now(tz=UTC).timestamp()))
    signature = compute_hmac_sha256(
        secret=NORTHSTAR_WEBHOOK_SECRET,
        message=timestamp.encode("utf-8") + b"." + body,
    )
    return SignedWebhook(
        path="/webhooks/northstar",
        headers={
            "content-type": "application/json",
            "x-northstar-timestamp": timestamp,
            "x-northstar-signature": f"sha256={signature}",
        },
        body=body,
    )


def meridian_webhook(operation: SandboxOperation, *, event_type: str) -> SignedWebhook:
    # Deliberately mixes camelCase and snake_case, mirroring the real provider's
    # inconsistent naming that the adapter has to tolerate.
    body = _canonical(
        {
            "notificationId": f"mnotif-{operation.id}-{len(operation.emitted_events) + 1}",
            "notificationType": event_type,
            "requestId": operation.id,
            "customer_ref": operation.external_reference,
            "status": operation.status,
            "reason": operation.failure_message,
            "reasonCode": operation.failure_code,
            "correlationId": operation.correlation_id,
            "occurredAt": datetime.now(tz=UTC).isoformat(),
        }
    )
    signature = compute_hmac_sha256(secret=MERIDIAN_WEBHOOK_SECRET, message=body)
    return SignedWebhook(
        path="/webhooks/meridian",
        headers={
            "content-type": "application/json",
            "x-meridian-signature": signature,
        },
        body=body,
    )


def cobalt_webhook(operation: SandboxOperation, *, event_type: str) -> SignedWebhook:
    failure = (
        {"code": operation.failure_code, "message": operation.failure_message}
        if operation.failure_code or operation.failure_message
        else None
    )
    body = _canonical(
        {
            "id": f"cbevt-{operation.id}-{len(operation.emitted_events) + 1}",
            "type": event_type,
            "created_at": datetime.now(tz=UTC).isoformat(),
            "job": {
                "job_id": operation.id,
                "kind": operation.kind,
                "status": operation.status,
                "subject": {"external_id": operation.external_reference},
                "trace": {"correlation_id": operation.correlation_id},
                "failure": failure,
            },
        }
    )
    timestamp = str(int(datetime.now(tz=UTC).timestamp()))
    signature = cobalt_sign(key_id=COBALT_KEY_ID, timestamp=timestamp, body=body)
    return SignedWebhook(
        path="/webhooks/cobalt",
        headers={
            "content-type": "application/json",
            "x-cobalt-timestamp": timestamp,
            "x-cobalt-key-id": COBALT_KEY_ID,
            "x-cobalt-signature": signature,
        },
        body=body,
    )
