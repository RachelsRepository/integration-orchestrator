"""Provider webhook endpoints.

One route serves all providers, with the provider identified by the path segment.
The alternative — a hand-written route per provider — would grow with every
onboarding and would tempt someone to put provider-specific parsing in the API
layer. Here the route does nothing but capture the raw bytes and headers; all
interpretation happens in the adapter.

Three properties this endpoint must have, and why:

*No bearer authentication.* Providers cannot present our tokens. Authenticity
comes from the signature over the body, which is a stronger claim anyway because
it also covers integrity.

*Raw bytes, never a parsed body.* Signatures are computed over exactly what was
transmitted. Parsing and re-serialising changes key order and whitespace, and the
signature stops verifying.

*2xx for everything except a verification failure.* Providers retry on non-2xx.
Returning an error for a duplicate or a deferred event would guarantee an endless
redelivery loop for an event we already handled correctly.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Request, Response, status

from integration_orchestrator.api.dependencies import CorrelationDep, IngestWebhookDep, SettingsDep
from integration_orchestrator.api.schemas.common import ErrorResponse
from integration_orchestrator.api.schemas.responses import WebhookAcknowledgement
from integration_orchestrator.application.dto.commands import IngestWebhookCommand
from integration_orchestrator.domain.contracts import InboundWebhook
from integration_orchestrator.domain.enums import WebhookProcessingStatus
from integration_orchestrator.domain.errors import ValidationError
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.observability.correlation import set_integration_request_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.post(
    "/{provider}",
    response_model=WebhookAcknowledgement,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Receive a provider webhook",
    responses={
        400: {"model": ErrorResponse, "description": "The body exceeds the size limit."},
        401: {
            "model": ErrorResponse,
            "description": "Signature, timestamp or replay verification failed.",
        },
        404: {"model": ErrorResponse, "description": "Unknown provider."},
    },
)
async def receive_webhook(
    provider: str,
    request: Request,
    use_case: IngestWebhookDep,
    settings: SettingsDep,
    correlation_id: CorrelationDep,
    response: Response,
) -> WebhookAcknowledgement:
    body = await request.body()
    if len(body) > settings.webhooks.max_body_bytes:
        raise ValidationError(
            "the webhook body exceeds the configured size limit",
            correlation_id=correlation_id.value,
            metadata={
                "limit_bytes": settings.webhooks.max_body_bytes,
                "received_bytes": len(body),
            },
        )

    webhook = InboundWebhook(
        provider=ProviderSlug.parse(provider),
        headers={key.lower(): value for key, value in request.headers.items()},
        body=body,
        received_at=datetime.now(tz=UTC),
        remote_address=request.client.host if request.client else None,
    )

    result = await use_case.execute(
        IngestWebhookCommand(
            webhook=webhook,
            correlation_id=correlation_id,
            metadata={"user_agent": request.headers.get("user-agent")},
        )
    )
    if result.integration_request_id:
        set_integration_request_id(str(result.integration_request_id))

    response.status_code = _status_for(result.status)
    return WebhookAcknowledgement(status=result.status.value, receipt_id=result.receipt_id)


def _status_for(processing_status: WebhookProcessingStatus) -> int:
    """Map a processing outcome onto an HTTP status a provider will interpret well."""
    if processing_status is WebhookProcessingStatus.PROCESSED:
        return status.HTTP_200_OK
    if processing_status in (
        WebhookProcessingStatus.DUPLICATE,
        WebhookProcessingStatus.DEFERRED,
        WebhookProcessingStatus.RECEIVED,
    ):
        return status.HTTP_202_ACCEPTED
    # A rejected receipt is the one case where the provider should retry or
    # alert, because it means the delivery could not be trusted.
    return status.HTTP_401_UNAUTHORIZED
