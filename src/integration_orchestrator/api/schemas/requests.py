"""Inbound request bodies.

Validation here is structural only: shapes, lengths and enum membership. Business
rules such as "does this provider support this operation" live in the application
layer, because they depend on runtime configuration rather than on the wire
format.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field, field_validator

from integration_orchestrator.api.schemas.common import ApiModel
from integration_orchestrator.domain.enums import OperationType

MAX_PAYLOAD_KEYS = 100
MAX_REASON_LENGTH = 500


class CreateIntegrationRequestBody(ApiModel):
    """Request body for creating an integration request."""

    provider: str = Field(
        min_length=2,
        max_length=40,
        description="Provider slug, for example 'northstar'.",
        examples=["northstar"],
    )
    operation_type: OperationType = Field(
        description="Normalized operation to perform.",
        examples=[OperationType.RESOURCE_PROVISION],
    )
    external_reference: str = Field(
        min_length=1,
        max_length=255,
        description="The caller's own identifier for this piece of work.",
        examples=["order-8814"],
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Provider-neutral attributes. The selected adapter maps these onto "
            "the provider's own request shape."
        ),
    )

    @field_validator("payload")
    @classmethod
    def _bound_payload(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Reject payloads large enough to be a problem downstream.

        The payload is stored as JSONB, fingerprinted for idempotency and echoed
        into provider requests. An unbounded object would make all three
        expensive, so the limit is applied at the edge where the caller can still
        be told exactly what went wrong.
        """
        if len(value) > MAX_PAYLOAD_KEYS:
            raise ValueError(f"payload must contain at most {MAX_PAYLOAD_KEYS} top-level keys")
        return value


class RetryRequestBody(ApiModel):
    """Optional body for an operator-initiated retry."""

    reason: str | None = Field(
        default=None,
        max_length=MAX_REASON_LENGTH,
        description="Why the retry is being requested. Recorded in the audit trail.",
    )


class CancelRequestBody(ApiModel):
    """Optional body for a cancellation."""

    reason: str | None = Field(
        default=None,
        max_length=MAX_REASON_LENGTH,
        description="Why the request is being cancelled. Recorded in the audit trail.",
    )
