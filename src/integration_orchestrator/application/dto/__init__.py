"""Application-level data transfer objects.

These are plain dataclasses rather than Pydantic models: the application layer
must not depend on a serialisation framework, and the API layer already owns the
job of validating and rendering the wire format.
"""

from integration_orchestrator.application.dto.commands import (
    CancelRequestCommand,
    CreateIntegrationRequestCommand,
    IngestWebhookCommand,
    RetryRequestCommand,
)
from integration_orchestrator.application.dto.queries import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    Cursor,
    IntegrationRequestFilter,
    Page,
)
from integration_orchestrator.application.dto.results import (
    CreateIntegrationRequestResult,
    WebhookIngestionResult,
)

__all__ = [
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "CancelRequestCommand",
    "CreateIntegrationRequestCommand",
    "CreateIntegrationRequestResult",
    "Cursor",
    "IngestWebhookCommand",
    "IntegrationRequestFilter",
    "Page",
    "RetryRequestCommand",
    "WebhookIngestionResult",
]
