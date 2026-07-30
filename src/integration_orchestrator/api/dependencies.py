"""FastAPI dependency providers.

The container is built once during startup and stored on ``app.state``. These
functions expose pieces of it to routes. They are the only place in the API layer
that touches the container directly, so a route signature never has to know how
its use case was constructed.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request

from integration_orchestrator.application.use_cases.cancel_request import (
    CancelIntegrationRequestUseCase,
)
from integration_orchestrator.application.use_cases.create_request import (
    CreateIntegrationRequestUseCase,
)
from integration_orchestrator.application.use_cases.ingest_webhook import IngestWebhookUseCase
from integration_orchestrator.application.use_cases.list_providers import ListProvidersUseCase
from integration_orchestrator.application.use_cases.queries import (
    GetAuditHistoryUseCase,
    GetIntegrationRequestUseCase,
    ListIntegrationRequestsUseCase,
)
from integration_orchestrator.application.use_cases.retry_request import (
    RetryIntegrationRequestUseCase,
)
from integration_orchestrator.composition import Container
from integration_orchestrator.config.settings import Settings
from integration_orchestrator.domain.value_objects import CorrelationId
from integration_orchestrator.infrastructure.security.tokens import TokenVerifier
from integration_orchestrator.observability.metrics import PrometheusMetrics

CORRELATION_HEADER = "X-Correlation-ID"


def get_container(request: Request) -> Container:
    """Return the wired container for this process."""
    container: Container = request.app.state.container
    return container


def get_settings_dep(request: Request) -> Settings:
    return get_container(request).settings


def get_metrics(request: Request) -> PrometheusMetrics:
    return get_container(request).metrics


def get_token_verifier(request: Request) -> TokenVerifier:
    return get_container(request).token_verifier


def get_correlation_id(request: Request) -> CorrelationId:
    """Resolve the correlation id bound to this HTTP request.

    The middleware has already parsed or generated it and stored it on the
    request state, so routes and use cases all see the same value.
    """
    value: str | None = getattr(request.state, "correlation_id", None)
    return CorrelationId.parse(value)


def get_create_request_use_case(request: Request) -> CreateIntegrationRequestUseCase:
    return get_container(request).use_cases.create_request


def get_request_use_case(request: Request) -> GetIntegrationRequestUseCase:
    return get_container(request).use_cases.get_request


def get_list_requests_use_case(request: Request) -> ListIntegrationRequestsUseCase:
    return get_container(request).use_cases.list_requests


def get_audit_history_use_case(request: Request) -> GetAuditHistoryUseCase:
    return get_container(request).use_cases.get_audit_history


def get_retry_use_case(request: Request) -> RetryIntegrationRequestUseCase:
    return get_container(request).use_cases.retry_request


def get_cancel_use_case(request: Request) -> CancelIntegrationRequestUseCase:
    return get_container(request).use_cases.cancel_request


def get_ingest_webhook_use_case(request: Request) -> IngestWebhookUseCase:
    return get_container(request).use_cases.ingest_webhook


def get_list_providers_use_case(request: Request) -> ListProvidersUseCase:
    return get_container(request).use_cases.list_providers


ContainerDep = Annotated[Container, Depends(get_container)]
SettingsDep = Annotated[Settings, Depends(get_settings_dep)]
MetricsDep = Annotated[PrometheusMetrics, Depends(get_metrics)]
CorrelationDep = Annotated[CorrelationId, Depends(get_correlation_id)]
CreateRequestDep = Annotated[CreateIntegrationRequestUseCase, Depends(get_create_request_use_case)]
GetRequestDep = Annotated[GetIntegrationRequestUseCase, Depends(get_request_use_case)]
ListRequestsDep = Annotated[ListIntegrationRequestsUseCase, Depends(get_list_requests_use_case)]
AuditHistoryDep = Annotated[GetAuditHistoryUseCase, Depends(get_audit_history_use_case)]
RetryRequestDep = Annotated[RetryIntegrationRequestUseCase, Depends(get_retry_use_case)]
CancelRequestDep = Annotated[CancelIntegrationRequestUseCase, Depends(get_cancel_use_case)]
IngestWebhookDep = Annotated[IngestWebhookUseCase, Depends(get_ingest_webhook_use_case)]
ListProvidersDep = Annotated[ListProvidersUseCase, Depends(get_list_providers_use_case)]
