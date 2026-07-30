"""A whole platform in one process.

The application assembled here is the real one: the real FastAPI app, the real
routers, the real middleware, the real authentication, the real use cases, the
real dispatcher, the real provider adapters and the real workers. Three
boundaries are replaced — PostgreSQL by the in-memory unit of work, Redis by
process-local resilience primitives, and Kafka by a recording publisher — and
provider HTTP is pointed at the sandbox services instead of the internet.

That combination is what makes these tests worth having: they exercise the paths
that only appear when the pieces are wired together, such as a webhook arriving
before the response that created the request it belongs to.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from asgi_lifespan import LifespanManager
from prometheus_client import CollectorRegistry
from pydantic import SecretStr

from integration_orchestrator.api.app import create_app
from integration_orchestrator.application.services.dispatcher import RequestDispatcher
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.application.services.reconciliation import ReconciliationService
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
from integration_orchestrator.composition import Container, UseCases
from integration_orchestrator.config.settings import (
    SANDBOX_COBALT_WEBHOOK_PUBLIC_KEY,
    AuthenticationType,
    Environment,
    KafkaSettings,
    ProviderSandboxSettings,
    ProviderSettings,
    Settings,
)
from integration_orchestrator.infrastructure.providers.registry import build_provider_registry
from integration_orchestrator.infrastructure.providers.sandbox.app import create_sandbox_app
from integration_orchestrator.infrastructure.providers.sandbox.signing import (
    COBALT_CLIENT_ID,
    COBALT_CLIENT_SECRET,
    MERIDIAN_API_KEY,
    MERIDIAN_WEBHOOK_SECRET,
    NORTHSTAR_CLIENT_ID,
    NORTHSTAR_CLIENT_SECRET,
    NORTHSTAR_WEBHOOK_SECRET,
)
from integration_orchestrator.infrastructure.resilience.bulkhead import ProviderBulkhead
from integration_orchestrator.infrastructure.resilience.policies import ConfiguredPolicyProvider
from integration_orchestrator.infrastructure.security.tokens import (
    Scope,
    TokenVerifier,
    issue_local_token,
)
from integration_orchestrator.infrastructure.system import RandomJitter, SystemClock, UuidGenerator
from integration_orchestrator.observability.metrics import PrometheusMetrics
from integration_orchestrator.workers.outbox_publisher import OutboxPublisherWorker
from integration_orchestrator.workers.reconciliation_worker import ReconciliationWorker
from integration_orchestrator.workers.retry_worker import RetryWorker
from integration_orchestrator.workers.webhook_processor import WebhookProcessorWorker
from tests.support.doubles import (
    AllowAllRateLimiter,
    MemoryCircuitBreaker,
    MemoryTokenCache,
    NullLockManager,
    RecordingPublisher,
)
from tests.support.memory_uow import MemoryStore, MemoryUnitOfWorkFactory

SANDBOX_ROOT = "http://sandbox.test"
API_ROOT = "http://orchestrator.test"


def _providers() -> dict[str, ProviderSettings]:
    return {
        "northstar": ProviderSettings(
            display_name="Northstar Connect",
            authentication_type=AuthenticationType.OAUTH2_CLIENT_CREDENTIALS,
            base_url=f"{SANDBOX_ROOT}/northstar",
            oauth_token_url=f"{SANDBOX_ROOT}/northstar/oauth/token",
            client_id=NORTHSTAR_CLIENT_ID,
            client_secret=SecretStr(NORTHSTAR_CLIENT_SECRET),
            webhook_secret=SecretStr(NORTHSTAR_WEBHOOK_SECRET),
            # Small so the retry-exhaustion path is reachable without waiting.
            max_attempts=2,
            backoff_base_seconds=0.01,
        ),
        "meridian": ProviderSettings(
            display_name="Meridian Services",
            authentication_type=AuthenticationType.API_KEY,
            base_url=f"{SANDBOX_ROOT}/meridian",
            api_key=SecretStr(MERIDIAN_API_KEY),
            webhook_secret=SecretStr(MERIDIAN_WEBHOOK_SECRET),
            max_attempts=2,
            backoff_base_seconds=0.01,
        ),
        "cobalt": ProviderSettings(
            display_name="Cobalt Network",
            authentication_type=AuthenticationType.OAUTH2_CLIENT_CREDENTIALS,
            base_url=f"{SANDBOX_ROOT}/cobalt",
            oauth_token_url=f"{SANDBOX_ROOT}/cobalt/oauth/token",
            client_id=COBALT_CLIENT_ID,
            client_secret=SecretStr(COBALT_CLIENT_SECRET),
            webhook_public_key=SANDBOX_COBALT_WEBHOOK_PUBLIC_KEY,
            max_attempts=2,
            backoff_base_seconds=0.01,
        ),
    }


def build_settings() -> Settings:
    return Settings(
        environment=Environment.TEST,
        # The sandbox is driven directly rather than mounted, so the API under
        # test contains nothing that would not exist in a deployment.
        provider_sandbox=ProviderSandboxSettings(enabled=True, mount_in_app=False),
        kafka=KafkaSettings(enabled=False),
        providers=_providers(),
    )


class StubEngine:
    """Stands in for the database engine the readiness probe pings."""

    def __init__(self, *, healthy: bool = True) -> None:
        self.healthy = healthy
        self.sync_engine = None

    def connect(self) -> StubEngine:
        return self

    async def __aenter__(self) -> StubEngine:
        if not self.healthy:
            raise ConnectionError("the database is unreachable")
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def execute(self, statement: object) -> None:
        del statement

    async def dispose(self) -> None:
        return None


class StubRedis:
    """Stands in for Redis in the readiness probe."""

    def __init__(self, *, healthy: bool = True) -> None:
        self.healthy = healthy

    async def ping(self) -> bool:
        if not self.healthy:
            raise ConnectionError("redis is unreachable")
        return True

    async def aclose(self) -> None:
        return None


@dataclass(slots=True)
class Harness:
    """Everything a workflow test needs to drive and inspect the platform."""

    api: httpx.AsyncClient
    sandbox: httpx.AsyncClient
    container: Container
    store: MemoryStore
    publisher: RecordingPublisher
    circuit_breaker: MemoryCircuitBreaker
    settings: Settings

    def auth(self, *roles: str) -> dict[str, str]:
        token = issue_local_token(
            self.settings.jwt, subject="e2e-client", roles=list(roles) or ["operator"]
        )
        return {"Authorization": f"Bearer {token}"}

    async def create_request(
        self,
        *,
        provider: str = "northstar",
        external_reference: str,
        operation_type: str = "resource_provision",
        payload: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        roles: tuple[str, ...] = ("operator",),
    ) -> httpx.Response:
        headers = self.auth(*roles)
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        return await self.api.post(
            "/api/v1/integration-requests",
            json={
                "provider": provider,
                "operation_type": operation_type,
                "external_reference": external_reference,
                "payload": payload or {"resource_type": "database", "region": "eu-west-1"},
            },
            headers=headers,
        )

    async def deliver_webhook(
        self, provider: str, provider_reference: str, *, event_type: str | None = None
    ) -> httpx.Response:
        """Have the sandbox sign a delivery, then post it at the webhook endpoint."""
        params = {"event_type": event_type} if event_type else None
        signed = await self.sandbox.post(
            f"{SANDBOX_ROOT}/_control/{provider}/emit/{provider_reference}", params=params
        )
        signed.raise_for_status()
        delivery = signed.json()
        return await self.api.post(
            f"/webhooks/{provider}",
            content=delivery["body"].encode("utf-8"),
            headers=delivery["headers"],
        )

    async def fetch(self, request_id: str) -> dict[str, Any]:
        response = await self.api.get(
            f"/api/v1/integration-requests/{request_id}", headers=self.auth()
        )
        response.raise_for_status()
        body: dict[str, Any] = response.json()
        return body

    async def audit_actions(self, request_id: str) -> list[str]:
        response = await self.api.get(
            f"/api/v1/integration-requests/{request_id}/audit", headers=self.auth()
        )
        response.raise_for_status()
        return [event["action"] for event in response.json()["events"]]

    def published_event_types(self) -> list[str]:
        return [envelope.event_type for envelope in self.publisher.published]


@pytest.fixture
async def harness() -> AsyncIterator[Harness]:
    settings = build_settings()
    metrics = PrometheusMetrics(CollectorRegistry())
    clock = SystemClock()
    ids = UuidGenerator()

    sandbox_app = create_sandbox_app(callback_base_url=None)
    provider_http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=sandbox_app), base_url=SANDBOX_ROOT
    )

    uow_factory = MemoryUnitOfWorkFactory()
    publisher = RecordingPublisher()
    circuit_breaker = MemoryCircuitBreaker()
    enabled = settings.enabled_providers()
    policies = ConfiguredPolicyProvider(enabled)
    bulkhead = ProviderBulkhead(enabled, metrics=metrics)

    registry = build_provider_registry(
        settings,
        http_client=provider_http,
        token_cache=MemoryTokenCache(),
        locks=NullLockManager(),
        circuit_breaker=circuit_breaker,
        rate_limiter=AllowAllRateLimiter(),
        bulkhead=bulkhead,
        metrics=metrics,
    )

    journal = WorkflowJournal(clock=clock, ids=ids)
    dispatcher = RequestDispatcher(
        registry=registry,
        journal=journal,
        policies=policies,
        uow_factory=uow_factory,
        clock=clock,
        jitter=RandomJitter(),
        metrics=metrics,
    )
    reconciliation = ReconciliationService(
        uow_factory=uow_factory,
        registry=registry,
        journal=journal,
        clock=clock,
        metrics=metrics,
        stale_after_seconds=settings.workers.reconciliation_stale_after_seconds,
        manual_review_after_seconds=settings.workers.reconciliation_manual_review_after_seconds,
    )

    container = Container(
        settings=settings,
        engine=StubEngine(),  # type: ignore[arg-type]
        redis=StubRedis(),  # type: ignore[arg-type]
        http_client=provider_http,
        publisher=publisher,
        registry=registry,
        uow_factory=uow_factory,  # type: ignore[arg-type]
        journal=journal,
        dispatcher=dispatcher,
        reconciliation=reconciliation,
        use_cases=UseCases(
            create_request=CreateIntegrationRequestUseCase(
                uow_factory=uow_factory,
                registry=registry,
                journal=journal,
                dispatcher=dispatcher,
                clock=clock,
                ids=ids,
                metrics=metrics,
            ),
            get_request=GetIntegrationRequestUseCase(uow_factory=uow_factory),
            list_requests=ListIntegrationRequestsUseCase(uow_factory=uow_factory),
            get_audit_history=GetAuditHistoryUseCase(uow_factory=uow_factory),
            retry_request=RetryIntegrationRequestUseCase(
                uow_factory=uow_factory, journal=journal, clock=clock
            ),
            cancel_request=CancelIntegrationRequestUseCase(
                uow_factory=uow_factory, registry=registry, journal=journal, clock=clock
            ),
            ingest_webhook=IngestWebhookUseCase(
                uow_factory=uow_factory,
                registry=registry,
                journal=journal,
                clock=clock,
                ids=ids,
                metrics=metrics,
                deferred_retry_seconds=0.0001,
            ),
            list_providers=ListProvidersUseCase(
                registry=registry,
                circuit_breaker=circuit_breaker,
                concurrency=bulkhead,
                clock=clock,
            ),
        ),
        metrics=metrics,
        token_verifier=TokenVerifier(settings.jwt),
        circuit_breaker=circuit_breaker,  # type: ignore[arg-type]
        rate_limiter=AllowAllRateLimiter(),  # type: ignore[arg-type]
        bulkhead=bulkhead,
        clock=clock,
        ids=ids,
        keys=None,  # type: ignore[arg-type]
    )

    app = create_app(settings=settings, container=container)
    async with (
        LifespanManager(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=API_ROOT) as api,
    ):
        yield Harness(
            api=api,
            sandbox=provider_http,
            container=container,
            store=uow_factory.store,
            publisher=publisher,
            circuit_breaker=circuit_breaker,
            settings=settings,
        )
    await provider_http.aclose()


@pytest.fixture
def outbox_worker(harness: Harness) -> OutboxPublisherWorker:
    return OutboxPublisherWorker(
        uow_factory=harness.container.uow_factory,
        publisher=harness.publisher,
        clock=harness.container.clock,
        metrics=harness.container.metrics,
        settings=harness.settings.workers,
    )


@pytest.fixture
def retry_worker(harness: Harness) -> RetryWorker:
    return RetryWorker(
        uow_factory=harness.container.uow_factory,
        dispatcher=harness.container.dispatcher,
        clock=harness.container.clock,
        metrics=harness.container.metrics,
        settings=harness.settings.workers,
    )


@pytest.fixture
def webhook_worker(harness: Harness) -> WebhookProcessorWorker:
    return WebhookProcessorWorker(
        uow_factory=harness.container.uow_factory,
        ingest=harness.container.use_cases.ingest_webhook,
        journal=harness.container.journal,
        clock=harness.container.clock,
        metrics=harness.container.metrics,
        settings=harness.settings.workers,
    )


@pytest.fixture
def reconciliation_worker(harness: Harness) -> ReconciliationWorker:
    return ReconciliationWorker(
        reconciliation=harness.container.reconciliation,
        metrics=harness.container.metrics,
        settings=harness.settings.workers,
    )


def now() -> datetime:
    return datetime.now(tz=UTC)


SCOPE_BY_ROLE = {
    "viewer": Scope.REQUESTS_READ,
    "integration-client": Scope.REQUESTS_WRITE,
    "operator": Scope.REQUESTS_CANCEL,
}
