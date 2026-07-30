"""The composition root.

This is the only module allowed to know about every layer at once. Everything
else receives its collaborators through constructor arguments, which is what
keeps the domain free of frameworks, keeps the application layer free of
SQLAlchemy and FastAPI, and makes each piece testable by substitution rather than
by patching.

Construction order matters and is not arbitrary:

1. Settings, then observability, so that anything failing later is logged in the
   configured format rather than printed.
2. Shared clients (database engine, Redis, httpx), which own connection pools.
3. Resilience primitives, which depend on Redis and on the policies derived from
   settings.
4. The provider registry, which depends on all of the above and wraps every
   adapter in the resilience decorator.
5. Application services and use cases, which depend only on ports.

Teardown runs in reverse, and every step is best-effort so that one stuck client
cannot prevent the rest from closing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import httpx
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.application.ports.messaging import EventPublisher
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
from integration_orchestrator.config.settings import Environment, Settings, get_settings
from integration_orchestrator.domain.enums import AuditAction, CircuitState
from integration_orchestrator.domain.value_objects import CorrelationId, ProviderSlug
from integration_orchestrator.infrastructure.db.session import (
    create_engine,
    create_session_factory,
    dispose_engine,
)
from integration_orchestrator.infrastructure.db.unit_of_work import SqlUnitOfWorkFactory
from integration_orchestrator.infrastructure.messaging.kafka import KafkaEventPublisher
from integration_orchestrator.infrastructure.messaging.memory import InMemoryEventPublisher
from integration_orchestrator.infrastructure.providers.http import create_http_client
from integration_orchestrator.infrastructure.providers.registry import (
    ProviderRegistry,
    build_provider_registry,
)
from integration_orchestrator.infrastructure.redis.circuit_breaker import RedisCircuitBreaker
from integration_orchestrator.infrastructure.redis.client import (
    KeyBuilder,
    close_redis,
    create_redis,
)
from integration_orchestrator.infrastructure.redis.locks import RedisLockManager
from integration_orchestrator.infrastructure.redis.rate_limiter import RedisRateLimiter
from integration_orchestrator.infrastructure.redis.token_cache import RedisTokenCache
from integration_orchestrator.infrastructure.resilience.bulkhead import ProviderBulkhead
from integration_orchestrator.infrastructure.resilience.policies import ConfiguredPolicyProvider
from integration_orchestrator.infrastructure.security.tokens import TokenVerifier
from integration_orchestrator.infrastructure.system import RandomJitter, SystemClock, UuidGenerator
from integration_orchestrator.observability.logging import configure_logging
from integration_orchestrator.observability.metrics import PrometheusMetrics
from integration_orchestrator.observability.tracing import configure_tracing, shutdown_tracing

logger = logging.getLogger(__name__)

CIRCUIT_EVENT_TYPES = {
    CircuitState.OPEN: "integration.provider.circuit_opened.v1",
    CircuitState.CLOSED: "integration.provider.circuit_closed.v1",
    CircuitState.HALF_OPEN: "integration.provider.circuit_half_opened.v1",
}

CIRCUIT_AUDIT_ACTIONS = {
    CircuitState.OPEN: AuditAction.CIRCUIT_OPENED,
    CircuitState.CLOSED: AuditAction.CIRCUIT_CLOSED,
    CircuitState.HALF_OPEN: AuditAction.CIRCUIT_CLOSED,
}


@dataclass(slots=True)
class UseCases:
    """Every application entry point, already wired."""

    create_request: CreateIntegrationRequestUseCase
    get_request: GetIntegrationRequestUseCase
    list_requests: ListIntegrationRequestsUseCase
    get_audit_history: GetAuditHistoryUseCase
    retry_request: RetryIntegrationRequestUseCase
    cancel_request: CancelIntegrationRequestUseCase
    ingest_webhook: IngestWebhookUseCase
    list_providers: ListProvidersUseCase


@dataclass(slots=True)
class Container:
    """Everything the process needs, with an explicit lifecycle.

    Held as a single object so the API and the worker runner share exactly one
    wiring path. A worker that built its own dependencies would eventually drift
    from the API's, and the resulting mismatch — a different retry policy, a
    different redaction rule — is the kind of bug that only shows up in
    production.
    """

    settings: Settings
    engine: AsyncEngine
    redis: Redis
    http_client: httpx.AsyncClient
    publisher: EventPublisher
    registry: ProviderRegistry
    uow_factory: SqlUnitOfWorkFactory
    journal: WorkflowJournal
    dispatcher: RequestDispatcher
    reconciliation: ReconciliationService
    use_cases: UseCases
    metrics: PrometheusMetrics
    token_verifier: TokenVerifier
    circuit_breaker: RedisCircuitBreaker
    rate_limiter: RedisRateLimiter
    bulkhead: ProviderBulkhead
    clock: SystemClock
    ids: UuidGenerator
    keys: KeyBuilder
    extras: dict[str, Any] = field(default_factory=dict)

    async def startup(self) -> None:
        """Start the components that hold long-lived connections."""
        await self.publisher.start()
        logger.info(
            "container started",
            extra={
                "environment": self.settings.environment.value,
                "providers": self.registry.slugs(),
                "kafka_enabled": self.settings.kafka.enabled,
            },
        )

    async def shutdown(self) -> None:
        """Release every resource, tolerating individual failures."""
        for label, close in (
            ("kafka", self.publisher.stop()),
            ("http", self.http_client.aclose()),
            ("redis", close_redis(self.redis)),
            ("database", dispose_engine(self.engine)),
        ):
            try:
                await close
            except Exception:
                logger.warning("a component failed to close cleanly", extra={"component": label})
        shutdown_tracing()
        logger.info("container stopped")


async def build_container(settings: Settings | None = None) -> Container:
    """Construct the fully wired application container."""
    settings = settings or get_settings()

    configure_logging(
        level=settings.log_level,
        service=settings.service_name,
        environment=settings.environment.value,
        version=settings.service_version,
        console=settings.log_console_renderer,
    )
    configure_tracing(
        settings.observability,
        service_name=settings.service_name,
        service_version=settings.service_version,
        environment=settings.environment.value,
    )

    metrics = PrometheusMetrics()
    clock = SystemClock()
    ids = UuidGenerator()
    jitter = RandomJitter()

    engine = create_engine(settings.database, environment=settings.environment)
    session_factory = create_session_factory(engine)
    uow_factory = SqlUnitOfWorkFactory(session_factory)

    redis = create_redis(settings.redis)
    keys = KeyBuilder(settings.redis.namespace)

    # TLS verification is disabled only against the local sandbox, which is
    # plain HTTP anyway; the flag exists so the setting is explicit rather than
    # an accident of the environment.
    http_client = create_http_client(verify=settings.environment is not Environment.TEST)

    provider_settings = settings.enabled_providers()
    policies = ConfiguredPolicyProvider(provider_settings)
    circuit_breaker = RedisCircuitBreaker(redis, keys, policies)
    rate_limiter = RedisRateLimiter(redis, keys, provider_settings)
    bulkhead = ProviderBulkhead(provider_settings, metrics=metrics)
    token_cache = RedisTokenCache(redis, keys)
    locks = RedisLockManager(redis, keys)

    journal = WorkflowJournal(clock=clock, ids=ids)

    async def on_circuit_change(
        provider: ProviderSlug, previous: CircuitState, new: CircuitState
    ) -> None:
        """Persist and publish a circuit breaker transition.

        Written through the journal like any other state change so the event
        reaches Kafka through the outbox rather than being emitted directly from
        the call path, where a broker outage would surface as a provider error.
        """
        metrics.set_gauge(
            "provider_circuit_state", new.numeric, labels={"provider": provider.value}
        )
        try:
            async with uow_factory() as uow:
                await journal.record_circuit_transition(
                    uow,
                    provider=provider,
                    previous_state=previous.value,
                    new_state=new.value,
                    event_type=CIRCUIT_EVENT_TYPES[new],
                    action=CIRCUIT_AUDIT_ACTIONS[new],
                    correlation_id=CorrelationId.generate(),
                    reason=f"the circuit moved from {previous.value} to {new.value}",
                )
                await uow.commit()
        except Exception:
            logger.exception(
                "could not record a circuit breaker transition",
                extra={"provider": provider.value, "new_state": new.value},
            )

    registry = build_provider_registry(
        settings,
        http_client=http_client,
        token_cache=token_cache,
        locks=locks,
        circuit_breaker=circuit_breaker,
        rate_limiter=rate_limiter,
        bulkhead=bulkhead,
        metrics=metrics,
        on_circuit_change=on_circuit_change,
    )

    dispatcher = RequestDispatcher(
        registry=registry,
        journal=journal,
        policies=policies,
        uow_factory=uow_factory,
        clock=clock,
        jitter=jitter,
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

    use_cases = UseCases(
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
            deferred_retry_seconds=settings.workers.webhook_deferred_retry_seconds,
        ),
        list_providers=ListProvidersUseCase(
            registry=registry,
            circuit_breaker=circuit_breaker,
            concurrency=bulkhead,
            clock=clock,
        ),
    )

    return Container(
        settings=settings,
        engine=engine,
        redis=redis,
        http_client=http_client,
        publisher=_build_publisher(settings),
        registry=registry,
        uow_factory=uow_factory,
        journal=journal,
        dispatcher=dispatcher,
        reconciliation=reconciliation,
        use_cases=use_cases,
        metrics=metrics,
        token_verifier=TokenVerifier(settings.jwt),
        circuit_breaker=circuit_breaker,
        rate_limiter=rate_limiter,
        bulkhead=bulkhead,
        clock=clock,
        ids=ids,
        keys=keys,
    )


def _build_publisher(settings: Settings) -> EventPublisher:
    """Choose the event publisher.

    The in-memory publisher is a test and local-development adapter. It is
    unreachable in a deployed environment because the settings validator refuses
    to construct a production-like configuration with Kafka disabled.
    """
    if settings.kafka.enabled:
        return KafkaEventPublisher(settings.kafka)
    logger.warning(
        "kafka is disabled; events will be published to an in-memory sink",
        extra={"environment": settings.environment.value},
    )
    return InMemoryEventPublisher()


WORKER_ACTOR = Actor.system()
