"""Fixtures shared by the whole suite.

The object graph built here mirrors the composition root: real use cases, real
services, real domain code, with only the process boundaries (database, provider
HTTP, Redis, Kafka) replaced by doubles. Tests therefore exercise the same
orchestration that runs in production.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime

import pytest

from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.application.services.dispatcher import RequestDispatcher
from integration_orchestrator.application.services.journal import WorkflowJournal
from integration_orchestrator.application.use_cases.cancel_request import (
    CancelIntegrationRequestUseCase,
)
from integration_orchestrator.application.use_cases.create_request import (
    CreateIntegrationRequestUseCase,
)
from integration_orchestrator.application.use_cases.ingest_webhook import IngestWebhookUseCase
from integration_orchestrator.application.use_cases.retry_request import (
    RetryIntegrationRequestUseCase,
)
from integration_orchestrator.domain.enums import ActorType
from integration_orchestrator.domain.policies import CircuitBreakerPolicy, RetryPolicy
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.system import (
    FixedJitter,
    FrozenClock,
    SequentialIdentifierGenerator,
)
from integration_orchestrator.observability.correlation import (
    set_correlation_id,
    set_integration_request_id,
    set_request_id,
)
from tests.support.builders import REFERENCE_TIME
from tests.support.doubles import FakeGateway, FakeRegistry, RecordingMetrics
from tests.support.memory_uow import MemoryStore, MemoryUnitOfWorkFactory


class StaticPolicyProvider:
    """Applies one retry and one circuit policy to every provider."""

    def __init__(
        self,
        *,
        retry: RetryPolicy | None = None,
        circuit: CircuitBreakerPolicy | None = None,
    ) -> None:
        self._retry = retry or RetryPolicy(
            max_attempts=3, base_seconds=2.0, multiplier=2.0, max_seconds=60.0
        )
        self._circuit = circuit or CircuitBreakerPolicy(failure_threshold=3, open_seconds=30.0)

    def retry_policy(self, provider: ProviderSlug) -> RetryPolicy:
        del provider
        return self._retry

    def circuit_policy(self, provider: ProviderSlug) -> CircuitBreakerPolicy:
        del provider
        return self._circuit


def _clear_correlation() -> None:
    set_correlation_id(None)
    set_request_id(None)
    set_integration_request_id(None)


@pytest.fixture(autouse=True)
def _reset_correlation() -> Iterator[None]:
    """Keep correlation context from leaking between tests."""
    _clear_correlation()
    yield
    _clear_correlation()


@pytest.fixture
def reference_time() -> datetime:
    return REFERENCE_TIME


@pytest.fixture
def clock(reference_time: datetime) -> FrozenClock:
    return FrozenClock(reference_time)


@pytest.fixture
def ids() -> SequentialIdentifierGenerator:
    return SequentialIdentifierGenerator()


@pytest.fixture
def jitter() -> FixedJitter:
    return FixedJitter(0.5)


@pytest.fixture
def metrics() -> RecordingMetrics:
    return RecordingMetrics()


@pytest.fixture
def uow_factory() -> MemoryUnitOfWorkFactory:
    return MemoryUnitOfWorkFactory()


@pytest.fixture
def store(uow_factory: MemoryUnitOfWorkFactory) -> MemoryStore:
    return uow_factory.store


@pytest.fixture
def policies() -> StaticPolicyProvider:
    return StaticPolicyProvider()


@pytest.fixture
def gateway() -> FakeGateway:
    return FakeGateway()


@pytest.fixture
def registry(gateway: FakeGateway) -> FakeRegistry:
    return FakeRegistry(gateway)


@pytest.fixture
def journal(clock: FrozenClock, ids: SequentialIdentifierGenerator) -> WorkflowJournal:
    return WorkflowJournal(clock=clock, ids=ids)


@pytest.fixture
def dispatcher(
    registry: FakeRegistry,
    journal: WorkflowJournal,
    policies: StaticPolicyProvider,
    uow_factory: MemoryUnitOfWorkFactory,
    clock: FrozenClock,
    jitter: FixedJitter,
    metrics: RecordingMetrics,
) -> RequestDispatcher:
    return RequestDispatcher(
        registry=registry,
        journal=journal,
        policies=policies,
        uow_factory=uow_factory,
        clock=clock,
        jitter=jitter,
        metrics=metrics,
    )


@pytest.fixture
def create_use_case(
    uow_factory: MemoryUnitOfWorkFactory,
    registry: FakeRegistry,
    journal: WorkflowJournal,
    dispatcher: RequestDispatcher,
    clock: FrozenClock,
    ids: SequentialIdentifierGenerator,
    metrics: RecordingMetrics,
) -> CreateIntegrationRequestUseCase:
    return CreateIntegrationRequestUseCase(
        uow_factory=uow_factory,
        registry=registry,
        journal=journal,
        dispatcher=dispatcher,
        clock=clock,
        ids=ids,
        metrics=metrics,
    )


@pytest.fixture
def ingest_use_case(
    uow_factory: MemoryUnitOfWorkFactory,
    registry: FakeRegistry,
    journal: WorkflowJournal,
    clock: FrozenClock,
    ids: SequentialIdentifierGenerator,
    metrics: RecordingMetrics,
) -> IngestWebhookUseCase:
    return IngestWebhookUseCase(
        uow_factory=uow_factory,
        registry=registry,
        journal=journal,
        clock=clock,
        ids=ids,
        metrics=metrics,
        deferred_retry_seconds=5.0,
    )


@pytest.fixture
def retry_use_case(
    uow_factory: MemoryUnitOfWorkFactory,
    journal: WorkflowJournal,
    clock: FrozenClock,
) -> RetryIntegrationRequestUseCase:
    return RetryIntegrationRequestUseCase(
        uow_factory=uow_factory,
        journal=journal,
        clock=clock,
    )


@pytest.fixture
def cancel_use_case(
    uow_factory: MemoryUnitOfWorkFactory,
    registry: FakeRegistry,
    journal: WorkflowJournal,
    clock: FrozenClock,
) -> CancelIntegrationRequestUseCase:
    return CancelIntegrationRequestUseCase(
        uow_factory=uow_factory,
        registry=registry,
        journal=journal,
        clock=clock,
    )


@pytest.fixture
def api_actor() -> Actor:
    return Actor(type=ActorType.API_CLIENT, id="operator@example.test")
