"""Ports: the interfaces the application layer depends on.

Every external capability the platform needs is declared here as a
:class:`typing.Protocol`. Implementations live in ``infrastructure`` and are
bound in the composition root, so no use case has a compile-time dependency on a
database driver, cache client, broker, or HTTP library.
"""

from integration_orchestrator.application.ports.messaging import EventPublisher
from integration_orchestrator.application.ports.provider_gateway import (
    ProviderGateway,
    ProviderRegistry,
)
from integration_orchestrator.application.ports.repositories import (
    AuditRepository,
    IdempotencyRepository,
    IntegrationRequestRepository,
    OutboxRepository,
    WebhookReceiptRepository,
)
from integration_orchestrator.application.ports.resilience import (
    CircuitBreaker,
    ConcurrencyLimiter,
    DistributedLock,
    LockManager,
    PolicyProvider,
    RateLimiter,
)
from integration_orchestrator.application.ports.security import CachedToken, TokenCache
from integration_orchestrator.application.ports.system import (
    Clock,
    IdentifierGenerator,
    JitterSource,
)
from integration_orchestrator.application.ports.unit_of_work import (
    UnitOfWork,
    UnitOfWorkFactory,
)

__all__ = [
    "AuditRepository",
    "CachedToken",
    "CircuitBreaker",
    "Clock",
    "ConcurrencyLimiter",
    "DistributedLock",
    "EventPublisher",
    "IdempotencyRepository",
    "IdentifierGenerator",
    "IntegrationRequestRepository",
    "JitterSource",
    "LockManager",
    "OutboxRepository",
    "PolicyProvider",
    "ProviderGateway",
    "ProviderRegistry",
    "RateLimiter",
    "TokenCache",
    "UnitOfWork",
    "UnitOfWorkFactory",
    "WebhookReceiptRepository",
]
