"""Unit of work port.

A unit of work is one database transaction exposing the repositories that
participate in it. Every state change in the platform writes the aggregate, its
audit event and its outbox event through the *same* unit of work, so those three
writes share a transaction boundary and cannot diverge.
"""

from __future__ import annotations

from types import TracebackType
from typing import Protocol, runtime_checkable

from integration_orchestrator.application.ports.repositories import (
    AuditRepository,
    IdempotencyRepository,
    IntegrationRequestRepository,
    OutboxRepository,
    WebhookReceiptRepository,
)
from integration_orchestrator.application.ports.workflow_repositories import (
    WorkflowDefinitionRepository,
    WorkflowExecutionRepository,
)


@runtime_checkable
class UnitOfWork(Protocol):
    """A transactional scope over the repositories.

    Used as an async context manager. Leaving the block without an explicit
    :meth:`commit` rolls back, so a forgotten commit loses work loudly instead of
    half-persisting a state change.
    """

    @property
    def requests(self) -> IntegrationRequestRepository: ...

    @property
    def webhooks(self) -> WebhookReceiptRepository: ...

    @property
    def audit(self) -> AuditRepository: ...

    @property
    def outbox(self) -> OutboxRepository: ...

    @property
    def idempotency(self) -> IdempotencyRepository: ...

    @property
    def workflow_definitions(self) -> WorkflowDefinitionRepository: ...

    @property
    def workflow_executions(self) -> WorkflowExecutionRepository: ...

    async def __aenter__(self) -> UnitOfWork: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None:
        """Commit the transaction."""
        ...

    async def rollback(self) -> None:
        """Roll the transaction back."""
        ...

    async def flush(self) -> None:
        """Send pending statements without committing.

        Needed where a unique constraint violation must be observed before the
        transaction ends, such as the idempotency insert race.
        """
        ...


@runtime_checkable
class UnitOfWorkFactory(Protocol):
    """Creates fresh units of work.

    Workers process each item in its own transaction, so they need a factory
    rather than a single long-lived unit of work: one poisoned record must not
    roll back the whole batch.
    """

    def __call__(self) -> UnitOfWork: ...
