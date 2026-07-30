"""SQLAlchemy unit of work.

One instance wraps one session and one transaction. Repositories are created
eagerly and share the session, which is what guarantees that an aggregate
update, its audit row and its outbox row land in the same transaction.

Exiting the context without committing rolls back. That is deliberate: a
forgotten commit should lose the work loudly rather than leave a half-applied
state change that looks committed to the caller.
"""

from __future__ import annotations

import logging
from types import TracebackType

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from integration_orchestrator.infrastructure.db.repositories import (
    SqlAuditRepository,
    SqlIdempotencyRepository,
    SqlIntegrationRequestRepository,
    SqlOutboxRepository,
    SqlWebhookReceiptRepository,
    translate_integrity_error,
)

logger = logging.getLogger(__name__)


class SqlUnitOfWork:
    """A transactional scope over the SQLAlchemy repositories."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None
        self._committed = False
        self._requests: SqlIntegrationRequestRepository | None = None
        self._webhooks: SqlWebhookReceiptRepository | None = None
        self._audit: SqlAuditRepository | None = None
        self._outbox: SqlOutboxRepository | None = None
        self._idempotency: SqlIdempotencyRepository | None = None

    async def __aenter__(self) -> SqlUnitOfWork:
        self._session = self._session_factory()
        self._committed = False
        self._requests = SqlIntegrationRequestRepository(self._session)
        self._webhooks = SqlWebhookReceiptRepository(self._session)
        self._audit = SqlAuditRepository(self._session)
        self._outbox = SqlOutboxRepository(self._session)
        self._idempotency = SqlIdempotencyRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        session = self._session
        if session is None:  # pragma: no cover - __aenter__ always runs first
            return
        try:
            if not self._committed:
                await session.rollback()
        finally:
            await session.close()
            self._session = None

    # -- repositories -------------------------------------------------------

    @property
    def requests(self) -> SqlIntegrationRequestRepository:
        return _require(self._requests, "requests")

    @property
    def webhooks(self) -> SqlWebhookReceiptRepository:
        return _require(self._webhooks, "webhooks")

    @property
    def audit(self) -> SqlAuditRepository:
        return _require(self._audit, "audit")

    @property
    def outbox(self) -> SqlOutboxRepository:
        return _require(self._outbox, "outbox")

    @property
    def idempotency(self) -> SqlIdempotencyRepository:
        return _require(self._idempotency, "idempotency")

    @property
    def session(self) -> AsyncSession:
        """Direct session access, used only by infrastructure-level helpers."""
        return _require(self._session, "session")

    # -- transaction control -------------------------------------------------

    async def commit(self) -> None:
        session = _require(self._session, "session")
        try:
            await session.commit()
        except IntegrityError as error:
            await session.rollback()
            raise translate_integrity_error(error) from error
        self._committed = True

    async def rollback(self) -> None:
        session = _require(self._session, "session")
        await session.rollback()

    async def flush(self) -> None:
        """Emit pending statements so constraint violations surface now.

        The idempotency insert relies on this: the unique-constraint violation
        has to be observable while the transaction is still open, so the loser of
        the race can roll back and replay the winner's result instead of failing
        the caller.
        """
        session = _require(self._session, "session")
        try:
            await session.flush()
        except IntegrityError as error:
            raise translate_integrity_error(error) from error


def _require[T](value: T | None, name: str) -> T:
    if value is None:
        raise RuntimeError(
            f"the unit of work must be entered before accessing '{name}'; "
            "use 'async with uow_factory() as uow'"
        )
    return value


class SqlUnitOfWorkFactory:
    """Creates units of work bound to a shared session factory."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    def __call__(self) -> SqlUnitOfWork:
        return SqlUnitOfWork(self._session_factory)
