"""Async engine and session management."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from integration_orchestrator.config.settings import DatabaseSettings, Environment

logger = logging.getLogger(__name__)


def create_engine(
    settings: DatabaseSettings, *, environment: Environment = Environment.LOCAL
) -> AsyncEngine:
    """Build the async engine.

    A server-side ``statement_timeout`` is set on every connection. Without it a
    single pathological query can hold a connection until the client gives up,
    and under load that is indistinguishable from a total outage. The timeout
    turns that failure into one failed request.

    Tests use ``NullPool`` so each test gets a genuinely fresh connection and
    cannot inherit transaction state from a previous one.
    """
    connect_args: dict[str, Any] = {
        "server_settings": {
            "application_name": "integration-orchestrator",
            "statement_timeout": str(settings.statement_timeout_ms),
        }
    }

    if environment is Environment.TEST:
        return create_async_engine(
            settings.url,
            echo=settings.echo,
            poolclass=NullPool,
            connect_args=connect_args,
        )

    return create_async_engine(
        settings.url,
        echo=settings.echo,
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        pool_timeout=settings.pool_timeout_seconds,
        pool_recycle=settings.pool_recycle_seconds,
        # Verifies a pooled connection is alive before handing it out, which
        # matters behind cloud load balancers that silently drop idle TCP.
        pool_pre_ping=True,
        connect_args=connect_args,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Build the session factory.

    ``expire_on_commit`` is disabled because the repositories map rows to domain
    objects immediately; expiring attributes after commit would trigger lazy
    refreshes on objects nobody is going to read again.
    """
    return async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        autoflush=False,
        class_=AsyncSession,
    )


async def dispose_engine(engine: AsyncEngine) -> None:
    """Close every pooled connection during shutdown."""
    await engine.dispose()
    logger.info("database connection pool disposed")
