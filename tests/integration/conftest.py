"""Fixtures for tests that need real infrastructure.

These tests are deliberately not hermetic. The behaviour they cover —
``FOR UPDATE SKIP LOCKED`` claiming, partial unique indexes, JSONB round trips,
timezone handling and Redis Lua atomicity — is behaviour of PostgreSQL and Redis
themselves. A fake would only prove the fake agrees with itself.

A backing service is found in one of two ways:

* ``TEST_DATABASE_URL`` / ``TEST_REDIS_URL`` when one is already running, which
  is how CI does it with service containers;
* otherwise a container started through testcontainers.

If neither is available the tests skip with a message that says what to start,
rather than failing and looking like a code defect.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from integration_orchestrator.config.settings import DatabaseSettings, Environment, RedisSettings
from integration_orchestrator.infrastructure.db.models import Base
from integration_orchestrator.infrastructure.db.session import (
    create_engine,
    create_session_factory,
    dispose_engine,
)
from integration_orchestrator.infrastructure.db.unit_of_work import SqlUnitOfWorkFactory

POSTGRES_IMAGE = "postgres:16-alpine"
REDIS_IMAGE = "redis:7-alpine"

SKIP_DATABASE = (
    "no PostgreSQL available: set TEST_DATABASE_URL, or start Docker so "
    "testcontainers can provide one (make up)"
)
SKIP_REDIS = (
    "no Redis available: set TEST_REDIS_URL, or start Docker so testcontainers "
    "can provide one (make up)"
)


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    """An async PostgreSQL URL pointing at a usable, empty-schema database."""
    supplied = os.getenv("TEST_DATABASE_URL")
    if supplied:
        yield _as_async_url(supplied)
        return

    container = _start(
        lambda: __import__(
            "testcontainers.postgres", fromlist=["PostgresContainer"]
        ).PostgresContainer(POSTGRES_IMAGE),
        SKIP_DATABASE,
    )
    try:
        yield _as_async_url(container.get_connection_url())
    finally:
        with suppress(Exception):
            container.stop()


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    supplied = os.getenv("TEST_REDIS_URL")
    if supplied:
        yield supplied
        return

    container = _start(
        lambda: __import__("testcontainers.redis", fromlist=["RedisContainer"]).RedisContainer(
            REDIS_IMAGE
        ),
        SKIP_REDIS,
    )
    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"
    finally:
        with suppress(Exception):
            container.stop()


@pytest_asyncio.fixture
async def engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    """A fresh schema per test.

    The schema is created from the ORM metadata rather than by running the
    migrations, so a broken migration fails in the test that is specifically
    about migrations instead of taking the whole suite down with it. Dropping
    and recreating per test costs a few milliseconds and removes every
    cross-test ordering dependency.
    """
    settings = DatabaseSettings(url=database_url)
    active = create_engine(settings, environment=Environment.TEST)
    async with active.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield active
    finally:
        await dispose_engine(active)


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest.fixture
def uow_factory(session_factory: async_sessionmaker[AsyncSession]) -> SqlUnitOfWorkFactory:
    return SqlUnitOfWorkFactory(session_factory)


@pytest_asyncio.fixture
async def redis_client(redis_url: str) -> AsyncIterator[object]:
    """A Redis client with an empty keyspace."""
    from integration_orchestrator.infrastructure.redis.client import close_redis, create_redis

    client = create_redis(RedisSettings(url=redis_url))
    try:
        await client.flushdb()
    except Exception as exc:
        await close_redis(client)
        pytest.skip(f"{SKIP_REDIS} ({type(exc).__name__})")
    try:
        yield client
    finally:
        await close_redis(client)


def _as_async_url(url: str) -> str:
    """Normalise any PostgreSQL URL onto the asyncpg driver.

    testcontainers hands back a psycopg2 URL and CI service containers use a
    bare ``postgresql://``; the application only speaks asyncpg.
    """
    for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+asyncpg://" + url[len(prefix) :]
    return url


def _start(factory, skip_reason: str):  # type: ignore[no-untyped-def]
    """Start a container, turning any unavailability into a skip."""
    try:
        container = factory()
        container.start()
    except Exception as exc:
        pytest.skip(f"{skip_reason} ({type(exc).__name__}: {exc})")
    return container
