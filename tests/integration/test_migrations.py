"""The migrations are the schema.

Creating tables from the ORM metadata is fine for repository tests, but a
deployment applies the Alembic revisions. If those two diverge, the suite can
pass against a schema that production never builds. This test applies the
migrations from scratch and asks Alembic whether anything is still missing.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, text

from integration_orchestrator.config.settings import reset_settings_cache
from integration_orchestrator.infrastructure.db.models import Base

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def migrated_database_url(database_url: str) -> str:
    """Apply every revision to a blank database and return a sync URL."""
    sync_url = _as_sync_url(database_url)
    engine = create_engine(sync_url)
    try:
        with engine.begin() as connection:
            # Wipe anything left by a previous session-scoped fixture. The
            # migration history itself is what this test is about.
            connection.execute(text("DROP SCHEMA public CASCADE"))
            connection.execute(text("CREATE SCHEMA public"))
    finally:
        engine.dispose()

    previous = os.environ.get("DATABASE__URL")
    os.environ["DATABASE__URL"] = database_url
    reset_settings_cache()
    try:
        config = Config(str(ROOT / "alembic.ini"))
        config.set_main_option("sqlalchemy.url", sync_url)
        command.upgrade(config, "head")
    finally:
        if previous is None:
            os.environ.pop("DATABASE__URL", None)
        else:
            os.environ["DATABASE__URL"] = previous
        reset_settings_cache()
    return sync_url


def test_the_migrations_produce_every_table_the_models_define(
    migrated_database_url: str,
) -> None:
    engine = create_engine(migrated_database_url)
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(
                connection,
                opts={"compare_type": True, "compare_server_default": True},
            )
            differences = compare_metadata(context, Base.metadata)
    finally:
        engine.dispose()

    assert differences == [], "the models have drifted from the migrations:\n" + "\n".join(
        f"  {difference}" for difference in differences
    )


def test_the_partial_outbox_index_only_covers_unpublished_rows(
    migrated_database_url: str,
) -> None:
    """An index over every row would grow forever for no benefit."""
    engine = create_engine(migrated_database_url)
    try:
        with engine.connect() as connection:
            definition = connection.execute(
                text(
                    """
                    SELECT indexdef
                    FROM pg_indexes
                    WHERE tablename = 'outbox_events'
                      AND indexname = 'ix_outbox_events_unpublished'
                    """
                )
            ).scalar_one()
    finally:
        engine.dispose()

    assert "WHERE" in definition
    assert "published_at IS NULL" in definition


def _as_sync_url(url: str) -> str:
    for prefix in ("postgresql+asyncpg://", "postgresql+psycopg://"):
        if url.startswith(prefix):
            return "postgresql://" + url[len(prefix) :]
    return url
