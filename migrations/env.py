"""Alembic environment.

The database URL comes from application settings rather than from ``alembic.ini``,
so a migration can never be run against a different database than the service
reads. The synchronous driver is used deliberately: migrations are a one-shot
administrative task, and an async engine here would add a moving part for no gain.
"""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from integration_orchestrator.config.settings import get_settings
from integration_orchestrator.infrastructure.db.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

settings = get_settings()
config.set_main_option("sqlalchemy.url", settings.database.sync_url)


def run_migrations_offline() -> None:
    """Emit SQL to stdout without connecting.

    Used to produce a reviewable script for environments where a human applies
    DDL through a change process rather than letting a deploy job do it.
    """
    context.configure(
        url=settings.database.sync_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Apply migrations against a live connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            # Every migration runs in one transaction, so a failure half way
            # through leaves the schema exactly as it was.
            transaction_per_migration=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
