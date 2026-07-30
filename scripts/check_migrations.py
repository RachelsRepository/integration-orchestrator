"""Fail if the ORM models have drifted from the migrations.

Model drift is silent and expensive: the code works locally against a database
someone built via ``create_all``, and the deployment fails because the column
was never migrated. This script asks Alembic to compare the metadata against a
freshly migrated database and exits non-zero if there is any difference.

It needs a real PostgreSQL instance, because the schema uses JSONB, partial
indexes and timezone-aware timestamps that no other backend reproduces faithfully.
"""

from __future__ import annotations

import sys

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import create_engine

from integration_orchestrator.config.settings import get_settings
from integration_orchestrator.infrastructure.db.models import Base


def main() -> int:
    settings = get_settings()
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", settings.database.sync_url)

    try:
        command.upgrade(config, "head")
    except Exception as exc:
        print(f"could not apply migrations: {exc}", file=sys.stderr)
        return 2

    engine = create_engine(settings.database.sync_url)
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(
                connection,
                opts={"compare_type": True, "compare_server_default": True},
            )
            differences = compare_metadata(context, Base.metadata)
    finally:
        engine.dispose()

    if not differences:
        print("the migrations are in sync with the models")
        return 0

    print("the models have drifted from the migrations:", file=sys.stderr)
    for difference in differences:
        print(f"  {difference}", file=sys.stderr)
    print(
        "\nRun 'make migration m=\"describe the change\"' and review the result.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
