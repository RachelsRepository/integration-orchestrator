"""Outbox dead-letter column and claim-index refinement.

Adds ``dead_lettered_at`` so exhausted publication attempts stop retrying and
become visible to operators. Rebuilds the unpublished partial index so
dead-lettered rows are excluded from the claim path the way published rows
already were.

Revision ID: 20260802_a1b2c3d4e5f6
Revises: 8f3c1a9d2b74
Create Date: 2026-08-02 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260802_a1b2c3d4e5f6"
down_revision: str | None = "8f3c1a9d2b74"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "outbox_events",
        sa.Column("dead_lettered_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.drop_index("ix_outbox_events_unpublished", table_name="outbox_events")
    op.create_index(
        "ix_outbox_events_unpublished",
        "outbox_events",
        ["next_attempt_at", "created_at"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL AND dead_lettered_at IS NULL"),
    )
    op.create_index(
        "ix_outbox_events_dead_lettered_at",
        "outbox_events",
        ["dead_lettered_at"],
        unique=False,
        postgresql_where=sa.text("dead_lettered_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_events_dead_lettered_at",
        table_name="outbox_events",
        postgresql_where=sa.text("dead_lettered_at IS NOT NULL"),
    )
    op.drop_index("ix_outbox_events_unpublished", table_name="outbox_events")
    op.create_index(
        "ix_outbox_events_unpublished",
        "outbox_events",
        ["next_attempt_at", "created_at"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_column("outbox_events", "dead_lettered_at")
