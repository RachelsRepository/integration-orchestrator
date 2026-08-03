"""Add workflow deadline and cancel metadata; allow timed_out status.

Revision ID: 20260803_d4e5f6a7b8c9
Revises: 20260803_c3d4e5f6a7b8
Create Date: 2026-08-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260803_d4e5f6a7b8c9"
down_revision: str | None = "20260803_c3d4e5f6a7b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_WORKFLOW_STATUSES = (
    "created",
    "queued",
    "running",
    "waiting",
    "retry_scheduled",
    "compensating",
    "compensated",
    "succeeded",
    "failed",
    "cancelled",
    "manual_review",
    "dead_lettered",
    "timed_out",
)


def upgrade() -> None:
    op.add_column(
        "workflow_executions",
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "workflow_executions",
        sa.Column("cancel_reason", sa.Text(), nullable=True),
    )
    op.add_column(
        "workflow_executions",
        sa.Column("deadline_processed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_workflow_executions_deadline_at",
        "workflow_executions",
        ["deadline_at"],
    )

    op.drop_constraint("ck_workflow_executions_status", "workflow_executions", type_="check")
    statuses = ", ".join(f"'{s}'" for s in _WORKFLOW_STATUSES)
    op.create_check_constraint(
        "ck_workflow_executions_status",
        "workflow_executions",
        f"status IN ({statuses})",
    )


def downgrade() -> None:
    op.drop_constraint("ck_workflow_executions_status", "workflow_executions", type_="check")
    prior = (
        "created",
        "queued",
        "running",
        "waiting",
        "retry_scheduled",
        "compensating",
        "compensated",
        "succeeded",
        "failed",
        "cancelled",
        "manual_review",
        "dead_lettered",
    )
    statuses = ", ".join(f"'{s}'" for s in prior)
    op.create_check_constraint(
        "ck_workflow_executions_status",
        "workflow_executions",
        f"status IN ({statuses})",
    )
    op.drop_index("ix_workflow_executions_deadline_at", table_name="workflow_executions")
    op.drop_column("workflow_executions", "deadline_processed_at")
    op.drop_column("workflow_executions", "cancel_reason")
    op.drop_column("workflow_executions", "deadline_at")
