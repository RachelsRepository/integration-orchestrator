"""Add workflow claim leases, owner subjects, and status checks.

Revision ID: 20260803_c3d4e5f6a7b8
Revises: 20260803_b2c3d4e5f6a7
Create Date: 2026-08-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260803_c3d4e5f6a7b8"
down_revision: str | None = "20260803_b2c3d4e5f6a7"
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
)

_STEP_STATUSES = (
    "pending",
    "ready",
    "running",
    "waiting",
    "retry_scheduled",
    "succeeded",
    "failed",
    "skipped",
    "compensating",
    "compensated",
    "cancelled",
    "dead_lettered",
)


def upgrade() -> None:
    op.add_column(
        "integration_requests",
        sa.Column("owner_subject", sa.String(length=128), nullable=True),
    )
    op.create_index(
        "ix_integration_requests_owner_subject",
        "integration_requests",
        ["owner_subject"],
    )

    op.add_column(
        "workflow_executions",
        sa.Column("owner_subject", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "workflow_executions",
        sa.Column("claim_lease_until", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_workflow_executions_owner_subject",
        "workflow_executions",
        ["owner_subject"],
    )
    op.create_index(
        "ix_workflow_executions_claim_lease_until",
        "workflow_executions",
        ["claim_lease_until"],
    )

    statuses = ", ".join(f"'{s}'" for s in _WORKFLOW_STATUSES)
    op.create_check_constraint(
        "ck_workflow_executions_status",
        "workflow_executions",
        f"status IN ({statuses})",
    )
    step_statuses = ", ".join(f"'{s}'" for s in _STEP_STATUSES)
    op.create_check_constraint(
        "ck_workflow_step_executions_status",
        "workflow_step_executions",
        f"status IN ({step_statuses})",
    )


def downgrade() -> None:
    op.drop_constraint("ck_workflow_step_executions_status", "workflow_step_executions", type_="check")
    op.drop_constraint("ck_workflow_executions_status", "workflow_executions", type_="check")
    op.drop_index("ix_workflow_executions_claim_lease_until", table_name="workflow_executions")
    op.drop_index("ix_workflow_executions_owner_subject", table_name="workflow_executions")
    op.drop_column("workflow_executions", "claim_lease_until")
    op.drop_column("workflow_executions", "owner_subject")
    op.drop_index("ix_integration_requests_owner_subject", table_name="integration_requests")
    op.drop_column("integration_requests", "owner_subject")
