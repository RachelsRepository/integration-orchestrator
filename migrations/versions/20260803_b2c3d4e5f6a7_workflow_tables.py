"""Workflow tables: definitions, executions, and step executions.

Revises the outbox dead-letter migration. Existing IntegrationRequest tables are
untouched; each forward step optionally links to one request row.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260803_b2c3d4e5f6a7"
down_revision: str | None = "20260802_a1b2c3d4e5f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflow_definitions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("steps", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("immutable", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("name", "version", name="uq_workflow_definitions_name_version"),
    )

    op.create_table(
        "workflow_executions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "definition_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("workflow_definitions.id", name="fk_workflow_executions_definition_id"),
            nullable=False,
        ),
        sa.Column("definition_name", sa.String(length=128), nullable=False),
        sa.Column("definition_version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("correlation_id", sa.String(length=64), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column("input_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("manual_review_reason", sa.Text(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("idempotency_key", name="uq_workflow_executions_idempotency_key"),
    )
    op.create_index("ix_workflow_executions_status", "workflow_executions", ["status"])
    op.create_index(
        "ix_workflow_executions_updated_at", "workflow_executions", ["updated_at"]
    )

    op.create_table(
        "workflow_step_executions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "workflow_execution_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(
                "workflow_executions.id",
                name="fk_workflow_step_executions_workflow_execution_id",
            ),
            nullable=False,
        ),
        sa.Column("step_key", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=40), nullable=False),
        sa.Column("operation_type", sa.String(length=48), nullable=False),
        sa.Column("depends_on", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("compensate_operation", sa.String(length=48), nullable=True),
        sa.Column("wait_for_webhook", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column(
            "integration_request_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(
                "integration_requests.id",
                name="fk_workflow_step_executions_integration_request_id",
            ),
            nullable=True,
        ),
        sa.Column(
            "compensation_request_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(
                "integration_requests.id",
                name="fk_workflow_step_executions_compensation_request_id",
            ),
            nullable=True,
        ),
        sa.Column("input_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("output_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "workflow_execution_id",
            "step_key",
            name="uq_workflow_step_executions_execution_step",
        ),
    )
    op.create_index(
        "ix_workflow_step_executions_status", "workflow_step_executions", ["status"]
    )
    op.create_index(
        "ix_workflow_step_executions_request_id",
        "workflow_step_executions",
        ["integration_request_id"],
    )


def downgrade() -> None:
    op.drop_table("workflow_step_executions")
    op.drop_table("workflow_executions")
    op.drop_table("workflow_definitions")
