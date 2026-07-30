"""SQLAlchemy table definitions.

Design notes that the migrations mirror:

*Status columns are constrained strings, not PostgreSQL native enums.* Native
enums require a DDL migration and an exclusive lock to add a value, which turns
"we support a new status" into a deployment event. A ``VARCHAR`` with a check
constraint gives the same protection against garbage data while allowing the
constraint to be replaced in a single fast statement.

*Payloads are JSONB rather than JSON.* JSONB is stored decomposed, so it can be
indexed and queried, and it strips insignificant whitespace and duplicate keys.
The cost is slightly slower writes, which is irrelevant next to the read
flexibility operators need when diagnosing a mapping problem.

*Timestamps are all ``TIMESTAMP WITH TIME ZONE``.* Storing naive timestamps in a
system that spans regions and receives provider timestamps in arbitrary offsets
is a data-corruption bug waiting for a daylight-saving transition.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from integration_orchestrator.domain.enums import (
    ActorType,
    AuditAction,
    OperationType,
    RequestStatus,
    WebhookProcessingStatus,
)

# A deterministic naming convention means Alembic autogenerate produces stable
# constraint names instead of database-assigned ones, which makes downgrades and
# constraint drops predictable.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Declarative base carrying the shared metadata."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def _enum_column(enum_type: type, name: str, length: int = 32) -> Enum:
    """Build a constrained-string column type from a Python enum.

    ``values_callable`` stores the enum *value* rather than its member name, so
    the database holds ``"retry_scheduled"`` and not ``"RETRY_SCHEDULED"``. That
    keeps rows readable in a psql session and matches what the API emits.
    """
    return Enum(
        enum_type,
        name=name,
        native_enum=False,
        length=length,
        values_callable=lambda enum: [member.value for member in enum],
        validate_strings=True,
    )


class IntegrationRequestModel(Base):
    """The ``integration_requests`` table."""

    __tablename__ = "integration_requests"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    provider: Mapped[str] = mapped_column(String(40), nullable=False)
    operation_type: Mapped[OperationType] = mapped_column(
        _enum_column(OperationType, "operation_type", length=48), nullable=False
    )
    external_reference: Mapped[str] = mapped_column(String(255), nullable=False)
    normalized_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    provider_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    provider_reference: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[RequestStatus] = mapped_column(
        _enum_column(RequestStatus, "request_status"), nullable=False
    )
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    correlation_id: Mapped[str] = mapped_column(String(128), nullable=False)
    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error_category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    manual_review_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")

    __table_args__ = (
        # Idempotency is enforced by the database, not by application logic. Two
        # concurrent creations with the same key cannot both succeed.
        UniqueConstraint("idempotency_key", name="uq_integration_requests_idempotency_key"),
        # A provider reference identifies exactly one operation at one provider,
        # so webhook correlation can never match two rows.
        UniqueConstraint(
            "provider",
            "provider_reference",
            name="uq_integration_requests_provider_provider_reference",
        ),
        CheckConstraint("attempt_count >= 0", name="attempt_count_non_negative"),
        CheckConstraint("version >= 0", name="version_non_negative"),
        # Drives the retry worker's claim query.
        Index("ix_integration_requests_status_next_retry_at", "status", "next_retry_at"),
        # Drives the default listing order and the provider filter.
        Index("ix_integration_requests_provider_created_at", "provider", "created_at"),
        # Callers look their own records up by their own identifier.
        Index("ix_integration_requests_external_reference", "external_reference"),
        # Keyset pagination seeks on this exact ordering.
        Index("ix_integration_requests_created_at_id", "created_at", "id"),
        # Reconciliation scans in-flight requests by staleness.
        Index("ix_integration_requests_status_updated_at", "status", "updated_at"),
    )


class WebhookReceiptModel(Base):
    """The ``webhook_receipts`` table."""

    __tablename__ = "webhook_receipts"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    provider: Mapped[str] = mapped_column(String(40), nullable=False)
    event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    provider_reference: Mapped[str | None] = mapped_column(String(255), nullable=True)
    signature_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    processing_status: Mapped[WebhookProcessingStatus] = mapped_column(
        _enum_column(WebhookProcessingStatus, "webhook_processing_status"), nullable=False
    )
    integration_request_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("integration_requests.id", ondelete="SET NULL"),
        nullable=True,
    )
    correlation_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # Provider-level webhook deduplication.
        UniqueConstraint("provider", "event_id", name="uq_webhook_receipts_provider_event_id"),
        CheckConstraint("attempt_count >= 0", name="attempt_count_non_negative"),
        Index(
            "ix_webhook_receipts_processing_status_received_at",
            "processing_status",
            "received_at",
        ),
        # Drives the deferred-webhook resolver.
        Index(
            "ix_webhook_receipts_processing_status_next_attempt_at",
            "processing_status",
            "next_attempt_at",
        ),
        Index("ix_webhook_receipts_integration_request_id", "integration_request_id"),
    )


class AuditEventModel(Base):
    """The ``audit_events`` table. Append-only by convention and by code path."""

    __tablename__ = "audit_events"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(128), nullable=False)
    action: Mapped[AuditAction] = mapped_column(
        _enum_column(AuditAction, "audit_action", length=64), nullable=False
    )
    actor: Mapped[ActorType] = mapped_column(
        _enum_column(ActorType, "actor_type", length=48), nullable=False
    )
    actor_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    previous_state: Mapped[str | None] = mapped_column(String(48), nullable=True)
    new_state: Mapped[str | None] = mapped_column(String(48), nullable=True)
    correlation_id: Mapped[str] = mapped_column(String(128), nullable=False)
    event_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict
    )
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        # The audit endpoint reads one aggregate's history in chronological order.
        Index(
            "ix_audit_events_aggregate_type_aggregate_id_occurred_at",
            "aggregate_type",
            "aggregate_id",
            "occurred_at",
        ),
        Index("ix_audit_events_correlation_id", "correlation_id"),
    )


class OutboxEventModel(Base):
    """The ``outbox_events`` table."""

    __tablename__ = "outbox_events"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    event_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    event_version: Mapped[int] = mapped_column(Integer, nullable=False)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(128), nullable=False)
    causation_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    partition_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        # Stable event ids are what let consumers deduplicate under at-least-once
        # delivery, so duplicates must be impossible on the producing side too.
        UniqueConstraint("event_id", name="uq_outbox_events_event_id"),
        CheckConstraint("event_version >= 1", name="event_version_positive"),
        CheckConstraint("attempt_count >= 0", name="attempt_count_non_negative"),
        # A partial index over unpublished rows only. The published rows are the
        # overwhelming majority and the publisher never looks at them, so
        # indexing them would grow the index without bound for no benefit.
        Index(
            "ix_outbox_events_unpublished",
            "next_attempt_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index("ix_outbox_events_published_at_created_at", "published_at", "created_at"),
        Index("ix_outbox_events_aggregate_id", "aggregate_id"),
    )


class IdempotencyRecordModel(Base):
    """The ``idempotency_records`` table."""

    __tablename__ = "idempotency_records"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    request_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("integration_requests.id", ondelete="CASCADE"),
        nullable=False,
    )
    response_status: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_idempotency_records_request_id", "request_id"),
        Index("ix_idempotency_records_expires_at", "expires_at"),
    )
