"""SQLAlchemy repository implementations.

Two patterns recur here and are worth calling out.

*Claiming work safely.* Both the retry worker and the outbox publisher need to
take a batch of rows that no other worker will also take. Selecting rows and then
updating them is not enough: two workers polling at the same moment read the same
rows and both act on them, so a request is dispatched twice and an event is
published twice. Every claim query therefore uses ``FOR UPDATE SKIP LOCKED``,
which makes PostgreSQL hand each worker a disjoint set and skip rows another
transaction already holds.

*Optimistic concurrency.* Updates carry the version the entity was loaded with
and fail when the stored version has moved. The row lock taken by
``get_for_update`` already prevents most interleaving, but the version check also
catches the case where a caller loaded without a lock and wrote back stale data.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import CursorResult, and_, delete, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from integration_orchestrator.application.dto.queries import (
    Cursor,
    IntegrationRequestFilter,
    Page,
)
from integration_orchestrator.domain.entities import IntegrationRequest, WebhookReceipt
from integration_orchestrator.domain.enums import RequestStatus, WebhookProcessingStatus
from integration_orchestrator.domain.errors import ConcurrencyConflictError, ConflictError
from integration_orchestrator.domain.records import (
    AuditEvent,
    IdempotencyRecord,
    OutboxEvent,
)
from integration_orchestrator.domain.state_machine import RECONCILABLE_STATUSES
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.db import mappers
from integration_orchestrator.infrastructure.db.models import (
    AuditEventModel,
    IdempotencyRecordModel,
    IntegrationRequestModel,
    OutboxEventModel,
    WebhookReceiptModel,
)


class SqlIntegrationRequestRepository:
    """PostgreSQL-backed integration request repository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        # Version tracking for optimistic concurrency. Populated on load and
        # advanced after each successful update so several transitions can be
        # applied within one transaction.
        self._loaded_versions: dict[UUID, int] = {}

    async def add(self, request: IntegrationRequest) -> None:
        self._session.add(mappers.request_to_row(request))
        self._loaded_versions[request.id] = request.version

    async def get(self, request_id: UUID) -> IntegrationRequest | None:
        row = await self._session.get(IntegrationRequestModel, request_id)
        if row is None:
            return None
        await self._session.refresh(row)
        return self._track(mappers.request_to_domain(row))

    async def get_for_update(self, request_id: UUID) -> IntegrationRequest | None:
        result = await self._session.execute(
            select(IntegrationRequestModel)
            .where(IntegrationRequestModel.id == request_id)
            .with_for_update()
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return self._track(mappers.request_to_domain(row))

    async def find_by_provider_reference(
        self, provider: ProviderSlug, provider_reference: str
    ) -> IntegrationRequest | None:
        result = await self._session.execute(
            select(IntegrationRequestModel)
            .where(
                IntegrationRequestModel.provider == provider.value,
                IntegrationRequestModel.provider_reference == provider_reference,
            )
            .with_for_update()
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return self._track(mappers.request_to_domain(row))

    async def update(self, request: IntegrationRequest) -> None:
        expected = self._loaded_versions.get(request.id, request.version - 1)
        result = await self._session.execute(
            update(IntegrationRequestModel)
            .where(
                IntegrationRequestModel.id == request.id,
                IntegrationRequestModel.version == expected,
            )
            .values(**mappers.request_update_values(request))
        )
        if _rowcount(result) == 0:
            raise ConcurrencyConflictError(
                str(request.id), correlation_id=request.correlation_id.value
            )
        self._loaded_versions[request.id] = request.version

    async def list(self, criteria: IntegrationRequestFilter) -> Page[IntegrationRequest]:
        statement = select(IntegrationRequestModel)
        statement = _apply_request_filters(statement, criteria)

        if criteria.cursor is not None:
            # Keyset seek: strictly "older than" the cursor position in the
            # (created_at DESC, id DESC) ordering.
            statement = statement.where(
                or_(
                    IntegrationRequestModel.created_at < criteria.cursor.created_at,
                    and_(
                        IntegrationRequestModel.created_at == criteria.cursor.created_at,
                        IntegrationRequestModel.id < criteria.cursor.request_id,
                    ),
                )
            )

        statement = statement.order_by(
            IntegrationRequestModel.created_at.desc(), IntegrationRequestModel.id.desc()
        ).limit(criteria.limit + 1)

        rows = list((await self._session.execute(statement)).scalars().all())
        has_more = len(rows) > criteria.limit
        page_rows = rows[: criteria.limit]
        items = [self._track(mappers.request_to_domain(row)) for row in page_rows]

        next_cursor: str | None = None
        if has_more and page_rows:
            last = page_rows[-1]
            next_cursor = Cursor(
                created_at=mappers.as_utc(last.created_at), request_id=last.id
            ).encode()
        return Page(items=items, next_cursor=next_cursor)

    async def claim_due_for_retry(
        self, *, now: datetime, limit: int
    ) -> Sequence[IntegrationRequest]:
        result = await self._session.execute(
            select(IntegrationRequestModel)
            .where(
                IntegrationRequestModel.status == RequestStatus.RETRY_SCHEDULED,
                IntegrationRequestModel.next_retry_at.is_not(None),
                IntegrationRequestModel.next_retry_at <= now,
            )
            .order_by(IntegrationRequestModel.next_retry_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [self._track(mappers.request_to_domain(row)) for row in result.scalars().all()]

    async def find_stale_in_flight(
        self, *, older_than: datetime, limit: int
    ) -> Sequence[IntegrationRequest]:
        result = await self._session.execute(
            select(IntegrationRequestModel)
            .where(
                IntegrationRequestModel.status.in_(list(RECONCILABLE_STATUSES)),
                IntegrationRequestModel.updated_at <= older_than,
            )
            .order_by(IntegrationRequestModel.updated_at.asc())
            .limit(limit)
        )
        return [self._track(mappers.request_to_domain(row)) for row in result.scalars().all()]

    def _track(self, request: IntegrationRequest) -> IntegrationRequest:
        self._loaded_versions[request.id] = request.version
        return request


def _apply_request_filters(statement: Any, criteria: IntegrationRequestFilter) -> Any:
    if criteria.provider is not None:
        statement = statement.where(IntegrationRequestModel.provider == criteria.provider.value)
    if criteria.statuses:
        statement = statement.where(IntegrationRequestModel.status.in_(list(criteria.statuses)))
    if criteria.operation_type is not None:
        statement = statement.where(
            IntegrationRequestModel.operation_type == criteria.operation_type
        )
    if criteria.external_reference is not None:
        statement = statement.where(
            IntegrationRequestModel.external_reference == criteria.external_reference
        )
    if criteria.created_after is not None:
        statement = statement.where(IntegrationRequestModel.created_at >= criteria.created_after)
    if criteria.created_before is not None:
        statement = statement.where(IntegrationRequestModel.created_at <= criteria.created_before)
    return statement


class SqlWebhookReceiptRepository:
    """PostgreSQL-backed webhook receipt repository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, receipt: WebhookReceipt) -> None:
        self._session.add(mappers.receipt_to_row(receipt))

    async def get(self, receipt_id: UUID) -> WebhookReceipt | None:
        row = await self._session.get(WebhookReceiptModel, receipt_id)
        return None if row is None else mappers.receipt_to_domain(row)

    async def find_by_event_id(
        self, provider: ProviderSlug, event_id: str
    ) -> WebhookReceipt | None:
        result = await self._session.execute(
            select(WebhookReceiptModel).where(
                WebhookReceiptModel.provider == provider.value,
                WebhookReceiptModel.event_id == event_id,
            )
        )
        row = result.scalar_one_or_none()
        return None if row is None else mappers.receipt_to_domain(row)

    async def update(self, receipt: WebhookReceipt) -> None:
        await self._session.execute(
            update(WebhookReceiptModel)
            .where(WebhookReceiptModel.id == receipt.id)
            .values(**mappers.receipt_update_values(receipt))
        )

    async def claim_deferred(self, *, now: datetime, limit: int) -> Sequence[WebhookReceipt]:
        result = await self._session.execute(
            select(WebhookReceiptModel)
            .where(
                WebhookReceiptModel.processing_status == WebhookProcessingStatus.DEFERRED,
                WebhookReceiptModel.next_attempt_at.is_not(None),
                WebhookReceiptModel.next_attempt_at <= now,
            )
            .order_by(WebhookReceiptModel.next_attempt_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [mappers.receipt_to_domain(row) for row in result.scalars().all()]


class SqlAuditRepository:
    """Append-only audit repository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(self, event: AuditEvent) -> None:
        self._session.add(mappers.audit_to_row(event))

    async def append_many(self, events: Sequence[AuditEvent]) -> None:
        self._session.add_all([mappers.audit_to_row(event) for event in events])

    async def list_for_aggregate(
        self, *, aggregate_type: str, aggregate_id: str, limit: int = 200
    ) -> Sequence[AuditEvent]:
        result = await self._session.execute(
            select(AuditEventModel)
            .where(
                AuditEventModel.aggregate_type == aggregate_type,
                AuditEventModel.aggregate_id == aggregate_id,
            )
            .order_by(AuditEventModel.occurred_at.asc(), AuditEventModel.id.asc())
            .limit(limit)
        )
        return [mappers.audit_to_domain(row) for row in result.scalars().all()]


class SqlOutboxRepository:
    """Transactional outbox repository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, event: OutboxEvent) -> None:
        self._session.add(mappers.outbox_to_row(event))

    async def add_many(self, events: Sequence[OutboxEvent]) -> None:
        self._session.add_all([mappers.outbox_to_row(event) for event in events])

    async def claim_unpublished(
        self, *, now: datetime, limit: int, lease_until: datetime
    ) -> Sequence[OutboxEvent]:
        result = await self._session.execute(
            select(OutboxEventModel)
            .where(
                OutboxEventModel.published_at.is_(None),
                OutboxEventModel.dead_lettered_at.is_(None),
                or_(
                    OutboxEventModel.next_attempt_at.is_(None),
                    OutboxEventModel.next_attempt_at <= now,
                ),
            )
            # Oldest first, so ordering within an aggregate is preserved on the
            # happy path. Strict global ordering is not promised; consumers are
            # required to tolerate reordering as well as redelivery.
            .order_by(OutboxEventModel.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = list(result.scalars().all())
        if not rows:
            return []
        # Push next_attempt_at into the future so another publisher that polls
        # after this transaction commits cannot reclaim the same rows while we
        # are still talking to the broker.
        await self._session.execute(
            update(OutboxEventModel)
            .where(OutboxEventModel.id.in_([row.id for row in rows]))
            .values(next_attempt_at=lease_until)
        )
        events = [mappers.outbox_to_domain(row) for row in rows]
        for event in events:
            event.next_attempt_at = lease_until
        return events

    async def mark_published(self, outbox_ids: Sequence[UUID], *, now: datetime) -> None:
        if not outbox_ids:
            return
        await self._session.execute(
            update(OutboxEventModel)
            .where(OutboxEventModel.id.in_(list(outbox_ids)))
            .values(published_at=now, last_error=None, next_attempt_at=None)
        )

    async def mark_failed(
        self, outbox_id: UUID, *, error: str, next_attempt_at: datetime, now: datetime
    ) -> None:
        del now
        await self._session.execute(
            update(OutboxEventModel)
            .where(OutboxEventModel.id == outbox_id)
            .values(
                attempt_count=OutboxEventModel.attempt_count + 1,
                last_error=error[:1000],
                next_attempt_at=next_attempt_at,
            )
        )

    async def mark_dead_lettered(self, outbox_id: UUID, *, error: str, now: datetime) -> None:
        await self._session.execute(
            update(OutboxEventModel)
            .where(OutboxEventModel.id == outbox_id)
            .values(
                attempt_count=OutboxEventModel.attempt_count + 1,
                last_error=error[:1000],
                next_attempt_at=None,
                dead_lettered_at=now,
            )
        )

    async def count_pending(self) -> int:
        result = await self._session.execute(
            select(func.count())
            .select_from(OutboxEventModel)
            .where(
                OutboxEventModel.published_at.is_(None),
                OutboxEventModel.dead_lettered_at.is_(None),
            )
        )
        return int(result.scalar_one())

    async def count_dead_lettered(self) -> int:
        result = await self._session.execute(
            select(func.count())
            .select_from(OutboxEventModel)
            .where(OutboxEventModel.dead_lettered_at.is_not(None))
        )
        return int(result.scalar_one())

    async def redrive_dead_lettered(self, outbox_ids: Sequence[UUID], *, now: datetime) -> int:
        if not outbox_ids:
            return 0
        result = await self._session.execute(
            update(OutboxEventModel)
            .where(
                OutboxEventModel.id.in_(list(outbox_ids)),
                OutboxEventModel.dead_lettered_at.is_not(None),
                OutboxEventModel.published_at.is_(None),
            )
            .values(
                dead_lettered_at=None,
                next_attempt_at=now,
                last_error=None,
            )
        )
        return _rowcount(result)

    async def purge_published_before(self, cutoff: datetime) -> int:
        """Delete published events older than ``cutoff``.

        The outbox is a queue, not an archive: audit rows are the durable
        history. Without pruning the table grows forever and the partial index
        on unpublished rows stops being the cheap lookup it was designed to be.
        """
        result = await self._session.execute(
            delete(OutboxEventModel).where(
                OutboxEventModel.published_at.is_not(None),
                OutboxEventModel.published_at < cutoff,
            )
        )
        return _rowcount(result)


class SqlIdempotencyRepository:
    """Idempotency record repository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, key: str) -> IdempotencyRecord | None:
        row = await self._session.get(IdempotencyRecordModel, key)
        return None if row is None else mappers.idempotency_to_domain(row)

    async def add(self, record: IdempotencyRecord) -> None:
        self._session.add(mappers.idempotency_to_row(record))


def _rowcount(result: object) -> int:
    """Read ``rowcount`` from a SQLAlchemy result without angering the type checker.

    ``Result.rowcount`` exists on cursor results but is typed inconsistently
    across SQLAlchemy versions; narrowing here keeps the call sites clean.
    """
    if isinstance(result, CursorResult):
        return int(result.rowcount or 0)
    return int(getattr(result, "rowcount", 0) or 0)


def translate_integrity_error(error: IntegrityError) -> ConflictError | IntegrityError:
    """Turn a unique-constraint violation into a domain conflict.

    Unique-constraint violations are how the platform arbitrates races, so they
    are an expected control-flow signal rather than an internal error. Foreign
    key and check violations are programming bugs and must not be silently
    converted into "retry the race" — that masked a real flush-ordering bug for
    idempotency inserts.
    """
    detail = str(getattr(error, "orig", error))
    lowered = detail.lower()
    if "unique" not in lowered and "duplicate key" not in lowered:
        return error
    constraint = _constraint_name(detail)
    return ConflictError(
        "the operation conflicts with an existing record",
        retryable=False,
        metadata={"constraint": constraint} if constraint else {},
    )


def _constraint_name(detail: str) -> str | None:
    marker = 'unique constraint "'
    lowered = detail.lower()
    index = lowered.find(marker)
    if index == -1:
        return None
    start = index + len(marker)
    end = detail.find('"', start)
    return detail[start:end] if end != -1 else None
