"""An in-memory unit of work.

This double is deliberately more than a dictionary. It reproduces the three
behaviours the application layer actually depends on:

* **Transaction isolation.** Reads return copies and writes are staged, so work
  that is never committed is never visible. A use case that forgets to commit
  fails here exactly as it would against PostgreSQL.
* **Unique constraint violations on flush.** The idempotency key and the
  ``(provider, provider_event_id)`` pair raise :class:`ConflictError` on
  ``flush()``, which is what the creation and webhook race paths are written
  against.
* **Optimistic concurrency.** Updates carry the version the entity was loaded
  with and fail when the stored version has moved on.

Without those, the tests would pass against a store that cannot express the
failure modes the production code spends most of its effort handling.
"""

from __future__ import annotations

import copy
from collections.abc import Callable, Sequence
from datetime import datetime
from types import TracebackType
from uuid import UUID

from integration_orchestrator.application.dto.queries import (
    Cursor,
    IntegrationRequestFilter,
    Page,
)
from integration_orchestrator.domain.entities import IntegrationRequest, WebhookReceipt
from integration_orchestrator.domain.enums import RequestStatus, WebhookProcessingStatus
from integration_orchestrator.domain.errors import ConcurrencyConflictError, ConflictError
from integration_orchestrator.domain.records import AuditEvent, IdempotencyRecord, OutboxEvent
from integration_orchestrator.domain.state_machine import RECONCILABLE_STATUSES
from integration_orchestrator.domain.value_objects import ProviderSlug


class MemoryStore:
    """The committed state shared by every unit of work in a test."""

    def __init__(self) -> None:
        self.requests: dict[UUID, IntegrationRequest] = {}
        self.receipts: dict[UUID, WebhookReceipt] = {}
        self.audit: list[AuditEvent] = []
        self.outbox: dict[UUID, OutboxEvent] = {}
        self.idempotency: dict[str, IdempotencyRecord] = {}
        self.commits = 0
        self.rollbacks = 0

    # -- convenience accessors used by assertions ---------------------------

    def audit_actions(self, aggregate_id: str | UUID | None = None) -> list[str]:
        wanted = str(aggregate_id) if aggregate_id is not None else None
        return [
            event.action.value
            for event in self.audit
            if wanted is None or event.aggregate_id == wanted
        ]

    def outbox_types(self) -> list[str]:
        return [event.event_type for event in self.outbox.values()]

    def unpublished(self) -> list[OutboxEvent]:
        return [event for event in self.outbox.values() if event.published_at is None]


class _Staged:
    def __init__(self) -> None:
        self.requests: dict[UUID, IntegrationRequest] = {}
        self.receipts: dict[UUID, WebhookReceipt] = {}
        self.audit: list[AuditEvent] = []
        self.outbox: dict[UUID, OutboxEvent] = {}
        self.idempotency: dict[str, IdempotencyRecord] = {}
        self.outbox_updates: list[tuple[UUID, dict[str, object]]] = []

    @property
    def is_empty(self) -> bool:
        return not (
            self.requests
            or self.receipts
            or self.audit
            or self.outbox
            or self.idempotency
            or self.outbox_updates
        )


class MemoryIntegrationRequestRepository:
    def __init__(self, store: MemoryStore, staged: _Staged) -> None:
        self._store = store
        self._staged = staged
        self._loaded_versions: dict[UUID, int] = {}

    async def add(self, request: IntegrationRequest) -> None:
        # Staged as a snapshot, not the live entity. The database stores the row
        # as it was when the statement ran; keeping the caller's object here
        # would let later in-memory mutations reach the store without an update.
        self._staged.requests[request.id] = copy.deepcopy(request)
        self._loaded_versions[request.id] = request.version

    async def get(self, request_id: UUID) -> IntegrationRequest | None:
        return self._read(request_id)

    async def get_for_update(self, request_id: UUID) -> IntegrationRequest | None:
        return self._read(request_id)

    async def find_by_provider_reference(
        self, provider: ProviderSlug, provider_reference: str
    ) -> IntegrationRequest | None:
        for request in self._visible().values():
            if request.provider == provider and request.provider_reference == provider_reference:
                return self._track(copy.deepcopy(request))
        return None

    async def update(self, request: IntegrationRequest) -> None:
        current = self._staged.requests.get(request.id) or self._store.requests.get(request.id)
        expected = self._loaded_versions.get(request.id, request.version - 1)
        if current is None or current.version != expected:
            raise ConcurrencyConflictError(
                str(request.id), correlation_id=request.correlation_id.value
            )
        self._staged.requests[request.id] = copy.deepcopy(request)
        self._loaded_versions[request.id] = request.version

    async def list(self, criteria: IntegrationRequestFilter) -> Page[IntegrationRequest]:
        rows = sorted(
            (row for row in self._visible().values() if _matches(row, criteria)),
            key=lambda row: (row.created_at, row.id),
            reverse=True,
        )
        if criteria.cursor is not None:
            anchor = (criteria.cursor.created_at, criteria.cursor.request_id)
            rows = [row for row in rows if (row.created_at, row.id) < anchor]

        page = rows[: criteria.limit]
        has_more = len(rows) > criteria.limit
        next_cursor = (
            Cursor(created_at=page[-1].created_at, request_id=page[-1].id).encode()
            if has_more and page
            else None
        )
        return Page(
            items=[self._track(copy.deepcopy(row)) for row in page], next_cursor=next_cursor
        )

    async def claim_due_for_retry(
        self, *, now: datetime, limit: int
    ) -> Sequence[IntegrationRequest]:
        due = [
            row
            for row in self._visible().values()
            if row.status is RequestStatus.RETRY_SCHEDULED
            and row.next_retry_at is not None
            and row.next_retry_at <= now
        ]
        due.sort(key=lambda row: row.next_retry_at or now)
        return [self._track(copy.deepcopy(row)) for row in due[:limit]]

    async def find_stale_in_flight(
        self, *, older_than: datetime, limit: int
    ) -> Sequence[IntegrationRequest]:
        stale = [
            row
            for row in self._visible().values()
            if row.status in RECONCILABLE_STATUSES and row.updated_at <= older_than
        ]
        stale.sort(key=lambda row: row.updated_at)
        return [self._track(copy.deepcopy(row)) for row in stale[:limit]]

    # -- internals ----------------------------------------------------------

    def _visible(self) -> dict[UUID, IntegrationRequest]:
        return {**self._store.requests, **self._staged.requests}

    def _read(self, request_id: UUID) -> IntegrationRequest | None:
        row = self._visible().get(request_id)
        return None if row is None else self._track(copy.deepcopy(row))

    def _track(self, request: IntegrationRequest) -> IntegrationRequest:
        self._loaded_versions[request.id] = request.version
        return request


def _matches(row: IntegrationRequest, criteria: IntegrationRequestFilter) -> bool:
    if criteria.provider is not None and row.provider != criteria.provider:
        return False
    if criteria.statuses and row.status not in criteria.statuses:
        return False
    if criteria.operation_type is not None and row.operation_type != criteria.operation_type:
        return False
    if (
        criteria.external_reference is not None
        and row.external_reference.value != criteria.external_reference
    ):
        return False
    if criteria.created_after is not None and row.created_at < criteria.created_after:
        return False
    return not (criteria.created_before is not None and row.created_at > criteria.created_before)


class MemoryWebhookReceiptRepository:
    def __init__(self, store: MemoryStore, staged: _Staged) -> None:
        self._store = store
        self._staged = staged

    async def add(self, receipt: WebhookReceipt) -> None:
        self._staged.receipts[receipt.id] = copy.deepcopy(receipt)

    async def get(self, receipt_id: UUID) -> WebhookReceipt | None:
        row = self._visible().get(receipt_id)
        return None if row is None else copy.deepcopy(row)

    async def find_by_event_id(
        self, provider: ProviderSlug, event_id: str
    ) -> WebhookReceipt | None:
        for row in self._visible().values():
            if row.provider == provider and row.event_id == event_id:
                return copy.deepcopy(row)
        return None

    async def update(self, receipt: WebhookReceipt) -> None:
        self._staged.receipts[receipt.id] = copy.deepcopy(receipt)

    async def claim_deferred(self, *, now: datetime, limit: int) -> Sequence[WebhookReceipt]:
        due = [
            row
            for row in self._visible().values()
            if row.processing_status is WebhookProcessingStatus.DEFERRED
            and row.next_attempt_at is not None
            and row.next_attempt_at <= now
        ]
        due.sort(key=lambda row: row.next_attempt_at or now)
        return [copy.deepcopy(row) for row in due[:limit]]

    def _visible(self) -> dict[UUID, WebhookReceipt]:
        return {**self._store.receipts, **self._staged.receipts}


class MemoryAuditRepository:
    def __init__(self, store: MemoryStore, staged: _Staged) -> None:
        self._store = store
        self._staged = staged

    async def append(self, event: AuditEvent) -> None:
        self._staged.audit.append(event)

    async def append_many(self, events: Sequence[AuditEvent]) -> None:
        self._staged.audit.extend(events)

    async def list_for_aggregate(
        self, *, aggregate_type: str, aggregate_id: str, limit: int = 200
    ) -> Sequence[AuditEvent]:
        rows = [
            event
            for event in [*self._store.audit, *self._staged.audit]
            if event.aggregate_type == aggregate_type and event.aggregate_id == aggregate_id
        ]
        rows.sort(key=lambda event: (event.occurred_at, str(event.id)))
        return rows[:limit]


class MemoryOutboxRepository:
    def __init__(self, store: MemoryStore, staged: _Staged) -> None:
        self._store = store
        self._staged = staged

    async def add(self, event: OutboxEvent) -> None:
        self._staged.outbox[event.id] = copy.deepcopy(event)

    async def add_many(self, events: Sequence[OutboxEvent]) -> None:
        for event in events:
            self._staged.outbox[event.id] = copy.deepcopy(event)

    async def claim_unpublished(self, *, now: datetime, limit: int) -> Sequence[OutboxEvent]:
        rows = [
            event
            for event in self._visible().values()
            if event.published_at is None
            and (event.next_attempt_at is None or event.next_attempt_at <= now)
        ]
        rows.sort(key=lambda event: event.created_at)
        return [copy.deepcopy(event) for event in rows[:limit]]

    async def mark_published(self, outbox_ids: Sequence[UUID], *, now: datetime) -> None:
        for outbox_id in outbox_ids:
            self._staged.outbox_updates.append(
                (outbox_id, {"published_at": now, "last_error": None, "next_attempt_at": None})
            )

    async def mark_failed(
        self, outbox_id: UUID, *, error: str, next_attempt_at: datetime, now: datetime
    ) -> None:
        del now
        self._staged.outbox_updates.append(
            (
                outbox_id,
                {
                    "attempt_count_increment": 1,
                    "last_error": error[:1000],
                    "next_attempt_at": next_attempt_at,
                },
            )
        )

    async def count_pending(self) -> int:
        return sum(1 for event in self._visible().values() if event.published_at is None)

    def _visible(self) -> dict[UUID, OutboxEvent]:
        return {**self._store.outbox, **self._staged.outbox}


class MemoryIdempotencyRepository:
    def __init__(self, store: MemoryStore, staged: _Staged) -> None:
        self._store = store
        self._staged = staged

    async def get(self, key: str) -> IdempotencyRecord | None:
        record = self._staged.idempotency.get(key) or self._store.idempotency.get(key)
        return None if record is None else copy.deepcopy(record)

    async def add(self, record: IdempotencyRecord) -> None:
        self._staged.idempotency[record.key] = record


class MemoryUnitOfWork:
    """One in-memory transaction."""

    def __init__(
        self, store: MemoryStore, *, before_flush: Callable[[], None] | None = None
    ) -> None:
        self._store = store
        self._before_flush = before_flush
        self._staged = _Staged()
        self.requests = MemoryIntegrationRequestRepository(store, self._staged)
        self.webhooks = MemoryWebhookReceiptRepository(store, self._staged)
        self.audit = MemoryAuditRepository(store, self._staged)
        self.outbox = MemoryOutboxRepository(store, self._staged)
        self.idempotency = MemoryIdempotencyRepository(store, self._staged)

    async def __aenter__(self) -> MemoryUnitOfWork:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.rollback()

    async def flush(self) -> None:
        """Apply the unique constraints the real schema enforces."""
        if self._before_flush is not None:
            # Lets a test land a concurrent writer's commit in the window between
            # this transaction's read and its insert, which is the only way to
            # reach the code that handles losing a unique-constraint race.
            self._before_flush()

        for key in self._staged.idempotency:
            if key in self._store.idempotency:
                raise ConflictError("duplicate idempotency key", metadata={"idempotency_key": key})

        committed_events = {
            (receipt.provider.value, receipt.event_id) for receipt in self._store.receipts.values()
        }
        for receipt in self._staged.receipts.values():
            if receipt.id in self._store.receipts:
                continue
            if (receipt.provider.value, receipt.event_id) in committed_events:
                raise ConflictError(
                    "duplicate webhook event",
                    metadata={"provider_event_id": receipt.event_id},
                )

    async def commit(self) -> None:
        await self.flush()
        self._store.requests.update(self._staged.requests)
        self._store.receipts.update(self._staged.receipts)
        self._store.audit.extend(self._staged.audit)
        self._store.outbox.update(self._staged.outbox)
        self._store.idempotency.update(self._staged.idempotency)
        for outbox_id, values in self._staged.outbox_updates:
            event = self._store.outbox.get(outbox_id)
            if event is None:
                continue
            if values.get("attempt_count_increment"):
                event.attempt_count += int(values["attempt_count_increment"])  # type: ignore[arg-type]
            if "published_at" in values:
                event.published_at = values["published_at"]  # type: ignore[assignment]
            if "last_error" in values:
                event.last_error = values["last_error"]  # type: ignore[assignment]
            if "next_attempt_at" in values:
                event.next_attempt_at = values["next_attempt_at"]  # type: ignore[assignment]
        self._store.commits += 1
        self._reset()

    async def rollback(self) -> None:
        if not self._staged.is_empty:
            self._store.rollbacks += 1
        self._reset()

    def _reset(self) -> None:
        self._staged = _Staged()
        self._rebind()

    def _rebind(self) -> None:
        self.requests._staged = self._staged
        self.webhooks._staged = self._staged
        self.audit._staged = self._staged
        self.outbox._staged = self._staged
        self.idempotency._staged = self._staged


class MemoryUnitOfWorkFactory:
    """Creates units of work over one shared store."""

    def __init__(self, store: MemoryStore | None = None) -> None:
        self.store = store or MemoryStore()
        #: Invoked once, immediately before the next flush. Used to simulate a
        #: concurrent writer committing mid-transaction.
        self.before_flush: Callable[[], None] | None = None

    def __call__(self) -> MemoryUnitOfWork:
        hook = self.before_flush
        self.before_flush = None
        return MemoryUnitOfWork(self.store, before_flush=hook)
