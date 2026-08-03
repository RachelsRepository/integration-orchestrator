"""Repository ports.

Repositories are expressed in terms of domain objects only. No SQLAlchemy type,
session, or row ever appears in a signature here, which is what allows the
application layer to be tested against in-memory doubles and the persistence
layer to be replaced without touching a use case.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

from integration_orchestrator.application.dto.queries import (
    IntegrationRequestFilter,
    Page,
)
from integration_orchestrator.domain.entities import IntegrationRequest, WebhookReceipt
from integration_orchestrator.domain.records import (
    AuditEvent,
    IdempotencyRecord,
    OutboxEvent,
)
from integration_orchestrator.domain.value_objects import ProviderSlug


@runtime_checkable
class IntegrationRequestRepository(Protocol):
    """Persistence for the integration request aggregate."""

    async def add(self, request: IntegrationRequest) -> None:
        """Insert a new request."""
        ...

    async def get(self, request_id: UUID) -> IntegrationRequest | None:
        """Load one request by id, or ``None`` when it does not exist."""
        ...

    async def get_for_update(self, request_id: UUID) -> IntegrationRequest | None:
        """Load one request holding a row lock for the current transaction.

        Used wherever a read-modify-write must not interleave with another
        worker, such as applying a webhook while a retry is in flight.
        """
        ...

    async def find_by_provider_reference(
        self, provider: ProviderSlug, provider_reference: str
    ) -> IntegrationRequest | None:
        """Resolve the request a provider webhook refers to."""
        ...

    async def update(self, request: IntegrationRequest) -> None:
        """Persist changes, enforcing the optimistic concurrency version.

        Implementations raise
        :class:`~integration_orchestrator.domain.errors.ConcurrencyConflictError`
        when the stored version no longer matches the one that was loaded.
        """
        ...

    async def list(self, criteria: IntegrationRequestFilter) -> Page[IntegrationRequest]:
        """Return a cursor-paginated page of requests."""
        ...

    async def claim_due_for_retry(
        self, *, now: datetime, limit: int
    ) -> Sequence[IntegrationRequest]:
        """Atomically claim retry-eligible requests for this worker.

        Implementations must make concurrent claims disjoint. Selecting rows and
        then updating them in a separate statement is not sufficient: two workers
        polling simultaneously would both see the same rows and both dispatch.
        """
        ...

    async def find_stale_in_flight(
        self, *, older_than: datetime, limit: int
    ) -> Sequence[IntegrationRequest]:
        """Find dispatching or pending requests that have stopped progressing."""
        ...


@runtime_checkable
class WebhookReceiptRepository(Protocol):
    """Persistence for inbound webhook receipts."""

    async def add(self, receipt: WebhookReceipt) -> None:
        """Insert a receipt."""
        ...

    async def get(self, receipt_id: UUID) -> WebhookReceipt | None:
        """Load one receipt by id."""
        ...

    async def find_by_event_id(
        self, provider: ProviderSlug, event_id: str
    ) -> WebhookReceipt | None:
        """Look up an existing receipt for provider-level deduplication."""
        ...

    async def update(self, receipt: WebhookReceipt) -> None:
        """Persist changes to a receipt."""
        ...

    async def claim_deferred(self, *, now: datetime, limit: int) -> Sequence[WebhookReceipt]:
        """Claim deferred receipts whose retry time has arrived."""
        ...


@runtime_checkable
class AuditRepository(Protocol):
    """Append-only audit storage."""

    async def append(self, event: AuditEvent) -> None:
        """Record one audit event."""
        ...

    async def append_many(self, events: Sequence[AuditEvent]) -> None:
        """Record several audit events in one round trip."""
        ...

    async def list_for_aggregate(
        self, *, aggregate_type: str, aggregate_id: str, limit: int = 200
    ) -> Sequence[AuditEvent]:
        """Return the audit history for one aggregate, oldest first."""
        ...


@runtime_checkable
class OutboxRepository(Protocol):
    """Transactional outbox storage."""

    async def add(self, event: OutboxEvent) -> None:
        """Stage one event for publication."""
        ...

    async def add_many(self, events: Sequence[OutboxEvent]) -> None:
        """Stage several events for publication."""
        ...

    async def claim_unpublished(
        self, *, now: datetime, limit: int, lease_until: datetime
    ) -> Sequence[OutboxEvent]:
        """Atomically claim a batch of events awaiting publication.

        ``lease_until`` is written onto each claimed row before the transaction
        commits. Without that lease, two publisher replicas that poll after the
        claim transaction ends (but before either has marked the rows published)
        would both select the same events and both publish them. At-least-once
        delivery already tolerates that on crash-after-ack; the lease stops it
        from being the steady-state behaviour under normal concurrency.
        """
        ...

    async def mark_published(self, outbox_ids: Sequence[UUID], *, now: datetime) -> None:
        """Mark events as published after the broker acknowledged them.

        Identified by :attr:`OutboxEvent.id`, the row's own key, rather than by
        ``event_id``. The two are different values: ``event_id`` is the stable
        identifier consumers deduplicate on and is deliberately unchanged across
        republication.
        """
        ...

    async def mark_failed(
        self, outbox_id: UUID, *, error: str, next_attempt_at: datetime, now: datetime
    ) -> None:
        """Record a publication failure and schedule another attempt."""
        ...

    async def mark_dead_lettered(self, outbox_id: UUID, *, error: str, now: datetime) -> None:
        """Stop retrying an event that has exhausted its publication budget."""
        ...

    async def count_pending(self) -> int:
        """Return unpublished events that are still eligible for publication."""
        ...

    async def count_dead_lettered(self) -> int:
        """Return events that exhausted retries and need an operator."""
        ...

    async def redrive_dead_lettered(self, outbox_ids: Sequence[UUID], *, now: datetime) -> int:
        """Clear dead-letter marks so claimed events may publish again.

        Returns the number of rows re-armed. Idempotent for ids that are not
        currently dead-lettered. Does not invent new events — redrive is a
        controlled operator action against existing outbox rows.
        """
        ...

    async def purge_published_before(self, cutoff: datetime) -> int:
        """Delete published events older than ``cutoff``. Returns the row count."""
        ...


@runtime_checkable
class IdempotencyRepository(Protocol):
    """Storage for HTTP request idempotency records."""

    async def get(self, key: str) -> IdempotencyRecord | None:
        """Load a previously stored idempotency record."""
        ...

    async def add(self, record: IdempotencyRecord) -> None:
        """Insert a record.

        Implementations rely on a unique constraint so that two concurrent
        requests carrying the same key cannot both succeed. The loser of that
        race is expected to surface a conflict the caller can convert into a
        replay of the winner's result.
        """
        ...
