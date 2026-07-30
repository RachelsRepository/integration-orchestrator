"""Repository behaviour against real PostgreSQL.

The interesting cases here are the ones an in-memory fake cannot honestly
reproduce: constraint arbitration between concurrent transactions, row-level
locking, JSONB round trips and timezone-aware timestamps.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from integration_orchestrator.application.dto.queries import Cursor, IntegrationRequestFilter
from integration_orchestrator.domain.enums import ActorType, AuditAction, RequestStatus
from integration_orchestrator.domain.errors import ConcurrencyConflictError, ConflictError
from integration_orchestrator.domain.records import AuditEvent, IdempotencyRecord
from integration_orchestrator.domain.value_objects import CorrelationId
from integration_orchestrator.infrastructure.db.unit_of_work import SqlUnitOfWorkFactory
from tests.support.builders import MERIDIAN, NORTHSTAR, make_outbox_event, make_request

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

NOW = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)


async def test_a_request_survives_a_round_trip_unchanged(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    request = make_request(
        payload={"quantity": 3, "nested": {"note": "unicode: ✓", "flag": True}},
        provider_reference="prv-1",
    )

    async with uow_factory() as uow:
        await uow.requests.add(request)
        await uow.commit()

    async with uow_factory() as uow:
        loaded = await uow.requests.get(request.id)

    assert loaded is not None
    assert loaded.normalized_payload == request.normalized_payload
    assert loaded.external_reference == request.external_reference
    assert loaded.created_at.tzinfo is not None
    assert loaded.created_at == request.created_at


async def test_two_requests_cannot_share_an_idempotency_key(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    """The uniqueness that makes idempotency safe is the database's job."""
    async with uow_factory() as uow:
        await uow.requests.add(make_request(idempotency_key="key-1"))
        await uow.commit()

    with pytest.raises(ConflictError):
        async with uow_factory() as uow:
            await uow.requests.add(make_request(idempotency_key="key-1"))
            await uow.commit()


async def test_two_requests_cannot_share_a_provider_reference(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    """Webhook correlation would be ambiguous if they could."""
    async with uow_factory() as uow:
        await uow.requests.add(make_request(provider_reference="prv-9"))
        await uow.commit()

    with pytest.raises(ConflictError):
        async with uow_factory() as uow:
            await uow.requests.add(make_request(provider_reference="prv-9"))
            await uow.commit()


async def test_the_same_reference_at_a_different_provider_is_allowed(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    """Two providers numbering their operations from 1 is not a conflict."""
    async with uow_factory() as uow:
        await uow.requests.add(make_request(provider=NORTHSTAR, provider_reference="op-1"))
        await uow.requests.add(make_request(provider=MERIDIAN, provider_reference="op-1"))
        await uow.commit()

    async with uow_factory() as uow:
        found = await uow.requests.find_by_provider_reference(MERIDIAN, "op-1")

    assert found is not None
    assert found.provider == MERIDIAN


async def test_a_stale_write_is_refused(uow_factory: SqlUnitOfWorkFactory) -> None:
    """Optimistic concurrency catches a writer that never took the lock."""
    request = make_request()
    async with uow_factory() as uow:
        await uow.requests.add(request)
        await uow.commit()

    async with uow_factory() as first, uow_factory() as second:
        loaded = await first.requests.get(request.id)
        assert loaded is not None
        stale = await second.requests.get(request.id)
        assert stale is not None

        loaded.begin_validation(now=NOW)
        await first.requests.update(loaded)
        await first.commit()

        stale.begin_validation(now=NOW)
        with pytest.raises(ConcurrencyConflictError):
            await second.requests.update(stale)


async def test_two_workers_never_claim_the_same_retry(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    """`FOR UPDATE SKIP LOCKED` is what stops a request being dispatched twice."""
    due = NOW - timedelta(seconds=30)
    async with uow_factory() as uow:
        for index in range(4):
            await uow.requests.add(
                make_request(
                    external_reference=f"ref-{index}",
                    status=RequestStatus.RETRY_SCHEDULED,
                    next_retry_at=due,
                )
            )
        await uow.commit()

    first = uow_factory()
    second = uow_factory()
    async with first, second:
        claimed_first = await first.requests.claim_due_for_retry(now=NOW, limit=2)
        claimed_second = await second.requests.claim_due_for_retry(now=NOW, limit=2)

        ids_first = {request.id for request in claimed_first}
        ids_second = {request.id for request in claimed_second}

    assert len(ids_first) == 2
    assert len(ids_second) == 2
    assert ids_first.isdisjoint(ids_second)


async def test_a_claim_ignores_a_retry_that_is_not_due_yet(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    async with uow_factory() as uow:
        await uow.requests.add(
            make_request(
                status=RequestStatus.RETRY_SCHEDULED, next_retry_at=NOW + timedelta(minutes=5)
            )
        )
        await uow.commit()

    async with uow_factory() as uow:
        assert await uow.requests.claim_due_for_retry(now=NOW, limit=10) == []


async def test_listing_pages_forwards_without_repeating_or_skipping(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    """Keyset pagination over rows that share a created_at second."""
    created = NOW
    async with uow_factory() as uow:
        for index in range(5):
            request = make_request(external_reference=f"ref-{index}")
            request.created_at = created
            await uow.requests.add(request)
        await uow.commit()

    seen: list[str] = []
    cursor: str | None = None
    for _ in range(5):
        async with uow_factory() as uow:
            page = await uow.requests.list(
                IntegrationRequestFilter(limit=2, cursor=Cursor.decode(cursor) if cursor else None)
            )
        seen.extend(request.external_reference.value for request in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break

    assert cursor is None
    assert sorted(seen) == [f"ref-{index}" for index in range(5)]
    assert len(seen) == len(set(seen))


async def test_listing_filters_by_provider_and_status(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    async with uow_factory() as uow:
        await uow.requests.add(make_request(provider=NORTHSTAR, status=RequestStatus.SUCCEEDED))
        await uow.requests.add(make_request(provider=MERIDIAN, status=RequestStatus.SUCCEEDED))
        await uow.requests.add(make_request(provider=NORTHSTAR, status=RequestStatus.FAILED))
        await uow.commit()

    async with uow_factory() as uow:
        page = await uow.requests.list(
            IntegrationRequestFilter(
                provider=NORTHSTAR,
                statuses=frozenset({RequestStatus.SUCCEEDED}),
                limit=10,
            )
        )

    assert len(page.items) == 1
    assert page.items[0].provider == NORTHSTAR


async def test_reconciliation_only_sees_requests_that_stopped_progressing(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    stale = make_request(status=RequestStatus.PENDING, provider_reference="prv-stale")
    stale.updated_at = NOW - timedelta(hours=2)
    fresh = make_request(status=RequestStatus.PENDING, provider_reference="prv-fresh")
    fresh.updated_at = NOW
    finished = make_request(status=RequestStatus.SUCCEEDED, provider_reference="prv-done")
    finished.updated_at = NOW - timedelta(hours=2)

    async with uow_factory() as uow:
        for request in (stale, fresh, finished):
            await uow.requests.add(request)
        await uow.commit()

    async with uow_factory() as uow:
        candidates = await uow.requests.find_stale_in_flight(
            older_than=NOW - timedelta(hours=1), limit=10
        )

    assert [request.id for request in candidates] == [stale.id]


async def test_the_audit_trail_reads_back_in_the_order_it_happened(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    request = make_request()
    async with uow_factory() as uow:
        await uow.requests.add(request)
        await uow.audit.append_many(
            [
                _audit(request.id, AuditAction.REQUEST_RECEIVED, NOW),
                _audit(request.id, AuditAction.DISPATCH_ATTEMPTED, NOW + timedelta(seconds=1)),
                _audit(request.id, AuditAction.PROVIDER_ACCEPTED, NOW + timedelta(seconds=2)),
            ]
        )
        await uow.commit()

    async with uow_factory() as uow:
        history = await uow.audit.list_for_aggregate(
            aggregate_type="integration_request", aggregate_id=str(request.id)
        )

    assert [event.action for event in history] == [
        AuditAction.REQUEST_RECEIVED,
        AuditAction.DISPATCH_ATTEMPTED,
        AuditAction.PROVIDER_ACCEPTED,
    ]


async def test_two_publishers_never_claim_the_same_outbox_event(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    """Duplicate publication is tolerable but should not be routine."""
    async with uow_factory() as uow:
        for _ in range(6):
            await uow.outbox.add(make_outbox_event(created_at=NOW))
        await uow.commit()

    first = uow_factory()
    second = uow_factory()
    async with first, second:
        batch_one = await first.outbox.claim_unpublished(now=NOW, limit=3)
        batch_two = await second.outbox.claim_unpublished(now=NOW, limit=3)
        ids_one = {event.id for event in batch_one}
        ids_two = {event.id for event in batch_two}

    assert len(ids_one) == 3
    assert ids_one.isdisjoint(ids_two)


async def test_a_published_event_is_not_claimed_again(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    event = make_outbox_event(created_at=NOW)
    async with uow_factory() as uow:
        await uow.outbox.add(event)
        await uow.commit()

    async with uow_factory() as uow:
        await uow.outbox.mark_published([event.id], now=NOW)
        await uow.commit()

    async with uow_factory() as uow:
        assert await uow.outbox.claim_unpublished(now=NOW, limit=10) == []
        assert await uow.outbox.count_pending() == 0


async def test_a_failed_publication_backs_off_before_being_retried(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    event = make_outbox_event(created_at=NOW)
    async with uow_factory() as uow:
        await uow.outbox.add(event)
        await uow.commit()

    async with uow_factory() as uow:
        await uow.outbox.mark_failed(
            event.id,
            error="broker unavailable",
            next_attempt_at=NOW + timedelta(seconds=30),
            now=NOW,
        )
        await uow.commit()

    async with uow_factory() as uow:
        assert await uow.outbox.claim_unpublished(now=NOW, limit=10) == []
        later = await uow.outbox.claim_unpublished(now=NOW + timedelta(minutes=1), limit=10)

    assert [claimed.id for claimed in later] == [event.id]
    assert later[0].attempt_count == 1


async def test_pruning_keeps_the_outbox_a_queue_rather_than_an_archive(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    old = make_outbox_event(created_at=NOW - timedelta(days=30))
    recent = make_outbox_event(created_at=NOW)
    async with uow_factory() as uow:
        await uow.outbox.add(old)
        await uow.outbox.add(recent)
        await uow.commit()

    async with uow_factory() as uow:
        await uow.outbox.mark_published([old.id], now=NOW - timedelta(days=30))
        await uow.outbox.mark_published([recent.id], now=NOW)
        await uow.commit()

    async with uow_factory() as uow:
        deleted = await uow.outbox.purge_published_before(NOW - timedelta(days=7))
        await uow.commit()

    assert deleted == 1


async def test_an_outbox_event_id_cannot_be_duplicated(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    """Consumers deduplicate on this id, so the producer must not reuse one."""
    event = make_outbox_event(created_at=NOW)
    duplicate = make_outbox_event(created_at=NOW)
    duplicate.event_id = event.event_id

    with pytest.raises(ConflictError):
        async with uow_factory() as uow:
            await uow.outbox.add(event)
            await uow.outbox.add(duplicate)
            await uow.commit()


async def test_only_one_of_two_concurrent_idempotency_inserts_wins(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    """The losing request replays the winner's result instead of failing.

    Both transactions are open simultaneously, so this exercises the real
    arbitration: the second insert blocks on the first's uncommitted row and
    only discovers the conflict when the first commits.
    """
    request = make_request(idempotency_key="shared-key")
    async with uow_factory() as uow:
        await uow.requests.add(request)
        await uow.commit()

    async def insert(uow_factory: SqlUnitOfWorkFactory) -> str:
        try:
            async with uow_factory() as uow:
                await uow.idempotency.add(
                    IdempotencyRecord(
                        key="shared-key",
                        fingerprint="fp",
                        request_id=request.id,
                        response_status=201,
                        created_at=NOW,
                    )
                )
                await uow.flush()
                await uow.commit()
        except ConflictError:
            return "conflict"
        return "won"

    outcomes = await asyncio.gather(insert(uow_factory), insert(uow_factory))

    assert sorted(outcomes) == ["conflict", "won"]


async def test_an_uncommitted_change_is_invisible_and_then_discarded(
    uow_factory: SqlUnitOfWorkFactory,
) -> None:
    """Leaving the scope without committing must lose the work, loudly."""
    request = make_request()

    async with uow_factory() as uow:
        await uow.requests.add(request)
        await uow.flush()

    async with uow_factory() as uow:
        assert await uow.requests.get(request.id) is None


def _audit(request_id: UUID, action: AuditAction, occurred_at: datetime) -> AuditEvent:
    return AuditEvent.for_request(
        event_id=uuid4(),
        request_id=request_id,
        action=action,
        actor=ActorType.SYSTEM,
        correlation_id=CorrelationId("corr-1"),
        occurred_at=occurred_at,
    )
