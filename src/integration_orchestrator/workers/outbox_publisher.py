"""The outbox publisher.

Reads events the application staged in the same transaction as the state change
they describe, publishes them to the broker, and marks them published. That
ordering is the entire point: a state change and its event either both survive a
crash or neither does.

Delivery is at-least-once and nothing here pretends otherwise. If the broker
acknowledges and the process dies before the row is updated, the event is
published again on restart. The ``event_id`` is stable across republication, so a
consumer that deduplicates on it sees the event once.

Publication happens outside any database transaction. Holding one open across a
broker round trip would pin a connection for the duration of a network call and
would make broker latency into database pressure.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timedelta

from integration_orchestrator.application.ports.messaging import EventPublisher
from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.application.ports.system import Clock
from integration_orchestrator.application.ports.unit_of_work import UnitOfWorkFactory
from integration_orchestrator.config.settings import WorkerSettings
from integration_orchestrator.domain.errors import EventPublicationError
from integration_orchestrator.domain.events import EventEnvelope
from integration_orchestrator.domain.records import OutboxEvent
from integration_orchestrator.workers.base import Worker

logger = logging.getLogger(__name__)

PRODUCER_NAME = "integration-orchestrator"

#: Ceiling on the publication backoff. A broker outage should be retried
#: patiently, but never so patiently that recovery takes an hour to notice.
MAX_RETRY_DELAY_SECONDS = 300.0


class OutboxPublisherWorker(Worker):
    """Publishes staged outbox events to the broker."""

    name = "outbox_publisher"

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        publisher: EventPublisher,
        clock: Clock,
        metrics: MetricsSink,
        settings: WorkerSettings,
    ) -> None:
        super().__init__(
            poll_interval_seconds=settings.outbox_poll_interval_seconds, metrics=metrics
        )
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._clock = clock
        self._settings = settings

    async def run_once(self) -> int:
        now = self._clock.now()

        async with self._uow_factory() as uow:
            # The claim locks the batch for this worker, so several publisher
            # replicas can run without publishing the same event twice.
            events = list(
                await uow.outbox.claim_unpublished(now=now, limit=self._settings.outbox_batch_size)
            )
            pending = await uow.outbox.count_pending()
            await uow.commit()

        self._metrics.set_gauge("outbox_pending_total", float(pending))
        if not events:
            return 0

        published, failures = await self._publish(events)

        async with self._uow_factory() as uow:
            if published:
                await uow.outbox.mark_published(
                    [event.id for event in published], now=self._clock.now()
                )
            for event, reason in failures:
                await uow.outbox.mark_failed(
                    event.id,
                    error=reason,
                    next_attempt_at=self._next_attempt_at(event),
                    now=self._clock.now(),
                )
            await uow.commit()

        for event in published:
            self._metrics.increment(
                "outbox_published_total", labels={"event_type": event.event_type}
            )
        for event, _ in failures:
            self._metrics.increment(
                "outbox_publish_failures_total", labels={"event_type": event.event_type}
            )

        logger.info(
            "published a batch of outbox events",
            extra={
                "worker": self.name,
                "published": len(published),
                "failed": len(failures),
                "pending_before": pending,
            },
        )
        return len(events)

    async def _publish(
        self, events: Sequence[OutboxEvent]
    ) -> tuple[list[OutboxEvent], list[tuple[OutboxEvent, str]]]:
        """Publish a batch, falling back to one-by-one on failure.

        The batch is attempted first because it is one broker round trip. If it
        fails, the events are retried individually so that one poisoned event
        cannot block every other event in the batch — otherwise a single
        unroutable topic would stall the whole event stream.
        """
        envelopes = [_to_envelope(event) for event in events]
        try:
            await self._publisher.publish_batch(envelopes)
        except EventPublicationError as exc:
            logger.warning(
                "batch publication failed; retrying events individually",
                extra={"worker": self.name, "batch_size": len(events), "reason": str(exc)},
            )
        else:
            return list(events), []

        published: list[OutboxEvent] = []
        failures: list[tuple[OutboxEvent, str]] = []
        for event, envelope in zip(events, envelopes, strict=True):
            try:
                await self._publisher.publish(envelope)
            except EventPublicationError as exc:
                failures.append((event, str(exc)))
            else:
                published.append(event)
        return published, failures

    def _next_attempt_at(self, event: OutboxEvent) -> datetime:
        """Back off exponentially, bounded, after a publication failure."""
        attempt = min(event.attempt_count + 1, self._settings.outbox_max_attempts)
        delay = min(
            self._settings.outbox_retry_base_seconds * (2 ** (attempt - 1)),
            MAX_RETRY_DELAY_SECONDS,
        )
        return self._clock.now() + timedelta(seconds=delay)


def _to_envelope(event: OutboxEvent) -> EventEnvelope:
    return EventEnvelope(
        event_id=event.event_id,
        event_type=event.event_type,
        event_version=event.event_version,
        aggregate_type=event.aggregate_type,
        aggregate_id=event.aggregate_id,
        occurred_at=event.created_at,
        correlation_id=event.correlation_id.value,
        causation_id=event.causation_id,
        producer=PRODUCER_NAME,
        payload=event.payload,
        metadata={"attempt": event.attempt_count, "partition_key": event.routing_key},
    )
