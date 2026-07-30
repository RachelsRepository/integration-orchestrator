"""Kafka event publisher.

Delivery guarantees, stated plainly: this is **at-least-once**. The producer
requires acknowledgement from all in-sync replicas and is idempotent at the
broker level, but the outbox row is only marked published *after* the broker
acknowledges. A crash between the acknowledgement and that update republishes the
event. Consumers must therefore deduplicate on ``event_id``, which is generated
once when the state change committed and never regenerated.

Messages are keyed on the aggregate id, so every event for one integration
request lands on the same partition and arrives in order relative to its
siblings. Ordering across different requests is not promised and consumers must
not assume it.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Sequence

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from integration_orchestrator.config.settings import KafkaSettings
from integration_orchestrator.domain.errors import EventPublicationError
from integration_orchestrator.domain.events import EventEnvelope

logger = logging.getLogger(__name__)


class KafkaEventPublisher:
    """Publishes event envelopes to Kafka."""

    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Connect the producer, bounded by the configured request timeout.

        A missing broker must not block process startup indefinitely. The API's
        liveness probe only asks whether this process is alive; readiness reports
        Kafka separately. If bootstrap fails here the publisher stays unstarted
        and ``healthy()`` returns false until an operator restarts after the
        broker is reachable.
        """
        if self._producer is not None:
            return
        producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.bootstrap_servers,
            client_id=self._settings.client_id,
            acks=self._settings.acks,
            compression_type=(
                None
                if self._settings.compression_type == "none"
                else self._settings.compression_type
            ),
            linger_ms=self._settings.linger_ms,
            request_timeout_ms=int(self._settings.request_timeout_seconds * 1000),
            # Broker-side deduplication of producer retries. It does not make the
            # end-to-end path exactly-once, but it does stop an internal retry
            # from writing the same record twice.
            enable_idempotence=True,
        )
        try:
            async with asyncio.timeout(self._settings.request_timeout_seconds):
                await producer.start()
        except (TimeoutError, KafkaError, OSError) as exc:
            # Best-effort cleanup: start() may have opened sockets before failing.
            try:
                await producer.stop()
            except Exception:
                logger.debug(
                    "discarded an error while aborting a failed kafka start",
                    exc_info=True,
                )
            raise EventPublicationError(
                f"kafka producer failed to start: {type(exc).__name__}"
            ) from exc
        self._producer = producer
        logger.info(
            "kafka producer started",
            extra={"bootstrap_servers": self._settings.bootstrap_servers},
        )

    async def stop(self) -> None:
        if self._producer is None:
            return
        # Flushes buffered records before closing, so a clean shutdown does not
        # silently drop events the outbox has already marked in flight.
        await self._producer.stop()
        self._producer = None
        logger.info("kafka producer stopped")

    async def publish(self, envelope: EventEnvelope) -> None:
        await self.publish_batch([envelope])

    async def publish_batch(self, envelopes: Sequence[EventEnvelope]) -> None:
        if not envelopes:
            return
        producer = self._require_producer()

        futures = []
        for envelope in envelopes:
            topic = self._settings.topic_for(envelope.event_type)
            futures.append(
                await producer.send(
                    topic,
                    value=_encode(envelope),
                    key=envelope.aggregate_id.encode("utf-8"),
                    headers=_headers(envelope),
                )
            )

        try:
            for future in futures:
                await future
        except KafkaError as exc:
            raise EventPublicationError(
                f"kafka rejected the publication: {type(exc).__name__}"
            ) from exc

    async def healthy(self) -> bool:
        if self._producer is None:
            return False
        try:
            # Cheap metadata read: confirms the client still has a live
            # connection without producing a probe record onto a real topic.
            return bool(self._producer.client.cluster.brokers())
        except KafkaError:
            return False

    def _require_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            raise EventPublicationError("the kafka producer has not been started")
        return self._producer


def _encode(envelope: EventEnvelope) -> bytes:
    return json.dumps(envelope.to_dict(), separators=(",", ":")).encode("utf-8")


def _headers(envelope: EventEnvelope) -> list[tuple[str, bytes]]:
    """Headers let consumers route and deduplicate without parsing the body."""
    headers = [
        ("event_id", str(envelope.event_id).encode("utf-8")),
        ("event_type", envelope.event_type.encode("utf-8")),
        ("event_version", str(envelope.event_version).encode("utf-8")),
        ("correlation_id", envelope.correlation_id.encode("utf-8")),
        ("producer", envelope.producer.encode("utf-8")),
    ]
    if envelope.causation_id:
        headers.append(("causation_id", envelope.causation_id.encode("utf-8")))
    return headers
