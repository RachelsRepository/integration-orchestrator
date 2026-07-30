"""In-memory event publisher.

A clearly isolated test and local-development adapter. It is selected only when
``KAFKA__ENABLED`` is false, and it never participates in a deployed
configuration: the settings validator rejects a production-like environment with
Kafka disabled.

Its purpose is to let the outbox publisher, the workers and the end-to-end tests
run without a broker, while still exercising the real publication path — the same
claim query, the same envelope construction, the same mark-published update.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from integration_orchestrator.domain.errors import EventPublicationError
from integration_orchestrator.domain.events import EventEnvelope

logger = logging.getLogger(__name__)


class InMemoryEventPublisher:
    """Records published envelopes in a list."""

    def __init__(self, *, fail_next: int = 0) -> None:
        self.published: list[EventEnvelope] = []
        self._started = False
        # Lets a test drive the outbox retry path deterministically.
        self._fail_next = fail_next

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def publish(self, envelope: EventEnvelope) -> None:
        await self.publish_batch([envelope])

    async def publish_batch(self, envelopes: Sequence[EventEnvelope]) -> None:
        if self._fail_next > 0:
            self._fail_next -= 1
            raise EventPublicationError("the in-memory publisher was told to fail")
        self.published.extend(envelopes)
        logger.debug("published events in memory", extra={"count": len(envelopes)})

    async def healthy(self) -> bool:
        return self._started

    def fail_next(self, count: int) -> None:
        """Make the next ``count`` publish calls fail."""
        self._fail_next = count

    def clear(self) -> None:
        self.published.clear()

    def types(self) -> list[str]:
        return [envelope.event_type for envelope in self.published]
