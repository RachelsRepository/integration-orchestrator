"""Event publication port."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from integration_orchestrator.domain.events import EventEnvelope


@runtime_checkable
class EventPublisher(Protocol):
    """Publishes envelopes to the message broker.

    Only the outbox publisher calls this. Application use cases stage events in
    the outbox instead, because a broker publish cannot join a database
    transaction: a process that commits the database and then dies before the
    publish leaves the world permanently inconsistent.
    """

    async def publish(self, envelope: EventEnvelope) -> None:
        """Publish one envelope, waiting for broker acknowledgement."""
        ...

    async def publish_batch(self, envelopes: Sequence[EventEnvelope]) -> None:
        """Publish several envelopes, waiting for acknowledgement of all of them."""
        ...

    async def start(self) -> None:
        """Open broker connections."""
        ...

    async def stop(self) -> None:
        """Flush in-flight messages and close connections."""
        ...

    async def healthy(self) -> bool:
        """Report whether the publisher currently has a usable connection."""
        ...
