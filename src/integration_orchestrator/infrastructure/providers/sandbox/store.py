"""In-memory state for the provider sandbox.

Each fake provider keeps its operations here, together with the attempt counters
that make "fail twice then succeed" repeatable. State is per process and is not
persisted: the sandbox exists to exercise the orchestrator, and a sandbox that
survived restarts would make tests depend on execution order.

Idempotency is modelled per provider, because that difference is the whole point
of having three of them. Northstar and Cobalt map an idempotency key back to the
original operation; Meridian ignores the key and happily creates a second one.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from integration_orchestrator.infrastructure.providers.sandbox.scenarios import (
    AttemptCounter,
    Scenario,
)


@dataclass(slots=True)
class SandboxOperation:
    """One operation held by a fake provider."""

    id: str
    external_reference: str
    kind: str
    status: str
    scenario: Scenario
    payload: dict[str, Any]
    correlation_id: str | None
    created_at: datetime
    updated_at: datetime
    idempotency_key: str | None = None
    failure_code: str | None = None
    failure_message: str | None = None
    emitted_events: list[str] = field(default_factory=list)

    def touch(self, status: str) -> None:
        self.status = status
        self.updated_at = datetime.now(tz=UTC)


class SandboxStore:
    """Operation storage and identifier generation for one fake provider."""

    def __init__(self, *, prefix: str) -> None:
        self._prefix = prefix
        self._operations: dict[str, SandboxOperation] = {}
        self._by_idempotency_key: dict[str, str] = {}
        self._sequence = itertools.count(1)
        self.attempts = AttemptCounter()

    def next_id(self) -> str:
        # Sequential rather than random so that a failing test names the same
        # operation on every run.
        return f"{self._prefix}-{next(self._sequence):08d}"

    def create(
        self,
        *,
        external_reference: str,
        kind: str,
        status: str,
        scenario: Scenario,
        payload: dict[str, Any],
        correlation_id: str | None,
        idempotency_key: str | None,
        honour_idempotency: bool,
    ) -> tuple[SandboxOperation, bool]:
        """Create an operation, returning it and whether it was deduplicated."""
        if honour_idempotency and idempotency_key:
            existing_id = self._by_idempotency_key.get(idempotency_key)
            if existing_id is not None:
                return self._operations[existing_id], True

        now = datetime.now(tz=UTC)
        operation = SandboxOperation(
            id=self.next_id(),
            external_reference=external_reference,
            kind=kind,
            status=status,
            scenario=scenario,
            payload=payload,
            correlation_id=correlation_id,
            created_at=now,
            updated_at=now,
            idempotency_key=idempotency_key,
        )
        self._operations[operation.id] = operation
        if honour_idempotency and idempotency_key:
            self._by_idempotency_key[idempotency_key] = operation.id
        return operation, False

    def get(self, operation_id: str) -> SandboxOperation | None:
        return self._operations.get(operation_id)

    def all(self) -> list[SandboxOperation]:
        return list(self._operations.values())

    def reset(self) -> None:
        """Clear all state. Used between test cases."""
        self._operations.clear()
        self._by_idempotency_key.clear()
        self._sequence = itertools.count(1)
        self.attempts.reset()
