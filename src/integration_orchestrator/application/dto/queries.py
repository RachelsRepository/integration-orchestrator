"""Query criteria and cursor pagination primitives.

Cursor pagination is used rather than offset pagination because integration
requests are created continuously. With ``LIMIT/OFFSET``, rows inserted between
two page fetches shift every subsequent page, so an operator paging through a
backlog silently skips records. A cursor anchored on ``(created_at, id)`` is
stable under concurrent inserts and lets PostgreSQL use the index for a keyset
seek instead of counting and discarding rows.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

from integration_orchestrator.domain.enums import OperationType, RequestStatus
from integration_orchestrator.domain.errors import ValidationError
from integration_orchestrator.domain.value_objects import ProviderSlug

DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 200


@dataclass(frozen=True, slots=True)
class Cursor:
    """An opaque position in a ``(created_at DESC, id DESC)`` ordering.

    Encoded rather than exposed as raw columns so the ordering can change later
    without breaking clients that stored a cursor.
    """

    created_at: datetime
    request_id: UUID

    def encode(self) -> str:
        raw = json.dumps(
            {"c": self.created_at.isoformat(), "i": str(self.request_id)},
            separators=(",", ":"),
        )
        return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")

    @classmethod
    def decode(cls, value: str) -> Cursor:
        try:
            padding = "=" * (-len(value) % 4)
            raw = base64.urlsafe_b64decode(value + padding).decode("utf-8")
            data = json.loads(raw)
            return cls(
                created_at=datetime.fromisoformat(data["c"]),
                request_id=UUID(data["i"]),
            )
        except (
            binascii.Error,
            UnicodeDecodeError,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            raise ValidationError("the supplied cursor is not valid") from exc


@dataclass(frozen=True, slots=True)
class Page[T]:
    """One page of results plus the cursor needed to fetch the next."""

    items: Sequence[T]
    next_cursor: str | None = None

    @property
    def has_more(self) -> bool:
        return self.next_cursor is not None


@dataclass(frozen=True, slots=True)
class IntegrationRequestFilter:
    """Server-side filter criteria for listing integration requests."""

    provider: ProviderSlug | None = None
    statuses: frozenset[RequestStatus] = field(default_factory=frozenset)
    operation_type: OperationType | None = None
    external_reference: str | None = None
    created_after: datetime | None = None
    created_before: datetime | None = None
    limit: int = DEFAULT_PAGE_SIZE
    cursor: Cursor | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.limit <= MAX_PAGE_SIZE:
            raise ValidationError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
        if (
            self.created_after is not None
            and self.created_before is not None
            and self.created_after > self.created_before
        ):
            raise ValidationError("created_after must not be later than created_before")

    def describe(self) -> dict[str, Any]:
        """Log-safe description of the applied filter."""
        return {
            "provider": self.provider.value if self.provider else None,
            "statuses": sorted(status.value for status in self.statuses) or None,
            "operation_type": self.operation_type.value if self.operation_type else None,
            "has_external_reference": self.external_reference is not None,
            "limit": self.limit,
            "paginated": self.cursor is not None,
        }
