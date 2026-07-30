"""Correlation context.

A correlation id is set once, at the edge, and read everywhere. Context
variables carry it through the async call stack without threading a parameter
through every function, and unlike thread locals they behave correctly when a
single thread interleaves many concurrent requests.

Anything that spawns a background task must copy the context or set the value
explicitly; workers do the latter, because their correlation id comes from the
record they are processing rather than from an inbound request.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token

_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)
_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
_integration_request_id: ContextVar[str | None] = ContextVar("integration_request_id", default=None)


def current_correlation_id() -> str | None:
    """Return the correlation id for the current context."""
    return _correlation_id.get()


def set_correlation_id(value: str | None) -> Token[str | None]:
    """Set the correlation id, returning a token that restores the previous one."""
    return _correlation_id.set(value)


def reset_correlation_id(token: Token[str | None]) -> None:
    _correlation_id.reset(token)


def current_request_id() -> str | None:
    """Return the transport-level request id (one HTTP call)."""
    return _request_id.get()


def set_request_id(value: str | None) -> Token[str | None]:
    return _request_id.set(value)


def current_integration_request_id() -> str | None:
    return _integration_request_id.get()


def set_integration_request_id(value: str | None) -> Token[str | None]:
    return _integration_request_id.set(value)


@contextmanager
def correlation_scope(
    *,
    correlation_id: str | None = None,
    request_id: str | None = None,
    integration_request_id: str | None = None,
) -> Iterator[None]:
    """Bind correlation values for the duration of a block.

    Used by workers, which process many records in one process and must not leak
    one record's identifiers into the next record's log lines.
    """
    tokens: list[tuple[ContextVar[str | None], Token[str | None]]] = []
    if correlation_id is not None:
        tokens.append((_correlation_id, _correlation_id.set(correlation_id)))
    if request_id is not None:
        tokens.append((_request_id, _request_id.set(request_id)))
    if integration_request_id is not None:
        tokens.append(
            (_integration_request_id, _integration_request_id.set(integration_request_id))
        )
    try:
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


def context_fields() -> dict[str, str]:
    """Return the currently bound identifiers, for log enrichment."""
    fields: dict[str, str] = {}
    correlation_id = _correlation_id.get()
    if correlation_id:
        fields["correlation_id"] = correlation_id
    request_id = _request_id.get()
    if request_id:
        fields["request_id"] = request_id
    integration_request_id = _integration_request_id.get()
    if integration_request_id:
        fields["integration_request_id"] = integration_request_id
    return fields
