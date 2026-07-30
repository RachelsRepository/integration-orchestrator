"""Integration request state machine.

The transition table is the single authority on which status changes are legal.
Entities never expose a status setter, so every change in the system passes
through :func:`assert_transition_allowed`. An illegal transition is a bug or a
provider behaving unexpectedly, and either way it must surface loudly as an
auditable domain error rather than quietly corrupting the record.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from integration_orchestrator.domain.enums import NormalizedStatus, RequestStatus
from integration_orchestrator.domain.errors import InvalidStateTransitionError

_TRANSITIONS: Mapping[RequestStatus, frozenset[RequestStatus]] = MappingProxyType(
    {
        # A freshly accepted request either proceeds to validation or is
        # cancelled before any provider work begins.
        RequestStatus.RECEIVED: frozenset(
            {
                RequestStatus.VALIDATING,
                RequestStatus.FAILED,
                RequestStatus.CANCELLED,
            }
        ),
        # Validation either produces a dispatchable request or a terminal
        # rejection. Validation failures are never retried: the input is wrong.
        RequestStatus.VALIDATING: frozenset(
            {
                RequestStatus.DISPATCHING,
                RequestStatus.FAILED,
                RequestStatus.CANCELLED,
            }
        ),
        # Dispatch is the only state that can reach every provider outcome:
        # synchronous success, asynchronous acceptance, a retryable failure, or
        # a terminal one. It can also reach manual review when a crash left the
        # request in dispatching with an unverifiable provider outcome.
        RequestStatus.DISPATCHING: frozenset(
            {
                RequestStatus.PENDING,
                RequestStatus.SUCCEEDED,
                RequestStatus.FAILED,
                RequestStatus.RETRY_SCHEDULED,
                RequestStatus.MANUAL_REVIEW,
                RequestStatus.CANCELLED,
            }
        ),
        # The provider owns the operation now. It completes via webhook or via
        # reconciliation polling.
        RequestStatus.PENDING: frozenset(
            {
                RequestStatus.SUCCEEDED,
                RequestStatus.FAILED,
                RequestStatus.CANCELLED,
                RequestStatus.MANUAL_REVIEW,
            }
        ),
        # A failed request is not necessarily finished: an operator can schedule
        # a retry, or reconciliation can escalate it for human judgement.
        RequestStatus.FAILED: frozenset(
            {
                RequestStatus.RETRY_SCHEDULED,
                RequestStatus.MANUAL_REVIEW,
                RequestStatus.CANCELLED,
            }
        ),
        # Retry scheduling is a holding state claimed by the retry worker.
        RequestStatus.RETRY_SCHEDULED: frozenset(
            {
                RequestStatus.DISPATCHING,
                RequestStatus.FAILED,
                RequestStatus.CANCELLED,
                RequestStatus.MANUAL_REVIEW,
            }
        ),
        # Manual review is the escape hatch for ambiguity. Only a deliberate
        # operator decision leaves it.
        RequestStatus.MANUAL_REVIEW: frozenset(
            {
                RequestStatus.DISPATCHING,
                RequestStatus.RETRY_SCHEDULED,
                RequestStatus.SUCCEEDED,
                RequestStatus.FAILED,
                RequestStatus.CANCELLED,
            }
        ),
        # Terminal states.
        RequestStatus.SUCCEEDED: frozenset(),
        RequestStatus.CANCELLED: frozenset(),
    }
)

# Statuses the retry worker is allowed to claim.
CLAIMABLE_FOR_RETRY: frozenset[RequestStatus] = frozenset({RequestStatus.RETRY_SCHEDULED})

# Statuses reconciliation inspects, because the provider may know more than we do.
RECONCILABLE_STATUSES: frozenset[RequestStatus] = frozenset(
    {RequestStatus.DISPATCHING, RequestStatus.PENDING}
)

# Statuses from which an operator may request a manual retry.
MANUALLY_RETRYABLE_STATUSES: frozenset[RequestStatus] = frozenset(
    {RequestStatus.FAILED, RequestStatus.MANUAL_REVIEW}
)

# Statuses from which cancellation may be attempted.
CANCELLABLE_STATUSES: frozenset[RequestStatus] = frozenset(
    {
        RequestStatus.RECEIVED,
        RequestStatus.VALIDATING,
        RequestStatus.PENDING,
        RequestStatus.RETRY_SCHEDULED,
        RequestStatus.FAILED,
        RequestStatus.MANUAL_REVIEW,
    }
)

# How a normalized provider status maps onto a request status. Adapters produce
# the left-hand side; only this table decides what it means for the workflow.
_STATUS_PROJECTION: Mapping[NormalizedStatus, RequestStatus | None] = MappingProxyType(
    {
        NormalizedStatus.ACCEPTED: RequestStatus.PENDING,
        NormalizedStatus.PENDING: RequestStatus.PENDING,
        NormalizedStatus.SUCCEEDED: RequestStatus.SUCCEEDED,
        NormalizedStatus.FAILED: RequestStatus.FAILED,
        NormalizedStatus.CANCELLED: RequestStatus.CANCELLED,
        # An unknown provider status intentionally has no projection. Callers
        # must decide explicitly, which in practice means manual review.
        NormalizedStatus.UNKNOWN: None,
    }
)


def allowed_transitions(current: RequestStatus) -> frozenset[RequestStatus]:
    """Return the statuses reachable from ``current``."""
    return _TRANSITIONS[current]


def can_transition(current: RequestStatus, requested: RequestStatus) -> bool:
    """Report whether a transition is legal without raising."""
    return requested in _TRANSITIONS[current]


def assert_transition_allowed(
    current: RequestStatus,
    requested: RequestStatus,
    *,
    aggregate_id: str | None = None,
    correlation_id: str | None = None,
) -> None:
    """Raise :class:`InvalidStateTransitionError` when a transition is illegal.

    Self-transitions are rejected as well. Re-applying the same status is almost
    always a duplicate delivery, and the caller must handle that explicitly
    rather than issuing a redundant write and a misleading audit entry.
    """
    if not can_transition(current, requested):
        raise InvalidStateTransitionError(
            current,
            requested,
            aggregate_id=aggregate_id,
            correlation_id=correlation_id,
        )


def project_normalized_status(status: NormalizedStatus) -> RequestStatus | None:
    """Map a provider-neutral status onto a request status.

    Returns ``None`` for :attr:`NormalizedStatus.UNKNOWN`, which callers must
    treat as "the provider said something we do not understand" rather than
    guessing an outcome.
    """
    return _STATUS_PROJECTION[status]


def is_forward_progress(current: RequestStatus, candidate: RequestStatus) -> bool:
    """Report whether applying ``candidate`` would advance the workflow.

    Used by webhook and reconciliation handling to make out-of-order delivery
    harmless. Providers do not guarantee webhook ordering, so a ``pending``
    notification can arrive after the ``succeeded`` one. Treating that as
    forward progress would move a completed request backwards.
    """
    if current == candidate:
        return False
    if current.is_terminal:
        return False
    return can_transition(current, candidate)
