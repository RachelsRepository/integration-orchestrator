"""Workflow and step state machines.

Separate from the integration-request machine so single-request behaviour stays
unchanged while multi-step saga orchestration gains its own transition rules.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from integration_orchestrator.domain.enums import WorkflowStatus, WorkflowStepStatus
from integration_orchestrator.domain.errors import InvalidStateTransitionError

_WORKFLOW_TRANSITIONS: Mapping[WorkflowStatus, frozenset[WorkflowStatus]] = MappingProxyType(
    {
        WorkflowStatus.CREATED: frozenset(
            {WorkflowStatus.QUEUED, WorkflowStatus.RUNNING, WorkflowStatus.CANCELLED}
        ),
        WorkflowStatus.QUEUED: frozenset(
            {
                WorkflowStatus.RUNNING,
                WorkflowStatus.CANCELLED,
                WorkflowStatus.FAILED,
                WorkflowStatus.TIMED_OUT,
            }
        ),
        WorkflowStatus.RUNNING: frozenset(
            {
                WorkflowStatus.WAITING,
                WorkflowStatus.RETRY_SCHEDULED,
                WorkflowStatus.COMPENSATING,
                WorkflowStatus.SUCCEEDED,
                WorkflowStatus.FAILED,
                WorkflowStatus.CANCELLED,
                WorkflowStatus.MANUAL_REVIEW,
                WorkflowStatus.TIMED_OUT,
            }
        ),
        WorkflowStatus.WAITING: frozenset(
            {
                WorkflowStatus.RUNNING,
                WorkflowStatus.COMPENSATING,
                WorkflowStatus.FAILED,
                WorkflowStatus.CANCELLED,
                WorkflowStatus.MANUAL_REVIEW,
                WorkflowStatus.TIMED_OUT,
            }
        ),
        WorkflowStatus.RETRY_SCHEDULED: frozenset(
            {
                WorkflowStatus.RUNNING,
                WorkflowStatus.COMPENSATING,
                WorkflowStatus.FAILED,
                WorkflowStatus.CANCELLED,
                WorkflowStatus.MANUAL_REVIEW,
                WorkflowStatus.TIMED_OUT,
            }
        ),
        WorkflowStatus.TIMED_OUT: frozenset(
            {
                WorkflowStatus.COMPENSATING,
                WorkflowStatus.CANCELLED,
                WorkflowStatus.MANUAL_REVIEW,
                WorkflowStatus.FAILED,
            }
        ),
        WorkflowStatus.COMPENSATING: frozenset(
            {
                WorkflowStatus.COMPENSATED,
                WorkflowStatus.MANUAL_REVIEW,
                WorkflowStatus.DEAD_LETTERED,
                WorkflowStatus.FAILED,
                WorkflowStatus.CANCELLED,
            }
        ),
        WorkflowStatus.MANUAL_REVIEW: frozenset(
            {
                WorkflowStatus.RUNNING,
                WorkflowStatus.COMPENSATING,
                WorkflowStatus.CANCELLED,
                WorkflowStatus.DEAD_LETTERED,
            }
        ),
        WorkflowStatus.SUCCEEDED: frozenset(),
        WorkflowStatus.COMPENSATED: frozenset(),
        WorkflowStatus.FAILED: frozenset({WorkflowStatus.MANUAL_REVIEW}),
        WorkflowStatus.CANCELLED: frozenset(),
        WorkflowStatus.DEAD_LETTERED: frozenset(),
    }
)

_STEP_TRANSITIONS: Mapping[WorkflowStepStatus, frozenset[WorkflowStepStatus]] = MappingProxyType(
    {
        WorkflowStepStatus.PENDING: frozenset(
            {WorkflowStepStatus.READY, WorkflowStepStatus.SKIPPED, WorkflowStepStatus.CANCELLED}
        ),
        WorkflowStepStatus.READY: frozenset(
            {
                WorkflowStepStatus.RUNNING,
                WorkflowStepStatus.SKIPPED,
                WorkflowStepStatus.CANCELLED,
            }
        ),
        WorkflowStepStatus.RUNNING: frozenset(
            {
                WorkflowStepStatus.WAITING,
                WorkflowStepStatus.RETRY_SCHEDULED,
                WorkflowStepStatus.SUCCEEDED,
                WorkflowStepStatus.FAILED,
                WorkflowStepStatus.CANCELLED,
            }
        ),
        WorkflowStepStatus.WAITING: frozenset(
            {
                WorkflowStepStatus.RUNNING,
                WorkflowStepStatus.SUCCEEDED,
                WorkflowStepStatus.FAILED,
                WorkflowStepStatus.CANCELLED,
            }
        ),
        WorkflowStepStatus.RETRY_SCHEDULED: frozenset(
            {
                WorkflowStepStatus.READY,
                WorkflowStepStatus.RUNNING,
                WorkflowStepStatus.FAILED,
                WorkflowStepStatus.CANCELLED,
            }
        ),
        WorkflowStepStatus.SUCCEEDED: frozenset(
            {WorkflowStepStatus.COMPENSATING, WorkflowStepStatus.CANCELLED}
        ),
        WorkflowStepStatus.FAILED: frozenset({WorkflowStepStatus.DEAD_LETTERED}),
        WorkflowStepStatus.SKIPPED: frozenset(),
        WorkflowStepStatus.COMPENSATING: frozenset(
            {
                WorkflowStepStatus.COMPENSATED,
                WorkflowStepStatus.RETRY_SCHEDULED,
                WorkflowStepStatus.DEAD_LETTERED,
                WorkflowStepStatus.FAILED,
            }
        ),
        WorkflowStepStatus.COMPENSATED: frozenset(),
        WorkflowStepStatus.CANCELLED: frozenset(),
        WorkflowStepStatus.DEAD_LETTERED: frozenset(),
    }
)


def assert_workflow_transition(current: WorkflowStatus, new: WorkflowStatus) -> None:
    allowed = _WORKFLOW_TRANSITIONS.get(current, frozenset())
    if new not in allowed:
        raise InvalidStateTransitionError(current, new)


def assert_step_transition(current: WorkflowStepStatus, new: WorkflowStepStatus) -> None:
    allowed = _STEP_TRANSITIONS.get(current, frozenset())
    if new not in allowed:
        raise InvalidStateTransitionError(current, new)


WORKFLOW_TRANSITION_MATRIX = _WORKFLOW_TRANSITIONS
STEP_TRANSITION_MATRIX = _STEP_TRANSITIONS
