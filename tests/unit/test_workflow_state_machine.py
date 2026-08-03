"""Workflow state machine transition matrix."""

from __future__ import annotations

import pytest

from integration_orchestrator.domain.enums import WorkflowStatus, WorkflowStepStatus
from integration_orchestrator.domain.errors import InvalidStateTransitionError
from integration_orchestrator.domain.workflow_state_machine import (
    assert_step_transition,
    assert_workflow_transition,
)

pytestmark = pytest.mark.unit


def test_workflow_running_can_wait_and_succeed() -> None:
    assert_workflow_transition(WorkflowStatus.RUNNING, WorkflowStatus.WAITING)
    assert_workflow_transition(WorkflowStatus.RUNNING, WorkflowStatus.SUCCEEDED)


def test_workflow_terminal_states_do_not_regress() -> None:
    with pytest.raises(InvalidStateTransitionError):
        assert_workflow_transition(WorkflowStatus.SUCCEEDED, WorkflowStatus.RUNNING)
    with pytest.raises(InvalidStateTransitionError):
        assert_workflow_transition(WorkflowStatus.COMPENSATED, WorkflowStatus.COMPENSATING)


def test_step_ready_to_running_to_succeeded() -> None:
    assert_step_transition(WorkflowStepStatus.READY, WorkflowStepStatus.RUNNING)
    assert_step_transition(WorkflowStepStatus.RUNNING, WorkflowStepStatus.SUCCEEDED)


def test_succeeded_step_can_enter_compensation() -> None:
    assert_step_transition(WorkflowStepStatus.SUCCEEDED, WorkflowStepStatus.COMPENSATING)
    assert_step_transition(WorkflowStepStatus.COMPENSATING, WorkflowStepStatus.COMPENSATED)
