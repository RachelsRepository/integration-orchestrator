"""Unit tests for subject-isolation object access."""

from __future__ import annotations

import pytest

from integration_orchestrator.api.ownership import assert_owner_access
from integration_orchestrator.domain.errors import NotFoundError
from integration_orchestrator.infrastructure.security.tokens import Principal, Scope

pytestmark = [pytest.mark.unit]


def test_isolation_disabled_allows_cross_subject() -> None:
    principal = Principal(subject="client-a", scopes=frozenset({Scope.REQUESTS_READ}))
    assert_owner_access(
        principal=principal, owner_subject="client-b", enforce=False, resource_label="x"
    )


def test_same_subject_allowed() -> None:
    principal = Principal(subject="client-a", scopes=frozenset({Scope.REQUESTS_READ}))
    assert_owner_access(
        principal=principal, owner_subject="client-a", enforce=True, resource_label="x"
    )


def test_cross_subject_hidden_as_not_found() -> None:
    principal = Principal(subject="client-a", scopes=frozenset({Scope.REQUESTS_READ}))
    with pytest.raises(NotFoundError):
        assert_owner_access(
            principal=principal, owner_subject="client-b", enforce=True, resource_label="x"
        )


def test_operator_admin_bypasses_isolation() -> None:
    principal = Principal(
        subject="ops",
        scopes=frozenset({Scope.REQUESTS_READ, Scope.OPERATIONS_ADMIN}),
    )
    assert_owner_access(
        principal=principal, owner_subject="client-b", enforce=True, resource_label="x"
    )
