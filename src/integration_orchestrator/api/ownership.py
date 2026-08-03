"""Object-level access checks for API principals.

The deployment unit is single-tenant (one organization, one database). Within
that boundary, ``SECURITY__ENFORCE_SUBJECT_ISOLATION`` ensures one API client
cannot read or mutate another client's requests and workflows by guessing UUIDs.
Operators with ``operations:admin`` retain full visibility for incident response.
"""

from __future__ import annotations

from integration_orchestrator.domain.errors import NotFoundError
from integration_orchestrator.infrastructure.security.tokens import Principal, Scope


def assert_owner_access(
    *,
    principal: Principal,
    owner_subject: str | None,
    enforce: bool,
    resource_label: str = "resource",
) -> None:
    """Raise :class:`NotFoundError` when the principal may not see the resource.

    Returning 404 rather than 403 avoids confirming that a UUID exists in another
    client's namespace.
    """
    if not enforce:
        return
    if principal.has_scope(Scope.OPERATIONS_ADMIN):
        return
    if owner_subject is None or owner_subject == principal.subject:
        return
    raise NotFoundError(f"{resource_label} not found")
