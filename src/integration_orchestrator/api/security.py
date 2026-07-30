"""Authentication and authorization dependencies.

Scopes are required per route through :func:`require_scope`, rather than checked
inside handlers. Declaring the requirement in the signature means an endpoint
cannot be added without deciding what authorization it needs, and the requirement
shows up in the generated OpenAPI document.

Webhook endpoints are deliberately excluded from bearer authentication. Providers
cannot present our tokens; their authenticity is established by signature
verification inside the adapter, which is the stronger check because it also
proves the body was not modified.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Annotated

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from integration_orchestrator.api.dependencies import get_correlation_id, get_token_verifier
from integration_orchestrator.domain.errors import AuthenticationError
from integration_orchestrator.domain.value_objects import CorrelationId
from integration_orchestrator.infrastructure.security.tokens import (
    Principal,
    Scope,
    TokenVerifier,
)

#: ``auto_error`` is off so a missing credential produces the platform's own
#: error envelope instead of Starlette's ``{"detail": "Not authenticated"}``.
bearer_scheme = HTTPBearer(auto_error=False, scheme_name="InternalBearer")


async def get_principal(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None,
    verifier: Annotated[TokenVerifier, Depends(get_token_verifier)] = None,  # type: ignore[assignment]
    correlation_id: Annotated[CorrelationId, Depends(get_correlation_id)] = None,  # type: ignore[assignment]
) -> Principal:
    """Authenticate the caller and return the resolved principal."""
    if credentials is None or not credentials.credentials:
        raise AuthenticationError(
            "a bearer token is required",
            correlation_id=correlation_id.value if correlation_id else None,
        )
    principal = verifier.verify(
        credentials.credentials,
        correlation_id=correlation_id.value if correlation_id else None,
    )
    # Recorded for the access log so every served request can be attributed.
    request.state.principal_subject = principal.subject
    return principal


PrincipalDep = Annotated[Principal, Depends(get_principal)]


def require_scope(scope: str) -> Callable[..., Principal]:
    """Build a dependency that enforces one scope."""

    def _dependency(
        principal: PrincipalDep,
        correlation_id: Annotated[CorrelationId, Depends(get_correlation_id)],
    ) -> Principal:
        principal.require(scope, correlation_id=correlation_id.value)
        return principal

    return _dependency


RequireRequestsRead = Annotated[Principal, Depends(require_scope(Scope.REQUESTS_READ))]
RequireRequestsWrite = Annotated[Principal, Depends(require_scope(Scope.REQUESTS_WRITE))]
RequireRequestsRetry = Annotated[Principal, Depends(require_scope(Scope.REQUESTS_RETRY))]
RequireRequestsCancel = Annotated[Principal, Depends(require_scope(Scope.REQUESTS_CANCEL))]
RequireProvidersRead = Annotated[Principal, Depends(require_scope(Scope.PROVIDERS_READ))]
