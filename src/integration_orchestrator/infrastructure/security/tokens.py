"""Internal API bearer token verification.

The orchestrator is a service-to-service platform, so it *verifies* tokens rather
than issuing them: an external identity provider signs them, and this module
checks signature, issuer, audience and expiry before extracting scopes.

A minting helper is included for local development only. It is guarded so it
cannot run against an RS256 configuration and is never reachable from an HTTP
route; it exists so ``make token`` can produce a token for the demonstration
script without standing up an identity provider.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import jwt
from jwt import PyJWTError

from integration_orchestrator.config.settings import JWTSettings
from integration_orchestrator.domain.errors import AuthenticationError, AuthorizationError

logger = logging.getLogger(__name__)


class Scope:
    """The scopes the internal API understands.

    Reads and writes are separated so that a dashboard or a support tool can be
    granted visibility without also gaining the ability to create provider-side
    effects. Retry and cancel are separate from create because they act on work
    that already exists and are the operations an operator performs during an
    incident.
    """

    REQUESTS_READ = "requests:read"
    REQUESTS_WRITE = "requests:write"
    REQUESTS_RETRY = "requests:retry"
    REQUESTS_CANCEL = "requests:cancel"
    PROVIDERS_READ = "providers:read"

    ALL: frozenset[str] = frozenset(
        {REQUESTS_READ, REQUESTS_WRITE, REQUESTS_RETRY, REQUESTS_CANCEL, PROVIDERS_READ}
    )


#: Coarse roles mapped to scope sets. Roles keep tokens readable while
#: authorization decisions stay expressed in terms of individual scopes.
ROLE_SCOPES: dict[str, frozenset[str]] = {
    "viewer": frozenset({Scope.REQUESTS_READ, Scope.PROVIDERS_READ}),
    "integration-client": frozenset(
        {Scope.REQUESTS_READ, Scope.REQUESTS_WRITE, Scope.PROVIDERS_READ}
    ),
    "operator": frozenset(Scope.ALL),
}


@dataclass(frozen=True, slots=True)
class Principal:
    """The authenticated caller."""

    subject: str
    scopes: frozenset[str]
    roles: frozenset[str] = field(default_factory=frozenset)
    token_id: str | None = None

    def has_scope(self, scope: str) -> bool:
        return scope in self.scopes

    def require(self, scope: str, *, correlation_id: str | None = None) -> None:
        """Raise unless the caller holds ``scope``."""
        if not self.has_scope(scope):
            raise AuthorizationError(scope, correlation_id=correlation_id)


class TokenVerifier:
    """Validates internal bearer tokens."""

    def __init__(self, settings: JWTSettings) -> None:
        self._settings = settings
        self._key = self._load_key()

    def _load_key(self) -> str:
        if self._settings.algorithm == "RS256":
            if self._settings.public_key_path is None:  # pragma: no cover - validated in settings
                raise ValueError("RS256 verification requires a public key path")
            return self._settings.public_key_path.read_text(encoding="utf-8")
        return self._settings.secret.get_secret_value()

    def verify(self, token: str, *, correlation_id: str | None = None) -> Principal:
        """Decode and validate a token, returning the authenticated principal."""
        try:
            claims: dict[str, Any] = jwt.decode(
                token,
                self._key,
                algorithms=[self._settings.algorithm],
                audience=self._settings.audience,
                issuer=self._settings.issuer,
                leeway=self._settings.leeway_seconds,
                # Explicit rather than implicit: an attacker who can suppress a
                # claim should not be able to suppress its validation.
                options={
                    "require": ["exp", "iat", "sub", "aud", "iss"],
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_aud": True,
                    "verify_iss": True,
                },
            )
        except PyJWTError as exc:
            # The reason is logged but not returned. Telling an unauthenticated
            # caller *why* their token failed is free reconnaissance.
            logger.info(
                "rejected a bearer token",
                extra={"reason": type(exc).__name__, "correlation_id": correlation_id},
            )
            raise AuthenticationError(
                "the supplied bearer token is not valid",
                correlation_id=correlation_id,
            ) from exc

        return Principal(
            subject=str(claims["sub"]),
            scopes=_resolve_scopes(claims),
            roles=frozenset(_as_list(claims.get("roles"))),
            token_id=claims.get("jti"),
        )


def _resolve_scopes(claims: dict[str, Any]) -> frozenset[str]:
    """Combine explicit scopes with those implied by roles.

    Unknown scopes are dropped rather than carried, so a token from a broader
    identity provider cannot smuggle in a permission this service never defined.
    """
    scopes = set(_as_list(claims.get("scope")))
    scopes.update(_as_list(claims.get("scopes")))
    for role in _as_list(claims.get("roles")):
        scopes.update(ROLE_SCOPES.get(role, frozenset()))
    return frozenset(scope for scope in scopes if scope in Scope.ALL)


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for item in value.split(" ") if item]
    if isinstance(value, (list, tuple, set, frozenset)):
        return [str(item) for item in value]
    return []


def issue_local_token(
    settings: JWTSettings,
    *,
    subject: str,
    roles: list[str],
    ttl_seconds: int | None = None,
) -> str:
    """Mint a token for local development and tests.

    Refuses to run under RS256 because a symmetric minting path would need the
    private key, and this helper must never be a reason for a private key to be
    present in a running service.
    """
    if settings.algorithm != "HS256":
        raise ValueError("local token minting is only supported with HS256")

    now = int(time.time())
    lifetime = ttl_seconds or settings.access_token_ttl_seconds
    payload = {
        "sub": subject,
        "iss": settings.issuer,
        "aud": settings.audience,
        "iat": now,
        "nbf": now,
        "exp": now + lifetime,
        "roles": roles,
        "scope": " ".join(sorted(_scopes_for_roles(roles))),
    }
    return jwt.encode(payload, settings.secret.get_secret_value(), algorithm="HS256")


def _scopes_for_roles(roles: list[str]) -> set[str]:
    scopes: set[str] = set()
    for role in roles:
        scopes.update(ROLE_SCOPES.get(role, frozenset()))
    return scopes
