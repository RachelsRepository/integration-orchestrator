"""Ports for credential handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from integration_orchestrator.domain.value_objects import ProviderSlug


@dataclass(frozen=True, slots=True)
class CachedToken:
    """A provider access token together with its expiry."""

    value: str
    expires_at: datetime
    token_type: str = "Bearer"

    def is_fresh(self, *, now: datetime, leeway_seconds: int) -> bool:
        """Report whether the token is still usable ``leeway_seconds`` from now.

        The leeway is what prevents a token expiring mid-flight: a token with two
        seconds left is treated as already expired rather than being sent on a
        request that takes three seconds to complete.
        """
        return (self.expires_at - now).total_seconds() > leeway_seconds

    @property
    def authorization_header(self) -> str:
        return f"{self.token_type} {self.value}"


@runtime_checkable
class TokenCache(Protocol):
    """Shared cache of provider access tokens.

    Shared rather than per-process so that scaling out does not multiply the
    load on providers' token endpoints, several of which rate limit far more
    aggressively than their operational APIs.
    """

    async def get(self, provider: ProviderSlug) -> CachedToken | None:
        """Return a cached token, or ``None`` when absent or expired."""
        ...

    async def set(self, provider: ProviderSlug, token: CachedToken) -> None:
        """Store a token with an expiry derived from its lifetime."""
        ...

    async def invalidate(self, provider: ProviderSlug) -> None:
        """Drop a cached token.

        Called when a provider rejects a token that had not yet expired, which
        happens after a credential rotation or a provider-side revocation.
        """
        ...
