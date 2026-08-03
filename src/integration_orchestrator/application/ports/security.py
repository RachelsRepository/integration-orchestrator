"""Ports for credential handling and webhook replay protection."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from integration_orchestrator.domain.value_objects import ProviderSlug

#: Header names used by the fictional providers. A real onboarding adds its own.
_SIGNATURE_HEADERS = (
    "x-northstar-signature",
    "x-meridian-signature",
    "x-cobalt-signature",
)


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


@runtime_checkable
class WebhookReplayGuard(Protocol):
    """Claims a signature digest so the same bytes cannot be accepted twice."""

    async def claim(self, provider: ProviderSlug, signature: str) -> bool:
        """Return True when this is the first sighting within the TTL window."""
        ...


class NullWebhookReplayGuard:
    """No-op guard for tests that do not exercise Redis replay protection."""

    async def claim(self, provider: ProviderSlug, signature: str) -> bool:
        del provider, signature
        return True


def extract_signature_header(headers: dict[str, str]) -> str | None:
    """Return the first known provider signature header, if present."""
    lowered = {key.lower(): value for key, value in headers.items()}
    for name in _SIGNATURE_HEADERS:
        value = lowered.get(name)
        if value:
            return value
    return None


def signature_replay_digest(signature: str) -> str:
    """Return a short digest of a signature, safe to use as a replay cache key.

    The signature itself is never stored: a stored signature is a reusable
    credential for any system trusting the same secret. A hash is enough to
    recognise a repeat.
    """
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()[:32]
