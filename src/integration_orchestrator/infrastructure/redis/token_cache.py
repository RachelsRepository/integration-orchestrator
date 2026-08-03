"""Redis-backed provider access token cache.

**The race this prevents.** When a token expires, every in-flight caller
discovers it at the same moment. Without coordination, all of them request a new
token simultaneously. Provider token endpoints are typically rate limited far
more aggressively than their operational APIs, so a fleet-wide stampede can be
throttled — turning an ordinary token refresh into an outage. Some providers also
invalidate the previous token on issue, so concurrent refreshes can invalidate
each other in a loop.

**How it is prevented.** Refresh is guarded by a distributed lock keyed on the
provider. One caller wins and fetches; the others wait briefly and re-read the
cache, finding the fresh token. If the lock cannot be taken at all — Redis is
down — the caller proceeds to fetch anyway, because a duplicate token request is
much less bad than failing every request to that provider.

**Proactive refresh.** A token is treated as expired while it still has a few
seconds of life left, so it never expires in the middle of a request that was
issued with it.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from cryptography.fernet import Fernet
from redis.asyncio import Redis
from redis.exceptions import RedisError

from integration_orchestrator.application.ports.security import CachedToken
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.redis.client import KeyBuilder
from integration_orchestrator.infrastructure.security.token_crypto import (
    decrypt_token_value,
    encrypt_token_value,
)

logger = logging.getLogger(__name__)

# Never store a token beyond this, even if a provider claims a longer lifetime.
_MAX_CACHE_SECONDS = 3600


class RedisTokenCache:
    """Shares provider access tokens across every process."""

    def __init__(self, client: Redis, keys: KeyBuilder, *, fernet: Fernet) -> None:
        self._client = client
        self._keys = keys
        self._fernet = fernet

    async def get(self, provider: ProviderSlug) -> CachedToken | None:
        try:
            raw = await self._client.get(self._keys.token(provider.value))
        except RedisError:
            logger.warning(
                "could not read a cached provider token; a fresh one will be fetched",
                extra={"provider": provider.value},
            )
            return None
        if raw is None:
            return None
        try:
            data = json.loads(raw)
            plaintext = decrypt_token_value(self._fernet, data["value"])
            if plaintext is None:
                logger.warning(
                    "discarding a token that could not be decrypted",
                    extra={"provider": provider.value},
                )
                await self.invalidate(provider)
                return None
            return CachedToken(
                value=plaintext,
                expires_at=datetime.fromisoformat(data["expires_at"]),
                token_type=data.get("token_type", "Bearer"),
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            # A malformed entry is not worth diagnosing at request time; drop it
            # and let the caller fetch a fresh token.
            logger.warning(
                "discarding a malformed cached token", extra={"provider": provider.value}
            )
            await self.invalidate(provider)
            return None

    async def set(self, provider: ProviderSlug, token: CachedToken) -> None:
        ttl = int((token.expires_at - datetime.now(tz=UTC)).total_seconds())
        if ttl <= 0:
            return
        payload = json.dumps(
            {
                "value": encrypt_token_value(self._fernet, token.value),
                "expires_at": token.expires_at.isoformat(),
                "token_type": token.token_type,
            }
        )
        try:
            await self._client.set(
                self._keys.token(provider.value),
                payload,
                ex=min(ttl, _MAX_CACHE_SECONDS),
            )
        except RedisError:
            # Caching is an optimisation. Losing it costs extra token requests,
            # not correctness.
            logger.warning("could not cache a provider token", extra={"provider": provider.value})

    async def invalidate(self, provider: ProviderSlug) -> None:
        try:
            await self._client.delete(self._keys.token(provider.value))
        except RedisError:
            logger.warning(
                "could not invalidate a cached provider token",
                extra={"provider": provider.value},
            )
