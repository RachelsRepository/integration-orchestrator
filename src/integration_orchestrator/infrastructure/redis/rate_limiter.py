"""Redis token-bucket rate limiter.

Rate limiting ourselves is cheaper than being rate limited. A provider's 429
costs a full round trip, consumes a bulkhead slot for its duration, and on some
providers counts against a quota that a burst can exhaust for everyone. Shedding
the request locally costs one Redis call.

The bucket refills continuously rather than in fixed windows, so a client that
has been idle can burst up to the bucket size and then settles to the sustained
rate. Fixed windows allow twice the intended rate across a window boundary.
"""

from __future__ import annotations

import logging
import time

from redis.asyncio import Redis
from redis.exceptions import RedisError

from integration_orchestrator.config.settings import ProviderSettings
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.redis.client import KeyBuilder

logger = logging.getLogger(__name__)

# Returns {allowed, seconds_until_next_token}. Refills lazily from the elapsed
# time since the last observation, which avoids a background refill process.
_TOKEN_BUCKET_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'updated_at')
local tokens = tonumber(bucket[1])
local updated_at = tonumber(bucket[2])

if tokens == nil or updated_at == nil then
    tokens = burst
    updated_at = now
end

local elapsed = math.max(0, now - updated_at)
tokens = math.min(burst, tokens + (elapsed * rate))

local allowed = 0
local retry_after = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
else
    retry_after = (1 - tokens) / rate
end

redis.call('HSET', key, 'tokens', tokens, 'updated_at', now)
redis.call('EXPIRE', key, ttl)
return {allowed, tostring(retry_after)}
"""


class RedisRateLimiter:
    """Per-provider client-side rate limiting."""

    def __init__(
        self,
        client: Redis,
        keys: KeyBuilder,
        provider_settings: dict[str, ProviderSettings],
    ) -> None:
        self._client = client
        self._keys = keys
        self._settings = provider_settings

    async def acquire(self, provider: ProviderSlug) -> bool:
        config = self._settings.get(provider.value)
        if config is None:
            return True
        try:
            allowed, _retry_after = await self._client.eval(
                _TOKEN_BUCKET_SCRIPT,
                1,
                self._keys.rate_limit(provider.value),
                _now(),
                config.rate_limit_per_second,
                config.rate_limit_burst,
                _ttl_seconds(config),
            )
        except RedisError:
            # Fail open, consistent with the circuit breaker: losing local rate
            # limiting is far less damaging than refusing all provider traffic.
            logger.warning(
                "rate limiter check failed; allowing the call",
                extra={"provider": provider.value},
            )
            return True
        return bool(int(allowed))

    async def retry_after(self, provider: ProviderSlug) -> float:
        config = self._settings.get(provider.value)
        if config is None:
            return 0.0
        try:
            data = await self._client.hmget(
                self._keys.rate_limit(provider.value), "tokens", "updated_at"
            )
        except RedisError:
            return 0.0
        tokens_raw, updated_raw = data
        if tokens_raw is None or updated_raw is None:
            return 0.0
        elapsed = max(0.0, _now() - float(updated_raw))
        tokens = min(
            float(config.rate_limit_burst),
            float(tokens_raw) + elapsed * config.rate_limit_per_second,
        )
        if tokens >= 1:
            return 0.0
        return (1 - tokens) / config.rate_limit_per_second


def _now() -> float:
    return time.time()


def _ttl_seconds(config: ProviderSettings) -> int:
    # Long enough that an idle bucket refills naturally rather than resetting to
    # full the moment it expires, which would leak burst capacity.
    return max(60, int((config.rate_limit_burst / config.rate_limit_per_second) * 4))
