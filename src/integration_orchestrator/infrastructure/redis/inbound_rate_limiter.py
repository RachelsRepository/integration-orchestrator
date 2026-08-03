"""Redis-backed inbound API rate limiting.

Shared across API replicas. High-risk mutations fail closed when Redis is
unavailable so quota cannot be bypassed by taking Redis offline.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError

from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.infrastructure.redis.client import KeyBuilder

logger = logging.getLogger(__name__)

_TOKEN_BUCKET = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end
local elapsed = math.max(0, now - ts)
tokens = math.min(burst, tokens + elapsed * rate)
local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end
redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, ttl)
return {allowed, tokens}
"""


@dataclass(frozen=True, slots=True)
class InboundLimit:
    name: str
    rate_per_second: float
    burst: int
    fail_closed: bool = True


class InboundRateLimiter:
    def __init__(
        self,
        client: Redis,
        keys: KeyBuilder,
        metrics: MetricsSink,
    ) -> None:
        self._client = client
        self._keys = keys
        self._metrics = metrics

    async def allow(
        self,
        *,
        scope: str,
        name: str,
        rate_per_second: float,
        burst: int,
        fail_closed: bool = True,
    ) -> bool:
        key = self._keys.build("inbound-rl", name, scope)
        try:
            result = await self._client.eval(
                _TOKEN_BUCKET,
                1,
                key,
                str(time.time()),
                str(rate_per_second),
                str(burst),
                "1",
                "120",
            )
            allowed = bool(int(result[0]))
            self._metrics.increment(
                "inbound_rate_limit_total",
                labels={
                    "limit": name,
                    "outcome": "allowed" if allowed else "rejected",
                },
            )
            return allowed
        except RedisError:
            self._metrics.increment(
                "inbound_rate_limit_total",
                labels={
                    "limit": name,
                    "outcome": "fail_closed" if fail_closed else "fail_open",
                },
            )
            logger.warning(
                "inbound rate limiter redis failure",
                extra={"limit": name, "fail_closed": fail_closed},
            )
            return not fail_closed


# Convenience presets for middleware / tests.
MUTATION_LIMIT = InboundLimit("mutations", rate_per_second=20.0, burst=40, fail_closed=True)
WEBHOOK_LIMIT = InboundLimit("webhooks", rate_per_second=50.0, burst=100, fail_closed=True)
READ_LIMIT = InboundLimit("reads", rate_per_second=100.0, burst=200, fail_closed=False)
