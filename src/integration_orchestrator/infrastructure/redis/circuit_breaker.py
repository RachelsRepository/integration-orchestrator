"""Redis-backed, provider-scoped circuit breaker.

**Why distributed state.** A process-local breaker is cheaper and has no external
dependency, but every replica has to independently rediscover that a provider is
down. With twenty API pods and a worker fleet, a provider outage produces twenty
separate failure storms before anyone stops calling, and each replica keeps
probing on its own schedule. Sharing the state in Redis means the fleet reacts
once. The cost is a network round trip on the hot path and a dependency that can
itself fail.

**Failing open.** Every Redis error is treated as "allow the call". A breaker
that blocks traffic when its own storage is unavailable converts a Redis outage
into a total outage, which is strictly worse than temporarily losing protection.
The provider timeout and bulkhead still bound the damage in that state.

**State transitions are Lua.** Read-modify-write over several fields has to be
atomic or two concurrent failures can both believe they crossed the threshold and
the half-open probe slot can be handed to more than one caller. The Lua scripts
mirror the pure decision functions in ``domain.policies``, which are what the
unit tests exercise; ``tests/unit/test_circuit_breaker_policy.py`` and the Redis
integration tests check that the two agree.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from redis.asyncio import Redis
from redis.exceptions import RedisError

from integration_orchestrator.application.ports.resilience import PolicyProvider
from integration_orchestrator.domain.enums import CircuitState
from integration_orchestrator.domain.policies import CircuitSnapshot
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.redis.client import KeyBuilder

logger = logging.getLogger(__name__)

# Keys expire after a period of inactivity, which is what bounds the failure
# window: failures that stopped arriving long enough ago stop counting.
_STATE_TTL_MULTIPLIER = 10
_MIN_STATE_TTL_SECONDS = 300

_ALLOW_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local open_seconds = tonumber(ARGV[2])
local half_open_max = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local state = redis.call('HGET', key, 'state')
if not state or state == 'closed' then
    return {1, 'closed'}
end

if state == 'open' then
    local opened_at = tonumber(redis.call('HGET', key, 'opened_at') or '0')
    if now - opened_at >= open_seconds then
        redis.call('HSET', key,
            'state', 'half_open',
            'half_open_probes', 1,
            'success_count', 0,
            'last_transition_at', now)
        redis.call('EXPIRE', key, ttl)
        return {1, 'half_open'}
    end
    return {0, 'open'}
end

local probes = tonumber(redis.call('HGET', key, 'half_open_probes') or '0')
if probes < half_open_max then
    redis.call('HINCRBY', key, 'half_open_probes', 1)
    redis.call('EXPIRE', key, ttl)
    return {1, 'half_open'}
end
return {0, 'half_open'}
"""

_FAILURE_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local state = redis.call('HGET', key, 'state') or 'closed'

if state == 'half_open' then
    redis.call('HSET', key,
        'state', 'open',
        'opened_at', now,
        'last_transition_at', now,
        'half_open_probes', 0,
        'success_count', 0)
    redis.call('EXPIRE', key, ttl)
    return {'open', 'half_open'}
end

if state == 'open' then
    redis.call('EXPIRE', key, ttl)
    return {'open', 'open'}
end

local failures = redis.call('HINCRBY', key, 'failure_count', 1)
redis.call('EXPIRE', key, ttl)
if failures >= threshold then
    redis.call('HSET', key,
        'state', 'open',
        'opened_at', now,
        'last_transition_at', now,
        'half_open_probes', 0,
        'success_count', 0)
    return {'open', 'closed'}
end
return {'closed', 'closed'}
"""

_SUCCESS_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local success_threshold = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local state = redis.call('HGET', key, 'state') or 'closed'

if state == 'half_open' then
    local successes = redis.call('HINCRBY', key, 'success_count', 1)
    if successes >= success_threshold then
        redis.call('HSET', key,
            'state', 'closed',
            'failure_count', 0,
            'success_count', 0,
            'half_open_probes', 0,
            'last_transition_at', now)
        redis.call('EXPIRE', key, ttl)
        return {'closed', 'half_open'}
    end
    redis.call('EXPIRE', key, ttl)
    return {'half_open', 'half_open'}
end

if state == 'open' then
    redis.call('EXPIRE', key, ttl)
    return {'open', 'open'}
end

redis.call('HSET', key, 'state', 'closed', 'failure_count', 0, 'success_count', 0)
redis.call('EXPIRE', key, ttl)
return {'closed', 'closed'}
"""


class RedisCircuitBreaker:
    """Provider-scoped circuit breaker sharing state across every replica."""

    def __init__(
        self,
        client: Redis,
        keys: KeyBuilder,
        policies: PolicyProvider,
    ) -> None:
        self._client = client
        self._keys = keys
        self._policies = policies

    async def state(self, provider: ProviderSlug) -> CircuitSnapshot:
        try:
            data = await self._client.hgetall(self._keys.circuit(provider.value))
        except RedisError:
            logger.warning(
                "could not read circuit breaker state; reporting closed",
                extra={"provider": provider.value},
            )
            return CircuitSnapshot(state=CircuitState.CLOSED)
        # decode_responses=True on the client, so the runtime values are str.
        return _snapshot_from_hash({str(key): str(value) for key, value in data.items()})

    async def allow(self, provider: ProviderSlug) -> bool:
        policy = self._policies.circuit_policy(provider)
        try:
            allowed, _ = await self._client.eval(
                _ALLOW_SCRIPT,
                1,
                self._keys.circuit(provider.value),
                _now_epoch(),
                policy.open_seconds,
                policy.half_open_max_probes,
                _ttl_seconds(policy.open_seconds),
            )
        except RedisError:
            logger.warning(
                "circuit breaker check failed; allowing the call",
                extra={"provider": provider.value},
            )
            return True
        return bool(int(allowed))

    async def record_failure(self, provider: ProviderSlug) -> CircuitState:
        policy = self._policies.circuit_policy(provider)
        try:
            new_state, _previous = await self._client.eval(
                _FAILURE_SCRIPT,
                1,
                self._keys.circuit(provider.value),
                _now_epoch(),
                policy.failure_threshold,
                _ttl_seconds(policy.open_seconds),
            )
        except RedisError:
            logger.warning(
                "could not record a circuit breaker failure",
                extra={"provider": provider.value},
            )
            return CircuitState.CLOSED
        return CircuitState(new_state)

    async def record_success(self, provider: ProviderSlug) -> CircuitState:
        policy = self._policies.circuit_policy(provider)
        try:
            new_state, _previous = await self._client.eval(
                _SUCCESS_SCRIPT,
                1,
                self._keys.circuit(provider.value),
                _now_epoch(),
                policy.success_threshold,
                _ttl_seconds(policy.open_seconds),
            )
        except RedisError:
            logger.warning(
                "could not record a circuit breaker success",
                extra={"provider": provider.value},
            )
            return CircuitState.CLOSED
        return CircuitState(new_state)

    async def reset(self, provider: ProviderSlug) -> None:
        try:
            await self._client.delete(self._keys.circuit(provider.value))
        except RedisError:
            logger.warning("could not reset a circuit breaker", extra={"provider": provider.value})


def _snapshot_from_hash(data: dict[str, str]) -> CircuitSnapshot:
    if not data:
        return CircuitSnapshot(state=CircuitState.CLOSED)
    return CircuitSnapshot(
        state=CircuitState(data.get("state", CircuitState.CLOSED.value)),
        failure_count=int(data.get("failure_count", 0)),
        success_count=int(data.get("success_count", 0)),
        opened_at=_epoch_to_datetime(data.get("opened_at")),
        last_transition_at=_epoch_to_datetime(data.get("last_transition_at")),
        half_open_probes=int(data.get("half_open_probes", 0)),
    )


def _epoch_to_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromtimestamp(float(value), tz=UTC)


def _now_epoch() -> float:
    return datetime.now(tz=UTC).timestamp()


def _ttl_seconds(open_seconds: float) -> int:
    return max(_MIN_STATE_TTL_SECONDS, int(open_seconds * _STATE_TTL_MULTIPLIER))
