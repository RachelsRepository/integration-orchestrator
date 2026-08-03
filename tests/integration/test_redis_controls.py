"""Redis-backed resilience controls against a real Redis.

The Lua scripts are what make the circuit breaker and rate limiter correct under
concurrency. A unit test of the pure policy functions cannot catch a script that
reads one field and writes another without a WATCH, so these tests exist.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from integration_orchestrator.application.ports.security import CachedToken
from integration_orchestrator.config.settings import ProviderSettings
from integration_orchestrator.domain.enums import CircuitState
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.redis.circuit_breaker import RedisCircuitBreaker
from integration_orchestrator.infrastructure.redis.client import KeyBuilder
from integration_orchestrator.infrastructure.redis.locks import RedisLockManager
from integration_orchestrator.infrastructure.redis.rate_limiter import RedisRateLimiter
from integration_orchestrator.infrastructure.redis.token_cache import RedisTokenCache
from integration_orchestrator.infrastructure.resilience.policies import ConfiguredPolicyProvider

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

NORTHSTAR = ProviderSlug.parse("northstar")


def _settings(
    *,
    failure_threshold: int = 3,
    open_seconds: float = 30.0,
    half_open_max_probes: int = 1,
    success_threshold: int = 2,
    rate_limit_per_second: float = 5.0,
    rate_limit_burst: int = 5,
) -> dict[str, ProviderSettings]:
    return {
        "northstar": ProviderSettings(
            display_name="Northstar Connect",
            circuit_failure_threshold=failure_threshold,
            circuit_open_seconds=open_seconds,
            circuit_half_open_max_probes=half_open_max_probes,
            circuit_success_threshold=success_threshold,
            rate_limit_per_second=rate_limit_per_second,
            rate_limit_burst=rate_limit_burst,
        )
    }


async def test_a_circuit_opens_after_the_configured_number_of_failures(
    redis_client: object,
) -> None:
    settings = _settings(failure_threshold=3)
    breaker = RedisCircuitBreaker(
        redis_client,  # type: ignore[arg-type]
        KeyBuilder("test"),
        ConfiguredPolicyProvider(settings),
    )

    for _ in range(3):
        await breaker.record_failure(NORTHSTAR)

    assert (await breaker.state(NORTHSTAR)).state is CircuitState.OPEN
    assert await breaker.allow(NORTHSTAR) is False


async def test_an_open_circuit_admits_a_probe_after_the_cool_down(
    redis_client: object,
) -> None:
    settings = _settings(failure_threshold=1, open_seconds=0.05)
    breaker = RedisCircuitBreaker(
        redis_client,  # type: ignore[arg-type]
        KeyBuilder("test"),
        ConfiguredPolicyProvider(settings),
    )
    await breaker.record_failure(NORTHSTAR)
    assert await breaker.allow(NORTHSTAR) is False

    await asyncio.sleep(0.08)

    assert await breaker.allow(NORTHSTAR) is True
    assert (await breaker.state(NORTHSTAR)).state is CircuitState.HALF_OPEN


async def test_half_open_only_admits_the_configured_number_of_probes(
    redis_client: object,
) -> None:
    settings = _settings(failure_threshold=1, open_seconds=0.01, half_open_max_probes=1)
    breaker = RedisCircuitBreaker(
        redis_client,  # type: ignore[arg-type]
        KeyBuilder("test"),
        ConfiguredPolicyProvider(settings),
    )
    await breaker.record_failure(NORTHSTAR)
    await asyncio.sleep(0.03)

    first, second = await asyncio.gather(breaker.allow(NORTHSTAR), breaker.allow(NORTHSTAR))

    assert sorted([first, second]) == [False, True]


async def test_enough_successes_close_a_half_open_circuit(redis_client: object) -> None:
    settings = _settings(
        failure_threshold=1, open_seconds=0.01, half_open_max_probes=2, success_threshold=2
    )
    breaker = RedisCircuitBreaker(
        redis_client,  # type: ignore[arg-type]
        KeyBuilder("test"),
        ConfiguredPolicyProvider(settings),
    )
    await breaker.record_failure(NORTHSTAR)
    await asyncio.sleep(0.03)
    assert await breaker.allow(NORTHSTAR) is True

    await breaker.record_success(NORTHSTAR)
    await breaker.record_success(NORTHSTAR)

    assert (await breaker.state(NORTHSTAR)).state is CircuitState.CLOSED


async def test_a_rate_limiter_admits_up_to_its_burst_and_then_refuses(
    redis_client: object,
) -> None:
    settings = _settings(rate_limit_per_second=1.0, rate_limit_burst=3)
    limiter = RedisRateLimiter(
        redis_client,  # type: ignore[arg-type]
        KeyBuilder("test"),
        settings,
    )

    decisions = [await limiter.acquire(NORTHSTAR) for _ in range(4)]

    assert decisions == [True, True, True, False]


async def test_a_distributed_lock_is_held_by_exactly_one_caller(
    redis_client: object,
) -> None:
    locks = RedisLockManager(redis_client, KeyBuilder("test"))  # type: ignore[arg-type]

    async with locks.lock("token-refresh:northstar", ttl_seconds=5, wait_seconds=0) as first:
        assert first is True
        async with locks.lock("token-refresh:northstar", ttl_seconds=5, wait_seconds=0) as second:
            assert second is False


async def test_a_token_is_cached_and_retrieved(redis_client: object) -> None:
    from pydantic import SecretStr

    from integration_orchestrator.infrastructure.security.token_crypto import derive_fernet

    cache = RedisTokenCache(
        redis_client,  # type: ignore[arg-type]
        KeyBuilder("test"),
        fernet=derive_fernet(SecretStr("test-token-encryption-secret")),
    )
    token = CachedToken(
        value="access-token-1",
        expires_at=datetime.now(tz=UTC) + timedelta(hours=1),
    )

    await cache.set(NORTHSTAR, token)
    cached = await cache.get(NORTHSTAR)

    assert cached is not None
    assert cached.value == "access-token-1"
    # Raw Redis value must not contain the plaintext bearer token.
    raw = await redis_client.get("test:token:northstar")  # type: ignore[attr-defined]
    assert raw is not None
    assert "access-token-1" not in str(raw)


async def test_an_already_expired_token_is_not_written(redis_client: object) -> None:
    from pydantic import SecretStr

    from integration_orchestrator.infrastructure.security.token_crypto import derive_fernet

    cache = RedisTokenCache(
        redis_client,  # type: ignore[arg-type]
        KeyBuilder("test"),
        fernet=derive_fernet(SecretStr("test-token-encryption-secret")),
    )

    await cache.set(
        NORTHSTAR,
        CachedToken(value="stale", expires_at=datetime.now(tz=UTC) - timedelta(seconds=1)),
    )

    assert await cache.get(NORTHSTAR) is None


async def test_redis_unavailability_fails_the_circuit_open(
    redis_client: object,
) -> None:
    """A Redis outage must degrade protection, never become a total outage."""
    await redis_client.aclose()  # type: ignore[attr-defined]

    breaker = RedisCircuitBreaker(
        redis_client,  # type: ignore[arg-type]
        KeyBuilder("test"),
        ConfiguredPolicyProvider(_settings()),
    )

    assert await breaker.allow(NORTHSTAR) is True
