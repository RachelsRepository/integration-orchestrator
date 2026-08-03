"""Inbound rate limiter allow / reject / fail-closed behaviour."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import RedisError

from integration_orchestrator.infrastructure.redis.inbound_rate_limiter import InboundRateLimiter

pytestmark = [pytest.mark.unit, pytest.mark.asyncio]


class _Keys:
    def build(self, *parts: str) -> str:
        return ":".join(parts)


class _Metrics:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, str]]] = []

    def increment(self, name: str, *, labels: dict[str, str] | None = None) -> None:
        self.events.append((name, labels or {}))

    def observe(self, *args: Any, **kwargs: Any) -> None:
        return None


async def test_inbound_limiter_allows_when_redis_returns_allowed() -> None:
    client = AsyncMock()
    client.eval = AsyncMock(return_value=[1, 9.0])
    metrics = _Metrics()
    limiter = InboundRateLimiter(client, _Keys(), metrics)  # type: ignore[arg-type]
    assert await limiter.allow(scope="t1", name="mutations", rate_per_second=10, burst=20)
    assert metrics.events[-1][1]["outcome"] == "allowed"


async def test_inbound_limiter_rejects_when_bucket_empty() -> None:
    client = AsyncMock()
    client.eval = AsyncMock(return_value=[0, 0.0])
    metrics = _Metrics()
    limiter = InboundRateLimiter(client, _Keys(), metrics)  # type: ignore[arg-type]
    assert not await limiter.allow(scope="t1", name="mutations", rate_per_second=10, burst=20)
    assert metrics.events[-1][1]["outcome"] == "rejected"


async def test_inbound_limiter_fail_closed_on_redis_error() -> None:
    client = AsyncMock()
    client.eval = AsyncMock(side_effect=RedisError("down"))
    metrics = _Metrics()
    limiter = InboundRateLimiter(client, _Keys(), metrics)  # type: ignore[arg-type]
    assert not await limiter.allow(
        scope="t1", name="mutations", rate_per_second=10, burst=20, fail_closed=True
    )
    assert metrics.events[-1][1]["outcome"] == "fail_closed"


async def test_inbound_limiter_fail_open_for_reads() -> None:
    client = AsyncMock()
    client.eval = AsyncMock(side_effect=RedisError("down"))
    metrics = _Metrics()
    limiter = InboundRateLimiter(client, _Keys(), metrics)  # type: ignore[arg-type]
    assert await limiter.allow(
        scope="t1", name="reads", rate_per_second=100, burst=200, fail_closed=False
    )
    assert metrics.events[-1][1]["outcome"] == "fail_open"
