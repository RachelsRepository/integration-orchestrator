"""Redis client construction and namespacing."""

from __future__ import annotations

import logging

from redis.asyncio import Redis

from integration_orchestrator.config.settings import RedisSettings

logger = logging.getLogger(__name__)


def create_redis(settings: RedisSettings) -> Redis:
    """Build the shared async Redis client.

    Timeouts are mandatory. Redis backs the circuit breaker and rate limiter,
    both of which sit directly in the request path, so an unbounded Redis wait
    would convert a Redis hiccup into a platform-wide stall — the exact failure
    those components exist to prevent.
    """
    return Redis.from_url(
        settings.url,
        socket_timeout=settings.socket_timeout_seconds,
        socket_connect_timeout=settings.socket_connect_timeout_seconds,
        max_connections=settings.max_connections,
        decode_responses=True,
        health_check_interval=30,
    )


async def close_redis(client: Redis) -> None:
    """Close the client and its connection pool during shutdown."""
    await client.aclose()
    logger.info("redis connection pool closed")


class KeyBuilder:
    """Builds namespaced Redis keys.

    Namespacing keeps several environments safely on one Redis instance and,
    more importantly, makes it obvious which keys belong to this service when
    someone is looking at a shared cluster during an incident.
    """

    def __init__(self, namespace: str) -> None:
        self._namespace = namespace.rstrip(":")

    def build(self, *parts: str) -> str:
        return ":".join([self._namespace, *parts])

    def token(self, provider: str) -> str:
        return self.build("token", provider)

    def circuit(self, provider: str) -> str:
        return self.build("circuit", provider)

    def rate_limit(self, provider: str) -> str:
        return self.build("ratelimit", provider)

    def lock(self, resource: str) -> str:
        return self.build("lock", resource)

    def webhook_replay(self, provider: str, signature_digest: str) -> str:
        return self.build("webhook-replay", provider, signature_digest)
