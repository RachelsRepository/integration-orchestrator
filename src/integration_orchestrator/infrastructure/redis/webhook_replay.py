"""Redis-backed webhook signature replay protection.

Signature verification proves authenticity; this cache proves freshness of the
*signature bytes themselves*. A captured webhook that still falls inside the
cryptographic replay window would otherwise pass verification on every delivery.
Recording a digest of the signature for a short TTL closes that gap without
storing the signature (a stored signature is a reusable credential).

When Redis is unavailable the guard fails open and logs: event-id uniqueness in
PostgreSQL remains the durable idempotency layer. Fail-closed would turn a Redis
outage into a complete webhook outage, which is worse than a temporary weakening
of the secondary replay defence.
"""

from __future__ import annotations

import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

from integration_orchestrator.application.ports.security import signature_replay_digest
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.redis.client import KeyBuilder

logger = logging.getLogger(__name__)


class RedisWebhookReplayGuard:
    """SET NX PX claim over a digest of the signature header."""

    def __init__(self, client: Redis, keys: KeyBuilder, *, ttl_seconds: int) -> None:
        self._client = client
        self._keys = keys
        self._ttl_ms = int(ttl_seconds * 1000)

    async def claim(self, provider: ProviderSlug, signature: str) -> bool:
        digest = signature_replay_digest(signature)
        key = self._keys.webhook_replay(provider.value, digest)
        try:
            acquired = await self._client.set(key, "1", nx=True, px=self._ttl_ms)
        except RedisError:
            logger.warning(
                "could not reach redis for webhook signature dedupe; "
                "falling back to durable event-id uniqueness",
                extra={"provider": provider.value},
            )
            return True
        return bool(acquired)
