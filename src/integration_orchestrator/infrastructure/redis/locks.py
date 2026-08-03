"""Redis-backed distributed locking.

The lock is a single-instance mutex: ``SET key token NX PX ttl`` to acquire, and
a compare-and-delete Lua script to release. Comparing the token before deleting
is what stops a slow holder whose lease already expired from deleting a lock that
a different worker has since acquired.

This is not a consensus algorithm and does not claim to be safe under Redis
failover. It is used for coordination that is merely wasteful to get wrong —
two workers refreshing the same provider token, for example — never for
correctness that the database is not already enforcing. Anything that must be
exactly-once is protected by a database constraint or a row lock instead.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from types import TracebackType

from redis.asyncio import Redis
from redis.exceptions import RedisError

from integration_orchestrator.infrastructure.redis.client import KeyBuilder

logger = logging.getLogger(__name__)

# Release only if we still hold the lease.
_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

_ACQUIRE_POLL_SECONDS = 0.05


class RedisLock:
    """One acquisition attempt of a named lock."""

    def __init__(
        self,
        client: Redis,
        key: str,
        *,
        ttl_seconds: float,
        wait_seconds: float,
        fail_closed: bool = False,
    ) -> None:
        self._client = client
        self._key = key
        self._ttl_ms = int(ttl_seconds * 1000)
        self._wait_seconds = wait_seconds
        self._fail_closed = fail_closed
        self._token = secrets.token_urlsafe(24)
        self._held = False

    async def __aenter__(self) -> bool:
        self._held = await self._acquire()
        return self._held

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._held:
            await self._release()

    async def _acquire(self) -> bool:
        deadline = asyncio.get_running_loop().time() + self._wait_seconds
        while True:
            try:
                acquired = await self._client.set(self._key, self._token, nx=True, px=self._ttl_ms)
            except RedisError:
                logger.warning(
                    "could not reach redis to acquire a lock",
                    extra={"resource": self._key, "fail_closed": self._fail_closed},
                )
                if self._fail_closed:
                    raise
                # Fail open: treat as not held so callers that only use the lock
                # for stampede prevention still proceed.
                return False
            if acquired:
                return True
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(_ACQUIRE_POLL_SECONDS)

    async def _release(self) -> None:
        try:
            await self._client.eval(_RELEASE_SCRIPT, 1, self._key, self._token)
        except RedisError:
            # The lease expires on its own, so a failed release delays other
            # waiters but cannot deadlock them.
            logger.warning(
                "could not release a distributed lock; it will expire on its own",
                extra={"resource": self._key},
            )
        finally:
            self._held = False


class RedisLockManager:
    """Creates :class:`RedisLock` instances."""

    def __init__(self, client: Redis, keys: KeyBuilder) -> None:
        self._client = client
        self._keys = keys

    def lock(
        self,
        resource: str,
        *,
        ttl_seconds: float = 30.0,
        wait_seconds: float = 5.0,
        fail_closed: bool = False,
    ) -> RedisLock:
        return RedisLock(
            self._client,
            self._keys.lock(resource),
            ttl_seconds=ttl_seconds,
            wait_seconds=wait_seconds,
            fail_closed=fail_closed,
        )
