# ADR 0005: Fail Open When Redis Is Unavailable

## Status

Accepted

## Context

Circuit breakers and rate limiters share state in Redis so a fleet of replicas
reacts once to a provider outage. Redis itself can fail. A breaker that refuses
traffic when it cannot read its own state converts a Redis outage into a total
outage.

## Decision

Every Redis error in the circuit breaker and rate limiter is treated as "allow
the call". The provider timeout budget and the in-process bulkhead still bound
damage. Distributed locks used for token refresh also proceed without the lock
rather than blocking authentication forever.

Correctness that must be exactly-once continues to be enforced by PostgreSQL
constraints and row locks, which are not optional.

## Consequences

- A Redis outage degrades protection rather than stopping the platform.
- During that window, each replica may independently rediscover a provider
  outage. That is accepted as cheaper than a self-inflicted total failure.
- Operators still see Redis errors in logs and readiness can surface Redis
  health separately from request handling.
