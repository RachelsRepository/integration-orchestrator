# ADR 0005: Fail Open When Redis Is Unavailable

## Status

Accepted

## Context

Circuit breakers and rate limiters share state in Redis so a fleet of replicas
reacts once to a provider outage. Redis itself can fail. A breaker that refuses
traffic when it cannot read its own state converts a Redis outage into a total
outage.

## Decision

Redis outage behaviour is **risk-based**, not globally fail-open:

| Control | Outage policy | Rationale |
|---|---|---|
| Circuit breaker | Fail open (allow) | Avoid turning Redis loss into a total provider outage |
| Provider rate limit | Fail open (allow) | Same; timeouts/bulkheads still bound damage |
| OAuth refresh lock | Fail open (proceed unlocked) by default; `fail_closed=True` available | Stampede is wasteful; credential corruption is rare for client-credentials |
| Webhook signature replay (Redis) | Fail open to claim; Postgres event-id uniqueness remains authoritative | Receipt durability is the correctness boundary |
| Inbound API rate limit | Fail closed for mutations/webhooks; fail open for reads | Mutations must not bypass quotas by killing Redis |
| Workflow/step claim | PostgreSQL `FOR UPDATE SKIP LOCKED` | Correctness does not depend on Redis |

Every path emits metrics (`inbound_rate_limit_total` outcomes include `fail_open` / `fail_closed`) and structured logs. Readiness continues to report Redis health separately from request handling.

Cached provider access tokens in Redis are Fernet-encrypted at rest. Encryption
does not change the fail-open refresh policy: if Redis is unavailable the
orchestrator fetches a fresh token rather than blocking provider traffic.

## Consequences

- A Redis outage degrades protection rather than stopping the platform for
  provider calls, while high-risk inbound mutations fail closed.
- During that window, each replica may independently rediscover a provider
  outage. That is accepted as cheaper than a self-inflicted total failure.
- Operators still see Redis errors in logs and readiness can surface Redis
  health separately from request handling.
