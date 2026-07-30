# ADR 0003: ProviderGateway Abstraction

## Status

Accepted

## Context

Northstar, Meridian, and Cobalt differ in authentication, field names,
idempotency support, completion model, and webhook signing. Encoding those
differences in use cases would make every new provider a rewrite of core logic.

## Decision

Define a `ProviderGateway` protocol that speaks only normalized commands and
results. Each adapter translates to and from its provider. A descriptor
declares capabilities. A resilience decorator wraps every gateway before it is
registered.

## Consequences

- Use cases never branch on provider slug.
- Capability checks (`supports_cancellation`, `supports_status_lookup`) happen
  against the descriptor.
- Contract tests can assert the same properties against every adapter.
- The sandbox implements the same HTTP surfaces the adapters call, so local
  demos and contract tests share one truth.
