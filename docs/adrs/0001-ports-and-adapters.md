# ADR 0001: Ports and Adapters

## Status

Accepted

## Context

The platform must talk to multiple external providers with inconsistent
authentication, payloads, and completion models, while remaining testable
without those providers and replaceable without rewriting use cases.

## Decision

Organize the codebase as Ports and Adapters (Hexagonal Architecture):

- The domain and application layers define behaviour and ports.
- Infrastructure implements those ports.
- The composition root is the only place that constructs concrete adapters.
- Import boundaries are enforced mechanically.

## Consequences

- Adding a provider means writing an adapter and registering it; use cases do
  not change.
- Unit tests can exercise orchestration against in-memory doubles.
- Contract tests exercise real adapters against the sandbox without involving
  the API layer.
- A violation of the layering rules fails CI rather than accumulating quietly.
