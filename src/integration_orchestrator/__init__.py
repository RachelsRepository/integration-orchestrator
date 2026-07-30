"""Integration Orchestrator.

A production-style integration platform that connects an internal application to
multiple external service providers through a normalized integration layer.

The package is organised as a hexagonal architecture:

``domain``
    Framework-free entities, value objects, state transition rules and errors.
``application``
    Use cases and orchestration services expressed purely in terms of ports.
``infrastructure``
    Concrete adapters: PostgreSQL, Redis, Kafka and provider HTTP clients.
``api``
    The FastAPI transport layer.
``workers``
    Background processes for the outbox, retries, reconciliation and deferred
    webhook resolution.
``composition``
    The single composition root where ports are bound to adapters.
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
