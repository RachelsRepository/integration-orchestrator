"""Application layer.

Contains use cases, orchestration services and the ports they depend on. This
package must not import FastAPI, SQLAlchemy, Redis, Kafka, httpx, or anything
from ``infrastructure``; an import-linter contract enforces that.
"""
