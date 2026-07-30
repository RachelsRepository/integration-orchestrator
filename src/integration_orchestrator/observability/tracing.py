"""OpenTelemetry tracing.

Spans cover the path a request actually takes: the API handler, the use case, the
repository work, the provider call, and the outbox publication that follows.
Because the same correlation id is attached as a span attribute and emitted on
every log line, a trace and its logs can be pivoted between without guessing.

Span attributes carry identifiers and outcomes only. Payloads and credentials are
never attached: trace backends are widely readable inside an organisation and
have retention policies nobody audits.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import Span, Status, StatusCode

from integration_orchestrator.config.settings import ObservabilitySettings
from integration_orchestrator.observability.correlation import current_correlation_id

logger = logging.getLogger(__name__)

TRACER_NAME = "integration-orchestrator"

_provider: TracerProvider | None = None


def configure_tracing(
    settings: ObservabilitySettings,
    *,
    service_name: str,
    service_version: str,
    environment: str,
) -> None:
    """Install the global tracer provider.

    Tracing is off by default. An exporter that cannot reach its collector
    retries in the background and, with a synchronous processor, would add
    latency to every request. The batch processor plus an explicit opt-in keeps
    that failure mode out of the default configuration.
    """
    global _provider
    if not settings.tracing_enabled:
        logger.info("tracing is disabled")
        return
    if _provider is not None:
        return

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": service_version,
            "deployment.environment": environment,
        }
    )
    provider = TracerProvider(
        resource=resource,
        # Parent-based so a sampling decision made upstream is honoured, which is
        # what keeps a distributed trace from being half-recorded.
        sampler=ParentBased(TraceIdRatioBased(settings.trace_sample_ratio)),
    )
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otlp_endpoint, insecure=True))
    )
    trace.set_tracer_provider(provider)
    _provider = provider
    logger.info("tracing enabled", extra={"otlp_endpoint": settings.otlp_endpoint})


def shutdown_tracing() -> None:
    """Flush pending spans during shutdown."""
    global _provider
    if _provider is not None:
        _provider.shutdown()
        _provider = None


def get_tracer() -> trace.Tracer:
    return trace.get_tracer(TRACER_NAME)


@contextmanager
def span(name: str, **attributes: Any) -> Iterator[Span]:
    """Start a span carrying the current correlation id.

    Falls back to a no-op span when tracing is disabled, so call sites never
    need to check whether tracing is configured.
    """
    tracer = get_tracer()
    with tracer.start_as_current_span(name) as active:
        correlation_id = current_correlation_id()
        if correlation_id:
            active.set_attribute("correlation_id", correlation_id)
        for key, value in attributes.items():
            if value is not None:
                active.set_attribute(key, value)
        try:
            yield active
        except Exception as exc:
            active.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            active.record_exception(exc)
            raise


def instrument_application(app: Any, *, engine: Any = None) -> None:
    """Attach automatic instrumentation to the app, HTTP client and database.

    Instrumentation is best-effort. A missing or incompatible instrumentation
    package must not stop the service starting, so failures are logged and
    swallowed here and nowhere else.
    """
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app, excluded_urls="/health/live,/health/ready,/metrics")
    except Exception:
        logger.warning("could not instrument the API layer", exc_info=True)

    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()
    except Exception:
        logger.warning("could not instrument the HTTP client", exc_info=True)

    if engine is not None:
        try:
            from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

            SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)
        except Exception:
            logger.warning("could not instrument the database engine", exc_info=True)
