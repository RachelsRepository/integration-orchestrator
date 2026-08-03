"""Prometheus metrics.

Every metric the platform exposes is declared here in one place. Declaring them
centrally rather than at each call site means the label sets cannot drift, and it
makes the cardinality of each metric reviewable: labels are provider, operation,
status and error category, all of which are bounded. Nothing is labelled with a
request id, an external reference, or any other unbounded value, because that is
how a metrics backend gets taken down.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from prometheus_client.core import REGISTRY as GLOBAL_REGISTRY

logger = logging.getLogger(__name__)

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# Latency buckets tuned for provider calls: sub-second matters, and anything
# beyond ten seconds is already past most of the configured timeout budgets.
_LATENCY_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)


class PrometheusMetrics:
    """Concrete metrics sink backed by ``prometheus_client``."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self._registry = registry if registry is not None else GLOBAL_REGISTRY
        self._counters: dict[str, Counter] = {}
        self._histograms: dict[str, Histogram] = {}
        self._gauges: dict[str, Gauge] = {}
        self._declare()

    # -- declaration --------------------------------------------------------

    def _declare(self) -> None:
        self._counter(
            "integration_requests_total",
            "Integration requests accepted by the API.",
            ("provider", "operation_type", "outcome"),
        )
        self._histogram(
            "integration_request_duration_seconds",
            "End-to-end duration of an integration request API call.",
            ("provider", "operation_type", "status"),
        )
        self._counter(
            "provider_requests_total",
            "Normalized provider operations attempted.",
            ("provider", "operation", "outcome"),
        )
        self._counter(
            "provider_http_requests_total",
            "Raw HTTP calls made to providers.",
            ("provider", "operation", "outcome"),
        )
        self._histogram(
            "provider_request_duration_seconds",
            "Latency of a single HTTP call to a provider.",
            ("provider", "operation"),
        )
        self._counter(
            "provider_failures_total",
            "Provider calls that failed, by normalized error category.",
            ("provider", "error_category"),
        )
        self._counter(
            "provider_timeouts_total",
            "Provider calls that exceeded their timeout budget.",
            ("provider",),
        )
        self._counter(
            "provider_rate_limits_total",
            "Provider responses indicating rate limiting.",
            ("provider",),
        )
        self._counter(
            "provider_bulkhead_rejections_total",
            "Calls rejected because a provider's concurrency limit was saturated.",
            ("provider",),
        )
        self._gauge(
            "provider_circuit_state",
            "Circuit breaker state: 0 closed, 1 half-open, 2 open.",
            ("provider",),
        )
        self._gauge(
            "provider_in_flight_requests",
            "Provider calls currently occupying a bulkhead slot in this process.",
            ("provider",),
        )
        self._counter("webhook_received_total", "Inbound webhooks received.", ("provider",))
        self._counter(
            "webhook_rejected_total",
            "Inbound webhooks that failed verification.",
            ("provider", "reason"),
        )
        self._counter(
            "webhook_duplicate_total",
            "Inbound webhooks recognised as duplicates.",
            ("provider",),
        )
        self._counter(
            "webhook_deferred_total",
            "Verified webhooks held because their operation was not known yet.",
            ("provider",),
        )
        self._counter(
            "retries_scheduled_total",
            "Retries scheduled after a retryable provider failure.",
            ("provider", "error_category"),
        )
        self._counter(
            "retries_exhausted_total",
            "Requests that ran out of retry attempts.",
            ("provider",),
        )
        self._counter(
            "reconciliation_mismatches_total",
            "Discrepancies found between local and provider state.",
            ("provider", "kind"),
        )
        self._counter(
            "idempotency_conflicts_total", "Idempotency keys reused with a different body.", ()
        )
        self._gauge("outbox_pending_total", "Outbox events awaiting publication.", ())
        self._gauge(
            "outbox_dead_lettered_total",
            "Outbox events that exhausted publication retries.",
            (),
        )
        self._counter(
            "outbox_dead_lettered_events_total",
            "Outbox events newly moved to the dead-letter state.",
            ("event_type",),
        )
        self._counter(
            "outbox_redriven_total",
            "Dead-lettered outbox events re-armed by an operator.",
            (),
        )
        self._counter(
            "outbox_purged_total",
            "Published outbox rows deleted by retention.",
            (),
        )
        self._counter(
            "outbox_publish_failures_total",
            "Outbox publication attempts that failed.",
            ("event_type",),
        )
        self._counter(
            "outbox_published_total", "Outbox events successfully published.", ("event_type",)
        )
        self._counter(
            "inbound_rate_limit_total",
            "Inbound API rate-limit decisions.",
            ("limit", "outcome"),
        )
        self._counter(
            "workflow_executions_total",
            "Workflow executions reaching a notable status.",
            ("definition", "status"),
        )
        self._histogram(
            "worker_batch_duration_seconds",
            "Duration of one worker batch.",
            ("worker",),
        )
        self._counter(
            "worker_batch_failures_total",
            "Worker batches that raised before completing.",
            ("worker",),
        )
        self._counter(
            "api_requests_total",
            "HTTP requests served by the internal API.",
            ("method", "route", "status"),
        )
        self._histogram(
            "api_request_duration_seconds",
            "Latency of an internal API request.",
            ("method", "route"),
        )

    def _counter(self, name: str, documentation: str, labels: tuple[str, ...]) -> None:
        self._counters[name] = Counter(
            name, documentation, labelnames=labels, registry=self._registry
        )

    def _histogram(self, name: str, documentation: str, labels: tuple[str, ...]) -> None:
        self._histograms[name] = Histogram(
            name,
            documentation,
            labelnames=labels,
            buckets=_LATENCY_BUCKETS,
            registry=self._registry,
        )

    def _gauge(self, name: str, documentation: str, labels: tuple[str, ...]) -> None:
        self._gauges[name] = Gauge(name, documentation, labelnames=labels, registry=self._registry)

    # -- sink interface -----------------------------------------------------

    def increment(
        self,
        name: str,
        *,
        labels: Mapping[str, str] | None = None,
        amount: float = 1.0,
    ) -> None:
        counter = self._counters.get(name)
        if counter is None:
            logger.debug("ignoring an undeclared counter", extra={"metric": name})
            return
        self._apply(counter, labels).inc(amount)

    def observe(self, name: str, value: float, *, labels: Mapping[str, str] | None = None) -> None:
        histogram = self._histograms.get(name)
        if histogram is None:
            logger.debug("ignoring an undeclared histogram", extra={"metric": name})
            return
        self._apply(histogram, labels).observe(value)

    def set_gauge(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        gauge = self._gauges.get(name)
        if gauge is None:
            logger.debug("ignoring an undeclared gauge", extra={"metric": name})
            return
        self._apply(gauge, labels).set(value)

    def render(self) -> bytes:
        """Render the registry in the Prometheus text exposition format."""
        return generate_latest(self._registry)

    @staticmethod
    def _apply(metric: Counter | Histogram | Gauge, labels: Mapping[str, str] | None):  # type: ignore[no-untyped-def]
        label_names = list(metric._labelnames)
        if not label_names:
            return metric
        values = {name: str((labels or {}).get(name, "unknown")) for name in label_names}
        return metric.labels(**values)


class NullMetrics:
    """No-op sink.

    Used in unit tests so that assertions target behaviour rather than metric
    bookkeeping, and so a test importing a use case does not have to build a
    Prometheus registry.
    """

    def increment(
        self,
        name: str,
        *,
        labels: Mapping[str, str] | None = None,
        amount: float = 1.0,
    ) -> None:
        return None

    def observe(self, name: str, value: float, *, labels: Mapping[str, str] | None = None) -> None:
        return None

    def set_gauge(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        return None
