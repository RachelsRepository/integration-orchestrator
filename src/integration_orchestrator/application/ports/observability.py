"""Metrics port.

The application layer records a handful of business-level measurements — retries
scheduled, retries exhausted, reconciliation mismatches, webhook outcomes — that
have no meaningful home in infrastructure. It reaches them through this port so
it never imports a metrics client.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable


@runtime_checkable
class MetricsSink(Protocol):
    """Records counters, histograms and gauges by name."""

    def increment(
        self,
        name: str,
        *,
        labels: Mapping[str, str] | None = None,
        amount: float = 1.0,
    ) -> None:
        """Add to a counter."""
        ...

    def observe(
        self,
        name: str,
        value: float,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        """Record an observation in a histogram."""
        ...

    def set_gauge(
        self,
        name: str,
        value: float,
        *,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        """Set a gauge to an absolute value."""
        ...
