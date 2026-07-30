"""Structured logging.

Logs are emitted as one JSON object per line. Free-text logs are unusable at
volume: correlating a provider failure with the retry it caused means filtering
on fields, and a log aggregator cannot filter on a sentence.

Every record automatically carries the service identity and whatever correlation
identifiers are bound to the current context, so call sites only supply what is
specific to the event they are describing.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

from integration_orchestrator.observability.correlation import context_fields
from integration_orchestrator.observability.redaction import redact

# Attributes the standard library puts on every record. Anything outside this set
# was supplied by the caller through ``extra=`` and belongs in the JSON output.
_STANDARD_RECORD_ATTRS: frozenset[str] = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)


class JsonFormatter(logging.Formatter):
    """Renders log records as single-line JSON."""

    def __init__(self, *, service: str, environment: str, version: str) -> None:
        super().__init__()
        self._service = service
        self._environment = environment
        self._version = version

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "service": self._service,
            "environment": self._environment,
            "version": self._version,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(context_fields())

        for key, value in record.__dict__.items():
            if key in _STANDARD_RECORD_ATTRS or key.startswith("_"):
                continue
            payload[key] = value

        if record.exc_info:
            # The type and message are safe and useful. The full traceback is
            # included because logs are internal; it never reaches an API caller.
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        return json.dumps(redact(payload), default=_fallback, separators=(",", ":"))


class ConsoleFormatter(logging.Formatter):
    """Human-readable formatter for local development only."""

    def __init__(self) -> None:
        super().__init__(fmt="%(asctime)s %(levelname)-8s %(name)s | %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _STANDARD_RECORD_ATTRS and not key.startswith("_")
        }
        extras.update(context_fields())
        if not extras:
            return base
        rendered = " ".join(f"{key}={value}" for key, value in sorted(extras.items()))
        return f"{base} [{rendered}]"


def configure_logging(
    *,
    level: str = "INFO",
    service: str = "integration-orchestrator",
    environment: str = "local",
    version: str = "0.1.0",
    console: bool = False,
) -> None:
    """Install the root logging configuration.

    Existing handlers are replaced rather than added to, so repeated calls (in
    tests, or when a worker starts inside an already-configured process) cannot
    produce duplicated output.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        ConsoleFormatter()
        if console
        else JsonFormatter(service=service, environment=environment, version=version)
    )

    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level.upper())

    # These libraries are informative at INFO but extremely noisy per request.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def _fallback(value: Any) -> str:
    """Render values the JSON encoder does not know about."""
    if isinstance(value, (set, frozenset)):
        return str(sorted(str(item) for item in value))
    return str(value)
