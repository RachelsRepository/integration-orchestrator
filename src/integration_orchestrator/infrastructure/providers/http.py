"""Shared HTTP plumbing for provider adapters.

Every provider call goes through :class:`ProviderHttpClient`, which applies the
timeout budget, injects authentication, records latency and outcome metrics,
performs the single credential-refresh retry on 401, and converts every failure
into a normalized provider error before it escapes.

Adapters therefore contain only translation logic: build a body, read a body, map
the fields. None of them re-implement error handling, which is what keeps their
behaviour consistent.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx

from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.config.settings import ProviderSettings
from integration_orchestrator.domain.errors import (
    ProviderAuthenticationError,
    ProviderError,
    ProviderResponseError,
)
from integration_orchestrator.domain.value_objects import ProviderSlug
from integration_orchestrator.infrastructure.providers.auth import ProviderAuthenticator
from integration_orchestrator.infrastructure.providers.errors import (
    classify_response,
    classify_transport_error,
)
from integration_orchestrator.observability.correlation import current_correlation_id

logger = logging.getLogger(__name__)

CORRELATION_HEADER = "X-Correlation-Id"
IDEMPOTENCY_HEADER = "Idempotency-Key"


@dataclass(frozen=True, slots=True)
class ProviderResponse:
    """A successful provider response, already parsed."""

    status_code: int
    body: dict[str, Any]
    headers: Mapping[str, str] = field(default_factory=dict)
    elapsed_ms: float = 0.0

    def metadata(self) -> dict[str, Any]:
        """Audit-safe facts about the exchange.

        Only the status, latency and the provider's own request identifier are
        kept. Response bodies stay inside the adapter: they are the most likely
        place for customer data to appear, and audit rows are read far more
        widely than provider payloads should be.
        """
        metadata: dict[str, Any] = {
            "http_status": self.status_code,
            "latency_ms": round(self.elapsed_ms, 2),
        }
        request_id = (
            self.headers.get("x-request-id")
            or self.headers.get("x-correlation-id")
            or self.headers.get("request-id")
        )
        if request_id:
            metadata["provider_request_id"] = request_id
        return metadata


class ProviderHttpClient:
    """Timeout-bounded, authenticated, instrumented HTTP access to one provider."""

    def __init__(
        self,
        *,
        slug: ProviderSlug,
        config: ProviderSettings,
        client: httpx.AsyncClient,
        authenticator: ProviderAuthenticator,
        metrics: MetricsSink,
    ) -> None:
        self._slug = slug
        self._config = config
        self._client = client
        self._authenticator = authenticator
        self._metrics = metrics
        self._timeout = httpx.Timeout(
            config.total_timeout_seconds,
            connect=config.connect_timeout_seconds,
        )

    @property
    def base_url(self) -> str:
        return self._config.base_url

    async def request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        operation: str = "unknown",
    ) -> ProviderResponse:
        """Perform one authenticated request against the provider."""
        url = f"{self._config.base_url}{path}"
        started = time.perf_counter()

        try:
            response = await self._send(
                method,
                url,
                json_body=json_body,
                headers=headers,
                idempotency_key=idempotency_key,
            )
            if response.status_code == 401 and self._authenticator.can_refresh:
                # Exactly one refresh-and-retry. A rotated or revoked token is
                # the common cause; anything else will fail the same way again,
                # and looping on 401 is how services get their credentials locked.
                logger.info(
                    "refreshing provider credentials after a 401 and retrying once",
                    extra={"provider": self._slug.value, "operation": operation},
                )
                await self._authenticator.invalidate()
                response = await self._send(
                    method,
                    url,
                    json_body=json_body,
                    headers=headers,
                    idempotency_key=idempotency_key,
                )
        except ProviderError:
            self._record(operation, outcome="error", started=started)
            raise
        except httpx.HTTPError as exc:
            self._record(operation, outcome="transport_error", started=started)
            raise classify_transport_error(
                exc, provider=self._slug.value, correlation_id=current_correlation_id()
            ) from exc

        elapsed_ms = (time.perf_counter() - started) * 1000

        if response.status_code >= 400:
            self._record(operation, outcome=f"http_{response.status_code}", started=started)
            raise classify_response(
                response,
                provider=self._slug.value,
                correlation_id=current_correlation_id(),
            )

        self._record(operation, outcome="success", started=started)
        return ProviderResponse(
            status_code=response.status_code,
            body=_parse_body(response, provider=self._slug.value),
            headers={key.lower(): value for key, value in response.headers.items()},
            elapsed_ms=elapsed_ms,
        )

    async def _send(
        self,
        method: str,
        url: str,
        *,
        json_body: dict[str, Any] | None,
        headers: Mapping[str, str] | None,
        idempotency_key: str | None,
    ) -> httpx.Response:
        request_headers: dict[str, str] = {"Accept": "application/json"}
        correlation_id = current_correlation_id()
        if correlation_id:
            request_headers[CORRELATION_HEADER] = correlation_id
        if idempotency_key:
            request_headers[IDEMPOTENCY_HEADER] = idempotency_key
        if headers:
            request_headers.update(headers)

        try:
            auth_headers = await self._authenticator.headers()
        except ProviderAuthenticationError:
            raise
        request_headers.update(auth_headers)

        return await self._client.request(
            method,
            url,
            json=json_body,
            headers=request_headers,
            timeout=self._timeout,
        )

    def _record(self, operation: str, *, outcome: str, started: float) -> None:
        duration = time.perf_counter() - started
        labels = {"provider": self._slug.value, "operation": operation}
        self._metrics.observe("provider_request_duration_seconds", duration, labels=labels)
        self._metrics.increment(
            "provider_http_requests_total", labels={**labels, "outcome": outcome}
        )


def _parse_body(response: httpx.Response, *, provider: str) -> dict[str, Any]:
    """Parse a JSON object body, tolerating an empty 204."""
    if response.status_code == 204 or not response.content:
        return {}
    try:
        body = response.json()
    except ValueError as exc:
        raise ProviderResponseError(
            "the provider returned a body that is not valid JSON",
            provider=provider,
            correlation_id=current_correlation_id(),
            metadata={"http_status": response.status_code},
        ) from exc
    if not isinstance(body, dict):
        raise ProviderResponseError(
            "the provider returned a JSON value that is not an object",
            provider=provider,
            correlation_id=current_correlation_id(),
            metadata={"http_status": response.status_code},
        )
    return body


def create_http_client(*, verify: bool = True) -> httpx.AsyncClient:
    """Build the shared httpx client.

    Connection limits are set explicitly. The default pool is generous enough
    that a slow provider can hold connections the other providers need, which is
    the same starvation the bulkhead exists to prevent one layer up.
    """
    return httpx.AsyncClient(
        verify=verify,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=40),
        follow_redirects=False,
        headers={"User-Agent": "integration-orchestrator/0.1"},
    )
