"""Fixtures that put real adapters in front of the real sandbox services.

The adapters here are the production classes, wired to the production HTTP
client, the production authenticators and the production error classification.
Only the socket is replaced: httpx talks to the sandbox ASGI application in
process instead of over a network. A hand-written stub of ``ProviderGateway``
would prove the orchestration works, but it would never catch a wrong header
name, a signature computed over the wrong bytes, or a status string the adapter
does not map — which is exactly what these tests exist to catch.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr

from integration_orchestrator.config.settings import (
    SANDBOX_COBALT_WEBHOOK_PUBLIC_KEY,
    AuthenticationType,
    ProviderSettings,
)
from integration_orchestrator.domain.contracts import (
    CreateProviderOperationCommand,
    InboundWebhook,
)
from integration_orchestrator.domain.enums import OperationType
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ExternalReference,
    ProviderSlug,
)
from integration_orchestrator.infrastructure.providers.auth import build_authenticator
from integration_orchestrator.infrastructure.providers.base import BaseProviderAdapter
from integration_orchestrator.infrastructure.providers.cobalt import CobaltAdapter
from integration_orchestrator.infrastructure.providers.http import ProviderHttpClient
from integration_orchestrator.infrastructure.providers.meridian import (
    API_KEY_HEADER as MERIDIAN_API_KEY_HEADER,
)
from integration_orchestrator.infrastructure.providers.meridian import (
    MeridianAdapter,
)
from integration_orchestrator.infrastructure.providers.northstar import NorthstarAdapter
from integration_orchestrator.infrastructure.providers.sandbox.app import create_sandbox_app
from integration_orchestrator.infrastructure.providers.sandbox.signing import (
    COBALT_CLIENT_ID,
    COBALT_CLIENT_SECRET,
    MERIDIAN_API_KEY,
    MERIDIAN_WEBHOOK_SECRET,
    NORTHSTAR_CLIENT_ID,
    NORTHSTAR_CLIENT_SECRET,
    NORTHSTAR_WEBHOOK_SECRET,
)
from tests.support.doubles import (
    MemoryTokenCache,
    NullLockManager,
    RecordingMetrics,
)

SANDBOX_ROOT = "http://sandbox.test"

REPLAY_WINDOW_SECONDS = 300


def provider_settings(slug: str) -> ProviderSettings:
    """Configuration pointing one provider at its sandbox service."""
    base = f"{SANDBOX_ROOT}/{slug}"
    if slug == "northstar":
        return ProviderSettings(
            display_name="Northstar Connect",
            authentication_type=AuthenticationType.OAUTH2_CLIENT_CREDENTIALS,
            base_url=base,
            oauth_token_url=f"{base}/oauth/token",
            client_id=NORTHSTAR_CLIENT_ID,
            client_secret=SecretStr(NORTHSTAR_CLIENT_SECRET),
            oauth_scope="operations.write",
            webhook_secret=SecretStr(NORTHSTAR_WEBHOOK_SECRET),
        )
    if slug == "meridian":
        return ProviderSettings(
            display_name="Meridian Services",
            authentication_type=AuthenticationType.API_KEY,
            base_url=base,
            api_key=SecretStr(MERIDIAN_API_KEY),
            webhook_secret=SecretStr(MERIDIAN_WEBHOOK_SECRET),
        )
    if slug == "cobalt":
        return ProviderSettings(
            display_name="Cobalt Network",
            authentication_type=AuthenticationType.OAUTH2_CLIENT_CREDENTIALS,
            base_url=base,
            oauth_token_url=f"{base}/oauth/token",
            client_id=COBALT_CLIENT_ID,
            client_secret=SecretStr(COBALT_CLIENT_SECRET),
            oauth_scope="operations.write operations.cancel",
            webhook_public_key=SANDBOX_COBALT_WEBHOOK_PUBLIC_KEY,
        )
    raise AssertionError(f"no sandbox configuration for '{slug}'")


ADAPTERS: dict[str, type[BaseProviderAdapter]] = {
    "northstar": NorthstarAdapter,
    "meridian": MeridianAdapter,
    "cobalt": CobaltAdapter,
}


@dataclass(slots=True)
class SandboxHarness:
    """An adapter, the sandbox behind it, and the controls a test needs."""

    app: FastAPI
    client: httpx.AsyncClient
    metrics: RecordingMetrics
    token_cache: MemoryTokenCache

    def adapter(
        self, slug: str, *, settings: ProviderSettings | None = None
    ) -> BaseProviderAdapter:
        """Build a real adapter over the sandbox transport."""
        config = settings or provider_settings(slug)
        provider = ProviderSlug(slug)
        authenticator = build_authenticator(
            slug=provider,
            config=config,
            client=self.client,
            token_cache=self.token_cache,
            locks=NullLockManager(),
            api_key_header=(MERIDIAN_API_KEY_HEADER if slug == "meridian" else "X-API-Key"),
        )
        http = ProviderHttpClient(
            slug=provider,
            config=config,
            client=self.client,
            authenticator=authenticator,
            metrics=self.metrics,
        )
        return ADAPTERS[slug](
            slug=provider,
            config=config,
            http=http,
            webhook_replay_window_seconds=REPLAY_WINDOW_SECONDS,
        )

    async def signed_webhook(
        self, slug: str, operation_id: str, *, event_type: str | None = None
    ) -> InboundWebhook:
        """Ask the sandbox for a correctly signed delivery it would have sent.

        Tests never construct signatures themselves: doing so would let a test
        agree with a broken adapter about the wrong signing scheme.
        """
        params = {"event_type": event_type} if event_type else None
        response = await self.client.post(
            f"{SANDBOX_ROOT}/_control/{slug}/emit/{operation_id}", params=params
        )
        response.raise_for_status()
        delivery = response.json()
        return InboundWebhook(
            provider=ProviderSlug(slug),
            headers={key.lower(): value for key, value in delivery["headers"].items()},
            body=delivery["body"].encode("utf-8"),
            received_at=datetime.now(tz=UTC),
        )

    async def operations(self, slug: str) -> list[dict[str, Any]]:
        response = await self.client.get(f"{SANDBOX_ROOT}/_control/{slug}/operations")
        response.raise_for_status()
        operations: list[dict[str, Any]] = response.json()["operations"]
        return operations


@pytest.fixture
async def sandbox() -> AsyncIterator[SandboxHarness]:
    """A sandbox with no callback URL, so it never delivers webhooks by itself."""
    app = create_sandbox_app(callback_base_url=None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url=SANDBOX_ROOT) as client:
        yield SandboxHarness(
            app=app,
            client=client,
            metrics=RecordingMetrics(),
            token_cache=MemoryTokenCache(),
        )


def create_command(
    reference: str,
    *,
    operation_type: OperationType = OperationType.RESOURCE_PROVISION,
    payload: dict[str, Any] | None = None,
    idempotency_key: str = "contract-idempotency-key",
    attempt: int = 1,
) -> CreateProviderOperationCommand:
    return CreateProviderOperationCommand(
        request_id=uuid4(),
        provider=ProviderSlug("northstar"),
        operation_type=operation_type,
        external_reference=ExternalReference(reference),
        payload=payload or {"resource_type": "database", "region": "eu-west-1"},
        correlation_id=CorrelationId("contract-correlation"),
        idempotency_key=idempotency_key,
        attempt=attempt,
    )
