"""Deterministic fake provider services.

Three ASGI applications that behave like Northstar Connect, Meridian Services and
Cobalt Network, including their authentication schemes, their differing
idempotency guarantees, their inconsistent field naming, and their signed
webhooks.

This exists so the adapters are exercised over real HTTP against realistic
responses. A hand-written stub of the ``ProviderGateway`` interface would test
the orchestration but would never catch a wrong header name, a signature computed
over the wrong bytes, or a status string the adapter does not map.

It is a sandbox, clearly isolated: it is never mounted unless
``PROVIDER_SANDBOX__ENABLED`` is set, and the settings validator refuses to build
a production-like configuration with it enabled.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qsl

import httpx
from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.responses import JSONResponse

from integration_orchestrator.infrastructure.providers.sandbox import webhooks as webhook_builders
from integration_orchestrator.infrastructure.providers.sandbox.scenarios import (
    RATE_LIMIT_RETRY_AFTER_SECONDS,
    SLOW_RESPONSE_SECONDS,
    TIMEOUT_RESPONSE_SECONDS,
    UNAVAILABLE_ATTEMPTS,
    Scenario,
)
from integration_orchestrator.infrastructure.providers.sandbox.signing import (
    COBALT_CLIENT_ID,
    COBALT_CLIENT_SECRET,
    MERIDIAN_API_KEY,
    NORTHSTAR_CLIENT_ID,
    NORTHSTAR_CLIENT_SECRET,
)
from integration_orchestrator.infrastructure.providers.sandbox.store import (
    SandboxOperation,
    SandboxStore,
)

logger = logging.getLogger(__name__)

TOKEN_LIFETIME_SECONDS = 300
WEBHOOK_DELAY_SECONDS = 0.2
#: The webhook-first scenario emits before the create response is returned, which
#: is what produces the race the deferred-receipt path exists to handle.
WEBHOOK_FIRST_DELAY_SECONDS = 0.0

_ISSUED_TOKENS: dict[str, datetime] = {}


class SandboxProvider:
    """Shared behaviour for the three fake providers."""

    def __init__(self, *, slug: str, prefix: str, callback_base_url: str | None) -> None:
        self.slug = slug
        self.store = SandboxStore(prefix=prefix)
        self.callback_base_url = callback_base_url
        self._tasks: set[asyncio.Task[None]] = set()

    # -- scenario handling --------------------------------------------------

    async def apply_pre_response_scenario(
        self, scenario: Scenario, *, reference: str
    ) -> Response | None:
        """Return an error response when the scenario demands one."""
        if scenario is Scenario.TIMEOUT:
            # Never answers inside the adapter's budget, producing a genuine
            # client-side timeout rather than a simulated error code.
            await asyncio.sleep(TIMEOUT_RESPONSE_SECONDS)
            return JSONResponse({"error": {"code": "timeout"}}, status_code=504)

        if scenario is Scenario.SLOW:
            await asyncio.sleep(SLOW_RESPONSE_SECONDS)
            return None

        if scenario is Scenario.RATE_LIMIT:
            return JSONResponse(
                {"error": {"code": "rate_limited", "message": "too many requests"}},
                status_code=429,
                headers={"Retry-After": str(RATE_LIMIT_RETRY_AFTER_SECONDS)},
            )

        if scenario is Scenario.ALWAYS_UNAVAILABLE:
            return JSONResponse({"error": {"code": "service_unavailable"}}, status_code=503)

        if scenario is Scenario.UNAVAILABLE_THEN_OK:
            attempt = self.store.attempts.record(f"unavailable:{reference}")
            if attempt <= UNAVAILABLE_ATTEMPTS:
                return JSONResponse(
                    {
                        "error": {
                            "code": "service_unavailable",
                            "message": f"attempt {attempt} of {UNAVAILABLE_ATTEMPTS + 1}",
                        }
                    },
                    status_code=503,
                )
            return None

        if scenario is Scenario.REJECT:
            return JSONResponse(
                {
                    "error": {
                        "code": "invalid_request",
                        "message": "the request was rejected by the provider",
                    }
                },
                status_code=400,
            )

        if scenario is Scenario.AUTH_CHALLENGE:
            attempt = self.store.attempts.record(f"auth:{reference}")
            if attempt == 1:
                return JSONResponse({"error": {"code": "token_expired"}}, status_code=401)
            return None

        return None

    # -- webhook emission ---------------------------------------------------

    def schedule_webhook(
        self,
        operation: SandboxOperation,
        *,
        builder: Callable[..., webhook_builders.SignedWebhook],
        event_type: str,
        delay_seconds: float = WEBHOOK_DELAY_SECONDS,
    ) -> None:
        """Deliver a webhook after a delay, if a callback base URL is configured."""
        if not self.callback_base_url:
            return
        task = asyncio.create_task(
            self._deliver_later(operation, builder, event_type, delay_seconds)
        )
        # Held so the event loop cannot garbage-collect a running task.
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _deliver_later(
        self,
        operation: SandboxOperation,
        builder: Callable[..., webhook_builders.SignedWebhook],
        event_type: str,
        delay_seconds: float,
    ) -> None:
        await asyncio.sleep(delay_seconds)
        base = self.callback_base_url
        if not base:
            return
        signed = builder(operation, event_type=event_type)
        operation.emitted_events.append(event_type)
        url = f"{base.rstrip('/')}{signed.path}"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(url, content=signed.body, headers=signed.headers)
            logger.info(
                "sandbox delivered a webhook",
                extra={
                    "provider": self.slug,
                    "event_type": event_type,
                    "operation_id": operation.id,
                    "http_status": response.status_code,
                },
            )
        except httpx.HTTPError:
            logger.warning(
                "sandbox could not deliver a webhook",
                extra={"provider": self.slug, "operation_id": operation.id},
            )


# ---------------------------------------------------------------------------
# Authentication helpers
# ---------------------------------------------------------------------------


def _issue_token(subject: str) -> dict[str, Any]:
    token = f"sandbox.{subject}.{int(datetime.now(tz=UTC).timestamp())}"
    _ISSUED_TOKENS[token] = datetime.now(tz=UTC) + timedelta(seconds=TOKEN_LIFETIME_SECONDS)
    return {
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": TOKEN_LIFETIME_SECONDS,
    }


def _bearer_is_valid(request: Request, *, subject: str) -> bool:
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("bearer "):
        return False
    token = header[7:].strip()
    expiry = _ISSUED_TOKENS.get(token)
    if expiry is None or expiry < datetime.now(tz=UTC):
        return False
    return token.startswith(f"sandbox.{subject}.")


def _unauthorized(message: str) -> JSONResponse:
    return JSONResponse({"error": {"code": "unauthorized", "message": message}}, status_code=401)


async def _read_form(request: Request) -> dict[str, str]:
    """Parse an ``application/x-www-form-urlencoded`` body.

    Starlette's own form parsing requires ``python-multipart``, which is a
    dependency the platform would only be carrying for the benefit of this
    sandbox. A token endpoint accepts one encoding, so decoding it here is
    cheaper than shipping a library into every deployment.
    """
    body = (await request.body()).decode("utf-8", errors="replace")
    return dict(parse_qsl(body, keep_blank_values=True))


async def _token_response(
    request: Request, *, client_id: str, secret: str, subject: str
) -> Response:
    form = await _read_form(request)
    if form.get("client_id") != client_id or form.get("client_secret") != secret:
        return _unauthorized("invalid client credentials")
    if form.get("grant_type") != "client_credentials":
        return JSONResponse({"error": {"code": "unsupported_grant_type"}}, status_code=400)
    return JSONResponse(_issue_token(subject))


# ---------------------------------------------------------------------------
# Northstar Connect
# ---------------------------------------------------------------------------


def build_northstar_router(provider: SandboxProvider) -> APIRouter:
    router = APIRouter()

    @router.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "provider": "northstar-connect"}

    @router.post("/oauth/token")
    async def token(request: Request) -> Response:
        return await _token_response(
            request,
            client_id=NORTHSTAR_CLIENT_ID,
            secret=NORTHSTAR_CLIENT_SECRET,
            subject="northstar",
        )

    @router.post("/operations")
    async def create_operation(request: Request) -> Response:
        if not _bearer_is_valid(request, subject="northstar"):
            return _unauthorized("a valid bearer token is required")

        body = await request.json()
        reference = str(body.get("reference", ""))
        scenario = Scenario.detect(reference)

        early = await provider.apply_pre_response_scenario(scenario, reference=reference)
        if early is not None:
            return early

        operation, deduplicated = provider.store.create(
            external_reference=reference,
            kind=str(body.get("operation", "provision")),
            status="queued",
            scenario=scenario,
            payload=dict(body.get("attributes") or {}),
            correlation_id=(body.get("client_context") or {}).get("correlation_id"),
            idempotency_key=request.headers.get("idempotency-key"),
            honour_idempotency=True,
        )

        if scenario is Scenario.NO_REFERENCE:
            return JSONResponse({"state": "queued"}, status_code=201)
        if scenario is Scenario.UNKNOWN_STATUS:
            return JSONResponse(
                {"operation_id": operation.id, "state": "awaiting_downstream_review"},
                status_code=201,
            )

        # Read before scheduling: completion mutates the stored status, and a
        # create response that already reported the final state would let an
        # adapter appear to work while never handling the asynchronous path.
        accepted_status = operation.status
        if not deduplicated:
            _schedule_completion(provider, operation, webhook_builders.northstar_webhook)

        return JSONResponse(
            {
                "operation_id": operation.id,
                "state": accepted_status,
                "deduplicated": deduplicated,
            },
            status_code=201,
        )

    return router


# ---------------------------------------------------------------------------
# Meridian Services
# ---------------------------------------------------------------------------


def build_meridian_router(provider: SandboxProvider) -> APIRouter:
    router = APIRouter()

    @router.get("/status")
    async def status() -> dict[str, str]:
        return {"status": "operational", "provider": "meridian-services"}

    @router.post("/service-requests")
    async def create_service_request(request: Request) -> Response:
        if request.headers.get("x-meridian-key") != MERIDIAN_API_KEY:
            return _unauthorized("a valid API key is required")

        body = await request.json()
        reference = str(body.get("customerRef", ""))
        scenario = Scenario.detect(reference)

        early = await provider.apply_pre_response_scenario(scenario, reference=reference)
        if early is not None:
            return early

        # Meridian ignores the idempotency header entirely, which is precisely
        # why the platform cannot rely on provider-side deduplication here.
        operation, _ = provider.store.create(
            external_reference=reference,
            kind=str(body.get("serviceCode", "SVC_PROVISION")),
            status="processing",
            scenario=scenario,
            payload=dict(body.get("parameters") or {}),
            correlation_id=(body.get("meta") or {}).get("correlationId"),
            idempotency_key=request.headers.get("idempotency-key"),
            honour_idempotency=False,
        )

        if scenario is Scenario.UNKNOWN_STATUS:
            return JSONResponse(
                {"requestId": operation.id, "status": "held_for_review"}, status_code=201
            )

        accepted_status = operation.status
        _schedule_completion(provider, operation, webhook_builders.meridian_webhook)
        # Note the camelCase key here versus snake_case on the status endpoint.
        return JSONResponse({"requestId": operation.id, "status": accepted_status}, status_code=201)

    @router.get("/service-requests/{request_id}")
    async def get_service_request(request: Request, request_id: str) -> Response:
        if request.headers.get("x-meridian-key") != MERIDIAN_API_KEY:
            return _unauthorized("a valid API key is required")

        operation = provider.store.get(request_id)
        if operation is None:
            return JSONResponse(
                {"code": "not_found", "message": "no such service request"}, status_code=404
            )
        return JSONResponse(
            {
                "request_id": operation.id,
                "state": operation.status,
                "customerRef": operation.external_reference,
                "reason": operation.failure_message,
                "reasonCode": operation.failure_code,
            }
        )

    return router


# ---------------------------------------------------------------------------
# Cobalt Network
# ---------------------------------------------------------------------------


def build_cobalt_router(provider: SandboxProvider) -> APIRouter:
    router = APIRouter()

    @router.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok", "provider": "cobalt-network"}

    @router.post("/oauth/token")
    async def token(request: Request) -> Response:
        return await _token_response(
            request,
            client_id=COBALT_CLIENT_ID,
            secret=COBALT_CLIENT_SECRET,
            subject="cobalt",
        )

    @router.post("/jobs")
    async def create_job(request: Request) -> Response:
        if not _bearer_is_valid(request, subject="cobalt"):
            return _unauthorized("a valid bearer token is required")

        body = await request.json()
        reference = str((body.get("subject") or {}).get("external_id", ""))
        scenario = Scenario.detect(reference)

        early = await provider.apply_pre_response_scenario(scenario, reference=reference)
        if early is not None:
            return early

        operation, deduplicated = provider.store.create(
            external_reference=reference,
            kind=str(body.get("kind", "resource.create")),
            status="accepted",
            scenario=scenario,
            payload=dict(body.get("spec") or {}),
            correlation_id=(body.get("trace") or {}).get("correlation_id"),
            idempotency_key=request.headers.get("idempotency-key"),
            honour_idempotency=True,
        )

        if scenario is Scenario.UNKNOWN_STATUS:
            return JSONResponse({"job_id": operation.id, "status": "quarantined"}, status_code=202)

        accepted_status = operation.status
        if scenario is Scenario.WEBHOOK_FIRST and not deduplicated:
            # Emitted before this response is returned, so the orchestrator can
            # receive the completion webhook before it has stored the job id.
            _schedule_completion(
                provider,
                operation,
                webhook_builders.cobalt_webhook,
                delay_seconds=WEBHOOK_FIRST_DELAY_SECONDS,
            )
        elif not deduplicated:
            _schedule_completion(provider, operation, webhook_builders.cobalt_webhook)

        return JSONResponse({"job_id": operation.id, "status": accepted_status}, status_code=202)

    @router.get("/jobs/{job_id}")
    async def get_job(request: Request, job_id: str) -> Response:
        if not _bearer_is_valid(request, subject="cobalt"):
            return _unauthorized("a valid bearer token is required")

        operation = provider.store.get(job_id)
        if operation is None:
            return JSONResponse(
                {"error": {"code": "not_found", "message": "no such job"}}, status_code=404
            )
        failure = (
            {"code": operation.failure_code, "message": operation.failure_message}
            if operation.failure_code
            else None
        )
        return JSONResponse(
            {
                "job_id": operation.id,
                "status": operation.status,
                "kind": operation.kind,
                "failure": failure,
            }
        )

    @router.post("/jobs/{job_id}/cancel")
    async def cancel_job(request: Request, job_id: str) -> Response:
        if not _bearer_is_valid(request, subject="cobalt"):
            return _unauthorized("a valid bearer token is required")

        operation = provider.store.get(job_id)
        if operation is None:
            return JSONResponse(
                {"error": {"code": "not_found", "message": "no such job"}}, status_code=404
            )
        if operation.status in ("succeeded", "failed"):
            # Already finished: the cancellation is refused, not failed.
            return JSONResponse({"job_id": operation.id, "status": operation.status})
        operation.touch("cancelled")
        return JSONResponse({"job_id": operation.id, "status": operation.status})

    return router


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------


def _schedule_completion(
    provider: SandboxProvider,
    operation: SandboxOperation,
    builder: Callable[..., webhook_builders.SignedWebhook],
    *,
    delay_seconds: float = WEBHOOK_DELAY_SECONDS,
) -> None:
    """Decide how an accepted operation eventually completes."""
    if operation.scenario is Scenario.ASYNC_FAILURE:
        operation.touch(_failed_status(provider.slug))
        operation.failure_code = "downstream_rejected"
        operation.failure_message = "the downstream system rejected the operation"
        provider.schedule_webhook(
            operation,
            builder=builder,
            event_type=_failure_event(provider.slug),
            delay_seconds=delay_seconds,
        )
        return

    operation.touch(_succeeded_status(provider.slug))
    provider.schedule_webhook(
        operation,
        builder=builder,
        event_type=_success_event(provider.slug),
        delay_seconds=delay_seconds,
    )


def _succeeded_status(slug: str) -> str:
    return {"northstar": "complete", "meridian": "fulfilled", "cobalt": "succeeded"}[slug]


def _failed_status(slug: str) -> str:
    return {"northstar": "error", "meridian": "rejected", "cobalt": "failed"}[slug]


def _success_event(slug: str) -> str:
    return {
        "northstar": "operation.completed",
        "meridian": "request.fulfilled",
        "cobalt": "job.succeeded",
    }[slug]


def _failure_event(slug: str) -> str:
    return {
        "northstar": "operation.failed",
        "meridian": "request.rejected",
        "cobalt": "job.failed",
    }[slug]


def build_control_router(providers: dict[str, SandboxProvider]) -> APIRouter:
    """Control endpoints used by tests and the demonstration script."""
    router = APIRouter()

    @router.post("/_control/reset")
    async def reset() -> dict[str, str]:
        for provider in providers.values():
            provider.store.reset()
        _ISSUED_TOKENS.clear()
        return {"status": "reset"}

    @router.post("/_control/{slug}/emit/{operation_id}")
    async def emit(slug: str, operation_id: str, event_type: str | None = None) -> Response:
        """Return a correctly signed webhook for an existing operation.

        Tests use this to replay a delivery, deliver it late, or deliver it twice
        without having to reimplement each provider's signing scheme.
        """
        provider = providers.get(slug)
        if provider is None:
            return JSONResponse({"error": "unknown provider"}, status_code=404)
        operation = provider.store.get(operation_id)
        if operation is None:
            return JSONResponse({"error": "unknown operation"}, status_code=404)

        builder = {
            "northstar": webhook_builders.northstar_webhook,
            "meridian": webhook_builders.meridian_webhook,
            "cobalt": webhook_builders.cobalt_webhook,
        }[slug]
        signed = builder(operation, event_type=event_type or _success_event(slug))
        operation.emitted_events.append(event_type or _success_event(slug))
        return JSONResponse(signed.as_dict())

    @router.get("/_control/{slug}/operations")
    async def list_operations(slug: str) -> Response:
        provider = providers.get(slug)
        if provider is None:
            return JSONResponse({"error": "unknown provider"}, status_code=404)
        return JSONResponse(
            {
                "operations": [
                    {
                        "id": operation.id,
                        "external_reference": operation.external_reference,
                        "status": operation.status,
                        "scenario": operation.scenario.value,
                        "emitted_events": operation.emitted_events,
                    }
                    for operation in provider.store.all()
                ]
            }
        )

    return router


def create_sandbox_app(*, callback_base_url: str | None = None) -> FastAPI:
    """Build the combined sandbox application.

    ``callback_base_url`` points at the orchestrator's API root. When set, the
    sandbox delivers webhooks by itself, which is what makes the local stack a
    genuine end-to-end demonstration rather than a set of endpoints someone has
    to poke manually.
    """
    providers = {
        "northstar": SandboxProvider(
            slug="northstar", prefix="ns-op", callback_base_url=callback_base_url
        ),
        "meridian": SandboxProvider(
            slug="meridian", prefix="mrd-req", callback_base_url=callback_base_url
        ),
        "cobalt": SandboxProvider(
            slug="cobalt", prefix="cbl-job", callback_base_url=callback_base_url
        ),
    }

    app = FastAPI(
        title="Provider sandbox",
        description=(
            "Deterministic fake implementations of Northstar Connect, Meridian "
            "Services and Cobalt Network. Never enabled outside local and test "
            "environments."
        ),
        docs_url="/docs",
        openapi_url="/openapi.json",
    )
    app.state.providers = providers
    app.include_router(build_northstar_router(providers["northstar"]), prefix="/northstar")
    app.include_router(build_meridian_router(providers["meridian"]), prefix="/meridian")
    app.include_router(build_cobalt_router(providers["cobalt"]), prefix="/cobalt")
    app.include_router(build_control_router(providers))
    return app
