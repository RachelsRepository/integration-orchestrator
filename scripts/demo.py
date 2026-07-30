"""Drive the platform through its interesting failure modes.

Runs against a stack started with ``make up``. Each scenario uses a reserved
external-reference prefix that the sandbox providers recognise, so the outcomes
are deterministic rather than dependent on timing or luck.

The point is to make the resilience behaviour observable: after running this you
can look at the request list, the audit trail, and the metrics endpoint and see
retries, circuit transitions, deferred webhooks and manual-review escalations
that actually happened rather than ones described in a document.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import httpx

from integration_orchestrator.config.settings import get_settings
from integration_orchestrator.infrastructure.providers.sandbox.scenarios import Scenario
from integration_orchestrator.infrastructure.security.tokens import issue_local_token

DEFAULT_BASE_URL = "http://localhost:8000"
POLL_INTERVAL_SECONDS = 1.0
POLL_TIMEOUT_SECONDS = 45.0


@dataclass(frozen=True, slots=True)
class DemoScenario:
    """One end-to-end story the platform should handle."""

    name: str
    provider: str
    operation_type: str
    reference_prefix: str
    expected_statuses: tuple[str, ...]
    explanation: str


SCENARIOS: tuple[DemoScenario, ...] = (
    DemoScenario(
        name="happy path with an asynchronous completion",
        provider="northstar",
        operation_type="resource_provision",
        reference_prefix="",
        expected_statuses=("succeeded",),
        explanation=(
            "Northstar accepts synchronously and completes by webhook. The request "
            "should move received -> dispatching -> pending -> succeeded."
        ),
    ),
    DemoScenario(
        name="transient unavailability recovered by retry",
        provider="northstar",
        operation_type="resource_provision",
        reference_prefix=Scenario.UNAVAILABLE_THEN_OK.prefix,
        expected_statuses=("succeeded", "pending"),
        explanation=(
            "The provider returns 503 twice and then succeeds. The request should "
            "pass through retry_scheduled and recover without operator action."
        ),
    ),
    DemoScenario(
        name="rate limiting honoured through Retry-After",
        provider="meridian",
        operation_type="access_grant",
        reference_prefix=Scenario.RATE_LIMIT.prefix,
        expected_statuses=("retry_scheduled", "succeeded", "pending", "failed"),
        explanation=(
            "Meridian answers 429 with a Retry-After header, which the backoff "
            "calculation respects instead of using its own schedule."
        ),
    ),
    DemoScenario(
        name="ambiguous timeout escalated rather than guessed",
        provider="meridian",
        operation_type="resource_provision",
        reference_prefix=Scenario.TIMEOUT.prefix,
        expected_statuses=("manual_review", "retry_scheduled"),
        explanation=(
            "The provider never answers. Because a timeout may hide a successful "
            "creation, exhausted retries escalate to manual review instead of "
            "declaring failure."
        ),
    ),
    DemoScenario(
        name="webhook arriving before the dispatch response",
        provider="cobalt",
        operation_type="resource_provision",
        reference_prefix=Scenario.WEBHOOK_FIRST.prefix,
        expected_statuses=("succeeded", "pending"),
        explanation=(
            "Cobalt emits the completion webhook before returning the job id. The "
            "receipt is deferred and applied once the reference lands."
        ),
    ),
    DemoScenario(
        name="permanent rejection is not retried",
        provider="cobalt",
        operation_type="access_revoke",
        reference_prefix=Scenario.REJECT.prefix,
        expected_statuses=("failed",),
        explanation=(
            "A 400 is not retryable, so the request fails immediately rather than "
            "consuming its retry budget on a request the provider will never accept."
        ),
    ),
    DemoScenario(
        name="a status the adapter does not recognise",
        provider="northstar",
        operation_type="resource_update",
        reference_prefix=Scenario.UNKNOWN_STATUS.prefix,
        expected_statuses=("manual_review",),
        explanation=(
            "An unmapped provider status is never coerced into success or failure; "
            "it goes to manual review so a human decides."
        ),
    ),
)


class DemoClient:
    """A very small API client for the demonstration."""

    def __init__(self, client: httpx.AsyncClient, token: str) -> None:
        self._client = client
        self._headers = {"Authorization": f"Bearer {token}"}

    async def create(self, scenario: DemoScenario) -> dict[str, Any]:
        reference = f"{scenario.reference_prefix}demo-{uuid.uuid4().hex[:8]}"
        response = await self._client.post(
            "/api/v1/integration-requests",
            json={
                "provider": scenario.provider,
                "operation_type": scenario.operation_type,
                "external_reference": reference,
                "payload": {"scenario": scenario.name, "region": "eu-west-1"},
            },
            headers={**self._headers, "Idempotency-Key": f"demo-{uuid.uuid4()}"},
        )
        response.raise_for_status()
        return response.json()

    async def get(self, request_id: str) -> dict[str, Any]:
        response = await self._client.get(
            f"/api/v1/integration-requests/{request_id}", headers=self._headers
        )
        response.raise_for_status()
        return response.json()

    async def audit(self, request_id: str) -> list[dict[str, Any]]:
        response = await self._client.get(
            f"/api/v1/integration-requests/{request_id}/audit", headers=self._headers
        )
        response.raise_for_status()
        return response.json()["events"]

    async def providers(self) -> list[dict[str, Any]]:
        response = await self._client.get("/api/v1/providers?probe=true", headers=self._headers)
        response.raise_for_status()
        return response.json()["providers"]


async def _settle(client: DemoClient, request_id: str, expected: Sequence[str]) -> dict[str, Any]:
    """Poll until the request reaches one of the expected states or time runs out."""
    deadline = asyncio.get_running_loop().time() + POLL_TIMEOUT_SECONDS
    latest: dict[str, Any] = {}
    while asyncio.get_running_loop().time() < deadline:
        latest = await client.get(request_id)
        if latest["status"] in expected:
            return latest
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    return latest


async def run_demo(base_url: str, token: str) -> int:
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as http:
        client = DemoClient(http, token)

        print("Provider catalogue")
        print("-" * 72)
        for provider in await client.providers():
            print(
                f"  {provider['provider']:<10} "
                f"healthy={provider['healthy']!s:<5} "
                f"circuit={provider['circuit_state']:<9} "
                f"auth={provider['authentication_type']}"
            )
        print()

        failures = 0
        for scenario in SCENARIOS:
            print(f"Scenario: {scenario.name}")
            print("-" * 72)
            print(f"  {scenario.explanation}")
            created = await client.create(scenario)
            request_id = created["id"]
            print(f"  request {request_id} created as '{created['status']}'")

            final = await _settle(client, request_id, scenario.expected_statuses)
            reached = final.get("status", "unknown")
            ok = reached in scenario.expected_statuses
            failures += 0 if ok else 1

            print(f"  settled as '{reached}' after {final.get('attempt_count', 0)} attempt(s)")
            if final.get("last_failure"):
                print(f"  last failure: {final['last_failure']['code']}")
            if final.get("manual_review_reason"):
                print(f"  escalation reason: {final['manual_review_reason']}")

            print("  audit trail:")
            for event in await client.audit(request_id):
                arrow = (
                    f"{event['previous_state']} -> {event['new_state']}"
                    if event["previous_state"] != event["new_state"]
                    else event["new_state"]
                )
                print(f"    {event['occurred_at']}  {event['action']:<30} {arrow}")
            print(f"  result: {'as expected' if ok else 'UNEXPECTED'}\n")

        print("=" * 72)
        if failures:
            print(f"{failures} scenario(s) did not reach an expected state")
        else:
            print("every scenario reached an expected state")
        return 1 if failures else 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the demonstration scenarios.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--token", default=None, help="Bearer token. Minted locally if omitted.")
    args = parser.parse_args(argv)

    settings = get_settings()
    token = args.token or issue_local_token(
        settings.jwt, subject="demo-operator", roles=["operator"]
    )

    try:
        return asyncio.run(run_demo(args.base_url, token))
    except httpx.HTTPError as exc:
        print(f"could not reach the API at {args.base_url}: {exc}", file=sys.stderr)
        print("Start the stack first with 'make up'.", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
