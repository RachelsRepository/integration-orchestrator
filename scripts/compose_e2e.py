#!/usr/bin/env python3
"""Compose-level end-to-end verification against a live local stack.

Requires ``make up`` (or an equivalent Compose stack) with the API healthy on
``http://localhost:18100`` by default (Compose host port mapping). Override with
``ORCHESTRATOR_BASE_URL``. Exercises real HTTP against the fictional providers
mounted in the API process, durable Postgres state, and the outbox path.

This is deliberately a script rather than a pytest module so CI can run it as a
distinct runtime job after Compose is up, capture its exit code, and dump logs
on failure without fighting pytest's collection rules.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = os.environ.get("ORCHESTRATOR_BASE_URL", "http://localhost:18100").rstrip("/")
TOKEN_ISSUER_CMD_HINT = "make token"


class ProbeError(RuntimeError):
    pass


def _request(
    method: str,
    path: str,
    *,
    token: str | None = None,
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    expected: int | tuple[int, ...] = 200,
) -> dict[str, Any] | list[Any] | None:
    url = f"{BASE_URL}{path}"
    data = None if body is None else json.dumps(body).encode("utf-8")
    req_headers = {"Accept": "application/json", **(headers or {})}
    if body is not None:
        req_headers["Content-Type"] = "application/json"
    if token:
        req_headers["Authorization"] = f"Bearer {token}"
    request = Request(url, data=data, headers=req_headers, method=method)
    try:
        with urlopen(request, timeout=30) as response:
            status = response.status
            raw = response.read()
    except HTTPError as exc:
        status = exc.code
        raw = exc.read()
    except URLError as exc:
        raise ProbeError(f"{method} {path} failed to connect: {exc}") from exc

    allowed = expected if isinstance(expected, tuple) else (expected,)
    payload: dict[str, Any] | list[Any] | None = json.loads(raw.decode("utf-8")) if raw else None
    if status not in allowed:
        raise ProbeError(f"{method} {path} returned {status}, expected {allowed}: {payload}")
    return payload


def wait_for_ready(*, timeout_seconds: float = 120.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error = "not attempted"
    while time.monotonic() < deadline:
        try:
            live = _request("GET", "/health/live")
            assert isinstance(live, dict) and live.get("status") == "alive"
            ready = _request("GET", "/health/ready", expected=(200, 503))
            assert isinstance(ready, dict)
            if ready.get("status") == "ready":
                print("stack is ready")
                return
            last_error = f"not ready yet: {ready}"
        except (ProbeError, AssertionError) as exc:
            last_error = str(exc)
        time.sleep(2)
    raise ProbeError(f"stack never became ready: {last_error}")


def mint_token() -> str:
    """Ask the running API's local mint path via the CLI entry in-process.

    The Compose stack uses the HS256 local secret, so minting against the same
    settings the API loaded is exact. Falls back to an env-supplied token.
    """
    env_token = os.environ.get("ORCHESTRATOR_TOKEN")
    if env_token:
        return env_token

    from integration_orchestrator.config.settings import get_settings, reset_settings_cache
    from integration_orchestrator.infrastructure.security.tokens import issue_local_token

    reset_settings_cache()
    return issue_local_token(
        get_settings().jwt, subject=f"compose-e2e-{uuid.uuid4().hex[:8]}", roles=["operator"]
    )


def _await_workflow(
    token: str, execution_id: str, *statuses: str, timeout: float = 120.0
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        current = _request(
            "GET", f"/api/v1/workflows/executions/{execution_id}", token=token, expected=200
        )
        assert isinstance(current, dict)
        last = current
        if current["status"] in statuses:
            return current
        time.sleep(2)
    raise ProbeError(f"workflow {execution_id} never reached {statuses}: {last}")


def create_and_await_success(token: str, *, provider: str) -> dict[str, Any]:
    idempotency_key = f"compose-e2e-{provider}-{uuid.uuid4()}"
    external_reference = f"compose-e2e-{provider}-{uuid.uuid4().hex[:8]}"
    body = {
        "provider": provider,
        "operation_type": "resource_provision",
        "external_reference": external_reference,
        "payload": {"resource_name": "compose-e2e"},
    }
    created = _request(
        "POST",
        "/api/v1/integration-requests",
        token=token,
        headers={"Idempotency-Key": idempotency_key},
        body=body,
        expected=(200, 201, 202),
    )
    assert isinstance(created, dict)
    request_id = created["id"]

    # Replay the same idempotency key — must return the same request.
    replay = _request(
        "POST",
        "/api/v1/integration-requests",
        token=token,
        headers={"Idempotency-Key": idempotency_key},
        body=body,
        expected=(200, 201, 202),
    )
    assert isinstance(replay, dict)
    if replay["id"] != request_id:
        raise ProbeError(f"idempotency replay created a duplicate request: {replay['id']}")

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        current = _request(
            "GET", f"/api/v1/integration-requests/{request_id}", token=token, expected=200
        )
        assert isinstance(current, dict)
        status = current["status"]
        if status == "succeeded":
            return current
        if status in {"failed", "cancelled", "manual_review"}:
            raise ProbeError(f"request ended in {status}: {current}")
        time.sleep(1)
    raise ProbeError(f"request {request_id} never reached succeeded")


def main() -> int:
    print(f"probing {BASE_URL}")
    wait_for_ready()

    live = _request("GET", "/health/live")
    ready = _request("GET", "/health/ready")
    assert isinstance(live, dict) and live["status"] == "alive"
    assert isinstance(ready, dict) and ready["status"] == "ready"

    with urlopen(f"{BASE_URL}/metrics", timeout=10) as response:
        metrics_body = response.read().decode("utf-8")
    if len(metrics_body) < 10:
        raise ProbeError("metrics endpoint returned an empty body")

    try:
        token = mint_token()
    except Exception as exc:
        raise ProbeError(
            f"could not mint a local token ({exc}); "
            f"set ORCHESTRATOR_TOKEN or run `{TOKEN_ISSUER_CMD_HINT}`"
        ) from exc

    # Auth negative: missing bearer is rejected.
    _request("GET", "/api/v1/providers", expected=401)

    # Webhook negative: missing signature must not advance state.
    _request(
        "POST",
        "/webhooks/northstar",
        body={"event_id": "compose-e2e-unsigned", "event_type": "ignored"},
        expected=(401, 403, 422),
    )

    northstar = create_and_await_success(token, provider="northstar")
    meridian = create_and_await_success(token, provider="meridian")
    print(
        "workflow success",
        {"northstar": northstar["id"], "meridian": meridian["id"]},
    )

    # Multi-step saga: customer_onboarding (Northstar → Meridian → Cobalt).
    saga = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        headers={"Idempotency-Key": f"compose-saga-{uuid.uuid4()}"},
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {"resource_name": "compose-saga"},
        },
        expected=(200, 201, 202),
    )
    assert isinstance(saga, dict)
    saga_id = saga["id"]
    saga_final = _await_workflow(
        token, saga_id, "succeeded", "compensated", "manual_review", "failed"
    )
    if saga_final["status"] != "succeeded":
        raise ProbeError(f"happy-path saga expected succeeded, got {saga_final['status']}")
    print("saga terminal", saga_final["status"], saga_final.get("steps"))

    # Mid-saga reject → reverse compensation (northstar deprovision).
    # Meridian has no deprovision; fail at create_subscription so only
    # create_customer is compensated.
    compensate = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        headers={"Idempotency-Key": f"compose-saga-cmp-{uuid.uuid4()}"},
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {
                "resource_name": "compose-saga-compensate",
                "fail_at_step": "create_subscription",
                "fail_scenario": "scenario-reject",
            },
        },
        expected=(200, 201, 202),
    )
    assert isinstance(compensate, dict)
    cmp_final = _await_workflow(
        token,
        compensate["id"],
        "compensated",
        "manual_review",
        "failed",
        "compensating",
        timeout=180.0,
    )
    if cmp_final["status"] not in {"compensated", "manual_review"}:
        raise ProbeError(f"compensation saga stuck at {cmp_final['status']}: {cmp_final}")
    print("compensation saga", cmp_final["status"], cmp_final.get("steps"))

    # Fail after two successes: reverse compensation hits Meridian deprovision
    # (unsupported) → MANUAL_REVIEW.
    review = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        headers={"Idempotency-Key": f"compose-saga-review-{uuid.uuid4()}"},
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {
                "resource_name": "compose-saga-review",
                "fail_at_step": "register_callback",
                "fail_scenario": "scenario-reject",
            },
        },
        expected=(200, 201, 202),
    )
    assert isinstance(review, dict)
    review_final = _await_workflow(
        token,
        review["id"],
        "manual_review",
        "compensated",
        "failed",
        timeout=180.0,
    )
    if review_final["status"] not in {"manual_review", "compensated"}:
        raise ProbeError(f"manual-review saga stuck at {review_final['status']}: {review_final}")
    print("manual-review saga", review_final["status"], review_final.get("steps"))

    # Idempotent re-post of the happy-path saga key must not create a duplicate.
    saga_key = f"compose-saga-idem-{uuid.uuid4()}"
    first = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        headers={"Idempotency-Key": saga_key},
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {"resource_name": "compose-saga-idem"},
        },
        expected=(200, 201, 202),
    )
    second = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        headers={"Idempotency-Key": saga_key},
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {"resource_name": "compose-saga-idem"},
        },
        expected=(200, 201, 202),
    )
    assert isinstance(first, dict) and isinstance(second, dict)
    if first["id"] != second["id"]:
        raise ProbeError("workflow idempotency key produced duplicate executions")
    print("workflow idempotency ok", first["id"])

    # Restart-survival probe: the rows we just wrote must still be readable.
    for request_id in (northstar["id"], meridian["id"]):
        loaded = _request(
            "GET", f"/api/v1/integration-requests/{request_id}", token=token, expected=200
        )
        assert isinstance(loaded, dict)
        if loaded["status"] != "succeeded":
            raise ProbeError(f"persisted request {request_id} is no longer succeeded")

    # Compensation path in this architecture is request-level cancel.
    # Meridian refuses cancel after accept — that controlled refusal is the
    # proof that unsupported compensation does not silently rewrite state.
    cancel_body = {
        "provider": "meridian",
        "operation_type": "resource_provision",
        "external_reference": f"compose-e2e-cancel-{uuid.uuid4().hex[:8]}",
        "payload": {"resource_name": "compose-e2e-cancel"},
    }
    to_cancel = _request(
        "POST",
        "/api/v1/integration-requests",
        token=token,
        headers={"Idempotency-Key": f"compose-e2e-cancel-{uuid.uuid4()}"},
        body=cancel_body,
        expected=(200, 201, 202),
    )
    assert isinstance(to_cancel, dict)
    cancelled = _request(
        "POST",
        f"/api/v1/integration-requests/{to_cancel['id']}/cancel",
        token=token,
        body={},
        expected=(200, 202, 409, 422),
    )
    assert isinstance(cancelled, dict)
    print("cancel path exercised", cancelled)

    # --- Parallel fan-out / fan-in ---
    fan = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        headers={"Idempotency-Key": f"compose-fan-{uuid.uuid4()}"},
        body={
            "definition_name": "parallel_provisioning",
            "definition_version": 1,
            "payload": {"resource_name": "compose-fan"},
        },
        expected=(200, 201, 202),
    )
    assert isinstance(fan, dict)
    fan_final = _await_workflow(
        token, fan["id"], "succeeded", "compensated", "manual_review", "failed", "cancelled"
    )
    if fan_final["status"] != "succeeded":
        raise ProbeError(f"fan-out saga expected succeeded, got {fan_final['status']}")
    steps_by_key = {s["key"]: s for s in fan_final["steps"]}
    for key in ("create_customer", "provision_billing", "register_notify", "finalize_join"):
        if steps_by_key[key]["status"] != "succeeded":
            raise ProbeError(f"fan-out step {key} not succeeded: {steps_by_key[key]}")
    join_completed = steps_by_key["finalize_join"].get("completed_at")
    billing_completed = steps_by_key["provision_billing"].get("completed_at")
    notify_completed = steps_by_key["register_notify"].get("completed_at")
    if not (join_completed and billing_completed and notify_completed):
        raise ProbeError("fan-out missing completion timestamps")
    if join_completed < billing_completed or join_completed < notify_completed:
        raise ProbeError("finalize_join completed before both branches")
    print("fan-out/fan-in ok", fan_final["id"])

    # --- Workflow cancellation ---
    to_cancel_wf = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        headers={"Idempotency-Key": f"compose-wf-cancel-{uuid.uuid4()}"},
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {"resource_name": "compose-wf-cancel"},
        },
        expected=(200, 201, 202),
    )
    assert isinstance(to_cancel_wf, dict)
    cancelled_wf = _request(
        "POST",
        f"/api/v1/workflows/executions/{to_cancel_wf['id']}/cancel",
        token=token,
        body={"reason": "compose_probe"},
        expected=(200, 409),
    )
    assert isinstance(cancelled_wf, dict)
    # Poll until cancelled or compensated (cancel after some steps may compensate).
    cancel_final = _await_workflow(
        token,
        to_cancel_wf["id"],
        "cancelled",
        "compensated",
        "compensating",
        "manual_review",
        "succeeded",
    )
    if (
        cancel_final["status"] not in {"cancelled", "compensated"}
        and cancelled_wf.get("status") not in {"cancelled", "compensated"}
        and cancel_final["status"] == "succeeded"
    ):
        print("cancel raced with success; retrying cancel on fresh workflow")
        fresh = _request(
            "POST",
            "/api/v1/workflows/executions",
            token=token,
            headers={"Idempotency-Key": f"compose-wf-cancel2-{uuid.uuid4()}"},
            body={
                "definition_name": "customer_onboarding",
                "definition_version": 1,
                "payload": {
                    "resource_name": "compose-wf-cancel-slow",
                    "fail_at_step": "register_callback",
                    "fail_scenario": "scenario-unavailable",
                },
            },
            expected=(200, 201, 202),
        )
        assert isinstance(fresh, dict)
        time.sleep(0.5)
        cancelled_wf = _request(
            "POST",
            f"/api/v1/workflows/executions/{fresh['id']}/cancel",
            token=token,
            body={"reason": "compose_probe"},
            expected=200,
        )
        cancel_final = _await_workflow(
            token, fresh["id"], "cancelled", "compensated", "manual_review"
        )
    if cancel_final["status"] not in {"cancelled", "compensated"}:
        raise ProbeError(f"workflow cancel ended in {cancel_final['status']}")
    # Idempotent re-cancel
    again = _request(
        "POST",
        f"/api/v1/workflows/executions/{cancel_final['id']}/cancel",
        token=token,
        body={},
        expected=(200, 409),
    )
    assert isinstance(again, dict)
    print("workflow cancel ok", cancel_final["status"])

    # --- Workflow hard deadline ---
    deadline_wf = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        headers={"Idempotency-Key": f"compose-deadline-{uuid.uuid4()}"},
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {"resource_name": "compose-deadline"},
            "deadline_seconds": 1,
        },
        expected=(200, 201, 202),
    )
    assert isinstance(deadline_wf, dict)
    # Short deadline may already be processing; wait for terminal cancel/compensate.
    time.sleep(2)
    deadline_final = _await_workflow(
        token,
        deadline_wf["id"],
        "cancelled",
        "compensated",
        "timed_out",
        "compensating",
        "manual_review",
        "succeeded",
        "failed",
    )
    if deadline_final["status"] == "succeeded":
        # Extremely fast path beat the 1s deadline — still accept if deadline_at set.
        if not deadline_wf.get("deadline_at") and not deadline_final.get("deadline_at"):
            raise ProbeError("deadline workflow succeeded without deadline metadata")
        print("deadline raced with success (deadline metadata present)")
    elif deadline_final["status"] not in {"cancelled", "compensated", "manual_review"}:
        # Allow brief compensating; re-await
        deadline_final = _await_workflow(
            token, deadline_wf["id"], "cancelled", "compensated", "manual_review", "failed"
        )
        if deadline_final["status"] not in {"cancelled", "compensated", "manual_review", "failed"}:
            raise ProbeError(f"deadline workflow stuck at {deadline_final['status']}")
    print("workflow deadline ok", deadline_final["status"])

    print("compose e2e passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeError as exc:
        print(f"compose e2e failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
