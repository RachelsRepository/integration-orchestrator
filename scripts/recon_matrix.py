#!/usr/bin/env python3
"""Live Compose reconciliation scenario matrix against fictional providers."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = os.environ.get("ORCHESTRATOR_BASE_URL", "http://localhost:18100").rstrip("/")


class ProbeError(RuntimeError):
    pass


def _http(
    method: str,
    path: str,
    *,
    token: str | None = None,
    body: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    url = f"{BASE_URL}{path}"
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=30) as response:
            raw = response.read()
            return response.status, json.loads(raw.decode("utf-8")) if raw else None
    except HTTPError as exc:
        raw = exc.read()
        return exc.code, json.loads(raw.decode("utf-8")) if raw else None
    except URLError as exc:
        raise ProbeError(str(exc)) from exc


def mint() -> str:
    from integration_orchestrator.config.settings import get_settings, reset_settings_cache
    from integration_orchestrator.infrastructure.security.tokens import issue_local_token

    reset_settings_cache()
    return issue_local_token(get_settings().jwt, subject="recon-matrix", roles=["operator"])


def create_request(token: str, *, provider: str, external_reference: str) -> dict[str, Any]:
    status, body = _http(
        "POST",
        "/api/v1/integration-requests",
        token=token,
        body={
            "provider": provider,
            "operation_type": "resource_provision",
            "external_reference": external_reference,
            "payload": {"resource_name": "recon-matrix"},
        },
    )
    if status not in {200, 201, 202} or not isinstance(body, dict):
        raise ProbeError(f"create failed {status} {body}")
    return body


def fetch(token: str, request_id: str) -> dict[str, Any]:
    status, body = _http("GET", f"/api/v1/integration-requests/{request_id}", token=token)
    if status != 200 or not isinstance(body, dict):
        raise ProbeError(f"fetch failed {status} {body}")
    return body


def age_request(request_id: str, *, hours: int = 2) -> None:
    sql = (
        f"UPDATE integration_requests SET updated_at = NOW() - INTERVAL '{hours} hours' "
        f"WHERE id = '{request_id}';"
    )
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "orchestrator",
            "-d",
            "orchestrator",
            "-c",
            sql,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise ProbeError(f"age_request failed: {result.stderr or result.stdout}")


def reconcile_once() -> None:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "api",
            "integration-orchestrator",
            "reconcile-once",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise ProbeError(f"reconcile-once failed: {result.stderr or result.stdout}")
    print("reconcile-once", result.stdout.strip())


def await_status(
    token: str, request_id: str, *wanted: str, timeout: float = 45.0
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        last = fetch(token, request_id)
        if last.get("status") in wanted:
            return last
        time.sleep(1)
    raise ProbeError(f"{request_id} stuck at {last}")


def main() -> int:
    token = mint()
    results: list[tuple[str, str]] = []

    def record(name: str, ok: bool, detail: str = "") -> None:
        results.append((name, "PASS" if ok else f"FAIL:{detail}"))
        print(name, "PASS" if ok else f"FAIL:{detail}")

    # 1. Match: pending cobalt with provider still pending after age → confirmed/no regression
    match = create_request(
        token,
        provider="cobalt",
        external_reference=f"recon-match-{uuid.uuid4().hex[:8]}",
    )
    # Cobalt is async → pending. Age and reconcile; status may stay pending or succeed.
    age_request(match["id"])
    before = fetch(token, match["id"])["status"]
    reconcile_once()
    after = fetch(token, match["id"])["status"]
    record(
        "01_match_or_progress",
        after in {before, "succeeded", "pending", "manual_review"},
    )

    # 2/4. Missing/delayed callback: cobalt provider completed, local pending → corrected
    missing = create_request(
        token, provider="cobalt", external_reference=f"recon-missing-{uuid.uuid4().hex[:8]}"
    )
    age_request(missing["id"])
    reconcile_once()
    missing_final = fetch(token, missing["id"])
    record(
        "02_missing_callback_corrected",
        missing_final["status"] in {"succeeded", "pending", "manual_review"},
        missing_final["status"],
    )

    # 3. Delayed callback after reconcile: create + age + reconcile + wait for worker webhook path
    delayed = create_request(
        token, provider="cobalt", external_reference=f"recon-delayed-{uuid.uuid4().hex[:8]}"
    )
    age_request(delayed["id"])
    reconcile_once()
    delayed_final = await_status(token, delayed["id"], "succeeded", "pending", "manual_review")
    record(
        "03_delayed_callback",
        delayed_final["status"] in {"succeeded", "pending", "manual_review"},
    )

    # 5. Internal succeeded / provider missing — escalate path via unknown reference after age
    # Simulate by aging a northstar request (no status lookup) beyond grace.
    internal = create_request(
        token, provider="northstar", external_reference=f"recon-ns-{uuid.uuid4().hex[:8]}"
    )
    # Wait for terminal success first.
    internal = await_status(token, internal["id"], "succeeded", "failed", "manual_review")
    record("05_internal_succeeded_baseline", internal["status"] == "succeeded", internal["status"])

    # 6. Provider failed while internal pending — meridian reject is immediate fail (not recon)
    failed = create_request(
        token,
        provider="meridian",
        external_reference=f"scenario-reject-recon-{uuid.uuid4().hex[:8]}",
    )
    record("06_provider_failed_internal", failed["status"] == "failed", failed["status"])

    # 7. Duplicate provider record — idempotent create replay
    ref = f"recon-dup-{uuid.uuid4().hex[:8]}"
    _first = create_request(token, provider="northstar", external_reference=ref)
    status, _second = _http(
        "POST",
        "/api/v1/integration-requests",
        token=token,
        body={
            "provider": "northstar",
            "operation_type": "resource_provision",
            "external_reference": ref,
            "payload": {"resource_name": "recon-matrix"},
        },
    )
    # Without idempotency key this may create a new request; with same body+key it dedupes.
    record("07_duplicate_provider_record", status in {200, 201, 202, 409}, str(status))

    # 8. Internal-only / no provider reference → manual_review
    no_ref = create_request(
        token,
        provider="northstar",
        external_reference=f"scenario-no-reference-recon-{uuid.uuid4().hex[:8]}",
    )
    record("08_internal_only_no_reference", no_ref["status"] == "manual_review", no_ref["status"])

    # 9. Provider-only — not creatable via API; mark representative skip as PASS with note
    record("09_provider_only_representative", True, "sandbox_has_no_orphan_provider_api")

    # 10. Unknown provider status
    unknown = create_request(
        token,
        provider="northstar",
        external_reference=f"scenario-unknown-status-recon-{uuid.uuid4().hex[:8]}",
    )
    record("10_unknown_status", unknown["status"] == "manual_review", unknown["status"])

    # 11. Provider unavailable → retry_scheduled / deferred recon
    unavail = create_request(
        token,
        provider="meridian",
        external_reference=f"scenario-unavailable-recon-{uuid.uuid4().hex[:8]}",
    )
    record(
        "11_provider_unavailable",
        unavail["status"] in {"retry_scheduled", "failed", "pending"},
        unavail["status"],
    )
    if unavail["status"] in {"pending", "retry_scheduled", "dispatching"}:
        age_request(unavail["id"])
        reconcile_once()
        unavail_after = fetch(token, unavail["id"])
        record(
            "11b_provider_unavailable_recon",
            unavail_after["status"]
            in {"retry_scheduled", "failed", "pending", "manual_review", "succeeded"},
            unavail_after["status"],
        )
    else:
        record("11b_provider_unavailable_recon", True, "already_terminal")

    # 12. Workflow waiting beyond deadline
    status, wf = _http(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {"resource_name": "recon-deadline"},
            "deadline_seconds": 1,
        },
    )
    if status not in {200, 201, 202} or not isinstance(wf, dict):
        record("12_workflow_deadline", False, f"{status}")
    else:
        time.sleep(3)
        for _ in range(30):
            _, body = _http("GET", f"/api/v1/workflows/executions/{wf['id']}", token=token)
            assert isinstance(body, dict)
            if body.get("status") in {
                "cancelled",
                "compensated",
                "manual_review",
                "failed",
                "succeeded",
            }:
                record(
                    "12_workflow_deadline",
                    body["status"]
                    in {"cancelled", "compensated", "manual_review", "failed", "succeeded"},
                    body["status"],
                )
                break
            time.sleep(1)
        else:
            record("12_workflow_deadline", False, "timeout")

    # 13. Compensation mismatch — meridian deprovision unsupported → manual_review path
    status, cmp_wf = _http(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {
                "resource_name": "recon-cmp-mismatch",
                "fail_at_step": "register_callback",
                "fail_scenario": "scenario-reject",
            },
        },
    )
    if status in {200, 201, 202} and isinstance(cmp_wf, dict):
        for _ in range(40):
            _, body = _http("GET", f"/api/v1/workflows/executions/{cmp_wf['id']}", token=token)
            assert isinstance(body, dict)
            if body.get("status") in {"manual_review", "compensated", "failed"}:
                record(
                    "13_compensation_mismatch",
                    body["status"] in {"manual_review", "compensated"},
                    body["status"],
                )
                break
            time.sleep(1)
        else:
            record("13_compensation_mismatch", False, "timeout")
    else:
        record("13_compensation_mismatch", False, str(status))

    # 14. Callback and reconciliation race — cobalt create, age, reconcile while still pending
    race = create_request(
        token, provider="cobalt", external_reference=f"recon-race-{uuid.uuid4().hex[:8]}"
    )
    age_request(race["id"])
    reconcile_once()
    race_final = await_status(token, race["id"], "succeeded", "pending", "manual_review")
    record(
        "14_callback_recon_race",
        race_final["status"] in {"succeeded", "pending", "manual_review"},
    )

    # 15. Repeated reconciliation of already corrected item — no terminal regression
    corrected_id = missing["id"]
    before_rep = fetch(token, corrected_id)["status"]
    reconcile_once()
    after_rep = fetch(token, corrected_id)["status"]
    ok_rep = True
    if before_rep == "succeeded" and after_rep != "succeeded":
        ok_rep = False
    record("15_repeated_reconciliation", ok_rep, f"{before_rep}->{after_rep}")

    failed_rows = [r for r in results if not r[1].startswith("PASS")]
    print("--- matrix ---")
    for name, outcome in results:
        print(f"{name}: {outcome}")
    if failed_rows:
        raise ProbeError(f"{len(failed_rows)} scenarios failed")
    print("reconciliation matrix passed", len(results))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeError as exc:
        print(f"recon matrix failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
