"""Who is allowed to do what, enforced at the edge."""

from __future__ import annotations

import time

import jwt
import pytest

from tests.e2e.conftest import Harness

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

WRITE_BODY = {
    "provider": "northstar",
    "operation_type": "resource_provision",
    "external_reference": "sec-0001",
    "payload": {},
}


async def test_a_request_without_a_token_is_refused(harness: Harness) -> None:
    response = await harness.api.post("/api/v1/integration-requests", json=WRITE_BODY)

    assert response.status_code == 401
    assert response.json()["error"]["category"] == "authentication"
    assert response.headers["WWW-Authenticate"].startswith("Bearer")


async def test_a_token_signed_with_the_wrong_key_is_refused(harness: Harness) -> None:
    forged = jwt.encode(
        {
            "sub": "attacker",
            "iss": harness.settings.jwt.issuer,
            "aud": harness.settings.jwt.audience,
            "iat": int(time.time()),
            "exp": int(time.time()) + 300,
            "roles": ["operator"],
        },
        "a-key-this-service-has-never-seen",
        algorithm="HS256",
    )

    response = await harness.api.post(
        "/api/v1/integration-requests",
        json=WRITE_BODY,
        headers={"Authorization": f"Bearer {forged}"},
    )

    assert response.status_code == 401


async def test_a_viewer_may_read_but_not_create(harness: Harness) -> None:
    created = (await harness.create_request(external_reference="sec-0002")).json()

    read = await harness.api.get(
        f"/api/v1/integration-requests/{created['id']}", headers=harness.auth("viewer")
    )
    write = await harness.api.post(
        "/api/v1/integration-requests", json=WRITE_BODY, headers=harness.auth("viewer")
    )

    assert read.status_code == 200
    assert write.status_code == 403
    assert write.json()["error"]["category"] == "authorization"


async def test_an_integration_client_may_create_but_not_cancel(harness: Harness) -> None:
    created = (
        await harness.create_request(
            provider="cobalt",
            external_reference="sec-0003",
            roles=("integration-client",),
        )
    ).json()

    cancel = await harness.api.post(
        f"/api/v1/integration-requests/{created['id']}/cancel",
        headers=harness.auth("integration-client"),
    )

    assert created["id"]
    assert cancel.status_code == 403


async def test_only_an_operator_may_retry(harness: Harness) -> None:
    created = (await harness.create_request(external_reference="sec-0004")).json()

    refused = await harness.api.post(
        f"/api/v1/integration-requests/{created['id']}/retry",
        headers=harness.auth("integration-client"),
    )

    assert refused.status_code == 403


async def test_an_authorization_failure_does_not_reveal_the_resource(
    harness: Harness,
) -> None:
    """A 404 here would confirm the identifier exists to a caller with no read scope."""
    created = (await harness.create_request(external_reference="sec-0005")).json()

    response = await harness.api.post(
        f"/api/v1/integration-requests/{created['id']}/cancel",
        headers=harness.auth("viewer"),
    )

    assert response.status_code == 403
    assert created["id"] not in response.text


async def test_an_unparseable_body_is_reported_without_echoing_it_back(
    harness: Harness,
) -> None:
    """Reflected input is how a credential in the wrong field ends up in a log."""
    response = await harness.api.post(
        "/api/v1/integration-requests",
        json={"provider": "northstar", "payload": {"secret": "s3cret-value"}},
        headers=harness.auth(),
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "request_validation_failed"
    assert "s3cret-value" not in response.text


async def test_an_unknown_provider_is_a_normalized_error(harness: Harness) -> None:
    response = await harness.api.post(
        "/api/v1/integration-requests",
        json={**WRITE_BODY, "provider": "nonexistent"},
        headers=harness.auth(),
    )

    # A provider the platform has never heard of is a malformed request, not a
    # resource that happens to be missing.
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "provider_not_configured"


async def test_an_unmatched_route_uses_the_platforms_error_envelope(
    harness: Harness,
) -> None:
    response = await harness.api.get("/api/v1/nothing-here", headers=harness.auth())

    assert response.status_code == 404
    assert set(response.json()["error"]) >= {"code", "message", "category", "retryable"}


async def test_an_operation_the_provider_cannot_perform_is_refused(
    harness: Harness,
) -> None:
    response = await harness.api.post(
        "/api/v1/integration-requests",
        json={
            "provider": "northstar",
            "operation_type": "access_grant",
            "external_reference": "sec-0006",
            "payload": {},
        },
        headers=harness.auth(),
    )

    assert response.status_code == 422
    assert response.json()["error"]["category"] == "unsupported_operation"
