"""Bearer token verification and scope resolution."""

from __future__ import annotations

import time

import jwt
import pytest

from integration_orchestrator.config.settings import JWTSettings
from integration_orchestrator.domain.errors import AuthenticationError, AuthorizationError
from integration_orchestrator.infrastructure.security.tokens import (
    ROLE_SCOPES,
    Principal,
    Scope,
    TokenVerifier,
    issue_local_token,
)

pytestmark = pytest.mark.unit

SETTINGS = JWTSettings()
OTHER_SETTINGS = JWTSettings(secret="a-completely-different-signing-secret")


@pytest.fixture
def verifier() -> TokenVerifier:
    return TokenVerifier(SETTINGS)


def _claims(**overrides: object) -> dict[str, object]:
    now = int(time.time())
    claims: dict[str, object] = {
        "sub": "service-account",
        "iss": SETTINGS.issuer,
        "aud": SETTINGS.audience,
        "iat": now,
        "exp": now + 300,
    }
    claims.update(overrides)
    return claims


def _encode(claims: dict[str, object], *, settings: JWTSettings = SETTINGS) -> str:
    return jwt.encode(claims, settings.secret.get_secret_value(), algorithm="HS256")


def test_a_valid_token_yields_the_principal(verifier: TokenVerifier) -> None:
    token = _encode(_claims(scope="requests:read providers:read", jti="token-1"))

    principal = verifier.verify(token)

    assert principal.subject == "service-account"
    assert principal.scopes == frozenset({Scope.REQUESTS_READ, Scope.PROVIDERS_READ})
    assert principal.token_id == "token-1"


def test_roles_expand_into_their_scopes(verifier: TokenVerifier) -> None:
    token = _encode(_claims(roles=["operator"]))

    principal = verifier.verify(token)

    assert principal.scopes == Scope.ALL
    assert principal.roles == frozenset({"operator"})


def test_a_scope_this_service_never_defined_is_dropped(verifier: TokenVerifier) -> None:
    """A token from a broader identity provider must not smuggle in permissions."""
    token = _encode(_claims(scope="requests:read admin:everything"))

    principal = verifier.verify(token)

    assert principal.scopes == frozenset({Scope.REQUESTS_READ})


def test_a_token_signed_with_another_key_is_rejected(verifier: TokenVerifier) -> None:
    token = _encode(_claims(), settings=OTHER_SETTINGS)

    with pytest.raises(AuthenticationError):
        verifier.verify(token)


def test_an_expired_token_is_rejected(verifier: TokenVerifier) -> None:
    now = int(time.time())
    token = _encode(_claims(iat=now - 7200, exp=now - 3600))

    with pytest.raises(AuthenticationError):
        verifier.verify(token)


def test_a_token_for_another_audience_is_rejected(verifier: TokenVerifier) -> None:
    token = _encode(_claims(aud="some-other-service"))

    with pytest.raises(AuthenticationError):
        verifier.verify(token)


def test_a_token_from_another_issuer_is_rejected(verifier: TokenVerifier) -> None:
    token = _encode(_claims(iss="https://attacker.example"))

    with pytest.raises(AuthenticationError):
        verifier.verify(token)


@pytest.mark.parametrize("missing", ["exp", "iat", "sub", "aud", "iss"])
def test_every_required_claim_is_enforced(verifier: TokenVerifier, missing: str) -> None:
    """A suppressed claim must not suppress its own validation."""
    claims = _claims()
    claims.pop(missing)

    with pytest.raises(AuthenticationError):
        verifier.verify(_encode(claims))


def test_an_unsigned_token_is_rejected(verifier: TokenVerifier) -> None:
    """The 'alg: none' attack, explicitly."""
    token = jwt.encode(_claims(), key="", algorithm="none")

    with pytest.raises(AuthenticationError):
        verifier.verify(token)


def test_the_rejection_reason_is_never_returned_to_the_caller(
    verifier: TokenVerifier,
) -> None:
    """Telling an unauthenticated caller why their token failed is reconnaissance."""
    with pytest.raises(AuthenticationError) as caught:
        verifier.verify("not-even-a-token")

    assert caught.value.message == "the supplied bearer token is not valid"
    assert caught.value.metadata == {}


# -- authorization ----------------------------------------------------------


def test_a_principal_without_the_scope_is_refused() -> None:
    principal = Principal(subject="s", scopes=frozenset({Scope.REQUESTS_READ}))

    principal.require(Scope.REQUESTS_READ)
    with pytest.raises(AuthorizationError) as caught:
        principal.require(Scope.REQUESTS_WRITE)

    assert caught.value.required_scope == Scope.REQUESTS_WRITE


def test_a_viewer_cannot_create_retry_or_cancel() -> None:
    """Read access must not confer the ability to cause provider side effects."""
    viewer = ROLE_SCOPES["viewer"]

    assert Scope.REQUESTS_WRITE not in viewer
    assert Scope.REQUESTS_RETRY not in viewer
    assert Scope.REQUESTS_CANCEL not in viewer


def test_an_integration_client_cannot_perform_operator_actions() -> None:
    client = ROLE_SCOPES["integration-client"]

    assert Scope.REQUESTS_WRITE in client
    assert Scope.REQUESTS_RETRY not in client
    assert Scope.REQUESTS_CANCEL not in client


def test_every_role_grants_only_scopes_this_service_defines() -> None:
    for scopes in ROLE_SCOPES.values():
        assert scopes <= Scope.ALL


# -- local minting ----------------------------------------------------------


def test_a_locally_minted_token_verifies(verifier: TokenVerifier) -> None:
    token = issue_local_token(SETTINGS, subject="demo", roles=["operator"])

    principal = verifier.verify(token)

    assert principal.subject == "demo"
    assert principal.scopes == Scope.ALL


def test_local_minting_refuses_to_run_under_rs256(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """The helper must never be a reason for a private key to be present."""
    key_path = tmp_path / "public.pem"
    key_path.write_text("-----BEGIN PUBLIC KEY-----\n", encoding="utf-8")
    settings = JWTSettings(algorithm="RS256", public_key_path=key_path)

    with pytest.raises(ValueError, match="HS256"):
        issue_local_token(settings, subject="demo", roles=["operator"])


def test_rs256_settings_require_key_material() -> None:
    with pytest.raises(ValueError, match="PUBLIC_KEY_PATH"):
        JWTSettings(algorithm="RS256")
