"""Webhook signature primitives."""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from integration_orchestrator.infrastructure.providers.signatures import (
    MAX_TIMESTAMP_SKEW_SECONDS,
    compute_hmac_sha256,
    parse_timestamp,
    sign_ed25519,
    signature_digest,
    verify_ed25519,
    verify_hmac_sha256,
    within_replay_window,
)

pytestmark = pytest.mark.unit

SECRET = "webhook-secret"
BODY = b'{"event_id":"evt-1","event_type":"operation.completed"}'
NOW = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)


def test_a_matching_hmac_signature_verifies() -> None:
    signature = compute_hmac_sha256(secret=SECRET, message=BODY)

    assert verify_hmac_sha256(secret=SECRET, message=BODY, signature=signature)


def test_an_algorithm_prefixed_signature_is_accepted() -> None:
    """Providers differ on whether they prefix the scheme; both forms are real."""
    signature = compute_hmac_sha256(secret=SECRET, message=BODY)

    assert verify_hmac_sha256(secret=SECRET, message=BODY, signature=f"sha256={signature}")


def test_a_signature_over_different_bytes_fails() -> None:
    """Re-serialising the body before verification would break exactly this."""
    signature = compute_hmac_sha256(secret=SECRET, message=BODY)
    tampered = BODY.replace(b"completed", b"failed   ")

    assert not verify_hmac_sha256(secret=SECRET, message=tampered, signature=signature)


def test_a_signature_from_another_secret_fails() -> None:
    signature = compute_hmac_sha256(secret="other-secret", message=BODY)

    assert not verify_hmac_sha256(secret=SECRET, message=BODY, signature=signature)


@pytest.mark.parametrize("signature", ["", "not-hex", "sha256="])
def test_malformed_signatures_are_rejected_without_raising(signature: str) -> None:
    assert not verify_hmac_sha256(secret=SECRET, message=BODY, signature=signature)


# -- asymmetric -------------------------------------------------------------


def test_an_ed25519_signature_verifies_against_the_public_key() -> None:
    """Holding only a public key means a compromise here cannot forge webhooks."""
    private_key = Ed25519PrivateKey.generate()
    from cryptography.hazmat.primitives import serialization

    private_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_b64 = base64.b64encode(
        private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        )
    ).decode("ascii")

    signature = sign_ed25519(private_key_bytes=private_bytes, message=BODY)

    assert verify_ed25519(public_key_b64=public_b64, message=BODY, signature_b64=signature)
    assert not verify_ed25519(
        public_key_b64=public_b64, message=b"tampered", signature_b64=signature
    )


@pytest.mark.parametrize(
    ("public_key", "signature"),
    [("not-base64!!", "AAAA"), ("AAAA", "not-base64!!"), ("", "")],
)
def test_malformed_asymmetric_material_is_rejected_without_raising(
    public_key: str, signature: str
) -> None:
    assert not verify_ed25519(public_key_b64=public_key, message=BODY, signature_b64=signature)


# -- timestamps and replay --------------------------------------------------


def test_unix_and_iso_timestamps_are_both_understood() -> None:
    assert parse_timestamp("1772366400") is not None
    assert parse_timestamp("2026-03-01T12:00:00Z") == NOW
    assert parse_timestamp("2026-03-01T12:00:00+00:00") == NOW


def test_a_naive_iso_timestamp_is_treated_as_utc() -> None:
    assert parse_timestamp("2026-03-01T12:00:00") == NOW


@pytest.mark.parametrize("value", [None, "", "not-a-timestamp"])
def test_unparseable_timestamps_return_none(value: str | None) -> None:
    assert parse_timestamp(value) is None


def test_a_fresh_timestamp_is_inside_the_replay_window() -> None:
    assert within_replay_window(NOW - timedelta(seconds=10), now=NOW)


def test_an_old_capture_is_outside_the_replay_window() -> None:
    stale = NOW - timedelta(seconds=MAX_TIMESTAMP_SKEW_SECONDS + 1)

    assert not within_replay_window(stale, now=NOW)


def test_a_timestamp_far_in_the_future_is_equally_suspicious() -> None:
    """Otherwise an attacker could mint a webhook that stays valid indefinitely."""
    future = NOW + timedelta(seconds=MAX_TIMESTAMP_SKEW_SECONDS + 1)

    assert not within_replay_window(future, now=NOW)


def test_a_missing_timestamp_never_passes_the_window_check() -> None:
    assert not within_replay_window(None, now=NOW)


def test_the_replay_cache_key_does_not_contain_the_signature() -> None:
    """A stored signature is a reusable credential; a digest of it is not."""
    signature = compute_hmac_sha256(secret=SECRET, message=BODY)

    digest = signature_digest(signature)

    assert signature not in digest
    assert len(digest) == 32
    assert digest == signature_digest(signature)
