"""Redaction: what must never leave the process."""

from __future__ import annotations

import pytest

from integration_orchestrator.observability.redaction import (
    MAX_DEPTH,
    MAX_SEQUENCE_ITEMS,
    MAX_STRING_LENGTH,
    REDACTED,
    is_sensitive_key,
    mask_secret,
    redact,
    redact_headers,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "key",
    [
        "password",
        "client_secret",
        "clientSecret",
        "CLIENT-SECRET",
        "access_token",
        "api_key",
        "apiKey",
        "authorization",
        "x-signature",
        "private_key",
        "email",
        "phone_number",
        "date_of_birth",
    ],
)
def test_credential_and_personal_keys_are_sensitive(key: str) -> None:
    assert is_sensitive_key(key)


@pytest.mark.parametrize(
    "key",
    ["provider", "status", "token_type", "authentication_type", "signature_scheme", "attempt"],
)
def test_descriptive_keys_that_merely_look_sensitive_are_kept(key: str) -> None:
    """Redacting ``token_type`` would remove information with no secret in it."""
    assert not is_sensitive_key(key)


def test_sensitive_values_are_replaced_at_every_level() -> None:
    redacted = redact(
        {
            "provider": "northstar",
            "connection": {"client_secret": "s3cret", "client_id": "public-id"},
            "items": [{"api_key": "k-1"}, {"quantity": 3}],
        }
    )

    assert redacted["provider"] == "northstar"
    assert redacted["connection"]["client_secret"] == REDACTED
    assert redacted["connection"]["client_id"] == "public-id"
    assert redacted["items"][0]["api_key"] == REDACTED
    assert redacted["items"][1]["quantity"] == 3


def test_a_container_named_after_credentials_is_redacted_whole() -> None:
    """Nothing under a key called ``credentials`` is worth risking."""
    assert redact({"credentials": {"client_id": "public-id"}})["credentials"] == REDACTED


def test_the_original_structure_is_not_mutated() -> None:
    original = {"client_secret": "s3cret"}

    redact(original)

    assert original["client_secret"] == "s3cret"


def test_deep_structures_are_summarised_rather_than_walked_forever() -> None:
    """An unbounded walk in the logging path is a denial-of-service vector."""
    deep: dict[str, object] = {"leaf": 1}
    for _ in range(MAX_DEPTH + 3):
        deep = {"nested": deep}

    rendered = redact(deep)

    flattened = str(rendered)
    assert "dict with" in flattened


def test_long_sequences_are_truncated_with_a_count() -> None:
    rendered = redact(list(range(MAX_SEQUENCE_ITEMS + 5)))

    assert len(rendered) == MAX_SEQUENCE_ITEMS + 1
    assert rendered[-1] == "[5 more items omitted]"


def test_long_strings_are_truncated() -> None:
    rendered = redact("x" * (MAX_STRING_LENGTH + 10))

    assert rendered.endswith("...[truncated]")
    assert len(rendered) == MAX_STRING_LENGTH + len("...[truncated]")


def test_headers_carrying_credentials_are_redacted() -> None:
    headers = redact_headers(
        {
            "authorization": "Bearer abc",
            "x-northstar-signature": "sig",
            "content-type": "application/json",
        }
    )

    assert headers["authorization"] == REDACTED
    assert headers["x-northstar-signature"] == REDACTED
    assert headers["content-type"] == "application/json"


def test_a_masked_secret_reveals_only_enough_to_tell_two_apart() -> None:
    assert mask_secret("supersecretvalue") == "********alue"
    assert mask_secret("tiny") == REDACTED
    assert mask_secret(None) == REDACTED
    assert "supersecret" not in mask_secret("supersecretvalue")
