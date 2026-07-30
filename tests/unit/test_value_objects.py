"""Value object validation and canonical fingerprinting."""

from __future__ import annotations

import pytest

from integration_orchestrator.domain.errors import ValidationError
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ExternalReference,
    IdempotencyKey,
    ProviderSlug,
    RequestFingerprint,
    canonical_json,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("value", ["northstar", "meridian-services", "cobalt_network"])
def test_valid_provider_slugs_are_accepted(value: str) -> None:
    assert ProviderSlug(value).value == value


@pytest.mark.parametrize("value", ["", "N", "Northstar", "1northstar", "has space", "a" * 41])
def test_malformed_provider_slugs_are_rejected(value: str) -> None:
    with pytest.raises(ValidationError):
        ProviderSlug(value)


def test_provider_slugs_are_normalized_when_parsed_from_input() -> None:
    assert ProviderSlug.parse("  NorthStar  ").value == "northstar"


def test_a_missing_provider_is_a_validation_error_not_a_crash() -> None:
    with pytest.raises(ValidationError):
        ProviderSlug.parse("   ")


def test_a_missing_correlation_id_is_generated_rather_than_rejected() -> None:
    """Callers should not lose tracing just because they forgot a header."""
    assert CorrelationId.parse(None).value
    assert CorrelationId.parse("  ").value


def test_an_oversized_correlation_id_is_truncated_when_parsed() -> None:
    assert len(CorrelationId.parse("x" * 500).value) == 128


@pytest.mark.parametrize("value", ["short", "has spaces!!", "a" * 256])
def test_malformed_idempotency_keys_are_rejected(value: str) -> None:
    with pytest.raises(ValidationError):
        IdempotencyKey(value)


def test_an_absent_idempotency_key_parses_to_none() -> None:
    assert IdempotencyKey.parse(None) is None
    assert IdempotencyKey.parse("") is None


def test_an_empty_external_reference_is_rejected() -> None:
    with pytest.raises(ValidationError):
        ExternalReference("  ")


# -- fingerprints -----------------------------------------------------------


def _fingerprint(payload: dict[str, object]) -> str:
    return RequestFingerprint.of(
        provider=ProviderSlug("northstar"),
        operation_type="resource_provision",
        external_reference=ExternalReference("order-1"),
        payload=payload,
    ).value


def test_key_order_does_not_change_the_fingerprint() -> None:
    """A client that serialises its JSON differently on retry must not conflict."""
    assert _fingerprint({"a": 1, "b": 2}) == _fingerprint({"b": 2, "a": 1})


def test_integral_floats_and_integers_hash_identically() -> None:
    assert _fingerprint({"quantity": 1.0}) == _fingerprint({"quantity": 1})


def test_a_different_payload_produces_a_different_fingerprint() -> None:
    assert _fingerprint({"quantity": 1}) != _fingerprint({"quantity": 2})


def test_the_provider_is_part_of_the_fingerprint() -> None:
    payload = {"quantity": 1}
    other = RequestFingerprint.of(
        provider=ProviderSlug("meridian"),
        operation_type="resource_provision",
        external_reference=ExternalReference("order-1"),
        payload=payload,
    )
    assert _fingerprint(payload) != other.value


def test_booleans_are_not_collapsed_into_numbers() -> None:
    assert _fingerprint({"flag": True}) != _fingerprint({"flag": 1})


def test_nested_structures_are_canonicalised_recursively() -> None:
    assert canonical_json({"b": [{"z": 1, "a": 2}]}) == '{"b":[{"a":2,"z":1}]}'


def test_a_fingerprint_must_be_a_sha256_digest() -> None:
    with pytest.raises(ValidationError):
        RequestFingerprint("too-short")
