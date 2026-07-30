"""Configuration validation.

The production-safety validator is the one that matters: it is what stops a
deployment starting with a development placeholder still in place, which is a
failure mode no amount of code review reliably catches.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from integration_orchestrator.config.settings import (
    SANDBOX_COBALT_WEBHOOK_PUBLIC_KEY,
    Environment,
    KafkaSettings,
    ProviderSandboxSettings,
    ProviderSettings,
    Settings,
    get_settings,
    reset_settings_cache,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _configuration_comes_from_the_test(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Build settings from declared defaults rather than the ambient environment.

    These tests assert what ``Settings`` does with a given configuration, so the
    configuration has to come from the test. CI exports ``ENVIRONMENT=test`` and
    ``KAFKA__ENABLED=false`` for the integration suite; without this isolation
    those values become a silent, invisible input to every assertion here — a
    default-only construction stops being default-only, and the production
    fixture below stops describing production.
    """
    prefixes = tuple(name.upper() for name in Settings.model_fields)
    nested = tuple(f"{prefix}__" for prefix in prefixes)
    for name in list(os.environ):
        upper = name.upper()
        if upper in prefixes or upper.startswith(nested):
            monkeypatch.delenv(name, raising=False)
    # ``env_file`` is resolved relative to the working directory, so a developer's
    # local .env would otherwise leak in the same way.
    monkeypatch.chdir(tmp_path)


def northstar_in_production() -> ProviderSettings:
    """A fully configured OAuth2 provider: real endpoints, rotated credentials."""
    return ProviderSettings(
        display_name="Northstar Connect",
        base_url="https://api.northstar.test",
        oauth_token_url="https://auth.northstar.test/oauth/token",
        client_id="northstar-issued-client-id",
        client_secret=SecretStr("rotated-northstar-credential"),
        webhook_secret=SecretStr("rotated-northstar-webhook"),
    )


def production(**overrides: object) -> Settings:
    """Build a production-shaped configuration that passes every safety rule."""
    base: dict[str, object] = {
        "environment": Environment.PRODUCTION,
        "provider_sandbox": ProviderSandboxSettings(enabled=False, mount_in_app=False),
        "log_console_renderer": False,
        "jwt": {"secret": SecretStr("a-real-signing-secret-from-the-secrets-manager")},
        "providers": {
            "northstar": northstar_in_production(),
            "meridian": ProviderSettings(enabled=False),
            "cobalt": ProviderSettings(enabled=False),
        },
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def test_local_defaults_construct_without_configuration() -> None:
    settings = Settings()

    assert settings.environment is Environment.LOCAL
    assert set(settings.providers) == {"northstar", "meridian", "cobalt"}


def test_a_production_configuration_with_real_values_is_accepted() -> None:
    assert production().environment is Environment.PRODUCTION


def test_the_provider_sandbox_cannot_run_in_production() -> None:
    with pytest.raises(ValidationError, match="sandbox"):
        production(provider_sandbox=ProviderSandboxSettings(enabled=True, mount_in_app=False))


def test_a_placeholder_jwt_secret_stops_production_starting() -> None:
    with pytest.raises(ValidationError, match="JWT__SECRET"):
        production(jwt={"secret": SecretStr("local-development-signing-secret")})


def test_a_placeholder_provider_credential_stops_production_starting() -> None:
    with pytest.raises(ValidationError, match="client_secret"):
        production(
            providers={
                "northstar": ProviderSettings(
                    base_url="https://api.northstar.test",
                    client_secret=SecretStr("northstar-local-secret"),
                )
            }
        )


def test_a_provider_still_pointing_at_localhost_stops_production_starting() -> None:
    with pytest.raises(ValidationError, match="local base URL"):
        production(
            providers={"northstar": ProviderSettings(base_url="http://localhost:8000/__sandbox__")}
        )


def test_a_provider_still_using_the_sandbox_token_endpoint_stops_production_starting() -> None:
    """A real API paired with a sandbox token URL fails on the first call."""
    with pytest.raises(ValidationError, match="local OAuth2 token URL"):
        production(
            providers={
                "northstar": ProviderSettings(
                    base_url="https://api.northstar.test",
                    oauth_token_url="http://localhost:8000/__sandbox__/northstar/oauth/token",
                    client_id="northstar-issued-client-id",
                    client_secret=SecretStr("rotated-northstar-credential"),
                    webhook_secret=SecretStr("rotated-northstar-webhook"),
                )
            }
        )


def test_a_provider_without_its_credentials_stops_production_starting() -> None:
    """Meridian authenticates by API key, so a missing key means every call 401s."""
    with pytest.raises(ValidationError, match="missing its api_key"):
        production(
            providers={
                "meridian": ProviderSettings(
                    base_url="https://api.meridian.test",
                    api_key=None,
                    webhook_secret=SecretStr("rotated-meridian-webhook"),
                )
            }
        )


def test_a_provider_without_webhook_material_stops_production_starting() -> None:
    """Unverifiable deliveries are rejected, so work would silently never complete."""
    with pytest.raises(ValidationError, match="webhook verification material"):
        production(
            providers={
                "northstar": ProviderSettings(
                    base_url="https://api.northstar.test",
                    oauth_token_url="https://auth.northstar.test/oauth/token",
                    client_id="northstar-issued-client-id",
                    client_secret=SecretStr("rotated-northstar-credential"),
                    webhook_secret=None,
                )
            }
        )


def test_the_sandbox_webhook_key_stops_production_starting() -> None:
    """The sandbox signing key is public; anyone could forge a delivery with it."""
    with pytest.raises(ValidationError, match="sandbox key"):
        production(
            providers={
                "cobalt": ProviderSettings(
                    base_url="https://api.cobalt.test",
                    oauth_token_url="https://auth.cobalt.test/oauth/token",
                    client_id="cobalt-issued-client-id",
                    client_secret=SecretStr("rotated-cobalt-credential"),
                    webhook_public_key=SANDBOX_COBALT_WEBHOOK_PUBLIC_KEY,
                )
            }
        )


def test_a_disabled_provider_is_not_held_to_production_rules() -> None:
    """A provider nobody is using should not block a deployment."""
    settings = production(
        providers={
            "northstar": northstar_in_production(),
            "meridian": ProviderSettings(enabled=False, base_url="http://localhost:8000"),
            "cobalt": ProviderSettings(enabled=False),
        }
    )

    assert settings.enabled_providers().keys() == {"northstar"}


def test_console_logging_cannot_be_enabled_in_production() -> None:
    with pytest.raises(ValidationError, match="JSON"):
        production(log_console_renderer=True)


def test_the_in_memory_publisher_cannot_be_selected_in_production() -> None:
    """Publishing into a process-local list would be silent event loss."""
    with pytest.raises(ValidationError, match="Kafka"):
        production(kafka=KafkaSettings(enabled=False))


def test_database_echo_cannot_be_enabled_in_production() -> None:
    with pytest.raises(ValidationError, match="echoing"):
        production(database={"echo": True})


def test_local_environments_are_free_to_use_placeholders() -> None:
    settings = Settings(environment=Environment.LOCAL)

    assert settings.provider_sandbox.enabled is True


def test_partial_provider_overrides_keep_the_rest_of_the_defaults() -> None:
    """Pydantic replaces the whole dict, so the merge has to be explicit."""
    settings = Settings(providers={"northstar": ProviderSettings(total_timeout_seconds=99.0)})

    northstar = settings.provider("northstar")
    assert northstar.total_timeout_seconds == 99.0
    assert northstar.display_name == "Northstar Connect"
    assert "meridian" in settings.providers


def test_an_unknown_provider_lookup_raises_a_key_error() -> None:
    with pytest.raises(KeyError):
        Settings().provider("nonexistent")


@pytest.mark.parametrize(
    ("event_type", "expected"),
    [
        ("integration.request.succeeded.v1", "integration.request"),
        ("provider.circuit.opened.v1", "provider.circuit"),
        ("short", "integration.short"),
    ],
)
def test_lifecycle_events_for_one_aggregate_share_a_topic(event_type: str, expected: str) -> None:
    assert KafkaSettings().topic_for(event_type) == expected


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql+asyncpg://user:p%40ss@db.example:5433/database?sslmode=require",
        "postgresql://user:p%40ss@db.example:5433/database?sslmode=require",
        "postgresql+psycopg://user:p%40ss@db.example:5433/database?sslmode=require",
        "postgresql+psycopg2://user:p%40ss@db.example:5433/database?sslmode=require",
    ],
)
def test_the_alembic_url_uses_a_synchronous_driver(database_url: str) -> None:
    """Alembic's runner is synchronous; handing it an asyncpg URL fails at import."""
    settings = Settings(database={"url": database_url})

    assert settings.database.sync_url == (
        "postgresql+psycopg://user:p%40ss@db.example:5433/database?sslmode=require"
    )


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql://user:password@localhost/database",
        "postgresql+psycopg://user:password@localhost/database",
        "postgresql+psycopg2://user:password@localhost/database",
    ],
)
def test_application_database_urls_always_use_asyncpg(database_url: str) -> None:
    settings = Settings(database={"url": database_url})

    assert settings.database.url == "postgresql+asyncpg://user:password@localhost/database"


def test_describing_the_configuration_never_reveals_a_secret() -> None:
    """`config` is meant to be safe to paste into an incident channel."""
    settings = Settings(
        jwt={"secret": SecretStr("the-real-signing-key")},
        providers={"meridian": ProviderSettings(api_key=SecretStr("the-real-api-key"))},
    )

    rendered = json.dumps(settings.describe())

    assert "the-real-signing-key" not in rendered
    assert "the-real-api-key" not in rendered
    assert rendered.count("**********") >= 2
    assert "Meridian Services" in rendered


def test_settings_are_parsed_once_per_process() -> None:
    reset_settings_cache()
    try:
        assert get_settings() is get_settings()
    finally:
        reset_settings_cache()
