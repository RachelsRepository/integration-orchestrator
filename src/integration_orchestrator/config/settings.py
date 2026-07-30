"""Typed, validated application settings.

Configuration is loaded from the process environment (and, for local development,
from a ``.env`` file). Nested settings use a double-underscore delimiter, so the
Northstar timeout budget is expressed as::

    PROVIDERS__NORTHSTAR__TOTAL_TIMEOUT_SECONDS=6.0

Production safety is enforced by validators rather than by convention: when
``ENVIRONMENT=production`` the settings object refuses to construct if any local
development placeholder is still in place.
"""

from __future__ import annotations

import functools
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_SANDBOX_ROOT = "http://localhost:8000/__sandbox__"
POSTGRESQL_ASYNC_DRIVER = "asyncpg"
POSTGRESQL_SYNC_DRIVER = "psycopg"

# Placeholder secrets shipped in .env.example. Their presence in a production
# environment indicates the deployment never received real credentials.
_LOCAL_PLACEHOLDER_MARKERS = ("local-", "changeme", "example", "placeholder")

# Hosts that only ever mean "this developer's machine". A provider endpoint
# still naming one of these in a deployed environment is a leftover default.
_LOCAL_HOST_MARKERS = ("localhost", "127.0.0.1", "0.0.0.0", "::1")  # noqa: S104

#: Base64 of the sandbox's Ed25519 webhook public key. Duplicated here as a
#: literal rather than imported, because configuration must not depend on
#: infrastructure; a test asserts the two never drift apart.
SANDBOX_COBALT_WEBHOOK_PUBLIC_KEY = "lySCb9L+MRe1IRjHP9/7MuZTTxyeIZPaEg6v623YYF4="


class Environment(StrEnum):
    """Deployment environment identity."""

    LOCAL = "local"
    TEST = "test"
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"

    @property
    def is_production_like(self) -> bool:
        return self in (Environment.STAGING, Environment.PRODUCTION)


class AuthenticationType(StrEnum):
    """How the orchestrator authenticates to a provider."""

    OAUTH2_CLIENT_CREDENTIALS = "oauth2_client_credentials"
    API_KEY = "api_key"


class DatabaseSettings(BaseModel):
    """PostgreSQL connection and pool configuration."""

    url: str = "postgresql+asyncpg://orchestrator:orchestrator@localhost:5432/orchestrator"
    pool_size: Annotated[int, Field(ge=1, le=100)] = 10
    max_overflow: Annotated[int, Field(ge=0, le=100)] = 5
    pool_timeout_seconds: Annotated[float, Field(gt=0)] = 10.0
    pool_recycle_seconds: Annotated[int, Field(gt=0)] = 1800
    statement_timeout_ms: Annotated[int, Field(gt=0)] = 15_000
    echo: bool = False

    @field_validator("url")
    @classmethod
    def _use_async_driver(cls, value: str) -> str:
        """Keep application database access on SQLAlchemy's asyncpg dialect."""
        return postgresql_url_with_driver(value, driver=POSTGRESQL_ASYNC_DRIVER)

    @property
    def sync_url(self) -> str:
        """Driver-synchronous URL, used by Alembic's migration runner."""
        return postgresql_url_with_driver(self.url, driver=POSTGRESQL_SYNC_DRIVER)


def postgresql_url_with_driver(url: str, *, driver: str) -> str:
    """Select a PostgreSQL driver while preserving the URL payload exactly.

    Only the scheme changes. Credentials (including percent-encoding), host,
    port, database name and query parameters remain byte-for-byte unchanged.
    Non-PostgreSQL URLs are returned unchanged so Pydantic can report any
    downstream configuration error in the usual place.
    """
    scheme, separator, payload = url.partition("://")
    if not separator or scheme.split("+", maxsplit=1)[0] != "postgresql":
        return url
    return f"postgresql+{driver}://{payload}"


class RedisSettings(BaseModel):
    """Redis connection configuration.

    Redis backs the provider token cache, the distributed lock, the circuit
    breaker and the provider rate limiter. It is a soft dependency: the circuit
    breaker and rate limiter fail open when Redis is unavailable so that a Redis
    outage degrades protection rather than blocking all provider traffic.
    """

    url: str = "redis://localhost:6379/0"
    socket_timeout_seconds: Annotated[float, Field(gt=0)] = 2.0
    socket_connect_timeout_seconds: Annotated[float, Field(gt=0)] = 2.0
    max_connections: Annotated[int, Field(ge=1)] = 50
    namespace: str = "integration-orchestrator"


class KafkaSettings(BaseModel):
    """Kafka producer configuration for outbox publication."""

    enabled: bool = True
    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "integration"
    client_id: str = "integration-orchestrator"
    request_timeout_seconds: Annotated[float, Field(gt=0)] = 10.0
    acks: Literal["all", "1", "0"] = "all"
    compression_type: Literal["gzip", "snappy", "lz4", "none"] = "gzip"
    linger_ms: Annotated[int, Field(ge=0)] = 20

    def topic_for(self, event_type: str) -> str:
        """Map an event type to its topic.

        ``integration.request.succeeded.v1`` becomes ``integration.request`` so
        that all lifecycle events for one aggregate share a topic and therefore
        share partition ordering when keyed by aggregate id.
        """
        parts = event_type.split(".")
        if len(parts) < 3:
            return f"{self.topic_prefix}.{event_type}"
        return ".".join(parts[:2])


class JWTSettings(BaseModel):
    """Internal API bearer token validation settings.

    HS256 is the local default because it requires no key material on disk.
    Deployed environments are expected to use RS256 with a public key mounted
    from a secrets manager.
    """

    algorithm: Literal["HS256", "RS256"] = "HS256"
    issuer: str = "https://auth.local.integration-orchestrator.test"
    audience: str = "integration-orchestrator"
    secret: SecretStr = SecretStr("local-development-signing-secret-not-for-production")
    public_key_path: Path | None = None
    leeway_seconds: Annotated[int, Field(ge=0, le=300)] = 10
    access_token_ttl_seconds: Annotated[int, Field(gt=0)] = 3600

    @model_validator(mode="after")
    def _validate_key_material(self) -> JWTSettings:
        if self.algorithm == "RS256" and self.public_key_path is None:
            raise ValueError("JWT__PUBLIC_KEY_PATH is required when JWT__ALGORITHM=RS256")
        return self


class ProviderSettings(BaseModel):
    """Per-provider configuration.

    Every knob that governs how the orchestrator talks to one provider lives
    here: credentials, timeout budget, retry bounds, circuit breaker thresholds,
    the bulkhead concurrency limit and the client-side rate limit. Providers are
    configured independently so that one misbehaving provider cannot consume the
    capacity reserved for the others.
    """

    enabled: bool = True
    display_name: str = ""
    authentication_type: AuthenticationType = AuthenticationType.API_KEY
    base_url: str = ""

    # Credentials. Optional because which ones apply depends on the auth type.
    client_id: str | None = None
    client_secret: SecretStr | None = None
    oauth_token_url: str | None = None
    oauth_scope: str | None = None
    api_key: SecretStr | None = None

    # Webhook verification material.
    webhook_secret: SecretStr | None = None
    webhook_public_key: str | None = None

    # Timeout budget. ``total_timeout_seconds`` bounds a single provider call
    # including connection establishment; it is not a per-attempt budget.
    total_timeout_seconds: Annotated[float, Field(gt=0, le=120)] = 6.0
    connect_timeout_seconds: Annotated[float, Field(gt=0, le=30)] = 2.0

    # Retry policy.
    max_attempts: Annotated[int, Field(ge=1, le=10)] = 3
    backoff_base_seconds: Annotated[float, Field(gt=0)] = 0.5
    backoff_multiplier: Annotated[float, Field(ge=1.0)] = 2.0
    backoff_max_seconds: Annotated[float, Field(gt=0)] = 60.0
    retry_after_cap_seconds: Annotated[float, Field(gt=0)] = 300.0

    # Bulkhead: the maximum number of in-flight calls to this provider from a
    # single process. Prevents a slow provider exhausting the worker pool.
    max_concurrency: Annotated[int, Field(ge=1, le=1000)] = 10
    acquire_timeout_seconds: Annotated[float, Field(gt=0)] = 2.0

    # Client-side rate limiting, expressed as a token bucket.
    rate_limit_per_second: Annotated[float, Field(gt=0)] = 20.0
    rate_limit_burst: Annotated[int, Field(ge=1)] = 40

    # Circuit breaker.
    circuit_failure_threshold: Annotated[int, Field(ge=1)] = 5
    circuit_open_seconds: Annotated[float, Field(gt=0)] = 30.0
    circuit_half_open_max_probes: Annotated[int, Field(ge=1)] = 1
    circuit_success_threshold: Annotated[int, Field(ge=1)] = 2

    # Token cache refresh headroom: refresh this many seconds before expiry so a
    # token never expires mid-flight.
    token_refresh_leeway_seconds: Annotated[int, Field(ge=0)] = 60

    @field_validator("base_url", "oauth_token_url")
    @classmethod
    def _strip_trailing_slash(cls, value: str | None) -> str | None:
        return value.rstrip("/") if value else value


class WebhookSettings(BaseModel):
    """Inbound webhook verification policy."""

    replay_window_seconds: Annotated[int, Field(ge=30, le=3600)] = 300
    max_body_bytes: Annotated[int, Field(ge=1024)] = 262_144
    signature_dedupe_ttl_seconds: Annotated[int, Field(ge=60)] = 900


class WorkerSettings(BaseModel):
    """Background worker cadence and batching."""

    outbox_poll_interval_seconds: Annotated[float, Field(gt=0)] = 1.0
    outbox_batch_size: Annotated[int, Field(ge=1, le=1000)] = 100
    outbox_max_attempts: Annotated[int, Field(ge=1)] = 10
    outbox_retry_base_seconds: Annotated[float, Field(gt=0)] = 2.0

    retry_poll_interval_seconds: Annotated[float, Field(gt=0)] = 2.0
    retry_batch_size: Annotated[int, Field(ge=1, le=500)] = 25

    reconciliation_interval_seconds: Annotated[float, Field(gt=0)] = 30.0
    reconciliation_batch_size: Annotated[int, Field(ge=1, le=500)] = 50
    reconciliation_stale_after_seconds: Annotated[int, Field(ge=10)] = 300
    reconciliation_manual_review_after_seconds: Annotated[int, Field(ge=60)] = 3600

    webhook_deferred_retry_seconds: Annotated[float, Field(gt=0)] = 15.0
    webhook_deferred_batch_size: Annotated[int, Field(ge=1, le=500)] = 50
    webhook_deferred_abandon_after_seconds: Annotated[int, Field(ge=60)] = 86_400

    shutdown_grace_seconds: Annotated[float, Field(gt=0)] = 15.0


class ObservabilitySettings(BaseModel):
    """Tracing and metrics configuration."""

    tracing_enabled: bool = False
    otlp_endpoint: str = "http://localhost:4317"
    trace_sample_ratio: Annotated[float, Field(ge=0.0, le=1.0)] = 1.0
    metrics_enabled: bool = True
    metrics_path: str = "/metrics"


class ProviderSandboxSettings(BaseModel):
    """Controls for the deterministic provider sandbox.

    The sandbox is a set of fake provider HTTP services used for local
    demonstration and testing. It is explicitly isolated from production paths:
    ``mount_in_app`` is rejected in production-like environments.
    """

    enabled: bool = True
    mount_in_app: bool = True
    mount_path: str = "/__sandbox__"
    #: Where the sandbox delivers its webhooks. It runs inside the API process,
    #: so the loopback default is correct both on a workstation and in a
    #: container; it is configurable because a split deployment would need the
    #: service address instead.
    callback_base_url: str = "http://localhost:8000"


def _default_providers() -> dict[str, ProviderSettings]:
    """Baseline configuration for the three fictional providers.

    Values here are safe local defaults. Every field can be overridden through
    the environment, and additional providers can be added purely through
    configuration plus an adapter registration.
    """
    return {
        "northstar": ProviderSettings(
            display_name="Northstar Connect",
            authentication_type=AuthenticationType.OAUTH2_CLIENT_CREDENTIALS,
            base_url=f"{DEFAULT_SANDBOX_ROOT}/northstar",
            oauth_token_url=f"{DEFAULT_SANDBOX_ROOT}/northstar/oauth/token",
            client_id="northstar-local-client",
            client_secret=SecretStr("northstar-local-secret"),
            oauth_scope="operations.write",
            webhook_secret=SecretStr("northstar-local-webhook-secret"),
            total_timeout_seconds=6.0,
            max_attempts=4,
            max_concurrency=16,
            rate_limit_per_second=25.0,
            circuit_failure_threshold=5,
            circuit_open_seconds=30.0,
        ),
        "meridian": ProviderSettings(
            display_name="Meridian Services",
            authentication_type=AuthenticationType.API_KEY,
            base_url=f"{DEFAULT_SANDBOX_ROOT}/meridian",
            api_key=SecretStr("meridian-local-api-key"),
            webhook_secret=SecretStr("meridian-local-webhook-secret"),
            total_timeout_seconds=5.0,
            max_attempts=3,
            max_concurrency=8,
            rate_limit_per_second=10.0,
            circuit_failure_threshold=5,
            circuit_open_seconds=45.0,
        ),
        "cobalt": ProviderSettings(
            display_name="Cobalt Network",
            authentication_type=AuthenticationType.OAUTH2_CLIENT_CREDENTIALS,
            base_url=f"{DEFAULT_SANDBOX_ROOT}/cobalt",
            oauth_token_url=f"{DEFAULT_SANDBOX_ROOT}/cobalt/oauth/token",
            client_id="cobalt-local-client",
            client_secret=SecretStr("cobalt-local-secret"),
            oauth_scope="operations.write operations.cancel",
            webhook_public_key=SANDBOX_COBALT_WEBHOOK_PUBLIC_KEY,
            total_timeout_seconds=8.0,
            max_attempts=4,
            max_concurrency=12,
            rate_limit_per_second=20.0,
            circuit_failure_threshold=4,
            circuit_open_seconds=60.0,
        ),
    }


class Settings(BaseSettings):
    """Root settings object."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
        case_sensitive=False,
    )

    environment: Environment = Environment.LOCAL
    service_name: str = "integration-orchestrator"
    service_version: str = "0.1.0"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_console_renderer: bool = False
    api_root_path: str = ""

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    jwt: JWTSettings = Field(default_factory=JWTSettings)
    webhooks: WebhookSettings = Field(default_factory=WebhookSettings)
    workers: WorkerSettings = Field(default_factory=WorkerSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    provider_sandbox: ProviderSandboxSettings = Field(default_factory=ProviderSandboxSettings)
    providers: dict[str, ProviderSettings] = Field(default_factory=_default_providers)

    @field_validator("providers", mode="after")
    @classmethod
    def _merge_provider_defaults(
        cls, value: dict[str, ProviderSettings]
    ) -> dict[str, ProviderSettings]:
        """Overlay environment-supplied provider settings onto the defaults.

        Pydantic replaces the whole dict when any nested key is supplied by the
        environment, which would silently drop the display name and auth type of
        a provider whose timeout was overridden. Merging field-by-field keeps
        partial overrides working as operators expect.
        """
        merged = _default_providers()
        for slug, supplied in value.items():
            key = slug.lower()
            base = merged.get(key)
            if base is None:
                merged[key] = supplied
                continue
            overrides = supplied.model_dump(exclude_unset=True)
            merged[key] = base.model_copy(update=overrides)
        return merged

    @model_validator(mode="after")
    def _validate_production_safety(self) -> Settings:
        if not self.environment.is_production_like:
            return self

        problems: list[str] = []

        if self.provider_sandbox.mount_in_app or self.provider_sandbox.enabled:
            problems.append(
                "the provider sandbox must be disabled outside local and test environments"
            )
        if self.log_console_renderer:
            problems.append("console log rendering must be disabled; deployed logs must be JSON")
        if not self.kafka.enabled:
            # With Kafka off the outbox publishes into an in-process list, so
            # every event would be lost on restart while the outbox rows were
            # marked published. That is silent data loss, not degraded service.
            problems.append(
                "Kafka must be enabled; the in-memory publisher is a local-only adapter"
            )
        if self.database.echo:
            problems.append("database statement echoing must be disabled")
        if self.jwt.algorithm == "HS256" and _looks_like_placeholder(
            self.jwt.secret.get_secret_value()
        ):
            problems.append("JWT__SECRET is still the development placeholder")

        for slug, provider in self.providers.items():
            # A provider nobody is calling cannot cause a production incident, so
            # it is not held to these rules.
            if provider.enabled:
                problems.extend(_provider_problems(slug, provider))

        if problems:
            joined = "; ".join(problems)
            raise ValueError(
                f"unsafe configuration for environment '{self.environment.value}': {joined}"
            )
        return self

    def provider(self, slug: str) -> ProviderSettings:
        """Return configuration for one provider.

        Raises ``KeyError`` when the provider is unknown, which callers translate
        into a normalized error rather than a 500.
        """
        return self.providers[slug.lower()]

    def enabled_providers(self) -> dict[str, ProviderSettings]:
        return {slug: cfg for slug, cfg in self.providers.items() if cfg.enabled}

    def describe(self) -> dict[str, Any]:
        """Return the effective configuration with every secret masked.

        Answers the first question of most incidents — what is this process
        actually running with — without turning the answer into a credential
        leak. Secrets are declared as ``SecretStr``, which serialises to a mask
        in JSON mode, so a new secret field is protected by its type rather than
        by remembering to add it to a deny-list here.
        """
        return self.model_dump(mode="json")


def _provider_problems(slug: str, provider: ProviderSettings) -> list[str]:
    """Production-safety problems for one enabled provider.

    Split out of the root validator so the provider rules can be read on their
    own, and so neither function turns into an unreadable decision tree.
    """
    return _provider_endpoint_problems(slug, provider) + _provider_credential_problems(
        slug, provider
    )


def _provider_endpoint_problems(slug: str, provider: ProviderSettings) -> list[str]:
    """Reject provider endpoints that are still the local sandbox."""
    problems: list[str] = []

    if not provider.base_url or _is_local_url(provider.base_url):
        problems.append(f"provider '{slug}' still points at a local base URL")

    if provider.authentication_type is not AuthenticationType.OAUTH2_CLIENT_CREDENTIALS:
        return problems

    # Checked alongside the base URL because a real API paired with a sandbox
    # token endpoint fails at the first call, not at deployment.
    if not provider.oauth_token_url:
        problems.append(f"provider '{slug}' has no OAuth2 token URL")
    elif _is_local_url(provider.oauth_token_url):
        problems.append(f"provider '{slug}' still points at a local OAuth2 token URL")

    return problems


def _provider_credential_problems(slug: str, provider: ProviderSettings) -> list[str]:
    """Reject missing, placeholder or sandbox provider credentials."""
    problems: list[str] = []

    if provider.authentication_type is AuthenticationType.OAUTH2_CLIENT_CREDENTIALS:
        if provider.client_id is None or provider.client_secret is None:
            problems.append(f"provider '{slug}' is missing its OAuth2 client credentials")
    elif provider.api_key is None:
        problems.append(f"provider '{slug}' is missing its api_key")

    # Provider work completes by webhook here. An adapter with no verification
    # material rejects every delivery, so requests would stay in flight until
    # reconciliation gave up — a silent failure worth catching at startup.
    if provider.webhook_secret is None and not provider.webhook_public_key:
        problems.append(f"provider '{slug}' has no webhook verification material")
    if provider.webhook_public_key == SANDBOX_COBALT_WEBHOOK_PUBLIC_KEY:
        problems.append(f"provider '{slug}' webhook_public_key is still the sandbox key")

    for label, secret in (
        ("client_secret", provider.client_secret),
        ("api_key", provider.api_key),
        ("webhook_secret", provider.webhook_secret),
    ):
        if secret is not None and _looks_like_placeholder(secret.get_secret_value()):
            problems.append(f"provider '{slug}' {label} is still a development placeholder")

    # ``client_id`` is not a secret, so the SecretStr scan above never sees it,
    # but a leftover development client id is just as broken.
    if provider.client_id is not None and _looks_like_placeholder(provider.client_id):
        problems.append(f"provider '{slug}' client_id is still a development placeholder")

    return problems


def _is_local_url(url: str) -> bool:
    lowered = url.lower()
    return any(marker in lowered for marker in _LOCAL_HOST_MARKERS)


def _looks_like_placeholder(secret: str) -> bool:
    lowered = secret.lower()
    return any(marker in lowered for marker in _LOCAL_PLACEHOLDER_MARKERS)


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton.

    Cached so that configuration is parsed and validated exactly once per
    process. Tests call :func:`reset_settings_cache` when they need to rebuild
    settings from a modified environment.
    """
    return Settings()


def reset_settings_cache() -> None:
    """Clear the settings cache. Intended for tests only."""
    get_settings.cache_clear()
