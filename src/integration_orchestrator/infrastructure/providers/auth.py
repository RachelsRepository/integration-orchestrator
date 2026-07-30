"""Provider authentication strategies.

Two schemes are implemented because the fictional providers use both, and the
difference is instructive: an API key is a static secret sent on every request,
while OAuth2 client credentials is a short-lived token that must be obtained,
cached, shared, refreshed before expiry, and invalidated when rejected.

Credentials arrive from configuration, which sources them from the environment
and, in a deployment, from a secrets manager. They are held as ``SecretStr`` so
that an accidental log of a settings object prints ``**********`` rather than the
secret, and they are never written to audit rows, events, or trace attributes.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable

import httpx

from integration_orchestrator.application.ports.resilience import LockManager
from integration_orchestrator.application.ports.security import CachedToken, TokenCache
from integration_orchestrator.config.settings import ProviderSettings
from integration_orchestrator.domain.errors import ProviderAuthenticationError
from integration_orchestrator.domain.value_objects import ProviderSlug

logger = logging.getLogger(__name__)

# How long a caller waits for another process to finish refreshing before giving
# up and refreshing itself. Short, because the alternative to waiting is a
# duplicate token request rather than a failure.
_REFRESH_LOCK_WAIT_SECONDS = 3.0
_REFRESH_LOCK_TTL_SECONDS = 15.0
_DEFAULT_TOKEN_LIFETIME_SECONDS = 3600


@runtime_checkable
class ProviderAuthenticator(Protocol):
    """Supplies authentication headers for provider calls."""

    async def headers(self) -> dict[str, str]:
        """Return the headers that authenticate a request."""
        ...

    async def invalidate(self) -> None:
        """Discard any cached credential after a provider rejected it."""
        ...

    @property
    def can_refresh(self) -> bool:
        """Whether invalidating and retrying once could plausibly help."""
        ...


class ApiKeyAuthenticator:
    """Static API key authentication.

    There is nothing to refresh: if the provider rejects the key, the key is
    wrong and retrying with the same key will fail identically. ``can_refresh``
    is therefore ``False``, which stops the HTTP client wasting an attempt.
    """

    def __init__(self, *, slug: ProviderSlug, config: ProviderSettings, header: str) -> None:
        if config.api_key is None:
            raise ProviderAuthenticationError(
                "no API key is configured for this provider", provider=slug.value
            )
        self._slug = slug
        self._header = header
        self._api_key = config.api_key.get_secret_value()

    async def headers(self) -> dict[str, str]:
        return {self._header: self._api_key}

    async def invalidate(self) -> None:
        return None

    @property
    def can_refresh(self) -> bool:
        return False


class OAuth2ClientCredentialsAuthenticator:
    """OAuth2 client credentials with a shared, lock-guarded token cache."""

    def __init__(
        self,
        *,
        slug: ProviderSlug,
        config: ProviderSettings,
        client: httpx.AsyncClient,
        token_cache: TokenCache,
        locks: LockManager,
    ) -> None:
        if not config.oauth_token_url or config.client_id is None or config.client_secret is None:
            raise ProviderAuthenticationError(
                "OAuth2 client credentials are not fully configured for this provider",
                provider=slug.value,
            )
        self._slug = slug
        self._config = config
        self._client = client
        self._token_cache = token_cache
        self._locks = locks
        self._token_url = config.oauth_token_url
        self._client_id = config.client_id
        self._client_secret = config.client_secret.get_secret_value()

    async def headers(self) -> dict[str, str]:
        token = await self._acquire_token()
        return {"Authorization": token.authorization_header}

    async def invalidate(self) -> None:
        await self._token_cache.invalidate(self._slug)

    @property
    def can_refresh(self) -> bool:
        return True

    async def _acquire_token(self) -> CachedToken:
        cached = await self._token_cache.get(self._slug)
        if cached is not None and cached.is_fresh(
            now=datetime.now(tz=UTC),
            leeway_seconds=self._config.token_refresh_leeway_seconds,
        ):
            return cached

        # Only one caller in the fleet should hit the token endpoint. The lock is
        # advisory: if it cannot be taken, fetching anyway is the lesser evil.
        async with self._locks.lock(
            f"token-refresh:{self._slug.value}",
            ttl_seconds=_REFRESH_LOCK_TTL_SECONDS,
            wait_seconds=_REFRESH_LOCK_WAIT_SECONDS,
        ) as acquired:
            if not acquired:
                # Someone else is refreshing. Re-read first: by now they have
                # very likely published a fresh token.
                recheck = await self._token_cache.get(self._slug)
                if recheck is not None and recheck.is_fresh(
                    now=datetime.now(tz=UTC),
                    leeway_seconds=self._config.token_refresh_leeway_seconds,
                ):
                    return recheck
                logger.info(
                    "refreshing a provider token without the coordination lock",
                    extra={"provider": self._slug.value},
                )
                return await self._fetch_and_cache()

            # Re-check inside the lock. Another process may have refreshed
            # between our first read and acquiring the lock.
            recheck = await self._token_cache.get(self._slug)
            if recheck is not None and recheck.is_fresh(
                now=datetime.now(tz=UTC),
                leeway_seconds=self._config.token_refresh_leeway_seconds,
            ):
                return recheck
            return await self._fetch_and_cache()

    async def _fetch_and_cache(self) -> CachedToken:
        token = await self._request_token()
        await self._token_cache.set(self._slug, token)
        return token

    async def _request_token(self) -> CachedToken:
        form = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        if self._config.oauth_scope:
            form["scope"] = self._config.oauth_scope

        try:
            response = await self._client.post(
                self._token_url,
                data=form,
                headers={"Accept": "application/json"},
                timeout=httpx.Timeout(
                    self._config.total_timeout_seconds,
                    connect=self._config.connect_timeout_seconds,
                ),
            )
        except httpx.HTTPError as exc:
            raise ProviderAuthenticationError(
                "could not reach the provider token endpoint",
                provider=self._slug.value,
                retryable=True,
                metadata={"transport_error": type(exc).__name__},
            ) from exc

        if response.status_code != 200:
            # The body of a failed token request routinely echoes the client id
            # and sometimes the submitted secret, so only the status is recorded.
            raise ProviderAuthenticationError(
                "the provider rejected our client credentials",
                provider=self._slug.value,
                metadata={"http_status": response.status_code},
            )

        try:
            body = response.json()
            access_token = body["access_token"]
        except (ValueError, KeyError, TypeError) as exc:
            raise ProviderAuthenticationError(
                "the provider returned a token response we could not parse",
                provider=self._slug.value,
            ) from exc

        expires_in = int(body.get("expires_in", _DEFAULT_TOKEN_LIFETIME_SECONDS))
        logger.info(
            "obtained a provider access token",
            extra={"provider": self._slug.value, "expires_in_seconds": expires_in},
        )
        return CachedToken(
            value=access_token,
            expires_at=datetime.now(tz=UTC) + timedelta(seconds=expires_in),
            token_type=body.get("token_type", "Bearer"),
        )


def build_authenticator(
    *,
    slug: ProviderSlug,
    config: ProviderSettings,
    client: httpx.AsyncClient,
    token_cache: TokenCache,
    locks: LockManager,
    api_key_header: str = "X-API-Key",
) -> ProviderAuthenticator:
    """Select the authenticator matching a provider's configured scheme."""
    from integration_orchestrator.config.settings import AuthenticationType

    if config.authentication_type is AuthenticationType.OAUTH2_CLIENT_CREDENTIALS:
        return OAuth2ClientCredentialsAuthenticator(
            slug=slug,
            config=config,
            client=client,
            token_cache=token_cache,
            locks=locks,
        )
    return ApiKeyAuthenticator(slug=slug, config=config, header=api_key_header)
