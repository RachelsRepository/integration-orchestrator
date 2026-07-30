"""The provider abstraction.

:class:`ProviderGateway` is the contract every provider adapter satisfies. Its
methods speak only in normalized commands and results, so orchestration code is
written once and works for every provider. Provider quirks — inconsistent field
names, different authentication schemes, whether cancellation exists at all —
are absorbed entirely inside the adapters.

:class:`ProviderRegistry` is how orchestration finds an adapter. Because lookup
is by slug against a registered map, adding a provider means registering an
adapter, not editing a conditional in a use case.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol, runtime_checkable

from integration_orchestrator.domain.contracts import (
    CancelProviderOperationCommand,
    CreateProviderOperationCommand,
    InboundWebhook,
    NormalizedWebhookEvent,
    ProviderHealthProbe,
    ProviderOperationResult,
    WebhookVerification,
)
from integration_orchestrator.domain.entities import ProviderDescriptor
from integration_orchestrator.domain.value_objects import ProviderSlug


@runtime_checkable
class ProviderGateway(Protocol):
    """One external provider, expressed in normalized terms.

    Implementations raise
    :class:`~integration_orchestrator.domain.errors.ProviderError` subclasses for
    transport and protocol failures, and return a
    :class:`~integration_orchestrator.domain.contracts.ProviderOperationResult`
    carrying an error for business rejections the provider reported cleanly. The
    distinction matters: the former is about reaching the provider, the latter is
    about what the provider decided.
    """

    @property
    def slug(self) -> ProviderSlug:
        """The provider's stable identifier."""
        ...

    def descriptor(self) -> ProviderDescriptor:
        """Static capabilities, used for routing decisions and the providers API."""
        ...

    async def create_operation(
        self, command: CreateProviderOperationCommand
    ) -> ProviderOperationResult:
        """Create an operation at the provider."""
        ...

    async def get_operation_status(self, provider_reference: str) -> ProviderOperationResult:
        """Fetch the provider's current view of an operation.

        Providers that cannot be polled raise
        :class:`~integration_orchestrator.domain.errors.UnsupportedOperationError`,
        which reconciliation treats as "cannot verify" rather than "not found".
        """
        ...

    async def cancel_operation(
        self, command: CancelProviderOperationCommand
    ) -> ProviderOperationResult:
        """Cancel an operation, where the provider supports it."""
        ...

    def validate_webhook(self, webhook: InboundWebhook) -> WebhookVerification:
        """Verify authenticity, integrity and freshness of an inbound webhook.

        Synchronous and side-effect free so that verification can be unit tested
        against recorded payloads without any I/O.
        """
        ...

    def normalize_webhook(self, webhook: InboundWebhook) -> NormalizedWebhookEvent:
        """Translate a verified webhook body into a normalized event."""
        ...

    async def health_check(self) -> ProviderHealthProbe:
        """Probe provider reachability for the providers endpoint."""
        ...


@runtime_checkable
class ProviderRegistry(Protocol):
    """Lookup of provider adapters by slug."""

    def get(self, slug: ProviderSlug) -> ProviderGateway:
        """Return the adapter for ``slug``.

        Raises
        :class:`~integration_orchestrator.domain.errors.ProviderNotConfiguredError`
        when the provider is unknown or disabled.
        """
        ...

    def has(self, slug: ProviderSlug) -> bool:
        """Report whether an enabled adapter exists for ``slug``."""
        ...

    def all(self) -> Iterable[ProviderGateway]:
        """Iterate every enabled adapter."""
        ...

    def descriptors(self) -> Iterable[ProviderDescriptor]:
        """Iterate the capabilities of every enabled provider."""
        ...
