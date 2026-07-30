"""Domain layer.

Contains entities, value objects, transition rules, policies and errors. This
package depends on nothing but the Python standard library; an import-linter
contract enforces that, so a stray framework import fails the build rather than
quietly coupling business rules to a database driver.
"""

from integration_orchestrator.domain.contracts import (
    CancelProviderOperationCommand,
    CreateProviderOperationCommand,
    InboundWebhook,
    NormalizedWebhookEvent,
    ProviderErrorInfo,
    ProviderHealthProbe,
    ProviderOperationResult,
    WebhookVerification,
)
from integration_orchestrator.domain.entities import (
    FailureDetail,
    IntegrationRequest,
    ProviderDescriptor,
    ProviderHealth,
    StatusTransition,
    WebhookReceipt,
)
from integration_orchestrator.domain.enums import (
    ActorType,
    AuditAction,
    CircuitState,
    ErrorCategory,
    NormalizedStatus,
    OperationType,
    RequestStatus,
    WebhookProcessingStatus,
)
from integration_orchestrator.domain.events import (
    CURRENT_EVENT_VERSION,
    EventEnvelope,
    EventType,
)
from integration_orchestrator.domain.policies import (
    CircuitBreakerPolicy,
    CircuitSnapshot,
    RetryPolicy,
)
from integration_orchestrator.domain.records import (
    AuditEvent,
    IdempotencyRecord,
    OutboxEvent,
)
from integration_orchestrator.domain.value_objects import (
    CorrelationId,
    ExternalReference,
    IdempotencyKey,
    ProviderSlug,
    RequestFingerprint,
    SignatureMetadata,
)

__all__ = [
    "CURRENT_EVENT_VERSION",
    "ActorType",
    "AuditAction",
    "AuditEvent",
    "CancelProviderOperationCommand",
    "CircuitBreakerPolicy",
    "CircuitSnapshot",
    "CircuitState",
    "CorrelationId",
    "CreateProviderOperationCommand",
    "ErrorCategory",
    "EventEnvelope",
    "EventType",
    "ExternalReference",
    "FailureDetail",
    "IdempotencyKey",
    "IdempotencyRecord",
    "InboundWebhook",
    "IntegrationRequest",
    "NormalizedStatus",
    "NormalizedWebhookEvent",
    "OperationType",
    "OutboxEvent",
    "ProviderDescriptor",
    "ProviderErrorInfo",
    "ProviderHealth",
    "ProviderHealthProbe",
    "ProviderOperationResult",
    "ProviderSlug",
    "RequestFingerprint",
    "RequestStatus",
    "RetryPolicy",
    "SignatureMetadata",
    "StatusTransition",
    "WebhookProcessingStatus",
    "WebhookReceipt",
    "WebhookVerification",
]
