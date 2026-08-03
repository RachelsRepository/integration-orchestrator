"""Built-in workflow definitions for local and Compose verification."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from integration_orchestrator.domain.enums import OperationType
from integration_orchestrator.domain.workflow import (
    WorkflowDefinition,
    WorkflowStepDefinition,
)

CUSTOMER_ONBOARDING = "customer_onboarding"
PARALLEL_PROVISIONING = "parallel_provisioning"


def customer_onboarding_v1(*, definition_id: UUID, now: datetime) -> WorkflowDefinition:
    """Northstar → Meridian → Cobalt (webhook) onboarding saga."""
    return WorkflowDefinition(
        id=definition_id,
        name=CUSTOMER_ONBOARDING,
        version=1,
        created_at=now,
        steps=(
            WorkflowStepDefinition(
                key="create_customer",
                provider="northstar",
                operation_type=OperationType.RESOURCE_PROVISION,
                compensate_operation=OperationType.RESOURCE_DEPROVISION,
                payload_template={"resource_name": "customer"},
            ),
            WorkflowStepDefinition(
                key="create_subscription",
                provider="meridian",
                operation_type=OperationType.RESOURCE_PROVISION,
                depends_on=("create_customer",),
                compensate_operation=OperationType.RESOURCE_DEPROVISION,
                payload_template={"resource_name": "subscription"},
            ),
            WorkflowStepDefinition(
                key="register_callback",
                provider="cobalt",
                operation_type=OperationType.RESOURCE_PROVISION,
                depends_on=("create_subscription",),
                compensate_operation=OperationType.RESOURCE_DEPROVISION,
                # Cobalt accepts asynchronously; sandbox still completes via
                # webhook, and the workflow hook resumes WAITING steps.
                wait_for_webhook=True,
                payload_template={"resource_name": "callback"},
            ),
        ),
    )


def parallel_provisioning_v1(*, definition_id: UUID, now: datetime) -> WorkflowDefinition:
    """Fan-out after Northstar, then fan-in join on Cobalt.

    create_customer (A)
        ├── provision_billing (B, Meridian)
        └── register_notify (C, Cobalt)
              └── finalize_join (D) depends on B and C
    """
    return WorkflowDefinition(
        id=definition_id,
        name=PARALLEL_PROVISIONING,
        version=1,
        created_at=now,
        steps=(
            WorkflowStepDefinition(
                key="create_customer",
                provider="northstar",
                operation_type=OperationType.RESOURCE_PROVISION,
                compensate_operation=OperationType.RESOURCE_DEPROVISION,
                payload_template={"resource_name": "customer"},
            ),
            WorkflowStepDefinition(
                key="provision_billing",
                provider="meridian",
                operation_type=OperationType.RESOURCE_PROVISION,
                depends_on=("create_customer",),
                compensate_operation=OperationType.RESOURCE_DEPROVISION,
                payload_template={"resource_name": "billing"},
            ),
            WorkflowStepDefinition(
                key="register_notify",
                provider="cobalt",
                operation_type=OperationType.RESOURCE_PROVISION,
                depends_on=("create_customer",),
                compensate_operation=OperationType.RESOURCE_DEPROVISION,
                wait_for_webhook=True,
                payload_template={"resource_name": "notify"},
            ),
            WorkflowStepDefinition(
                key="finalize_join",
                provider="northstar",
                operation_type=OperationType.RESOURCE_PROVISION,
                depends_on=("provision_billing", "register_notify"),
                compensate_operation=OperationType.RESOURCE_DEPROVISION,
                payload_template={"resource_name": "join"},
            ),
        ),
    )
