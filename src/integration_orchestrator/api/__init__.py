"""HTTP interface.

The API layer translates between HTTP and the application layer and does nothing
else. It parses untrusted input into value objects, enforces authentication and
authorization, and renders results. It contains no orchestration logic, so the
same use cases are driven identically by the workers.
"""

from integration_orchestrator.api.app import create_app

__all__ = ["create_app"]
