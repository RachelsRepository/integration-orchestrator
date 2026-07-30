"""Architecture boundaries, enforced as tests.

`import-linter` checks the same rules from configuration and runs in CI, but it
is a separate command that is easy to forget locally. These tests read the source
with `ast` and fail in the ordinary test run, which is where a boundary
violation is cheapest to notice — right after it is written.

The rules exist because the value of this structure is entirely in what it
forbids. A domain that quietly imports SQLAlchemy is no longer a domain model,
it is a persistence model with extra steps.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "src" / "integration_orchestrator"

#: Third-party packages that must never appear in the inner layers. Anything
#: that ties code to a framework, a driver or a wire protocol.
FRAMEWORKS = frozenset(
    {
        "fastapi",
        "starlette",
        "sqlalchemy",
        "alembic",
        "asyncpg",
        "redis",
        "aiokafka",
        "httpx",
        "uvicorn",
        "prometheus_client",
        "jwt",
        "opentelemetry",
    }
)


@dataclass(frozen=True, slots=True)
class Import:
    """One import statement, with enough context to report it usefully."""

    module: str
    source: Path
    line: int

    @property
    def top_level(self) -> str:
        return self.module.split(".", 1)[0]

    def is_internal(self, layer: str) -> bool:
        return self.module.startswith(f"integration_orchestrator.{layer}")

    def __str__(self) -> str:
        return f"{self.source.name}:{self.line} imports {self.module}"


def imports_of(layer: str) -> list[Import]:
    """Collect every module imported by a layer, including inside functions.

    Deferred imports are collected deliberately: moving an import into a
    function body hides it from a naive scan but does not make the dependency
    any less real.
    """
    found: list[Import] = []
    for path in sorted((PACKAGE_ROOT / layer).rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found.extend(Import(alias.name, path, node.lineno) for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                found.append(Import(node.module, path, node.lineno))
    return found


def test_the_domain_layer_exists_where_the_tests_think_it_does() -> None:
    """Guards the other tests: a bad path would make them all pass vacuously."""
    assert (PACKAGE_ROOT / "domain" / "entities.py").is_file()
    assert len(imports_of("domain")) > 10


def test_the_domain_imports_no_framework() -> None:
    offenders = [imp for imp in imports_of("domain") if imp.top_level in FRAMEWORKS]

    assert not offenders, "the domain must stay pure:\n" + "\n".join(str(i) for i in offenders)


@pytest.mark.parametrize("forbidden", ["application", "infrastructure", "api", "workers", "config"])
def test_the_domain_imports_no_outer_layer(forbidden: str) -> None:
    offenders = [imp for imp in imports_of("domain") if imp.is_internal(forbidden)]

    assert not offenders, "\n".join(str(i) for i in offenders)


def test_the_application_layer_imports_no_framework() -> None:
    """Use cases talk to ports. A driver import here means a leaked adapter."""
    offenders = [imp for imp in imports_of("application") if imp.top_level in FRAMEWORKS]

    assert not offenders, "\n".join(str(i) for i in offenders)


@pytest.mark.parametrize("forbidden", ["infrastructure", "api", "workers"])
def test_the_application_layer_imports_no_outer_layer(forbidden: str) -> None:
    offenders = [imp for imp in imports_of("application") if imp.is_internal(forbidden)]

    assert not offenders, "\n".join(str(i) for i in offenders)


def test_the_infrastructure_layer_never_imports_the_api() -> None:
    """Adapters are driven by the application, never by the transport."""
    offenders = [imp for imp in imports_of("infrastructure") if imp.is_internal("api")]

    assert not offenders, "\n".join(str(i) for i in offenders)


def test_the_workers_never_import_the_api() -> None:
    offenders = [imp for imp in imports_of("workers") if imp.is_internal("api")]

    assert not offenders, "\n".join(str(i) for i in offenders)


def test_the_api_layer_builds_no_adapter_of_its_own() -> None:
    """Wiring belongs in the composition root.

    The API is allowed to name infrastructure types for annotations and to reach
    for the container, but a router that constructs a concrete adapter has
    quietly become a second composition root, and the two will diverge.
    """
    allowed = {
        "integration_orchestrator.composition",
        "integration_orchestrator.infrastructure.security.tokens",
        "integration_orchestrator.infrastructure.providers.sandbox.app",
    }
    offenders = [
        imp
        for imp in imports_of("api")
        if imp.is_internal("infrastructure") and imp.module not in allowed
    ]

    assert not offenders, "\n".join(str(i) for i in offenders)


def test_the_observability_layer_stays_independent_of_the_business_layers() -> None:
    """Logging and metrics are used by everything, so they may depend on nothing."""
    offenders = [
        imp
        for imp in imports_of("observability")
        if any(imp.is_internal(layer) for layer in ("application", "infrastructure", "api"))
    ]

    assert not offenders, "\n".join(str(i) for i in offenders)


def test_no_module_outside_the_sandbox_imports_the_sandbox() -> None:
    """The fake providers must not be reachable from a production path.

    Only the composition root and the API's explicitly guarded mount may name
    them, and both do it behind a configuration flag that production rejects.
    """
    allowed_importers = {"composition.py", "app.py", "demo.py"}
    offenders = [
        imp
        for layer in ("domain", "application", "infrastructure", "workers", "config")
        for imp in imports_of(layer)
        if "providers.sandbox" in imp.module
        and "sandbox" not in imp.source.parts
        and imp.source.name not in allowed_importers
    ]

    assert not offenders, "\n".join(str(i) for i in offenders)
