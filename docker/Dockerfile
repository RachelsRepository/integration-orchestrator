# Multi-stage build.
#
# The builder stage installs dependencies from the lock file so the image is
# reproducible: the same commit produces the same dependency tree, which is what
# makes "it worked in CI" a meaningful statement.
#
# The runtime stage carries no build toolchain, no lock file resolver and no
# development dependencies. It runs as a non-root user with a read-only-friendly
# layout, because a process that never needs to write to its own image should not
# be able to.
#
# Both stages use WORKDIR /app so the virtualenv is created at the same absolute
# path the runtime stage will use. Console-script shebangs embed that path; a
# builder that installed into /build/.venv and then copied to /app/.venv would
# leave every entrypoint pointing at a path that no longer exists.

FROM python:3.12-slim-bookworm AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

COPY --from=ghcr.io/astral-sh/uv:0.12.0 /uv /usr/local/bin/uv

WORKDIR /app

# Dependencies are installed before the source is copied, so editing application
# code does not invalidate the dependency layer.
# Hatchling reads both files while building the project metadata.
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN uv sync --frozen --no-dev --no-editable


FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PATH="/app/.venv/bin:$PATH"

# curl is present for the container healthcheck. Everything else stays out.
RUN apt-get update \
    && apt-get install --no-install-recommends -y curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1001 orchestrator \
    && useradd --system --uid 1001 --gid orchestrator --create-home orchestrator

WORKDIR /app

COPY --from=builder --chown=orchestrator:orchestrator /app/.venv /app/.venv
COPY --chown=orchestrator:orchestrator src /app/src
COPY --chown=orchestrator:orchestrator migrations /app/migrations
COPY --chown=orchestrator:orchestrator alembic.ini /app/alembic.ini
COPY --chown=orchestrator:orchestrator scripts /app/scripts

USER orchestrator

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD curl --fail --silent http://localhost:8000/health/live || exit 1

# Invoke via ``python -m`` so startup does not depend on a relocatable console
# script shebang. Single worker per container: scaling is horizontal, so each
# process has a predictable connection-pool footprint against PostgreSQL.
CMD ["python", "-m", "uvicorn", "integration_orchestrator.api.app:create_app", \
     "--factory", "--host", "0.0.0.0", "--port", "8000", \
     "--no-access-log", "--proxy-headers"]
