"""Shared API schema primitives."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    """Base for every API model.

    ``extra="forbid"`` on request bodies is a deliberate strictness: silently
    accepting an unknown field means a client that misspells ``external_reference``
    gets a 201 and no operation, and discovers the mistake in production.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class ErrorBody(ApiModel):
    """The normalized error envelope returned by every failing endpoint."""

    code: str = Field(description="Stable machine-readable error code.")
    message: str = Field(description="Human-readable description, safe to log.")
    category: str = Field(description="Normalized error category.")
    retryable: bool = Field(description="Whether repeating the request could succeed.")
    provider: str | None = Field(default=None, description="Provider involved, if any.")
    provider_code: str | None = Field(
        default=None, description="The provider's own error code, when it supplied one."
    )
    correlation_id: str | None = Field(
        default=None, description="Correlation id for locating this failure in the logs."
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional non-sensitive context."
    )


class ErrorResponse(ApiModel):
    """Top-level error response body."""

    error: ErrorBody


class PageMeta(ApiModel):
    """Cursor pagination metadata."""

    next_cursor: str | None = Field(
        default=None,
        description="Opaque cursor for the next page. Absent when the page is the last one.",
    )
    has_more: bool = Field(description="Whether more results are available.")
