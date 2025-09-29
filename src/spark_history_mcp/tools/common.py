"""
Common utilities, constants, configuration, and conversions for tools.

This module centralizes:
- Shared constants and unit conversions
- Lightweight config resolution for tool thresholds/limits
- Safe helpers used across multiple tools
- Client resolution from the MCP app context
"""

from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar

import sys

from ..core.app import mcp

from pydantic import Field
from pydantic_settings import BaseSettings

BYTES_IN_GB = 1024 * 1024 * 1024
NS_PER_MIN = 1000 * 1000 * 1000 * 60
MS_PER_MIN = 1000 * 60

# Defaults used across tools
DEFAULT_INTERVAL_MINUTES = 1
MAX_INTERVALS = 10_000
SIGNIFICANCE_DEFAULT = 0.1


class ToolConfig(BaseSettings):
    """Configuration values used across tools.

    Load order precedence (highest to lowest):
    - Explicit overrides passed to ``get_config``
    - Environment variables with ``SHS_`` prefix
    - Defaults in this class
    """

    # Comparison and significance thresholds
    significance_threshold: float = Field(
        default=SIGNIFICANCE_DEFAULT, description="Default significance threshold for comparisons"
    )

    # Matching thresholds
    stage_match_similarity: float = Field(
        default=0.75, description="Minimum name similarity (0-1) to consider stages a match"
    )
    time_overlap_window_s: int = Field(
        default=300, description="Window in seconds for considering stage time overlap"
    )

    # Timeline settings
    default_interval_minutes: int = Field(
        default=DEFAULT_INTERVAL_MINUTES,
        description="Timeline interval size in minutes",
    )
    timeline_max_intervals: int = Field(
        default=MAX_INTERVALS, description="Hard cap on timeline intervals"
    )

    # Heuristics for recommendations
    gc_pressure_threshold: float = Field(
        default=0.2, description="GC time / executor runtime threshold"
    )
    high_spill_bytes: int = Field(
        default=500 * 1024 * 1024, description="Spill threshold in bytes (default 500MB)"
    )
    min_stage_duration_s: int = Field(
        default=10, description="Minimum stage duration in seconds for analysis"
    )
    min_task_count: int = Field(default=10, description="Minimum tasks for reliable stats")

    # SQL pagination and defaults
    sql_page_size: int = Field(default=100, description="Page size for SQL listings")
    include_running_defaults: bool = Field(
        default=False, description="Default for including running entities in rankings"
    )

    # Debug/validation
    debug_validate_schema: bool = Field(
        default=False, description="Validate outputs against schema in debug mode"
    )

    # Thresholds used in recommendations
    large_stage_diff_seconds: int = Field(
        default=60, description="Threshold for flagging large stage time differences"
    )

    model_config = {
        "env_prefix": "SHS_",
    }


def get_config(**overrides: Any) -> ToolConfig:
    """Resolve tool configuration.

    Args:
        overrides: Optional explicit values that take precedence over env/defaults.

    Returns:
        ToolConfig instance combining env/defaults with overrides.
    """
    base_config = ToolConfig()
    if not overrides:
        return base_config

    merged = base_config.model_dump()
    merged.update({k: v for k, v in overrides.items() if v is not None})
    return ToolConfig(**merged)


def bytes_to_gb(value: float | int) -> float:
    if not value:
        return 0.0
    return float(value) / BYTES_IN_GB


def ns_to_min(value: float | int) -> float:
    if not value:
        return 0.0
    return float(value) / NS_PER_MIN


def ms_to_min(value: float | int) -> float:
    if not value:
        return 0.0
    return float(value) / MS_PER_MIN


def pct_change(val1: float, val2: float) -> str:
    if val1 == 0:
        return "N/A" if val2 == 0 else "+∞"
    change = ((val2 - val1) / val1) * 100
    sign = "+" if change >= 0 else ""
    return f"{sign}{change:.1f}%"


T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


def safe_get(fn: Callable[[], T], default: Optional[T] = None) -> Optional[T]:
    """Execute ``fn`` and return its result; on exception return ``default``.

    Keeps try/except noise out of callers for non-critical paths.
    """
    try:
        return fn()
    except Exception:
        return default


def resolve_legacy_tool(name: str, fallback: F) -> F:
    """Return a callable patched via the legacy ``tools`` module if available.

    Many unit tests patch ``spark_history_mcp.tools.tools`` functions directly.
    After the refactor to modular packages those patches no longer affected the
    imported callables captured at module load time. This helper checks whether
    the legacy module defines an override and, if so, returns it.
    """

    tools_module = sys.modules.get("spark_history_mcp.tools.tools")
    if tools_module:
        candidate = getattr(tools_module, name, None)
        if callable(candidate):
            return candidate  # type: ignore[return-value]
    return fallback


def get_active_mcp_context():
    """Return the active MCP request context if one exists."""

    try:
        return mcp.get_context()
    except ValueError:
        pass

    tools_module = sys.modules.get("spark_history_mcp.tools.tools")
    if tools_module:
        try:
            return tools_module.mcp.get_context()
        except Exception:
            return None
    return None


def get_client(ctx, server: Optional[str] = None):
    """Resolve a Spark client from MCP context and optional server name.

    Mirrors logic used across tools to pick a named client or the default.
    """
    if ctx is None:
        raise ValueError(
            "Spark MCP context is not available outside of a request"
        )
    clients = ctx.request_context.lifespan_context.clients
    default_client = ctx.request_context.lifespan_context.default_client

    if server:
        client = clients.get(server)
        if client:
            return client
    if default_client:
        return default_client
    raise ValueError(
        "No Spark client found. Please specify a valid server name or set a default server."
    )


def get_server_key(server: Optional[str]) -> str:
    """Canonicalize a server key for caches and logs."""
    return server or "__default__"


def get_client_or_default(ctx, server_name: Optional[str] = None):
    """Backward-compatible alias widely used across tool modules.

    Older tests patch ``spark_history_mcp.tools.tools.get_client_or_default`` in
    place. When we refactored into modular packages those patches no longer hit
    this helper because modules captured the original function reference. To
    preserve the legacy behaviour we look for a patched implementation on the
    legacy ``tools`` module and delegate to it when present.
    """

    tools_module = sys.modules.get("spark_history_mcp.tools.tools")
    if tools_module:
        patched_getter = getattr(tools_module, "get_client_or_default", None)
        if patched_getter and patched_getter is not get_client_or_default:
            return patched_getter(ctx, server_name)

    analysis_module = sys.modules.get("spark_history_mcp.tools.analysis")
    if analysis_module:
        patched_getter = getattr(analysis_module, "get_client_or_default", None)
        if patched_getter and patched_getter is not get_client_or_default:
            return patched_getter(ctx, server_name)

    if ctx is None:
        ctx = get_active_mcp_context()
    return get_client(ctx, server_name)
