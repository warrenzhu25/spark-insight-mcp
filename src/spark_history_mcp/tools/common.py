"""
Common utilities, constants, configuration, and conversions for tools.

This module centralizes:
- Shared constants and unit conversions
- Lightweight config resolution for tool thresholds/limits
- Safe helpers used across multiple tools
- Client resolution from the MCP app context
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Callable, Optional, TypeVar

from pydantic_settings import BaseSettings
from pydantic import Field

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


@lru_cache(maxsize=1)
def _base_config() -> ToolConfig:
    """Load base config from environment once (env_prefix=SHS_)."""
    return ToolConfig()


def get_config(**overrides: Any) -> ToolConfig:
    """Resolve tool configuration.

    Args:
        overrides: Optional explicit values that take precedence over env/defaults.

    Returns:
        ToolConfig instance combining env/defaults with overrides.
    """
    if not overrides:
        return _base_config()
    merged = _base_config().model_dump()
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


def safe_get(fn: Callable[[], T], default: Optional[T] = None) -> Optional[T]:
    """Execute ``fn`` and return its result; on exception return ``default``.

    Keeps try/except noise out of callers for non-critical paths.
    """
    try:
        return fn()
    except Exception:
        return default


def get_client(ctx, server: Optional[str] = None):
    """Resolve a Spark client from MCP context and optional server name.

    Mirrors logic used across tools to pick a named client or the default.
    """
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
