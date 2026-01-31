"""
Common utilities, constants, configuration, and conversions for tools.

This module centralizes:
- Shared constants and unit conversions
- Lightweight config resolution for tool thresholds/limits
- Safe helpers used across multiple tools
- Client resolution from the MCP app context
"""

from __future__ import annotations

import sys
from typing import Any, Callable, Optional, TypeVar

from pydantic import Field
from pydantic_settings import BaseSettings

from ..core.app import mcp

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
        default=SIGNIFICANCE_DEFAULT,
        description="Default significance threshold for comparisons",
    )

    # Matching thresholds
    stage_match_similarity: float = Field(
        default=0.75,
        description="Minimum name similarity (0-1) to consider stages a match",
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
        default=500 * 1024 * 1024,
        description="Spill threshold in bytes (default 500MB)",
    )
    min_stage_duration_s: int = Field(
        default=10, description="Minimum stage duration in seconds for analysis"
    )
    min_task_count: int = Field(
        default=10, description="Minimum tasks for reliable stats"
    )

    # SQL pagination and defaults
    sql_page_size: int = Field(default=100, description="Page size for SQL listings")
    include_running_defaults: bool = Field(
        default=False, description="Default for including running entities in rankings"
    )

    # Debug/validation
    debug_validate_schema: bool = Field(
        default=False, description="Validate outputs against schema in debug mode"
    )

    # Output shaping
    compact_tool_output: bool = Field(
        default=True,
        description="Return compact summaries from tool outputs by default",
    )
    compact_list_limit: int = Field(
        default=50, description="Max items to include in compact list outputs"
    )
    compact_property_sample_limit: int = Field(
        default=25, description="Max property entries to sample in compact outputs"
    )
    compact_recommendations_limit: int = Field(
        default=5, description="Max recommendations to include in compact outputs"
    )
    compact_timeline_limit: int = Field(
        default=100, description="Max timeline entries to include in compact outputs"
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


def compact_output(data: Any, compact: Optional[bool] = None) -> Any:
    """Return compacted tool output when enabled.

    Args:
        data: Tool output to compact.
        compact: Optional override for compaction behavior.

    Returns:
        Compacted output if enabled; otherwise original data.
    """
    cfg = get_config()
    use_compact = cfg.compact_tool_output if compact is None else compact
    if not use_compact:
        return data
    return _compact_data(data, cfg)


def compact_dict(
    data: dict[str, Any], compact: Optional[bool] = None
) -> dict[str, Any]:
    """Compact a dict-based tool output when enabled.

    Strips redundant keys, truncates recommendation/timeline lists,
    and adds truncation metadata.
    """
    cfg = get_config()
    use_compact = cfg.compact_tool_output if compact is None else compact
    if not use_compact or not isinstance(data, dict):
        return data
    return _compact_dict(data, cfg)


def _compact_dict(data: dict[str, Any], cfg: ToolConfig) -> dict[str, Any]:
    """Recursively compact a dict output."""
    result = {}
    for key, value in data.items():
        if key == "recommendations" and isinstance(value, list):
            limit = cfg.compact_recommendations_limit
            result[key] = value[:limit]
            if len(value) > limit:
                result["_recommendations_truncated"] = {
                    "total": len(value),
                    "returned": limit,
                }
        elif key == "timeline" and isinstance(value, list):
            limit = cfg.compact_timeline_limit
            result[key] = value[:limit]
            if len(value) > limit:
                result["_timeline_truncated"] = {
                    "total": len(value),
                    "returned": limit,
                }
        elif isinstance(value, dict):
            result[key] = _compact_dict(value, cfg)
        else:
            result[key] = value
    return result


def strip_applications_metadata(data: dict[str, Any]) -> dict[str, Any]:
    """Remove redundant 'applications' key from sub-comparison results."""
    if isinstance(data, dict):
        return {k: v for k, v in data.items() if k != "applications"}
    return data


def _compact_data(data: Any, cfg: ToolConfig) -> Any:
    from ..models.spark_types import (
        ApplicationEnvironmentInfo,
        ApplicationInfo,
        ExecutorSummary,
        JobData,
        StageData,
    )

    if isinstance(data, list):
        return _compact_list(data, cfg)
    if isinstance(data, ApplicationInfo):
        return _compact_application(data)
    if isinstance(data, JobData):
        return _compact_job(data)
    if isinstance(data, StageData):
        return _compact_stage(data)
    if isinstance(data, ExecutorSummary):
        return _compact_executor(data)
    if isinstance(data, ApplicationEnvironmentInfo):
        return _compact_environment(data, cfg)
    return data


def _compact_list(items: list[Any], cfg: ToolConfig) -> dict[str, Any]:
    total = len(items)
    limit = max(0, cfg.compact_list_limit)
    sliced = items[:limit] if limit else []
    compacted = [_compact_data(item, cfg) for item in sliced]
    return {
        "items": compacted,
        "summary": {
            "total": total,
            "returned": len(compacted),
            "truncated": total > len(compacted),
        },
    }


def _dt_iso(value: Any) -> Optional[str]:
    return value.isoformat() if value else None


def _compact_application(app) -> dict[str, Any]:
    attempts = list(app.attempts or [])
    latest = attempts[-1] if attempts else None
    status = None
    if latest is not None:
        status = "COMPLETED" if latest.completed else "RUNNING"
    return {
        "id": app.id,
        "name": app.name,
        "status": status,
        "attempts": len(attempts),
        "latest_attempt": {
            "attempt_id": getattr(latest, "attempt_id", None),
            "start_time": _dt_iso(getattr(latest, "start_time", None)),
            "end_time": _dt_iso(getattr(latest, "end_time", None)),
            "last_updated": _dt_iso(getattr(latest, "last_updated", None)),
            "duration_ms": getattr(latest, "duration", None),
            "spark_user": getattr(latest, "spark_user", None),
            "spark_version": getattr(latest, "app_spark_version", None),
        }
        if latest is not None
        else None,
        "cores_per_executor": app.cores_per_executor,
        "memory_per_executor_mb": app.memory_per_executor_mb,
    }


def _compact_job(job) -> dict[str, Any]:
    duration_ms = None
    if job.submission_time and job.completion_time:
        duration_ms = int(
            (job.completion_time - job.submission_time).total_seconds() * 1000
        )
    return {
        "job_id": job.job_id,
        "name": job.name,
        "status": job.status,
        "submission_time": _dt_iso(job.submission_time),
        "completion_time": _dt_iso(job.completion_time),
        "duration_ms": duration_ms,
        "num_tasks": job.num_tasks,
        "num_active_tasks": job.num_active_tasks,
        "num_completed_tasks": job.num_completed_tasks,
        "num_failed_tasks": job.num_failed_tasks,
        "num_skipped_tasks": job.num_skipped_tasks,
        "num_completed_stages": job.num_completed_stages,
        "num_failed_stages": job.num_failed_stages,
    }


def _compact_stage(stage) -> dict[str, Any]:
    duration_ms = None
    if stage.first_task_launched_time and stage.completion_time:
        duration_ms = int(
            (stage.completion_time - stage.first_task_launched_time).total_seconds()
            * 1000
        )
    return {
        "stage_id": stage.stage_id,
        "attempt_id": stage.attempt_id,
        "name": stage.name,
        "status": stage.status,
        "submission_time": _dt_iso(stage.submission_time),
        "first_task_launched_time": _dt_iso(stage.first_task_launched_time),
        "completion_time": _dt_iso(stage.completion_time),
        "duration_ms": duration_ms,
        "num_tasks": stage.num_tasks,
        "num_complete_tasks": stage.num_complete_tasks,
        "num_failed_tasks": stage.num_failed_tasks,
        "num_killed_tasks": stage.num_killed_tasks,
        "input_bytes": stage.input_bytes,
        "output_bytes": stage.output_bytes,
        "shuffle_read_bytes": stage.shuffle_read_bytes,
        "shuffle_write_bytes": stage.shuffle_write_bytes,
        "memory_bytes_spilled": stage.memory_bytes_spilled,
        "disk_bytes_spilled": stage.disk_bytes_spilled,
    }


def _compact_executor(executor) -> dict[str, Any]:
    return {
        "id": executor.id,
        "host_port": executor.host_port,
        "is_active": executor.is_active,
        "total_cores": executor.total_cores,
        "max_tasks": executor.max_tasks,
        "active_tasks": executor.active_tasks,
        "completed_tasks": executor.completed_tasks,
        "failed_tasks": executor.failed_tasks,
        "total_duration_ms": executor.total_duration,
        "total_gc_time_ms": executor.total_gc_time,
        "total_input_bytes": executor.total_input_bytes,
        "total_shuffle_read": executor.total_shuffle_read,
        "total_shuffle_write": executor.total_shuffle_write,
        "memory_used": executor.memory_used,
        "disk_used": executor.disk_used,
        "add_time": _dt_iso(executor.add_time),
        "remove_time": _dt_iso(executor.remove_time),
        "remove_reason": executor.remove_reason,
    }


def _kv_pairs_to_dict(pairs: Any) -> dict[str, Any]:
    if not pairs:
        return {}
    if isinstance(pairs, dict):
        return dict(pairs)
    return {str(k): v for k, v in pairs}


def _summarize_kv(
    pairs: Any, keys_of_interest: list[str], cfg: ToolConfig
) -> dict[str, Any]:
    mapping = _kv_pairs_to_dict(pairs)
    if not mapping:
        return {"count": 0, "selected": {}, "sample": {}, "truncated": False}
    selected = {k: mapping[k] for k in keys_of_interest if k in mapping}
    remaining_keys = [k for k in mapping.keys() if k not in selected]
    remaining_keys.sort()
    sample_keys = remaining_keys[: cfg.compact_property_sample_limit]
    sample = {k: mapping[k] for k in sample_keys}
    return {
        "count": len(mapping),
        "selected": selected,
        "sample": sample,
        "truncated": len(remaining_keys) > len(sample_keys),
    }


_SPARK_PROPERTY_KEYS = [
    "spark.app.name",
    "spark.master",
    "spark.submit.deployMode",
    "spark.executor.instances",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.driver.cores",
    "spark.driver.memory",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.dynamicAllocation.enabled",
    "spark.dynamicAllocation.minExecutors",
    "spark.dynamicAllocation.maxExecutors",
    "spark.serializer",
]


def _compact_environment(env, cfg: ToolConfig) -> dict[str, Any]:
    return {
        "runtime": {
            "java_version": getattr(env.runtime, "java_version", None),
            "java_home": getattr(env.runtime, "java_home", None),
            "scala_version": getattr(env.runtime, "scala_version", None),
        },
        "spark_properties": _summarize_kv(
            env.spark_properties, _SPARK_PROPERTY_KEYS, cfg
        ),
        "hadoop_properties": _summarize_kv(env.hadoop_properties, [], cfg),
        "system_properties": _summarize_kv(env.system_properties, [], cfg),
        "metrics_properties": _summarize_kv(env.metrics_properties, [], cfg),
        "classpath_entries": _summarize_kv(env.classpath_entries, [], cfg),
        "resource_profiles": {
            "count": len(env.resource_profiles or []),
        },
    }


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
        raise ValueError("Spark MCP context is not available outside of a request")
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
