"""
Centralized stage metrics aggregation using dynamic field extraction.

This module provides a unified approach to aggregating stage metrics,
replacing multiple hardcoded implementations with dynamic field discovery.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable

from ..models.spark_types import StageData, StageStatus
from ..utils.model_fields import get_aggregatable_fields


def aggregate_stage_metrics(
    stages: Iterable[Any],
    include_duration: bool = True,
) -> Dict[str, Any]:
    """
    Aggregate metrics across all stages using dynamic field extraction.

    Uses model introspection to discover aggregatable fields from StageData,
    ensuring all numeric metrics are included without hardcoded lists.

    Note: This function returns RAW values without unit conversion.
    Fields like executor_cpu_time remain in nanoseconds,
    shuffle_write_time remains in nanoseconds, etc.
    Callers are responsible for applying appropriate unit conversions.

    Args:
        stages: Iterable of stage objects (StageData or similar)
        include_duration: Whether to calculate duration from timestamps

    Returns:
        Dictionary with:
        - Summed numeric metrics (executor_run_time, input_bytes, etc.)
        - Stage counts (total, completed, failed)
        - Optional duration calculation
    """
    # Convert to list to allow multiple iterations
    stages_list = list(stages)

    if not stages_list:
        return _empty_aggregation()

    # Get aggregatable fields dynamically from StageData model
    fields = get_aggregatable_fields(StageData)

    # Initialize aggregation dict
    aggregated: Dict[str, float] = {}

    # Aggregate each field across all stages
    # Note: We extract RAW values (no scale_factor), letting callers handle conversion
    for field in fields:
        if not field.aggregatable:
            continue

        total = 0.0
        for stage in stages_list:
            # Get raw value without scale factor
            value = getattr(stage, field.name, None)
            if value is not None:
                total += float(value)
        aggregated[field.name] = total

    # Calculate stage counts
    total_stages = len(stages_list)
    completed = 0
    failed = 0

    for stage in stages_list:
        status = getattr(stage, "status", None)
        if status == StageStatus.COMPLETE or str(status).upper() in {
            "COMPLETE",
            "COMPLETED",
        }:
            completed += 1
        elif status == StageStatus.FAILED or str(status).upper() == "FAILED":
            failed += 1

    # Build result with standard keys for backward compatibility
    result: Dict[str, Any] = {
        "total_stages": total_stages,
        "completed_stages": completed,
        "failed_stages": failed,
    }

    # Add aggregated metrics with descriptive keys
    result.update(aggregated)

    # Calculate duration from timestamps if requested
    if include_duration:
        total_duration = 0.0
        for stage in stages_list:
            duration = _calculate_stage_duration(stage)
            total_duration += duration
        result["total_stage_duration_ms"] = total_duration

        # Average duration
        if total_stages > 0:
            result["avg_stage_duration_ms"] = total_duration / total_stages
        else:
            result["avg_stage_duration_ms"] = 0.0

    return result


def aggregate_stage_metrics_for_summary(
    stages: Iterable[Any],
) -> Dict[str, Any]:
    """
    Aggregate stage metrics formatted for app summary output.

    This is a convenience wrapper that returns metrics with the keys
    expected by the get_app_summary tool for backward compatibility.

    Args:
        stages: Iterable of stage objects

    Returns:
        Dictionary with raw aggregated values (no unit conversion)
    """
    return aggregate_stage_metrics(stages, include_duration=True)


def _calculate_stage_duration(stage: Any) -> float:
    """Calculate stage duration in milliseconds from timestamps."""
    completion = getattr(stage, "completion_time", None)
    submission = getattr(stage, "submission_time", None)

    if completion and submission:
        try:
            return (completion - submission).total_seconds() * 1000
        except (TypeError, AttributeError):
            pass
    return 0.0


def _empty_aggregation() -> Dict[str, Any]:
    """Return an empty aggregation result."""
    return {
        "total_stages": 0,
        "completed_stages": 0,
        "failed_stages": 0,
        "total_stage_duration_ms": 0.0,
        "avg_stage_duration_ms": 0.0,
    }


def get_aggregated_field_names() -> list[str]:
    """
    Return the list of field names that will be aggregated.

    Useful for understanding which metrics are included without
    actually performing aggregation.

    Returns:
        List of field names that are aggregatable
    """
    fields = get_aggregatable_fields(StageData)
    return [f.name for f in fields if f.aggregatable]


def aggregate_stage_metrics_for_comparison(
    stages: Iterable[Any],
) -> Dict[str, Any]:
    """
    Aggregate stage metrics formatted for comparison tools.

    Returns metrics with keys prefixed by 'total_' and proper unit conversions
    to match the format expected by compare_app_stages_aggregated and similar tools.

    Args:
        stages: Iterable of stage objects

    Returns:
        Dictionary with prefixed keys and converted units:
        - total_input_bytes, total_output_bytes, etc. (byte metrics)
        - total_executor_run_time_ms, total_gc_time_ms (ms metrics)
        - total_executor_cpu_time_ms (ns converted to ms)
        - total_tasks, total_failed_tasks (counts)
        - avg_stage_duration_ms
    """
    stages_list = list(stages)

    if not stages_list:
        return {
            "total_stages": 0,
            "total_input_bytes": 0,
            "total_output_bytes": 0,
            "total_shuffle_read_bytes": 0,
            "total_shuffle_write_bytes": 0,
            "total_memory_spilled_bytes": 0,
            "total_disk_spilled_bytes": 0,
            "total_executor_run_time_ms": 0,
            "total_executor_cpu_time_ms": 0,
            "total_gc_time_ms": 0,
            "avg_stage_duration_ms": 0,
            "total_tasks": 0,
            "total_failed_tasks": 0,
        }

    # Use the base aggregation
    agg = aggregate_stage_metrics(stages_list, include_duration=True)

    # Map to comparison format with total_ prefix
    # Values from aggregate_stage_metrics are RAW, so apply unit conversions here
    return {
        "total_stages": agg.get("total_stages", len(stages_list)),
        "total_input_bytes": agg.get("input_bytes", 0),
        "total_output_bytes": agg.get("output_bytes", 0),
        "total_shuffle_read_bytes": agg.get("shuffle_read_bytes", 0),
        "total_shuffle_write_bytes": agg.get("shuffle_write_bytes", 0),
        "total_memory_spilled_bytes": agg.get("memory_bytes_spilled", 0),
        "total_disk_spilled_bytes": agg.get("disk_bytes_spilled", 0),
        "total_executor_run_time_ms": agg.get("executor_run_time", 0),
        # executor_cpu_time is in nanoseconds, convert to milliseconds
        "total_executor_cpu_time_ms": agg.get("executor_cpu_time", 0) / 1_000_000.0,
        "total_gc_time_ms": agg.get("jvm_gc_time", 0),
        "avg_stage_duration_ms": agg.get("avg_stage_duration_ms", 0),
        "total_tasks": int(agg.get("num_tasks", 0)),
        "total_failed_tasks": int(agg.get("num_failed_tasks", 0)),
    }
