"""
Stage-specific comparison tools for MCP server.

This module contains tools for comparing stage performance between Spark applications
including finding top stage differences and detailed stage-level comparisons.
"""

import logging
from typing import Any, Dict, Optional

from ...core.app import mcp
from .. import fetchers as fetcher_tools
from .. import matching as matching_tools
from .utils import calculate_stage_duration, resolve_client


@mcp.tool()
def find_top_stage_differences(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None,
    top_n: int = 5,
    similarity_threshold: float = 0.6,
) -> Dict[str, Any]:
    """
    Find the top N stages with the most significant time differences between two Spark applications.

    Identifies matching stages between applications based on name similarity and returns
    the stages with the largest time differences along with detailed performance comparisons.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server: Optional server name to use (uses default if not specified)
        top_n: Number of top stage differences to return (default: 5)
        similarity_threshold: Minimum similarity for stage name matching (default: 0.6)

    Returns:
        Dictionary containing:
        - applications: Basic info about both applications
        - top_stage_differences: List of top N stages with biggest time differences
          Each entry includes stage details, time differences, and performance metrics
    """
    # Get application info
    app1 = fetcher_tools.fetch_app(app_id1, server)
    app2 = fetcher_tools.fetch_app(app_id2, server)

    stages1 = fetcher_tools.fetch_stages(
        app_id=app_id1, server=server, with_summaries=True
    )
    stages2 = fetcher_tools.fetch_stages(
        app_id=app_id2, server=server, with_summaries=True
    )

    if not stages1 or not stages2:
        return _create_no_stages_error(
            app1, app2, app_id1, app_id2, stages1, stages2, top_n, similarity_threshold
        )

    # Find matching stages between applications via shared matcher
    matches = matching_tools.match_stages(stages1, stages2, similarity_threshold)

    if not matches:
        return _create_no_matches_error(
            app1, app2, app_id1, app_id2, stages1, stages2, top_n, similarity_threshold
        )

    # Calculate time differences for matching stages
    stage_differences = []

    for m in matches:
        stage1, stage2, similarity = m.stage1, m.stage2, m.similarity

        duration1 = calculate_stage_duration(stage1)
        duration2 = calculate_stage_duration(stage2)

        if duration1 > 0 and duration2 > 0:
            time_diff = abs(duration2 - duration1)
            time_diff_percent = (time_diff / max(duration1, duration2)) * 100

            stage_differences.append(
                {
                    "stage1": stage1,
                    "stage2": stage2,
                    "similarity": similarity,
                    "duration1": duration1,
                    "duration2": duration2,
                    "time_difference_seconds": time_diff,
                    "time_difference_percent": time_diff_percent,
                    "slower_app": "app1" if duration1 > duration2 else "app2",
                }
            )

    if not stage_differences:
        return _create_no_durations_error(
            app1, app2, app_id1, app_id2, stages1, stages2, top_n, similarity_threshold
        )

    # Sort by time difference and get top N
    top_differences = sorted(
        stage_differences, key=lambda x: x["time_difference_seconds"], reverse=True
    )[:top_n]

    # Get detailed comparisons for top different stages
    detailed_comparisons = []

    for diff in top_differences:
        stage1, stage2 = diff["stage1"], diff["stage2"]

        # Build stage comparison
        stage_comparison = {
            "stage_name": stage1.name,
            "similarity_score": diff["similarity"],
            "app1_stage": {
                "stage_id": stage1.stage_id,
                "name": stage1.name,
                "status": stage1.status,
                "duration_seconds": diff["duration1"],
            },
            "app2_stage": {
                "stage_id": stage2.stage_id,
                "name": stage2.name,
                "status": stage2.status,
                "duration_seconds": diff["duration2"],
            },
            "time_difference": {
                "absolute_seconds": diff["time_difference_seconds"],
                "percentage": diff["time_difference_percent"],
                "slower_application": diff["slower_app"],
            },
            "stage_metrics_comparison": _build_stage_metrics_comparison(stage1, stage2),
            "executor_analysis": _build_executor_analysis(stage1, stage2),
        }

        detailed_comparisons.append(stage_comparison)

    # Calculate summary statistics
    total_diff = sum(
        comp["time_difference"]["absolute_seconds"] for comp in detailed_comparisons
    )
    avg_diff = total_diff / len(detailed_comparisons) if detailed_comparisons else 0.0
    max_diff = (
        max(
            comp["time_difference"]["absolute_seconds"] for comp in detailed_comparisons
        )
        if detailed_comparisons
        else 0.0
    )

    stage_summary = {
        "matched_stages": len(detailed_comparisons),
        "total_time_difference_seconds": total_diff,
        "average_time_difference_seconds": avg_diff,
        "max_time_difference_seconds": max_diff,
    }

    analysis_parameters = {
        "requested_top_n": top_n,
        "similarity_threshold": similarity_threshold,
        "available_stages_app1": len(stages1),
        "available_stages_app2": len(stages2),
        "matched_stages": len(detailed_comparisons),
    }

    return {
        "applications": {
            "app1": {"id": app_id1, "name": app1.name},
            "app2": {"id": app_id2, "name": app2.name},
        },
        "top_stage_differences": detailed_comparisons,
        "analysis_parameters": analysis_parameters,
        "stage_summary": stage_summary,
    }


@mcp.tool()
def compare_stages(
    app_id1: str,
    app_id2: str,
    stage_id1: int,
    stage_id2: int,
    server: Optional[str] = None,
    significance_threshold: float = 0.1,
) -> Dict[str, Any]:
    """
    Compare specific stages between two Spark applications.

    Focuses on median and max values from distributions, showing only metrics
    with significant differences to reduce noise and highlight actionable insights.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        stage_id1: Stage ID from first application
        stage_id2: Stage ID from second application
        server: Optional server name to use (uses default if not specified)
        significance_threshold: Minimum difference threshold to include metric (default: 0.1)

    Returns:
        Dictionary containing stage comparison with significant differences only
    """
    client = resolve_client(server)

    try:
        # Get stage data with summaries
        stage1 = client.get_stage_attempt(
            app_id=app_id1,
            stage_id=stage_id1,
            attempt_id=0,
            details=False,
            with_summaries=True,
        )
        stage2 = client.get_stage_attempt(
            app_id=app_id2,
            stage_id=stage_id2,
            attempt_id=0,
            details=False,
            with_summaries=True,
        )

        # Get task metric distributions
        try:
            task_dist1 = client.get_stage_task_summary(
                app_id=app_id1, stage_id=stage_id1, attempt_id=0
            )
            stage1.task_metrics_distributions = task_dist1
        except Exception as e:
            # Task metrics not available for this stage
            logging.debug(
                f"Task metrics not available for app {app_id1}, stage {stage_id1}: {e}"
            )

        try:
            task_dist2 = client.get_stage_task_summary(
                app_id=app_id2, stage_id=stage_id2, attempt_id=0
            )
            stage2.task_metrics_distributions = task_dist2
        except Exception as e:
            # Task metrics not available for this stage
            logging.debug(
                f"Task metrics not available for app {app_id2}, stage {stage_id2}: {e}"
            )

    except Exception as e:
        return {
            "error": f"Failed to retrieve stage data: {str(e)}",
            "stages": {
                "stage1": {"app_id": app_id1, "stage_id": stage_id1},
                "stage2": {"app_id": app_id2, "stage_id": stage_id2},
            },
        }

    # Build comparison result
    result = {
        "stage_comparison": {
            "stage1": {
                "app_id": app_id1,
                "stage_id": stage_id1,
                "name": stage1.name,
                "status": stage1.status,
            },
            "stage2": {
                "app_id": app_id2,
                "stage_id": stage_id2,
                "name": stage2.name,
                "status": stage2.status,
            },
        },
        "significant_differences": {},
        "summary": {
            "significance_threshold": significance_threshold,
            "total_differences_found": 0,
        },
    }

    # Compare stage-level metrics
    stage_metrics = _compare_stage_level_metrics(stage1, stage2, significance_threshold)
    if stage_metrics:
        result["significant_differences"]["stage_metrics"] = stage_metrics

    # Compare task metrics distributions if available
    task_metrics = _compare_task_distributions(stage1, stage2, significance_threshold)
    if task_metrics:
        result["significant_differences"]["task_metrics"] = task_metrics

    # Update summary
    result["summary"]["total_differences_found"] = len(
        result["significant_differences"].get("stage_metrics", {})
    ) + len(result["significant_differences"].get("task_metrics", {}))

    return result


def _create_no_stages_error(
    app1,
    app2,
    app_id1: str,
    app_id2: str,
    stages1,
    stages2,
    top_n: int,
    similarity_threshold: float,
) -> Dict[str, Any]:
    """Create error response when no stages are found."""
    return {
        "error": "No stages found in one or both applications",
        "applications": {
            "app1": {"id": app_id1, "name": app1.name, "stage_count": len(stages1)},
            "app2": {"id": app_id2, "name": app2.name, "stage_count": len(stages2)},
        },
        "analysis_parameters": {
            "requested_top_n": top_n,
            "similarity_threshold": similarity_threshold,
            "available_stages_app1": len(stages1),
            "available_stages_app2": len(stages2),
            "matched_stages": 0,
        },
        "stage_summary": {
            "matched_stages": 0,
            "total_time_difference_seconds": 0.0,
            "average_time_difference_seconds": 0.0,
            "max_time_difference_seconds": 0.0,
        },
    }


def _create_no_matches_error(
    app1,
    app2,
    app_id1: str,
    app_id2: str,
    stages1,
    stages2,
    top_n: int,
    similarity_threshold: float,
) -> Dict[str, Any]:
    """Create error response when no matching stages are found."""
    return {
        "error": f"No matching stages found between applications (similarity threshold: {similarity_threshold})",
        "applications": {
            "app1": {"id": app_id1, "name": app1.name, "stage_count": len(stages1)},
            "app2": {"id": app_id2, "name": app2.name, "stage_count": len(stages2)},
        },
        "analysis_parameters": {
            "requested_top_n": top_n,
            "similarity_threshold": similarity_threshold,
            "available_stages_app1": len(stages1),
            "available_stages_app2": len(stages2),
            "matched_stages": 0,
        },
        "stage_summary": {
            "matched_stages": 0,
            "total_time_difference_seconds": 0.0,
            "average_time_difference_seconds": 0.0,
            "max_time_difference_seconds": 0.0,
        },
        "suggestion": "Try lowering the similarity_threshold parameter or check that applications are performing similar operations",
    }


def _create_no_durations_error(
    app1,
    app2,
    app_id1: str,
    app_id2: str,
    stages1,
    stages2,
    top_n: int,
    similarity_threshold: float,
) -> Dict[str, Any]:
    """Create error response when no calculable durations are found."""
    return {
        "error": "No stages with calculable durations found",
        "applications": {
            "app1": {"id": app_id1, "name": app1.name},
            "app2": {"id": app_id2, "name": app2.name},
        },
        "analysis_parameters": {
            "requested_top_n": top_n,
            "similarity_threshold": similarity_threshold,
            "available_stages_app1": len(stages1),
            "available_stages_app2": len(stages2),
            "matched_stages": 0,
        },
        "stage_summary": {
            "matched_stages": 0,
            "total_time_difference_seconds": 0.0,
            "average_time_difference_seconds": 0.0,
            "max_time_difference_seconds": 0.0,
        },
    }


def _build_stage_metrics_comparison(stage1, stage2) -> Dict[str, Any]:
    """Build detailed stage metrics comparison."""

    def safe_get_metric(stage, attr: str, default=0):
        """Safely get stage metric with fallback."""
        try:
            value = getattr(stage, attr, default)
            return value if value is not None else default
        except Exception:
            return default

    return {
        "duration": {
            "app1_ms": safe_get_metric(stage1, "execution_time", 0),
            "app2_ms": safe_get_metric(stage2, "execution_time", 0),
            "difference_ms": safe_get_metric(stage2, "execution_time", 0)
            - safe_get_metric(stage1, "execution_time", 0),
        },
        "tasks": {
            "app1_total": safe_get_metric(stage1, "num_tasks", 0),
            "app2_total": safe_get_metric(stage2, "num_tasks", 0),
            "app1_failed": safe_get_metric(stage1, "num_failed_tasks", 0),
            "app2_failed": safe_get_metric(stage2, "num_failed_tasks", 0),
        },
        "io_metrics": {
            "app1_input_bytes": safe_get_metric(stage1, "input_bytes", 0),
            "app2_input_bytes": safe_get_metric(stage2, "input_bytes", 0),
            "app1_output_bytes": safe_get_metric(stage1, "output_bytes", 0),
            "app2_output_bytes": safe_get_metric(stage2, "output_bytes", 0),
        },
        "shuffle_metrics": {
            "app1_read_bytes": safe_get_metric(stage1, "shuffle_read_bytes", 0),
            "app2_read_bytes": safe_get_metric(stage2, "shuffle_read_bytes", 0),
            "app1_write_bytes": safe_get_metric(stage1, "shuffle_write_bytes", 0),
            "app2_write_bytes": safe_get_metric(stage2, "shuffle_write_bytes", 0),
        },
        "memory_metrics": {
            "app1_spill_bytes": safe_get_metric(stage1, "memory_bytes_spilled", 0),
            "app2_spill_bytes": safe_get_metric(stage2, "memory_bytes_spilled", 0),
            "app1_disk_spill_bytes": safe_get_metric(stage1, "disk_bytes_spilled", 0),
            "app2_disk_spill_bytes": safe_get_metric(stage2, "disk_bytes_spilled", 0),
        },
    }


def _build_executor_analysis(stage1, stage2) -> Dict[str, Any]:
    """Build executor analysis for stage comparison."""
    # Simplified executor analysis - could be expanded with more detailed metrics
    return {
        "executor_run_time": {
            "app1_ms": getattr(stage1, "executor_run_time", 0) or 0,
            "app2_ms": getattr(stage2, "executor_run_time", 0) or 0,
        },
        "executor_cpu_time": {
            "app1_ns": getattr(stage1, "executor_cpu_time", 0) or 0,
            "app2_ns": getattr(stage2, "executor_cpu_time", 0) or 0,
        },
        "gc_time": {
            "app1_ms": getattr(stage1, "jvm_gc_time", 0) or 0,
            "app2_ms": getattr(stage2, "jvm_gc_time", 0) or 0,
        },
    }


def _compare_stage_level_metrics(
    stage1, stage2, significance_threshold: float
) -> Dict[str, Any]:
    """Compare stage-level metrics with significance filtering."""

    def calculate_difference(val1: float, val2: float) -> Optional[Dict[str, Any]]:
        if val1 == 0 and val2 == 0:
            return None

        # Avoid division by zero
        denominator = max(abs(val1), abs(val2), 1)
        diff_ratio = abs(val1 - val2) / denominator

        if diff_ratio >= significance_threshold:
            change_pct = ((val2 - val1) / max(abs(val1), 1)) * 100
            return {
                "stage1": val1,
                "stage2": val2,
                "change": f"{change_pct:+.1f}%",
                "significance": diff_ratio,
            }
        return None

    stage_metrics = {}

    # Duration comparison
    duration1 = calculate_stage_duration(stage1)
    duration2 = calculate_stage_duration(stage2)

    duration_diff = calculate_difference(duration1, duration2)
    if duration_diff:
        stage_metrics["duration_seconds"] = duration_diff

    # Compare numeric stage attributes
    numeric_attrs = [
        "num_tasks",
        "num_active_tasks",
        "num_complete_tasks",
        "num_failed_tasks",
        "executor_run_time",
        "executor_cpu_time",
        "jvm_gc_time",
        "input_bytes",
        "input_records",
        "output_bytes",
        "output_records",
        "shuffle_read_bytes",
        "shuffle_read_records",
        "shuffle_write_bytes",
        "shuffle_write_records",
        "memory_bytes_spilled",
        "disk_bytes_spilled",
    ]

    for attr in numeric_attrs:
        val1 = getattr(stage1, attr, 0) or 0
        val2 = getattr(stage2, attr, 0) or 0
        diff = calculate_difference(float(val1), float(val2))
        if diff:
            stage_metrics[attr] = diff

    return stage_metrics


def _compare_task_distributions(
    stage1, stage2, significance_threshold: float
) -> Dict[str, Any]:
    """Compare task metric distributions if available."""
    task_metrics = {}

    # Check if both stages have task distributions
    dist1 = getattr(stage1, "task_metrics_distributions", None)
    dist2 = getattr(stage2, "task_metrics_distributions", None)

    if not (dist1 and dist2):
        return task_metrics

    # Compare key distribution metrics (median and max values)
    distribution_fields = [
        ("executor_deserialize_time", "deserialize_time"),
        ("executor_deserialize_cpu_time", "deserialize_cpu_time"),
        ("executor_run_time", "executor_run_time"),
        ("executor_cpu_time", "executor_cpu_time"),
        ("result_size", "result_size"),
        ("jvm_gc_time", "jvm_gc_time"),
        ("result_serialization_time", "result_serialization_time"),
        ("memory_bytes_spilled", "memory_bytes_spilled"),
        ("disk_bytes_spilled", "disk_bytes_spilled"),
    ]

    def safe_get_dist_value(dist, field: str, stat: str) -> float:
        """Safely extract distribution value."""
        try:
            field_data = getattr(dist, field, None)
            if field_data:
                value = getattr(field_data, stat, 0.0)
                # Handle mocked objects and ensure we get a float
                return (
                    float(value)
                    if value is not None and str(value) != "MagicMock"
                    else 0.0
                )
            return 0.0
        except (TypeError, ValueError, AttributeError):
            return 0.0

    for attr_name, field_name in distribution_fields:
        # Compare median values
        median1 = safe_get_dist_value(dist1, field_name, "median")
        median2 = safe_get_dist_value(dist2, field_name, "median")

        if median1 > 0 or median2 > 0:
            denominator = max(abs(median1), abs(median2), 1)
            diff_ratio = abs(median1 - median2) / denominator

            if diff_ratio >= significance_threshold:
                change_pct = ((median2 - median1) / max(abs(median1), 1)) * 100
                task_metrics[f"{attr_name}_median"] = {
                    "stage1": median1,
                    "stage2": median2,
                    "change": f"{change_pct:+.1f}%",
                    "significance": diff_ratio,
                }

    return task_metrics
