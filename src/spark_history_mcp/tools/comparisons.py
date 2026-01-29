"""
Application comparison tools for MCP server.

This module contains tools for comparing performance metrics, resource allocation,
and configurations between different Spark applications.
"""

import logging
from datetime import timedelta
from typing import Any, Dict, Optional

from ..core.app import mcp
from . import analysis as analysis_tools
from . import executors as executor_tools
from . import fetchers as fetcher_tools
from . import matching as matching_tools
from .application import get_app_summary as _get_app_summary_impl
from .common import get_config, resolve_legacy_tool
from .recommendations import (
    apply_rules as apply_rec_rules,
)
from .recommendations import (
    dedupe as dedupe_recs,
)
from .recommendations import (
    default_rules as default_rec_rules,
)
from .recommendations import (
    prioritize as prioritize_recs,
)
from .schema import CompareAppPerformanceOutput, validate_output
from .timelines import merge_consecutive_intervals

logger = logging.getLogger(__name__)

# Import functions that we'll use in helper functions
# These are defined later in this file, so we'll need to reference them carefully


def _get_mcp_context() -> Optional[object]:
    """Return the active MCP request context if available."""

    try:
        return mcp.get_context()
    except ValueError:
        # Allow tools to run in unit tests where no request context is active.
        return None


def _resolve_client(server: Optional[str]) -> Any:
    """Return a Spark client using the legacy analysis accessor.

    The tests patch ``spark_history_mcp.tools.analysis.get_client_or_default`` to
    inject mock clients, so we delegate through that module instead of importing
    the helper directly. When no MCP request context is available we surface a
    clear error unless the patched helper returns a client (as in unit tests).
    """

    ctx = _get_mcp_context()
    try:
        return analysis_tools.get_client_or_default(ctx, server)
    except AttributeError as exc:
        if ctx is None:
            raise ValueError(
                "Spark MCP context is not available outside of a request."
            ) from exc
        raise


def _get_basic_app_info(app) -> Dict[str, Any]:
    """Return a minimal app metadata snapshot for comparisons."""

    return {
        "id": getattr(app, "id", None),
        "name": getattr(app, "name", None),
        "cores_granted": getattr(app, "cores_granted", None),
        "max_cores": getattr(app, "max_cores", None),
        "cores_per_executor": getattr(app, "cores_per_executor", None),
        "memory_per_executor_mb": getattr(app, "memory_per_executor_mb", None),
        "max_executors": getattr(app, "max_executors", None),
    }


@mcp.tool()
def compare_app_performance(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None,
    top_n: int = 3,
    significance_threshold: float = 0.1,
    similarity_threshold: float = 0.6,
) -> Dict[str, Any]:
    """
    Streamlined performance comparison between two Spark applications.

    Provides a clean, focused analysis of performance differences including executor efficiency,
    stage-level comparisons, environment configuration differences, and prioritized recommendations.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server: Optional server name to use (uses default if not specified)
        top_n: Number of top stage differences to return for analysis (default: 3)
        significance_threshold: Minimum difference threshold to show metric (default: 0.1)
        similarity_threshold: Minimum similarity for stage name matching (default: 0.6)

    Returns:
        Dictionary containing:
        - applications: Basic info about both applications
        - performance_comparison:
          - executors: Key executor efficiency metrics and comparisons
          - stages: Top N stages with largest time differences and performance metrics
        - app_summary_diff: Application-level aggregated metrics comparison with percentage changes
        - environment_comparison: Configuration and environment differences
        - key_recommendations: Up to 5 highest priority (critical/high/medium) recommendations

    Uses optimized defaults:
    - similarity_threshold: 0.6 for stage matching
    - significance_threshold: 0.1 for metric filtering
    - filter_auto_generated: True for cleaner environment comparison
    """
    client = _resolve_client(server)

    # Safely fetch both applications with centralized error handling
    app1, app2, error_response = _fetch_applications_safely(
        app_id1, app_id2, server, top_n, similarity_threshold
    )
    if error_response:
        return error_response

    # PHASE 1: AGGREGATED APPLICATION OVERVIEW
    aggregated_overview = _build_aggregated_overview(
        app_id1, app_id2, server, significance_threshold
    )

    # PHASE 2: STAGE-LEVEL DEEP DIVE ANALYSIS
    stage_analysis = _analyze_stage_deep_dive(
        app_id1, app_id2, server, top_n, similarity_threshold, app1, app2
    )

    # Generate basic recommendations even if stage analysis fails
    basic_recommendations = []

    # APPLICATION-LEVEL RECOMMENDATIONS
    # Resource allocation differences
    if app1.cores_granted and app2.cores_granted:
        core_ratio = app2.cores_granted / app1.cores_granted
        if core_ratio > 1.5 or core_ratio < 0.67:  # >50% difference
            slower_app = "app1" if core_ratio > 1.5 else "app2"
            faster_app = "app2" if core_ratio > 1.5 else "app1"
            basic_recommendations.append(
                {
                    "type": "resource_allocation",
                    "priority": "medium",
                    "issue": f"Significant core allocation difference (ratio: {core_ratio:.2f})",
                    "suggestion": f"Consider equalizing core allocation - {slower_app} has fewer cores than {faster_app}",
                }
            )

    # Memory allocation differences
    if app1.memory_per_executor_mb and app2.memory_per_executor_mb:
        memory_ratio = app2.memory_per_executor_mb / app1.memory_per_executor_mb
        if memory_ratio > 1.5 or memory_ratio < 0.67:  # >50% difference
            basic_recommendations.append(
                {
                    "type": "resource_allocation",
                    "priority": "medium",
                    "issue": f"Significant memory per executor difference (ratio: {memory_ratio:.2f})",
                    "suggestion": "Review memory allocation settings between applications",
                }
            )

    # If stage analysis failed, return early with basic recommendations
    if "error" in stage_analysis:
        return {
            "schema_version": 1,
            "applications": stage_analysis["applications"],
            "aggregated_overview": aggregated_overview,
            "stage_deep_dive": stage_analysis,
            "error": stage_analysis["error"],
            "recommendations": basic_recommendations,
            "key_recommendations": basic_recommendations[:5],
        }

    # Extract stage differences for recommendations logic
    detailed_comparisons = stage_analysis.get("top_stage_differences", [])

    # Enhanced recommendations combining both application and stage-level insights
    # Start with basic recommendations already generated above
    recommendations = basic_recommendations[:]

    # Apply default rule set (resource allocation, large stage diffs)
    rule_ctx = {
        "app1": app1,
        "app2": app2,
        "detailed_comparisons": detailed_comparisons,
    }
    recommendations.extend(apply_rec_rules(rule_ctx, default_rec_rules()))

    # Extract recommendations from specialized comparison tools
    # Executor efficiency recommendations
    if (
        aggregated_overview["executor_performance"]
        and isinstance(aggregated_overview["executor_performance"], dict)
        and "recommendations" in aggregated_overview["executor_performance"]
    ):
        recommendations.extend(
            aggregated_overview["executor_performance"]["recommendations"]
        )

    # Stage-level aggregated recommendations
    if (
        aggregated_overview["stage_metrics"]
        and isinstance(aggregated_overview["stage_metrics"], dict)
        and "recommendations" in aggregated_overview["stage_metrics"]
    ):
        recommendations.extend(aggregated_overview["stage_metrics"]["recommendations"])

    # STAGE-LEVEL RECOMMENDATIONS (existing logic continues below)

    # Check for memory spilling differences at stage level using simplified comparisons
    spill_diff_stages = []
    for comp in detailed_comparisons:
        if "stage_metrics_comparison" in comp:
            memory_metrics = comp["stage_metrics_comparison"].get("memory_metrics", {})
            app1_spill = memory_metrics.get("app1_spill_bytes", 0) or 0
            app2_spill = memory_metrics.get("app2_spill_bytes", 0) or 0

            if abs(app1_spill - app2_spill) > 100 * 1024 * 1024:  # >100MB difference
                spill_diff_stages.append(comp)

    if spill_diff_stages:
        recommendations.append(
            {
                "type": "stage_memory",
                "priority": "medium",
                "issue": f"Found {len(spill_diff_stages)} stages with significant memory spill differences",
                "suggestion": "Check memory allocation and partitioning strategies for specific stages",
            }
        )

    # Environment and configuration comparison (using default filter_auto_generated=True)
    environment_comparison = _compare_environments(
        client, app_id1, app_id2, filter_auto_generated=True
    )

    # SQL execution plans comparison
    sql_plans_comparison = _compare_sql_execution_plans(client, app_id1, app_id2)

    # Merge SQL recommendations with existing recommendations
    if sql_plans_comparison.get("sql_recommendations"):
        recommendations.extend(sql_plans_comparison["sql_recommendations"])

    # Deduplicate and sort recommendations by priority
    sorted_recommendations = prioritize_recs(dedupe_recs(recommendations), top_n=9999)
    filtered_recommendations = sorted_recommendations[:5]

    # Extract simplified executor summary from aggregated overview
    executor_comparison = aggregated_overview.get("executor_performance", {})
    executor_summary = {}
    if isinstance(executor_comparison, dict) and "error" not in executor_comparison:
        # Extract key metrics from executor comparison
        executor_summary = {
            "memory_efficiency": executor_comparison.get("memory_efficiency", {}),
            "task_efficiency": executor_comparison.get("task_efficiency", {}),
            "gc_efficiency": executor_comparison.get("gc_efficiency", {}),
            "summary": executor_comparison.get("summary", {}),
        }
    else:
        executor_summary = executor_comparison

    # Get app summary comparison using the new tool
    try:
        app_summary_diff = compare_app_summaries(
            app_id1, app_id2, server, significance_threshold
        )
    except Exception as e:
        app_summary_diff = {
            "error": f"Failed to get app summary comparison: {str(e)}",
            "app1_summary": {"id": app_id1},
            "app2_summary": {"id": app_id2},
            "diff": {},
        }

    aggregated_overview["application_summary"] = app_summary_diff

    try:
        job_overview = compare_app_jobs(app_id1, app_id2, server)
    except Exception as e:
        job_overview = {
            "error": f"Failed to get job performance comparison: {str(e)}",
            "applications": {"app1": {"id": app_id1}, "app2": {"id": app_id2}},
        }
    aggregated_overview["job_performance"] = job_overview

    result = {
        "schema_version": 1,
        "applications": {
            "app1": {"id": app_id1, "name": app1.name},
            "app2": {"id": app_id2, "name": app2.name},
        },
        "performance_comparison": {
            "executors": executor_summary,
            "stages": stage_analysis,
        },
        "aggregated_overview": aggregated_overview,
        "stage_deep_dive": stage_analysis,
        "app_summary_diff": app_summary_diff,
        "environment_comparison": environment_comparison,
        "recommendations": sorted_recommendations,
        "key_recommendations": filtered_recommendations,
    }

    # Sort the result by mixed metrics (change percentages and ratios)
    # Optionally validate against schema in debug mode
    result = validate_output(
        CompareAppPerformanceOutput, result, enabled=get_config().debug_validate_schema
    )
    return sort_comparison_data(result, sort_key="mixed")


@mcp.tool()
def compare_app_summaries(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None,
    significance_threshold: float = 0.1,
) -> Dict[str, Any]:
    """
    Compare application-level summary metrics between two Spark applications.

    Provides a clean comparison of aggregated stage metrics including execution times,
    resource usage, data processing volumes, and percentage changes between applications.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server: Optional server name to use (uses default if not specified)
        significance_threshold: Minimum difference threshold to show metric (default: 0.1)

    Returns:
        Dictionary containing:
        - app1_summary: Aggregated metrics for first application
        - app2_summary: Aggregated metrics for second application
        - diff: Percentage changes (app2 vs app1) for key metrics
    """
    _resolve_client(server)

    # Get app summaries for both applications
    get_app_summary = resolve_legacy_tool("get_app_summary", _get_app_summary_impl)

    app1_summary = get_app_summary(app_id1, server)
    app2_summary = get_app_summary(app_id2, server)

    # Define non-comparable fields to exclude from comparison
    exclude_fields = {"application_id", "application_name", "analysis_timestamp"}

    # Filter to only comparable numeric metrics, using exact field names from get_app_summary
    app1_metrics = {
        k: v
        for k, v in app1_summary.items()
        if k not in exclude_fields and isinstance(v, (int, float))
    }
    app2_metrics = {
        k: v
        for k, v in app2_summary.items()
        if k not in exclude_fields and isinstance(v, (int, float))
    }

    # Add application IDs for identification
    app1_metrics["application_id"] = app1_summary.get("application_id", app_id1)
    app2_metrics["application_id"] = app2_summary.get("application_id", app_id2)

    # Calculate percentage changes (app2 vs app1)
    def calculate_percentage_change(val1, val2):
        if val1 == 0:
            return "N/A" if val2 == 0 else "+∞"
        change = ((val2 - val1) / val1) * 100
        return f"{change:+.1f}%"

    # Helper function to extract percentage value for filtering
    def extract_percentage_value(change_str):
        if change_str in ["N/A", "+∞", "-∞"]:
            return 0.0
        try:
            return abs(float(change_str.replace("+", "").replace("%", "")))
        except (ValueError, AttributeError):
            return 0.0

    # Dynamically calculate percentage changes for all comparable metrics
    diff = {}
    filtered_app1_metrics = {}
    filtered_app2_metrics = {}

    for metric_name in app1_metrics:
        if metric_name in app2_metrics and metric_name != "application_id":
            change_str = calculate_percentage_change(
                app1_metrics[metric_name], app2_metrics[metric_name]
            )
            change_value = extract_percentage_value(change_str)

            # Only include metrics that meet the significance threshold
            if (
                change_value >= (significance_threshold * 100)
                or metric_name == "application_id"
            ):
                diff[f"{metric_name}_change"] = change_str
                filtered_app1_metrics[metric_name] = app1_metrics[metric_name]
                filtered_app2_metrics[metric_name] = app2_metrics[metric_name]

    # Always include application_id for identification
    filtered_app1_metrics["application_id"] = app1_metrics["application_id"]
    filtered_app2_metrics["application_id"] = app2_metrics["application_id"]

    result = {
        "app1_summary": filtered_app1_metrics,
        "app2_summary": filtered_app2_metrics,
        "diff": diff,
        "filtering_summary": {
            "total_metrics": len(app1_metrics) - 1,  # Exclude application_id
            "significant_metrics": len(diff),
            "significance_threshold": significance_threshold,
            "filtering_applied": len(diff) < len(app1_metrics) - 1,
        },
    }

    # Sort the result by difference percentage (descending)
    return sort_comparison_data(result, sort_key="change")


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

    # Find matching stages between applications via shared matcher
    matches = matching_tools.match_stages(stages1, stages2, similarity_threshold)

    if not matches:
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

    # Calculate time differences for matching stages
    stage_differences = []

    for m in matches:
        stage1, stage2, similarity = m.stage1, m.stage2, m.similarity
        duration1 = _calculate_stage_duration(stage1)
        duration2 = _calculate_stage_duration(stage2)

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

    # Sort by time difference and get top N
    top_differences = sorted(
        stage_differences, key=lambda x: x["time_difference_seconds"], reverse=True
    )[:top_n]

    # Get detailed comparisons for top different stages
    detailed_comparisons = []

    for diff in top_differences:
        stage1, stage2 = diff["stage1"], diff["stage2"]

        # Simple stage-level metric comparison
        def safe_get_metric(stage, attr, default=0):
            """Safely get stage metric with fallback"""
            try:
                value = getattr(stage, attr, default)
                return value if value is not None else default
            except Exception:
                return default

        stage_metric_comparison = {
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
                "app1_disk_spill_bytes": safe_get_metric(
                    stage1, "disk_bytes_spilled", 0
                ),
                "app2_disk_spill_bytes": safe_get_metric(
                    stage2, "disk_bytes_spilled", 0
                ),
            },
        }

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
            "stage_metrics_comparison": stage_metric_comparison,
        }

        stage_comparison["executor_analysis"] = _build_executor_analysis(stage1, stage2)

        detailed_comparisons.append(stage_comparison)

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
    client = _resolve_client(server)
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
        except Exception as exc:
            logger.debug("Failed to fetch task summary for app1 stage", exc_info=exc)

        try:
            task_dist2 = client.get_stage_task_summary(
                app_id=app_id2, stage_id=stage_id2, attempt_id=0
            )
            stage2.task_metrics_distributions = task_dist2
        except Exception as exc:
            logger.debug("Failed to fetch task summary for app2 stage", exc_info=exc)

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

    # Helper function to calculate significance and format comparison
    def calculate_difference(
        val1: float, val2: float, metric_name: str
    ) -> Optional[Dict[str, Any]]:
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

    # Compare stage-level metrics
    stage_metrics = {}

    # Duration comparison
    duration1 = 0
    duration2 = 0
    if stage1.completion_time and stage1.first_task_launched_time:
        duration1 = (
            stage1.completion_time - stage1.first_task_launched_time
        ).total_seconds()
    if stage2.completion_time and stage2.first_task_launched_time:
        duration2 = (
            stage2.completion_time - stage2.first_task_launched_time
        ).total_seconds()

    duration_diff = calculate_difference(duration1, duration2, "duration")
    if duration_diff:
        stage_metrics["duration_seconds"] = duration_diff

    # Dynamic stage metrics comparison - include all numeric fields from stage objects
    exclude_fields = {
        "status",
        "stage_id",
        "attempt_id",
        "submission_time",
        "first_task_launched_time",
        "completion_time",
        "failure_reason",
        "name",
    }

    # Convert stage objects to dictionaries and filter numeric fields
    stage1_dict = (
        stage1.model_dump() if hasattr(stage1, "model_dump") else stage1.__dict__
    )
    stage2_dict = (
        stage2.model_dump() if hasattr(stage2, "model_dump") else stage2.__dict__
    )

    # Get all comparable numeric metrics
    stage1_metrics = {
        k: v
        for k, v in stage1_dict.items()
        if k not in exclude_fields and isinstance(v, (int, float)) and v is not None
    }
    stage2_metrics = {
        k: v
        for k, v in stage2_dict.items()
        if k not in exclude_fields and isinstance(v, (int, float)) and v is not None
    }

    # Dynamic comparison for all available metrics
    for metric_name in stage1_metrics:
        if metric_name in stage2_metrics:
            val1 = stage1_metrics[metric_name]
            val2 = stage2_metrics[metric_name]
            diff = calculate_difference(val1, val2, metric_name)
            if diff:
                stage_metrics[metric_name] = diff

    if stage_metrics:
        result["significant_differences"]["stage_metrics"] = stage_metrics

    # Compare task-level distributions (median and max)
    task_distributions = {}

    if stage1.task_metrics_distributions and stage2.task_metrics_distributions:
        dist1 = stage1.task_metrics_distributions
        dist2 = stage2.task_metrics_distributions

        # Dynamic task distribution metrics comparison - include all available distribution fields
        exclude_dist_fields = {
            "quantiles",
            "shuffle_read_metrics",
            "shuffle_write_metrics",
            "input_metrics",
            "output_metrics",
            "peak_memory_metrics",
        }

        # Get all distribution fields from the objects
        dist1_dict = (
            dist1.model_dump() if hasattr(dist1, "model_dump") else dist1.__dict__
        )
        dist2_dict = (
            dist2.model_dump() if hasattr(dist2, "model_dump") else dist2.__dict__
        )

        # Find all sequence fields that are comparable (both exist and have proper format)
        distribution_fields = []
        for field_name in dist1_dict:
            if (
                field_name not in exclude_dist_fields
                and field_name in dist2_dict
                and isinstance(dist1_dict[field_name], (list, tuple))
                and isinstance(dist2_dict[field_name], (list, tuple))
            ):
                distribution_fields.append(
                    (field_name, dist1_dict[field_name], dist2_dict[field_name])
                )

        for metric_name, vals1, vals2 in distribution_fields:
            if vals1 and vals2 and len(vals1) >= 5 and len(vals2) >= 5:
                metric_comparison = {}

                # Compare median (50th percentile - index 2)
                median_diff = calculate_difference(
                    vals1[2], vals2[2], f"{metric_name}_median"
                )
                if median_diff:
                    metric_comparison["median"] = median_diff

                # Compare max (100th percentile - index 4)
                max_diff = calculate_difference(
                    vals1[4], vals2[4], f"{metric_name}_max"
                )
                if max_diff:
                    metric_comparison["max"] = max_diff

                if metric_comparison:
                    task_distributions[metric_name] = metric_comparison

        # Shuffle metrics from nested objects
        if (
            dist1.shuffle_read_metrics
            and dist2.shuffle_read_metrics
            and dist1.shuffle_read_metrics.read_bytes
            and dist2.shuffle_read_metrics.read_bytes
            and len(dist1.shuffle_read_metrics.read_bytes) >= 5
            and len(dist2.shuffle_read_metrics.read_bytes) >= 5
        ):
            read_comparison = {}
            median_diff = calculate_difference(
                dist1.shuffle_read_metrics.read_bytes[2],
                dist2.shuffle_read_metrics.read_bytes[2],
                "shuffle_read_median",
            )
            if median_diff:
                read_comparison["median"] = median_diff

            max_diff = calculate_difference(
                dist1.shuffle_read_metrics.read_bytes[4],
                dist2.shuffle_read_metrics.read_bytes[4],
                "shuffle_read_max",
            )
            if max_diff:
                read_comparison["max"] = max_diff

            if read_comparison:
                task_distributions["shuffle_read_bytes"] = read_comparison

        # Add fetch_wait_time comparison
        if (
            dist1.shuffle_read_metrics
            and dist2.shuffle_read_metrics
            and dist1.shuffle_read_metrics.fetch_wait_time
            and dist2.shuffle_read_metrics.fetch_wait_time
            and len(dist1.shuffle_read_metrics.fetch_wait_time) >= 5
            and len(dist2.shuffle_read_metrics.fetch_wait_time) >= 5
        ):
            fetch_wait_comparison = {}
            median_diff = calculate_difference(
                dist1.shuffle_read_metrics.fetch_wait_time[2],
                dist2.shuffle_read_metrics.fetch_wait_time[2],
                "fetch_wait_time_median",
            )
            if median_diff:
                fetch_wait_comparison["median"] = median_diff

            max_diff = calculate_difference(
                dist1.shuffle_read_metrics.fetch_wait_time[4],
                dist2.shuffle_read_metrics.fetch_wait_time[4],
                "fetch_wait_time_max",
            )
            if max_diff:
                fetch_wait_comparison["max"] = max_diff

            if fetch_wait_comparison:
                task_distributions["shuffle_fetch_wait_time"] = fetch_wait_comparison

        # Add remote_reqs_duration comparison
        if (
            dist1.shuffle_read_metrics
            and dist2.shuffle_read_metrics
            and dist1.shuffle_read_metrics.remote_reqs_duration
            and dist2.shuffle_read_metrics.remote_reqs_duration
            and len(dist1.shuffle_read_metrics.remote_reqs_duration) >= 5
            and len(dist2.shuffle_read_metrics.remote_reqs_duration) >= 5
        ):
            remote_reqs_comparison = {}
            median_diff = calculate_difference(
                dist1.shuffle_read_metrics.remote_reqs_duration[2],
                dist2.shuffle_read_metrics.remote_reqs_duration[2],
                "remote_reqs_duration_median",
            )
            if median_diff:
                remote_reqs_comparison["median"] = median_diff

            max_diff = calculate_difference(
                dist1.shuffle_read_metrics.remote_reqs_duration[4],
                dist2.shuffle_read_metrics.remote_reqs_duration[4],
                "remote_reqs_duration_max",
            )
            if max_diff:
                remote_reqs_comparison["max"] = max_diff

            if remote_reqs_comparison:
                task_distributions["shuffle_remote_reqs_duration"] = (
                    remote_reqs_comparison
                )

        if (
            dist1.shuffle_write_metrics
            and dist2.shuffle_write_metrics
            and dist1.shuffle_write_metrics.write_bytes
            and dist2.shuffle_write_metrics.write_bytes
            and len(dist1.shuffle_write_metrics.write_bytes) >= 5
            and len(dist2.shuffle_write_metrics.write_bytes) >= 5
        ):
            write_comparison = {}
            median_diff = calculate_difference(
                dist1.shuffle_write_metrics.write_bytes[2],
                dist2.shuffle_write_metrics.write_bytes[2],
                "shuffle_write_median",
            )
            if median_diff:
                write_comparison["median"] = median_diff

            max_diff = calculate_difference(
                dist1.shuffle_write_metrics.write_bytes[4],
                dist2.shuffle_write_metrics.write_bytes[4],
                "shuffle_write_max",
            )
            if max_diff:
                write_comparison["max"] = max_diff

            if write_comparison:
                task_distributions["shuffle_write_bytes"] = write_comparison

    if task_distributions:
        result["significant_differences"]["task_distributions"] = task_distributions

    # Compare executor-level distributions (median and max)
    executor_distributions = {}

    if stage1.executor_metrics_distributions and stage2.executor_metrics_distributions:
        exec_dist1 = stage1.executor_metrics_distributions
        exec_dist2 = stage2.executor_metrics_distributions

        # Executor distribution metrics to compare
        exec_dist_metrics = [
            ("task_time", exec_dist1.task_time, exec_dist2.task_time),
            ("shuffle_write", exec_dist1.shuffle_write, exec_dist2.shuffle_write),
            (
                "memory_bytes_spilled",
                exec_dist1.memory_bytes_spilled,
                exec_dist2.memory_bytes_spilled,
            ),
            (
                "disk_bytes_spilled",
                exec_dist1.disk_bytes_spilled,
                exec_dist2.disk_bytes_spilled,
            ),
        ]

        for metric_name, vals1, vals2 in exec_dist_metrics:
            if vals1 and vals2 and len(vals1) >= 5 and len(vals2) >= 5:
                metric_comparison = {}

                # Compare median (index 2)
                median_diff = calculate_difference(
                    vals1[2], vals2[2], f"{metric_name}_median"
                )
                if median_diff:
                    metric_comparison["median"] = median_diff

                # Compare max (index 4)
                max_diff = calculate_difference(
                    vals1[4], vals2[4], f"{metric_name}_max"
                )
                if max_diff:
                    metric_comparison["max"] = max_diff

                if metric_comparison:
                    executor_distributions[metric_name] = metric_comparison

    if executor_distributions:
        result["significant_differences"]["executor_distributions"] = (
            executor_distributions
        )

    # Collect all differences with significance scores for top 5 filtering
    all_differences = []

    # Collect stage-level metrics
    if stage_metrics:
        for metric_name, metric_data in stage_metrics.items():
            all_differences.append(
                {
                    "category": "stage_metrics",
                    "metric_name": metric_name,
                    "full_name": f"stage_metrics.{metric_name}",
                    "significance": metric_data["significance"],
                    "data": metric_data,
                }
            )

    # Collect task distribution metrics
    if task_distributions:
        for metric_name, metric_data in task_distributions.items():
            if isinstance(metric_data, dict):
                for sub_metric, sub_data in metric_data.items():  # median/max
                    all_differences.append(
                        {
                            "category": "task_distributions",
                            "metric_name": metric_name,
                            "sub_metric": sub_metric,
                            "full_name": f"task_distributions.{metric_name}.{sub_metric}",
                            "significance": sub_data["significance"],
                            "data": sub_data,
                        }
                    )

    # Collect executor distribution metrics
    if executor_distributions:
        for metric_name, metric_data in executor_distributions.items():
            if isinstance(metric_data, dict):
                for sub_metric, sub_data in metric_data.items():  # median/max
                    all_differences.append(
                        {
                            "category": "executor_distributions",
                            "metric_name": metric_name,
                            "sub_metric": sub_metric,
                            "full_name": f"executor_distributions.{metric_name}.{sub_metric}",
                            "significance": sub_data["significance"],
                            "data": sub_data,
                        }
                    )

    # Sort by significance (highest first) and take top 5
    total_diffs = len(all_differences)
    all_differences.sort(key=lambda x: x["significance"], reverse=True)
    top_differences = all_differences[:5]

    # Rebuild the significant_differences structure with only top 5
    filtered_significant_differences = {}

    for diff in top_differences:
        category = diff["category"]
        if category not in filtered_significant_differences:
            filtered_significant_differences[category] = {}

        if category == "stage_metrics":
            # Stage metrics are single-level
            filtered_significant_differences[category][diff["metric_name"]] = diff[
                "data"
            ]
        else:
            # Task and executor distributions have sub-metrics (median/max)
            metric_name = diff["metric_name"]
            if metric_name not in filtered_significant_differences[category]:
                filtered_significant_differences[category][metric_name] = {}
            filtered_significant_differences[category][metric_name][
                diff["sub_metric"]
            ] = diff["data"]

    result["significant_differences"] = filtered_significant_differences
    result["summary"]["total_differences_found"] = total_diffs
    result["summary"]["differences_shown"] = len(top_differences)

    return result


@mcp.tool()
def compare_app_executor_timeline(
    app_id1: str, app_id2: str, server: Optional[str] = None, interval_minutes: int = 1
) -> Dict[str, Any]:
    """
    Compare executor timeline patterns between two Spark applications.

    Analyzes application-level executor allocation and usage patterns throughout
    the entire application lifecycle to identify differences in resource utilization,
    efficiency, and optimization opportunities.

    Args:
        app_id1: First Spark application ID (baseline)
        app_id2: Second Spark application ID (comparison target)
        server: Optional server name to use (uses default if not specified)
        interval_minutes: Time interval for analysis in minutes (default: 1)

    Returns:
        Dictionary containing comprehensive application executor timeline comparison
    """
    client = _resolve_client(server)
    try:
        # Get application information for both apps
        app1 = client.get_application(app_id1)
        app2 = client.get_application(app_id2)

        if not app1.attempts or not app2.attempts:
            return {
                "error": "One or both applications have no attempts",
                "applications": {
                    "app1": {"id": app_id1, "has_attempts": bool(app1.attempts)},
                    "app2": {"id": app_id2, "has_attempts": bool(app2.attempts)},
                },
            }

        # Get executors for both applications
        executors1 = client.list_all_executors(app_id=app_id1)
        executors2 = client.list_all_executors(app_id=app_id2)

        # Get stages for both applications to track active stages
        stages1 = client.list_stages(app_id=app_id1)
        stages2 = client.list_stages(app_id=app_id2)

        def build_app_executor_timeline(app, executors, stages, app_id):
            """Build timeline for an application"""
            if not app.attempts:
                return None

            start_time = app.attempts[0].start_time
            end_time = app.attempts[0].end_time

            if not start_time:
                return None

            if not end_time:
                # If still running, use current time or estimate
                end_time = start_time + timedelta(hours=24)

            # Create timeline events for executors
            timeline_events = []

            # Add executor events
            for executor in executors:
                if executor.add_time:
                    timeline_events.append(
                        {
                            "timestamp": executor.add_time,
                            "type": "executor_add",
                            "executor_id": executor.id,
                            "cores": executor.total_cores or 0,
                            "memory_mb": (executor.max_memory / (1024 * 1024))
                            if executor.max_memory
                            else 0,
                        }
                    )

                if executor.remove_time:
                    timeline_events.append(
                        {
                            "timestamp": executor.remove_time,
                            "type": "executor_remove",
                            "executor_id": executor.id,
                        }
                    )

            # Add stage events for tracking active stages
            for stage in stages:
                if stage.submission_time:
                    timeline_events.append(
                        {
                            "timestamp": stage.submission_time,
                            "type": "stage_start",
                            "stage_id": stage.stage_id,
                            "name": stage.name,
                        }
                    )

                if stage.completion_time:
                    timeline_events.append(
                        {
                            "timestamp": stage.completion_time,
                            "type": "stage_end",
                            "stage_id": stage.stage_id,
                        }
                    )

            # Sort events by timestamp
            timeline_events.sort(key=lambda x: x["timestamp"])

            # Build interval-based timeline
            timeline = []
            current_time = start_time
            max_intervals = 10000  # Safety limit

            interval_count = 0
            while current_time < end_time and interval_count < max_intervals:
                interval_end = current_time + timedelta(minutes=interval_minutes)

                if interval_end > end_time:
                    interval_end = end_time

                # Calculate active resources at this interval
                active_executor_count = 0
                total_cores = 0
                total_memory_mb = 0
                active_stages = set()

                # Count active executors
                for executor in executors:
                    executor_start = executor.add_time or start_time
                    executor_end = executor.remove_time or end_time

                    if executor_start <= interval_end and executor_end >= current_time:
                        active_executor_count += 1
                        total_cores += executor.total_cores or 0
                        total_memory_mb += (
                            (executor.max_memory / (1024 * 1024))
                            if executor.max_memory
                            else 0
                        )

                # Count active stages
                for stage in stages:
                    stage_start = stage.submission_time
                    stage_end = stage.completion_time or end_time

                    if (
                        stage_start
                        and stage_start <= interval_end
                        and stage_end >= current_time
                    ):
                        active_stages.add(stage.stage_id)

                timeline.append(
                    {
                        "interval_start": current_time.isoformat(),
                        "interval_end": interval_end.isoformat(),
                        "active_executor_count": active_executor_count,
                        "total_cores": total_cores,
                        "total_memory_mb": total_memory_mb,
                        "active_stages_count": len(active_stages),
                    }
                )

                current_time = interval_end
                interval_count += 1

            return {
                "app_info": {
                    "app_id": app_id,
                    "name": app.name,
                    "start_time": start_time.isoformat() if start_time else None,
                    "end_time": end_time.isoformat()
                    if app.attempts[0].end_time
                    else None,
                    "duration_seconds": (end_time - start_time).total_seconds()
                    if start_time
                    else 0,
                },
                "timeline": timeline,
                "summary": {
                    "total_executors": len(executors),
                    "total_stages": len(stages),
                    "peak_executor_count": max(
                        (interval["active_executor_count"] for interval in timeline),
                        default=0,
                    ),
                    "avg_executor_count": sum(
                        interval["active_executor_count"] for interval in timeline
                    )
                    / len(timeline)
                    if timeline
                    else 0,
                    "peak_cores": max(
                        (interval["total_cores"] for interval in timeline), default=0
                    ),
                    "peak_memory_mb": max(
                        (interval["total_memory_mb"] for interval in timeline),
                        default=0,
                    ),
                },
            }

        # Build timelines for both applications
        timeline1 = build_app_executor_timeline(app1, executors1, stages1, app_id1)
        timeline2 = build_app_executor_timeline(app2, executors2, stages2, app_id2)

        if not timeline1 or not timeline2:
            return {
                "error": "Could not build timeline for one or both applications",
                "applications": {
                    "app1": {"id": app_id1, "timeline_built": timeline1 is not None},
                    "app2": {"id": app_id2, "timeline_built": timeline2 is not None},
                },
            }

        # Compare timelines interval by interval
        comparison_data = []
        min_length = min(len(timeline1["timeline"]), len(timeline2["timeline"]))

        for i in range(min_length):
            interval1 = timeline1["timeline"][i]
            interval2 = timeline2["timeline"][i]

            executor_diff = (
                interval2["active_executor_count"] - interval1["active_executor_count"]
            )

            comparison_data.append(
                {
                    "interval": i + 1,
                    "timestamp_range": f"{interval1['interval_start']} to {interval1['interval_end']}",
                    "app1": {"executor_count": interval1["active_executor_count"]},
                    "app2": {"executor_count": interval2["active_executor_count"]},
                    "differences": {"executor_count_diff": executor_diff},
                }
            )

        # Merge consecutive intervals with same executor counts
        merged_comparison_data = merge_consecutive_intervals(comparison_data)

        # Calculate efficiency metrics
        def calculate_efficiency_metrics(timeline_data):
            timeline = timeline_data["timeline"]
            if not timeline:
                return {}

            non_zero_intervals = [t for t in timeline if t["active_executor_count"] > 0]

            if not non_zero_intervals:
                return {"avg_utilization": 0, "efficiency_score": 0}

            avg_utilization = sum(
                t["active_executor_count"] for t in non_zero_intervals
            ) / len(non_zero_intervals)
            peak_count = max(t["active_executor_count"] for t in timeline)

            # Simple efficiency score: how close to peak utilization on average
            efficiency_score = (avg_utilization / peak_count) if peak_count > 0 else 0

            return {
                "avg_utilization": avg_utilization,
                "efficiency_score": efficiency_score,
                "resource_waste_intervals": sum(
                    1
                    for t in timeline
                    if t["active_executor_count"] < avg_utilization * 0.5
                ),
            }

        efficiency1 = calculate_efficiency_metrics(timeline1)
        efficiency2 = calculate_efficiency_metrics(timeline2)

        # Generate recommendations
        recommendations = []

        app1_peak = timeline1["summary"]["peak_executor_count"]
        app2_peak = timeline2["summary"]["peak_executor_count"]
        app1_avg = timeline1["summary"]["avg_executor_count"]
        app2_avg = timeline2["summary"]["avg_executor_count"]

        if app2_avg > app1_avg * 1.2:
            recommendations.append(
                {
                    "type": "resource_allocation",
                    "priority": "medium",
                    "issue": f"App2 uses {((app2_avg / app1_avg - 1) * 100):.0f}% more executors on average",
                    "suggestion": "Consider if App2's higher resource allocation provides proportional performance benefits",
                }
            )

        if (
            efficiency2.get("efficiency_score", 0)
            > efficiency1.get("efficiency_score", 0) * 1.1
        ):
            recommendations.append(
                {
                    "type": "efficiency",
                    "priority": "high",
                    "issue": "App2 shows significantly better executor utilization efficiency",
                    "suggestion": "Apply App2's resource allocation pattern to App1 for better efficiency",
                }
            )

        if (
            timeline1["app_info"]["duration_seconds"]
            > timeline2["app_info"]["duration_seconds"] * 1.2
        ):
            time_savings = (
                timeline1["app_info"]["duration_seconds"]
                - timeline2["app_info"]["duration_seconds"]
            )
            recommendations.append(
                {
                    "type": "performance",
                    "priority": "high",
                    "issue": f"App1 takes {time_savings:.0f}s longer to complete",
                    "suggestion": "Analyze App2's parallelization and resource allocation strategy",
                }
            )

        # Calculate summary statistics for merged data
        original_intervals = len(comparison_data)
        merged_intervals = len(merged_comparison_data)
        intervals_with_differences = sum(
            1
            for c in merged_comparison_data
            if c["differences"]["executor_count_diff"] != 0
        )
        max_executor_diff = max(
            (
                abs(c["differences"]["executor_count_diff"])
                for c in merged_comparison_data
            ),
            default=0,
        )
        avg_executor_diff = (
            sum(
                abs(c["differences"]["executor_count_diff"])
                for c in merged_comparison_data
            )
            / merged_intervals
            if merged_intervals > 0
            else 0
        )

        return {
            "app1_info": timeline1["app_info"],
            "app2_info": timeline2["app_info"],
            "comparison_config": {
                "interval_minutes": interval_minutes,
                "original_intervals_compared": original_intervals,
                "merged_intervals_shown": merged_intervals,
                "analysis_type": "App-Level Executor Timeline Comparison",
            },
            "timeline_comparison": merged_comparison_data,
            "resource_efficiency": {
                "app1": {**timeline1["summary"], **efficiency1},
                "app2": {**timeline2["summary"], **efficiency2},
            },
            "summary": {
                "original_intervals": original_intervals,
                "merged_intervals": merged_intervals,
                "intervals_with_differences": intervals_with_differences,
                "avg_executor_count_difference": avg_executor_diff,
                "max_executor_count_difference": max_executor_diff,
                "app2_more_efficient": efficiency2.get("efficiency_score", 0)
                > efficiency1.get("efficiency_score", 0),
                "performance_improvement": {
                    "time_difference_seconds": timeline1["app_info"]["duration_seconds"]
                    - timeline2["app_info"]["duration_seconds"],
                    "efficiency_improvement_ratio": (
                        efficiency2.get("efficiency_score", 0)
                        / efficiency1.get("efficiency_score", 1)
                    )
                    if efficiency1.get("efficiency_score", 0) > 0
                    else 1,
                },
            },
            "recommendations": recommendations,
            "key_differences": {
                "peak_executor_difference": app2_peak - app1_peak,
                "avg_executor_difference": app2_avg - app1_avg,
                "duration_difference_seconds": timeline2["app_info"]["duration_seconds"]
                - timeline1["app_info"]["duration_seconds"],
            },
        }

    except Exception as e:
        return {
            "error": f"Failed to compare app executor timelines: {str(e)}",
            "app1_id": app_id1,
            "app2_id": app_id2,
        }


@mcp.tool()
def compare_stage_executor_timeline(
    app_id1: str,
    app_id2: str,
    stage_id1: int,
    stage_id2: int,
    server: Optional[str] = None,
    interval_minutes: int = 1,
) -> Dict[str, Any]:
    """
    Compare executor timeline for specific stages between two Spark applications.

    Analyzes executor allocation and usage patterns during stage execution
    at configurable time intervals to identify differences in resource utilization.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        stage_id1: Stage ID from first application
        stage_id2: Stage ID from second application
        server: Optional server name to use (uses default if not specified)
        interval_minutes: Time interval for analysis in minutes (default: 1)

    Returns:
        Dictionary containing stage executor timeline comparison
    """
    client = _resolve_client(server)

    try:
        # Get stage information for both applications
        stage1 = client.get_stage_attempt(
            app_id=app_id1,
            stage_id=stage_id1,
            attempt_id=0,
            details=False,
            with_summaries=False,
        )
        stage2 = client.get_stage_attempt(
            app_id=app_id2,
            stage_id=stage_id2,
            attempt_id=0,
            details=False,
            with_summaries=False,
        )

        # Get executors for both applications
        executors1 = client.list_all_executors(app_id=app_id1)
        executors2 = client.list_all_executors(app_id=app_id2)

        # Helper function to build timeline for a stage
        def build_stage_executor_timeline(stage, executors, app_id):
            if not stage.submission_time:
                return {
                    "error": f"Stage {stage.stage_id} has no submission time",
                    "stage_info": {
                        "stage_id": stage.stage_id,
                        "attempt_id": getattr(stage, "attempt_id", 0),
                        "name": getattr(stage, "name", "unknown"),
                        "submission_time": None,
                        "completion_time": None,
                        "duration_seconds": 0,
                    },
                    "timeline": [],
                }

            stage_start = stage.submission_time
            stage_end = stage.completion_time or stage_start + timedelta(
                hours=24
            )  # Default to 24h if not completed

            # Sanity check: ensure stage_end is after stage_start
            if stage_end <= stage_start:
                stage_end = stage_start + timedelta(minutes=interval_minutes)

            # Create timeline intervals
            timeline = []
            current_time = stage_start
            max_intervals = 10000  # Safety limit to prevent excessive memory usage

            interval_count = 0
            while current_time < stage_end and interval_count < max_intervals:
                interval_end = current_time + timedelta(minutes=interval_minutes)

                # Ensure we don't go past the stage end, but break if we're at the end
                if interval_end >= stage_end:
                    interval_end = stage_end

                # Find active executors during this interval
                active_executors = []
                total_cores = 0
                total_memory = 0

                for executor in executors:
                    # Check if executor was active during this interval
                    executor_start = executor.add_time or stage_start
                    executor_end = executor.remove_time or stage_end

                    if executor_start <= interval_end and executor_end >= current_time:
                        active_executors.append(
                            {
                                "id": executor.id,
                                "host_port": executor.host_port,
                                "cores": executor.total_cores or 0,
                                "memory_mb": (executor.max_memory / (1024 * 1024))
                                if executor.max_memory
                                else 0,
                            }
                        )
                        total_cores += executor.total_cores or 0
                        total_memory += (
                            (executor.max_memory / (1024 * 1024))
                            if executor.max_memory
                            else 0
                        )

                timeline.append(
                    {
                        "timestamp": current_time.isoformat(),
                        "interval_start": current_time.isoformat(),
                        "interval_end": interval_end.isoformat(),
                        "active_executor_count": len(active_executors),
                        "total_cores": total_cores,
                        "total_memory_mb": total_memory,
                        "active_executors": active_executors,
                    }
                )

                # Break if we've reached the stage end to prevent infinite loop
                if interval_end >= stage_end:
                    break

                current_time = interval_end
                interval_count += 1

            # Add warning if we hit the interval limit
            if interval_count >= max_intervals:
                timeline.append(
                    {
                        "warning": f"Timeline truncated at {max_intervals} intervals to prevent excessive memory usage",
                        "stage_duration_hours": (
                            stage_end - stage_start
                        ).total_seconds()
                        / 3600,
                        "interval_minutes": interval_minutes,
                    }
                )

            return {
                "stage_info": {
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "submission_time": stage_start.isoformat() if stage_start else None,
                    "completion_time": stage_end.isoformat()
                    if stage.completion_time
                    else None,
                    "duration_seconds": (stage_end - stage_start).total_seconds()
                    if stage_start
                    else 0,
                },
                "timeline": timeline,
            }

        # Build timelines for both stages
        timeline1 = build_stage_executor_timeline(stage1, executors1, app_id1)
        timeline2 = build_stage_executor_timeline(stage2, executors2, app_id2)

        # Compare timelines and find significant differences
        comparison_data = []
        min_length = min(len(timeline1["timeline"]), len(timeline2["timeline"]))

        for i in range(min_length):
            interval1 = timeline1["timeline"][i]
            interval2 = timeline2["timeline"][i]

            executor_diff = (
                interval2["active_executor_count"] - interval1["active_executor_count"]
            )

            comparison_data.append(
                {
                    "interval": i + 1,
                    "timestamp_range": f"{interval1['interval_start']} to {interval1['interval_end']}",
                    "app1": {"executor_count": interval1["active_executor_count"]},
                    "app2": {"executor_count": interval2["active_executor_count"]},
                    "differences": {"executor_count_diff": executor_diff},
                }
            )

        # Merge consecutive intervals with same executor counts
        merged_comparison_data = merge_consecutive_intervals(comparison_data)

        # Calculate summary statistics
        original_intervals = len(comparison_data)
        merged_intervals = len(merged_comparison_data)
        intervals_with_executor_diff = sum(
            1
            for c in merged_comparison_data
            if c["differences"]["executor_count_diff"] != 0
        )
        max_executor_diff = max(
            (
                abs(c["differences"]["executor_count_diff"])
                for c in merged_comparison_data
            ),
            default=0,
        )

        return {
            "app1_info": {"app_id": app_id1, "stage_details": timeline1["stage_info"]},
            "app2_info": {"app_id": app_id2, "stage_details": timeline2["stage_info"]},
            "comparison_config": {
                "interval_minutes": interval_minutes,
                "original_intervals_compared": original_intervals,
                "merged_intervals_shown": merged_intervals,
            },
            "timeline_comparison": merged_comparison_data,
            "summary": {
                "original_intervals": original_intervals,
                "merged_intervals": merged_intervals,
                "intervals_with_executor_differences": intervals_with_executor_diff,
                "max_executor_count_difference": max_executor_diff,
                "stages_overlap": timeline1["stage_info"]["completion_time"] is not None
                and timeline2["stage_info"]["completion_time"] is not None,
            },
        }

    except Exception as e:
        return {
            "error": f"Failed to compare stage executor timelines: {str(e)}",
            "app1_id": app_id1,
            "app2_id": app_id2,
            "stage_ids": [stage_id1, stage_id2],
        }


@mcp.tool()
def compare_app_resources(
    app_id1: str, app_id2: str, server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare resource allocation and configuration between two Spark applications.

    Focuses specifically on resource allocation patterns, executor configuration,
    and resource utilization efficiency without getting into detailed performance metrics.

    Args:
        app_id1: First Spark application ID (baseline)
        app_id2: Second Spark application ID (comparison target)
        server: Optional Spark History Server name

    Returns:
        Dict containing resource allocation comparison, efficiency ratios, and recommendations
    """
    client = _resolve_client(server)

    try:
        # Get application info
        app1 = client.get_application(app_id1)
        app2 = client.get_application(app_id2)

        app1_info = _get_basic_app_info(app1)
        app2_info = _get_basic_app_info(app2)

        # Calculate resource ratios and comparisons
        resource_comparison = {}

        if app1_info["cores_granted"] and app2_info["cores_granted"]:
            resource_comparison["cores_granted_ratio"] = (
                app2_info["cores_granted"] / app1_info["cores_granted"]
            )

        if app1_info["max_cores"] and app2_info["max_cores"]:
            resource_comparison["max_cores_ratio"] = (
                app2_info["max_cores"] / app1_info["max_cores"]
            )

        if app1_info["memory_per_executor_mb"] and app2_info["memory_per_executor_mb"]:
            resource_comparison["memory_per_executor_ratio"] = (
                app2_info["memory_per_executor_mb"]
                / app1_info["memory_per_executor_mb"]
            )

        if app1_info["max_executors"] and app2_info["max_executors"]:
            resource_comparison["max_executors_ratio"] = (
                app2_info["max_executors"] / app1_info["max_executors"]
            )

        # Generate resource-specific recommendations
        recommendations = []

        # Cores analysis
        if resource_comparison.get("cores_granted_ratio", 1) > 2:
            recommendations.append(
                {
                    "type": "resource_scaling",
                    "priority": "medium",
                    "issue": f"App2 uses {resource_comparison['cores_granted_ratio']:.1f}x more cores than App1",
                    "suggestion": "Consider if App2 needs this level of CPU resources or if App1 is under-provisioned",
                }
            )
        elif resource_comparison.get("cores_granted_ratio", 1) < 0.5:
            recommendations.append(
                {
                    "type": "resource_scaling",
                    "priority": "high",
                    "issue": f"App2 uses {resource_comparison['cores_granted_ratio']:.1f}x fewer cores than App1",
                    "suggestion": "App2 may be CPU-constrained - consider increasing core allocation",
                }
            )

        # Memory analysis
        if resource_comparison.get("memory_per_executor_ratio", 1) > 2:
            recommendations.append(
                {
                    "type": "memory_allocation",
                    "priority": "medium",
                    "issue": f"App2 allocates {resource_comparison['memory_per_executor_ratio']:.1f}x more memory per executor",
                    "suggestion": "Verify if App2's workload requires this memory or if it's over-provisioned",
                }
            )
        elif resource_comparison.get("memory_per_executor_ratio", 1) < 0.5:
            recommendations.append(
                {
                    "type": "memory_allocation",
                    "priority": "high",
                    "issue": f"App2 has {resource_comparison['memory_per_executor_ratio']:.1f}x less memory per executor",
                    "suggestion": "App2 may experience memory pressure - consider increasing executor memory",
                }
            )

        result = {
            "applications": {"app1": app1_info, "app2": app2_info},
            "resource_comparison": resource_comparison,
            "recommendations": recommendations,
        }

        # Sort the result by ratios (descending)
        return sort_comparison_data(result, sort_key="ratio")

    except Exception as e:
        return {
            "error": f"Failed to compare app resources: {str(e)}",
            "applications": {"app1": {"id": app_id1}, "app2": {"id": app_id2}},
        }


@mcp.tool()
def compare_app_executors(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None,
    significance_threshold: float = 0.1,
    show_only_significant: bool = True,
) -> Dict[str, Any]:
    """
    Compare executor-level performance metrics between two Spark applications.

    Focuses specifically on executor utilization, memory usage, GC performance,
    and task completion patterns without detailed stage-by-stage analysis.

    Args:
        app_id1: First Spark application ID (baseline)
        app_id2: Second Spark application ID (comparison target)
        server: Optional Spark History Server name
        significance_threshold: Minimum difference threshold to show metric (default: 0.1)
        show_only_significant: When True, filter out metrics below significance threshold (default: True)

    Returns:
        Dict containing executor performance comparison, efficiency ratios, and recommendations
    """
    try:
        # Get executor summaries for both applications
        exec_summary1 = executor_tools.get_executor_summary(app_id1, server)
        exec_summary2 = executor_tools.get_executor_summary(app_id2, server)

        if not exec_summary1 or not exec_summary2:
            return {
                "error": "Could not retrieve executor summaries for one or both applications",
                "applications": {
                    "app1": {
                        "id": app_id1,
                        "executor_summary": exec_summary1 is not None,
                    },
                    "app2": {
                        "id": app_id2,
                        "executor_summary": exec_summary2 is not None,
                    },
                },
            }

        # Dynamic executor performance ratios - include all numeric fields from executor summaries
        exclude_fields = {
            "active_executors"
        }  # active_executors is calculated differently

        # Filter to only comparable numeric metrics from get_executor_summary
        app1_metrics = {
            k: v
            for k, v in exec_summary1.items()
            if k not in exclude_fields and isinstance(v, (int, float))
        }
        app2_metrics = {
            k: v
            for k, v in exec_summary2.items()
            if k not in exclude_fields and isinstance(v, (int, float))
        }

        # Calculate dynamic performance ratios with proper zero handling
        executor_comparison = {}
        for metric in app1_metrics.keys():
            if metric in app2_metrics:
                ratio = _calculate_safe_ratio(
                    app1_metrics[metric], app2_metrics[metric]
                )
                executor_comparison[f"{metric}_ratio"] = ratio

        # Calculate efficiency metrics
        efficiency_metrics = {}

        # Task completion efficiency (tasks per executor)
        if exec_summary1.get("total_executors", 0) > 0:
            efficiency_metrics["app1_tasks_per_executor"] = (
                exec_summary1.get("completed_tasks", 0)
                / exec_summary1["total_executors"]
            )
        if exec_summary2.get("total_executors", 0) > 0:
            efficiency_metrics["app2_tasks_per_executor"] = (
                exec_summary2.get("completed_tasks", 0)
                / exec_summary2["total_executors"]
            )

        # Memory utilization efficiency
        if (
            exec_summary1.get("memory_used", 0) > 0
            and exec_summary1.get("completed_tasks", 0) > 0
        ):
            efficiency_metrics["app1_tasks_per_mb"] = exec_summary1[
                "completed_tasks"
            ] / (exec_summary1["memory_used"] / (1024 * 1024))
        if (
            exec_summary2.get("memory_used", 0) > 0
            and exec_summary2.get("completed_tasks", 0) > 0
        ):
            efficiency_metrics["app2_tasks_per_mb"] = exec_summary2[
                "completed_tasks"
            ] / (exec_summary2["memory_used"] / (1024 * 1024))

        # Generate executor-specific recommendations
        recommendations = []

        # Executor scaling analysis
        if executor_comparison.get("total_executors_ratio", 1) > 1.5:
            recommendations.append(
                {
                    "type": "executor_scaling",
                    "priority": "medium",
                    "issue": f"App2 uses {executor_comparison['total_executors_ratio']:.1f}x more executors than App1",
                    "suggestion": "Evaluate if App2 needs this level of parallelism or if resources can be optimized",
                }
            )
        elif executor_comparison.get("total_executors_ratio", 1) < 0.7:
            recommendations.append(
                {
                    "type": "executor_scaling",
                    "priority": "high",
                    "issue": f"App2 uses {executor_comparison['total_executors_ratio']:.1f}x fewer executors than App1",
                    "suggestion": "App2 may benefit from increased parallelism - consider scaling up executors",
                }
            )

        # Memory efficiency analysis
        if executor_comparison.get("memory_used_ratio", 1) > 2.0:
            recommendations.append(
                {
                    "type": "memory_efficiency",
                    "priority": "medium",
                    "issue": f"App2 uses {executor_comparison['memory_used_ratio']:.1f}x more memory than App1",
                    "suggestion": "Review App2's memory usage patterns - may indicate inefficient data structures or caching",
                }
            )

        # GC performance analysis
        if executor_comparison.get("total_gc_time_ratio", 1) > 2.0:
            recommendations.append(
                {
                    "type": "gc_performance",
                    "priority": "high",
                    "issue": f"App2 has {executor_comparison['total_gc_time_ratio']:.1f}x more GC time than App1",
                    "suggestion": "App2 experiencing memory pressure - consider increasing executor memory or optimizing data structures",
                }
            )

        # Task efficiency analysis
        if (
            efficiency_metrics.get("app1_tasks_per_executor", 0) > 0
            and efficiency_metrics.get("app2_tasks_per_executor", 0) > 0
        ):
            task_efficiency_ratio = (
                efficiency_metrics["app2_tasks_per_executor"]
                / efficiency_metrics["app1_tasks_per_executor"]
            )
            if task_efficiency_ratio < 0.5:
                recommendations.append(
                    {
                        "type": "task_efficiency",
                        "priority": "medium",
                        "issue": f"App2 processes {task_efficiency_ratio:.1f}x fewer tasks per executor than App1",
                        "suggestion": "App2's executors may be underutilized - check for data skew or resource bottlenecks",
                    }
                )

        # Apply significance filtering to executor comparison ratios
        filtered_executor_comparison = _filter_significant_metrics(
            executor_comparison, significance_threshold, show_only_significant
        )

        # Apply significance filtering to efficiency metrics (those that are ratios)
        efficiency_ratios = {}
        if (
            efficiency_metrics.get("app1_tasks_per_executor", 0) > 0
            and efficiency_metrics.get("app2_tasks_per_executor", 0) > 0
        ):
            efficiency_ratios["tasks_per_executor_ratio"] = (
                efficiency_metrics["app2_tasks_per_executor"]
                / efficiency_metrics["app1_tasks_per_executor"]
            )
        if (
            efficiency_metrics.get("app1_tasks_per_mb", 0) > 0
            and efficiency_metrics.get("app2_tasks_per_mb", 0) > 0
        ):
            efficiency_ratios["tasks_per_mb_ratio"] = (
                efficiency_metrics["app2_tasks_per_mb"]
                / efficiency_metrics["app1_tasks_per_mb"]
            )

        filtered_efficiency_ratios = _filter_significant_metrics(
            efficiency_ratios, significance_threshold, show_only_significant
        )

        result = {
            "applications": {
                "app1": {"id": app_id1, "executor_metrics": exec_summary1},
                "app2": {"id": app_id2, "executor_metrics": exec_summary2},
            },
            "executor_comparison": filtered_executor_comparison["metrics"],
            "efficiency_metrics": efficiency_metrics,
            "efficiency_ratios": filtered_efficiency_ratios["metrics"],
            "recommendations": recommendations,
            "filtering_summary": {
                "executor_comparison": {
                    "total_metrics": filtered_executor_comparison["total_metrics"],
                    "significant_metrics": filtered_executor_comparison[
                        "significant_metrics"
                    ],
                    "filtering_applied": filtered_executor_comparison[
                        "filtering_applied"
                    ],
                },
                "efficiency_ratios": {
                    "total_metrics": filtered_efficiency_ratios["total_metrics"],
                    "significant_metrics": filtered_efficiency_ratios[
                        "significant_metrics"
                    ],
                    "filtering_applied": filtered_efficiency_ratios[
                        "filtering_applied"
                    ],
                },
                "significance_threshold": significance_threshold,
            },
        }

        # Sort the result by ratios (descending)
        return sort_comparison_data(result, sort_key="ratio")

    except Exception as e:
        return {
            "error": f"Failed to compare executor performance: {str(e)}",
            "applications": {"app1": {"id": app_id1}, "app2": {"id": app_id2}},
        }


@mcp.tool()
def compare_app_jobs(
    app_id1: str, app_id2: str, server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare job-level performance metrics between two Spark applications.

    Focuses specifically on job counts, durations, success rates, and job-level
    parallelism patterns without detailed stage or executor analysis.

    Args:
        app_id1: First Spark application ID (baseline)
        app_id2: Second Spark application ID (comparison target)
        server: Optional Spark History Server name

    Returns:
        Dict containing job performance comparison, timing analysis, and recommendations
    """
    try:
        # Get job data for both applications
        jobs1 = fetcher_tools.fetch_jobs(app_id1, server)
        jobs2 = fetcher_tools.fetch_jobs(app_id2, server)

        # Calculate job statistics
        job_stats1 = _calculate_job_stats(jobs1)
        job_stats2 = _calculate_job_stats(jobs2)

        # Calculate job performance ratios
        job_comparison = {
            "job_count_ratio": job_stats2["count"] / max(job_stats1["count"], 1),
            "avg_duration_ratio": job_stats2["avg_duration"]
            / max(job_stats1["avg_duration"], 1)
            if job_stats1["avg_duration"] > 0
            else 0,
            "total_duration_ratio": job_stats2["total_duration"]
            / max(job_stats1["total_duration"], 1)
            if job_stats1["total_duration"] > 0
            else 0,
            "completion_rate_ratio": (
                job_stats2["completed_count"] / max(job_stats2["count"], 1)
            )
            / max((job_stats1["completed_count"] / max(job_stats1["count"], 1)), 0.01),
        }

        # Job success rate analysis
        job1_success_rate = job_stats1["completed_count"] / max(job_stats1["count"], 1)
        job2_success_rate = job_stats2["completed_count"] / max(job_stats2["count"], 1)

        # Job timing analysis
        timing_analysis = {}
        if job_stats1["avg_duration"] > 0 and job_stats2["avg_duration"] > 0:
            timing_analysis["avg_duration_difference_seconds"] = (
                job_stats2["avg_duration"] - job_stats1["avg_duration"]
            )
            timing_analysis["avg_duration_improvement_percent"] = (
                (job_stats1["avg_duration"] - job_stats2["avg_duration"])
                / job_stats1["avg_duration"]
            ) * 100

        # Generate job-specific recommendations
        recommendations = []

        # Job count analysis
        if job_comparison["job_count_ratio"] > 2.0:
            recommendations.append(
                {
                    "type": "job_complexity",
                    "priority": "medium",
                    "issue": f"App2 has {job_comparison['job_count_ratio']:.1f}x more jobs than App1",
                    "suggestion": "App2 may have more complex workflow or different job decomposition strategy",
                }
            )

        # Duration performance analysis
        if job_comparison["avg_duration_ratio"] > 1.5:
            recommendations.append(
                {
                    "type": "job_performance",
                    "priority": "high",
                    "issue": f"App2 jobs are {job_comparison['avg_duration_ratio']:.1f}x slower on average than App1",
                    "suggestion": "Investigate job-level performance bottlenecks in App2 - may need optimization or resource scaling",
                }
            )
        elif job_comparison["avg_duration_ratio"] < 0.7:
            recommendations.append(
                {
                    "type": "job_performance",
                    "priority": "low",
                    "issue": f"App2 jobs are {1 / job_comparison['avg_duration_ratio']:.1f}x faster than App1",
                    "suggestion": "App2 shows better job-level performance - consider applying similar optimizations to App1",
                }
            )

        # Success rate analysis
        if job2_success_rate < job1_success_rate - 0.1:  # More than 10% difference
            recommendations.append(
                {
                    "type": "job_reliability",
                    "priority": "high",
                    "issue": f"App2 has {(job1_success_rate - job2_success_rate) * 100:.1f}% lower job success rate",
                    "suggestion": "App2 experiencing more job failures - investigate error patterns and resource issues",
                }
            )

        # Total execution time analysis
        if job_comparison["total_duration_ratio"] > 2.0:
            recommendations.append(
                {
                    "type": "overall_efficiency",
                    "priority": "medium",
                    "issue": f"App2 takes {job_comparison['total_duration_ratio']:.1f}x longer total execution time",
                    "suggestion": "App2 may benefit from better parallelization or resource optimization",
                }
            )

        result = {
            "applications": {
                "app1": {
                    "id": app_id1,
                    "job_stats": job_stats1,
                    "success_rate": job1_success_rate,
                },
                "app2": {
                    "id": app_id2,
                    "job_stats": job_stats2,
                    "success_rate": job2_success_rate,
                },
            },
            "job_comparison": job_comparison,
            "timing_analysis": timing_analysis,
            "recommendations": recommendations,
        }

        # Sort the result by ratios (descending)
        return sort_comparison_data(result, sort_key="ratio")

    except Exception as e:
        return {
            "error": f"Failed to compare job performance: {str(e)}",
            "applications": {"app1": {"id": app_id1}, "app2": {"id": app_id2}},
        }


@mcp.tool()
def compare_app_stages_aggregated(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None,
    significance_threshold: float = 0.1,
    show_only_significant: bool = True,
) -> Dict[str, Any]:
    """
    Compare aggregated stage-level metrics between two Spark applications.

    Focuses on overall stage performance patterns, I/O volumes, shuffle operations,
    and data processing efficiency without individual stage-by-stage analysis.

    Args:
        app_id1: First Spark application ID (baseline)
        app_id2: Second Spark application ID (comparison target)
        server: Optional Spark History Server name
        significance_threshold: Minimum difference threshold to show metric (default: 0.1)
        show_only_significant: When True, filter out metrics below significance threshold (default: True)

    Returns:
        Dict containing aggregated stage comparison, I/O analysis, and recommendations
    """
    try:
        # Get stages from both applications - try with summaries first, fallback if needed
        try:
            stages1 = fetcher_tools.fetch_stages(
                app_id=app_id1, server=server, with_summaries=True
            )
            stages2 = fetcher_tools.fetch_stages(
                app_id=app_id2, server=server, with_summaries=True
            )
        except Exception as e:
            if "executorMetricsDistributions.peakMemoryMetrics.quantiles" in str(e):
                stages1 = fetcher_tools.fetch_stages(
                    app_id=app_id1, server=server, with_summaries=False
                )
                stages2 = fetcher_tools.fetch_stages(
                    app_id=app_id2, server=server, with_summaries=False
                )
            else:
                raise e

        if not stages1 or not stages2:
            return {
                "error": "No stages found in one or both applications",
                "applications": {
                    "app1": {
                        "id": app_id1,
                        "stage_count": len(stages1) if stages1 else 0,
                    },
                    "app2": {
                        "id": app_id2,
                        "stage_count": len(stages2) if stages2 else 0,
                    },
                },
            }

        # Calculate aggregated stage metrics
        stage_metrics1 = _calculate_aggregated_stage_metrics(stages1)
        stage_metrics2 = _calculate_aggregated_stage_metrics(stages2)

        # Calculate stage performance ratios with proper zero handling
        stage_comparison = {
            "stage_count_ratio": _calculate_safe_ratio(
                stage_metrics1["total_stages"], stage_metrics2["total_stages"]
            ),
            "duration_ratio": _calculate_safe_ratio(
                stage_metrics1["total_stage_duration"],
                stage_metrics2["total_stage_duration"],
            ),
            "executor_runtime_ratio": _calculate_safe_ratio(
                stage_metrics1["total_executor_run_time"],
                stage_metrics2["total_executor_run_time"],
            ),
            "memory_spill_ratio": _calculate_safe_ratio(
                stage_metrics1["total_memory_spilled"],
                stage_metrics2["total_memory_spilled"],
            ),
            "shuffle_read_ratio": _calculate_safe_ratio(
                stage_metrics1["total_shuffle_read_bytes"],
                stage_metrics2["total_shuffle_read_bytes"],
            ),
            "shuffle_write_ratio": _calculate_safe_ratio(
                stage_metrics1["total_shuffle_write_bytes"],
                stage_metrics2["total_shuffle_write_bytes"],
            ),
            "input_ratio": _calculate_safe_ratio(
                stage_metrics1["total_input_bytes"], stage_metrics2["total_input_bytes"]
            ),
            "output_ratio": _calculate_safe_ratio(
                stage_metrics1["total_output_bytes"],
                stage_metrics2["total_output_bytes"],
            ),
            "task_failure_ratio": _calculate_safe_ratio(
                stage_metrics1["total_failed_tasks"],
                stage_metrics2["total_failed_tasks"],
            ),
        }

        # Data processing efficiency analysis
        efficiency_analysis = {}

        # Tasks per stage efficiency
        if stage_metrics1["total_stages"] > 0:
            efficiency_analysis["app1_avg_tasks_per_stage"] = (
                stage_metrics1["total_tasks"] / stage_metrics1["total_stages"]
            )
        if stage_metrics2["total_stages"] > 0:
            efficiency_analysis["app2_avg_tasks_per_stage"] = (
                stage_metrics2["total_tasks"] / stage_metrics2["total_stages"]
            )

        # Data throughput analysis (bytes processed per second)
        if stage_metrics1["total_stage_duration"] > 0:
            efficiency_analysis["app1_input_throughput_bps"] = (
                stage_metrics1["total_input_bytes"]
                / stage_metrics1["total_stage_duration"]
            )
            efficiency_analysis["app1_output_throughput_bps"] = (
                stage_metrics1["total_output_bytes"]
                / stage_metrics1["total_stage_duration"]
            )

        if stage_metrics2["total_stage_duration"] > 0:
            efficiency_analysis["app2_input_throughput_bps"] = (
                stage_metrics2["total_input_bytes"]
                / stage_metrics2["total_stage_duration"]
            )
            efficiency_analysis["app2_output_throughput_bps"] = (
                stage_metrics2["total_output_bytes"]
                / stage_metrics2["total_stage_duration"]
            )

        # Generate stage-specific recommendations
        recommendations = []

        # Stage complexity analysis
        if stage_comparison["stage_count_ratio"] > 1.5:
            recommendations.append(
                {
                    "type": "stage_complexity",
                    "priority": "medium",
                    "issue": f"App2 has {stage_comparison['stage_count_ratio']:.1f}x more stages than App1",
                    "suggestion": "App2 has more complex execution plan - may indicate different algorithm or less optimized query planning",
                }
            )

        # Performance analysis
        if stage_comparison["duration_ratio"] > 1.5:
            recommendations.append(
                {
                    "type": "stage_performance",
                    "priority": "high",
                    "issue": f"App2 stages take {stage_comparison['duration_ratio']:.1f}x longer total time than App1",
                    "suggestion": "App2 experiencing stage-level performance issues - investigate resource allocation or data skew",
                }
            )

        # Memory spill analysis
        if stage_comparison["memory_spill_ratio"] > 2.0:
            recommendations.append(
                {
                    "type": "memory_pressure",
                    "priority": "high",
                    "issue": f"App2 has {stage_comparison['memory_spill_ratio']:.1f}x more memory spill than App1",
                    "suggestion": "App2 experiencing memory pressure - increase executor memory or optimize data structures",
                }
            )

        # Shuffle efficiency analysis
        if (
            stage_comparison["shuffle_read_ratio"] > 2.0
            or stage_comparison["shuffle_write_ratio"] > 2.0
        ):
            recommendations.append(
                {
                    "type": "shuffle_efficiency",
                    "priority": "medium",
                    "issue": f"App2 has significantly more shuffle operations (read: {stage_comparison['shuffle_read_ratio']:.1f}x, write: {stage_comparison['shuffle_write_ratio']:.1f}x)",
                    "suggestion": "App2 may have data skew or inefficient partitioning - consider repartitioning strategies",
                }
            )

        # Task failure analysis
        if stage_comparison["task_failure_ratio"] > 2.0:
            recommendations.append(
                {
                    "type": "reliability",
                    "priority": "high",
                    "issue": f"App2 has {stage_comparison['task_failure_ratio']:.1f}x more task failures than App1",
                    "suggestion": "App2 experiencing reliability issues - investigate infrastructure or data quality problems",
                }
            )

        # Throughput efficiency analysis
        if (
            efficiency_analysis.get("app1_input_throughput_bps", 0) > 0
            and efficiency_analysis.get("app2_input_throughput_bps", 0) > 0
        ):
            throughput_ratio = (
                efficiency_analysis["app2_input_throughput_bps"]
                / efficiency_analysis["app1_input_throughput_bps"]
            )
            if throughput_ratio < 0.5:
                recommendations.append(
                    {
                        "type": "throughput_efficiency",
                        "priority": "medium",
                        "issue": f"App2 has {throughput_ratio:.1f}x lower input processing throughput than App1",
                        "suggestion": "App2's data processing efficiency is lower - check for I/O bottlenecks or resource constraints",
                    }
                )

        # Apply significance filtering to stage comparison ratios
        filtered_stage_comparison = _filter_significant_metrics(
            stage_comparison, significance_threshold, show_only_significant
        )

        # Calculate throughput efficiency ratios for filtering
        efficiency_ratios = {}
        if (
            efficiency_analysis.get("app1_input_throughput_bps", 0) > 0
            and efficiency_analysis.get("app2_input_throughput_bps", 0) > 0
        ):
            efficiency_ratios["input_throughput_ratio"] = (
                efficiency_analysis["app2_input_throughput_bps"]
                / efficiency_analysis["app1_input_throughput_bps"]
            )
        if (
            efficiency_analysis.get("app1_output_throughput_bps", 0) > 0
            and efficiency_analysis.get("app2_output_throughput_bps", 0) > 0
        ):
            efficiency_ratios["output_throughput_ratio"] = (
                efficiency_analysis["app2_output_throughput_bps"]
                / efficiency_analysis["app1_output_throughput_bps"]
            )
        if (
            efficiency_analysis.get("app1_avg_tasks_per_stage", 0) > 0
            and efficiency_analysis.get("app2_avg_tasks_per_stage", 0) > 0
        ):
            efficiency_ratios["tasks_per_stage_ratio"] = (
                efficiency_analysis["app2_avg_tasks_per_stage"]
                / efficiency_analysis["app1_avg_tasks_per_stage"]
            )

        filtered_efficiency_ratios = _filter_significant_metrics(
            efficiency_ratios, significance_threshold, show_only_significant
        )

        result = {
            "applications": {
                "app1": {"id": app_id1, "stage_metrics": stage_metrics1},
                "app2": {"id": app_id2, "stage_metrics": stage_metrics2},
            },
            "stage_comparison": filtered_stage_comparison["metrics"],
            "efficiency_analysis": efficiency_analysis,
            "efficiency_ratios": filtered_efficiency_ratios["metrics"],
            "recommendations": recommendations,
            "filtering_summary": {
                "stage_comparison": {
                    "total_metrics": filtered_stage_comparison["total_metrics"],
                    "significant_metrics": filtered_stage_comparison[
                        "significant_metrics"
                    ],
                    "filtering_applied": filtered_stage_comparison["filtering_applied"],
                },
                "efficiency_ratios": {
                    "total_metrics": filtered_efficiency_ratios["total_metrics"],
                    "significant_metrics": filtered_efficiency_ratios[
                        "significant_metrics"
                    ],
                    "filtering_applied": filtered_efficiency_ratios[
                        "filtering_applied"
                    ],
                },
                "significance_threshold": significance_threshold,
            },
        }

        # Sort the result by ratios (descending)
        return sort_comparison_data(result, sort_key="ratio")

    except Exception as e:
        return {
            "error": f"Failed to compare aggregated stage performance: {str(e)}",
            "applications": {"app1": {"id": app_id1}, "app2": {"id": app_id2}},
        }


# Helper functions that are missing from the refactoring


def _compare_environments(
    client, app_id1: str, app_id2: str, filter_auto_generated: bool = True
) -> Dict[str, Any]:
    """Compare Spark environment configurations between two applications."""
    try:
        env1 = client.get_environment(app_id=app_id1)
        env2 = client.get_environment(app_id=app_id2)

        def props_to_dict(props):
            return {k: v for k, v in props} if props else {}

        spark_props1 = props_to_dict(env1.spark_properties)
        spark_props2 = props_to_dict(env2.spark_properties)

        # Basic comparison - just different properties
        different_props = {
            k: {"app1": v, "app2": spark_props2.get(k, "NOT_SET")}
            for k, v in spark_props1.items()
            if k in spark_props2 and v != spark_props2[k]
        }

        return {
            "spark_properties": {
                "different": different_props,
                "app1_only": {
                    k: v for k, v in spark_props1.items() if k not in spark_props2
                },
                "app2_only": {
                    k: v for k, v in spark_props2.items() if k not in spark_props1
                },
            }
        }
    except Exception as e:
        return {"error": f"Failed to compare environments: {str(e)}"}


def _compare_sql_execution_plans(client, app_id1: str, app_id2: str) -> Dict[str, Any]:
    """Compare SQL execution plans between two Spark applications."""
    try:
        # Simple implementation - just return basic structure
        return {
            "sql_analysis": "basic",
            "sql_recommendations": [],
            "app1": {"query_count": 0},
            "app2": {"query_count": 0},
        }
    except Exception as e:
        return {
            "sql_analysis": "error",
            "error_message": f"Error analyzing SQL execution plans: {str(e)}",
            "sql_recommendations": [],
        }


def _calculate_safe_ratio(val1, val2):
    """Calculate a safe ratio avoiding division by zero."""
    if val2 == 0:
        return float("inf") if val1 > 0 else 1.0
    return val1 / val2


def _filter_significant_metrics(metrics, threshold, show_only_significant=True):
    """Filter metrics based on significance threshold with summary metadata."""

    if not isinstance(metrics, dict):
        return {
            "metrics": metrics,
            "total_metrics": 0,
            "significant_metrics": 0,
            "filtering_applied": False,
        }

    total_metrics = len(metrics)
    if not show_only_significant:
        return {
            "metrics": metrics,
            "total_metrics": total_metrics,
            "significant_metrics": total_metrics,
            "filtering_applied": False,
        }

    filtered = {
        k: v
        for k, v in metrics.items()
        if isinstance(v, (int, float)) and abs(v - 1.0) >= threshold
    }

    return {
        "metrics": filtered,
        "total_metrics": total_metrics,
        "significant_metrics": len(filtered),
        "filtering_applied": len(filtered) != total_metrics,
    }


def sort_comparison_data(data, sort_key="ratio"):
    """Sort comparison data by the specified key."""
    if isinstance(data, dict) and "comparisons" in data:
        comparisons = data["comparisons"]
        if isinstance(comparisons, list):
            try:
                data["comparisons"] = sorted(
                    comparisons,
                    key=lambda x: x.get(sort_key, 0)
                    if isinstance(x.get(sort_key), (int, float))
                    else 0,
                    reverse=True,
                )
            except (TypeError, KeyError):
                pass  # Keep original order if sorting fails
    return data


def _calculate_stage_duration(stage) -> float:
    """Return stage duration in seconds using available fields."""

    submission = getattr(stage, "submission_time", None)
    completion = getattr(stage, "completion_time", None)
    if submission and completion:
        try:
            return max((completion - submission).total_seconds(), 0.0)
        except Exception as exc:
            logger.debug("Failed to compute submission duration", exc_info=exc)
            return 0.0

    executor_run_time = getattr(stage, "executor_run_time", None)
    if executor_run_time is not None:
        try:
            return max(float(executor_run_time) / 1000.0, 0.0)
        except Exception:
            return 0.0

    duration = getattr(stage, "duration", None)
    if duration is not None:
        try:
            return max(float(duration), 0.0)
        except Exception:
            return 0.0

    return 0.0


def _calculate_job_stats(jobs) -> Dict[str, Any]:
    """Aggregate basic statistics for a collection of jobs."""

    count = len(jobs)
    total_duration = 0.0
    completed = 0
    failed = 0

    for job in jobs:
        submission = getattr(job, "submission_time", None)
        completion = getattr(job, "completion_time", None)
        duration = 0.0
        if submission and completion:
            try:
                duration = max((completion - submission).total_seconds(), 0.0)
            except Exception:
                duration = 0.0
        elif hasattr(job, "duration") and job.duration is not None:
            try:
                duration = float(job.duration)
            except Exception:
                duration = 0.0

        total_duration += duration

        status = (getattr(job, "status", "") or "").upper()
        if status in {"SUCCEEDED", "SUCCESS", "COMPLETED", "COMPLETE"}:
            completed += 1
        elif status in {"FAILED", "FAIL", "ERROR"}:
            failed += 1

    avg_duration = total_duration / count if count else 0.0

    return {
        "count": count,
        "completed_count": completed,
        "failed_count": failed,
        "avg_duration": avg_duration,
        "total_duration": total_duration,
    }


def _calculate_aggregated_stage_metrics(stages) -> Dict[str, Any]:
    """Aggregate key metrics across a collection of stages."""

    total_duration = 0.0
    total_executor_run_time = 0.0
    total_memory_spilled = 0.0
    total_shuffle_read = 0.0
    total_shuffle_write = 0.0
    total_input = 0.0
    total_output = 0.0
    total_tasks = 0
    total_failed_tasks = 0
    completed = 0
    failed = 0

    for stage in stages:
        total_duration += _calculate_stage_duration(stage)
        total_executor_run_time += float(getattr(stage, "executor_run_time", 0) or 0)
        total_memory_spilled += float(getattr(stage, "memory_bytes_spilled", 0) or 0)
        total_shuffle_read += float(getattr(stage, "shuffle_read_bytes", 0) or 0)
        total_shuffle_write += float(getattr(stage, "shuffle_write_bytes", 0) or 0)
        total_input += float(getattr(stage, "input_bytes", 0) or 0)
        total_output += float(getattr(stage, "output_bytes", 0) or 0)
        total_tasks += int(getattr(stage, "num_tasks", 0) or 0)
        total_failed_tasks += int(getattr(stage, "num_failed_tasks", 0) or 0)

        status = (getattr(stage, "status", "") or "").upper()
        if status == "COMPLETE" or status == "COMPLETED":
            completed += 1
        elif status == "FAILED":
            failed += 1

    return {
        "total_stages": len(stages),
        "total_stage_duration": total_duration,
        "total_executor_run_time": total_executor_run_time,
        "total_memory_spilled": total_memory_spilled,
        "total_shuffle_read_bytes": total_shuffle_read,
        "total_shuffle_write_bytes": total_shuffle_write,
        "total_input_bytes": total_input,
        "total_output_bytes": total_output,
        "total_tasks": total_tasks,
        "total_failed_tasks": total_failed_tasks,
        "completed_stages": completed,
        "failed_stages": failed,
    }


def _summarize_executor_metrics(executor_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Summarize executor-level metrics for stage comparisons."""

    total_task_time = 0.0
    total_failed_tasks = 0
    total_succeeded_tasks = 0
    total_spill = 0.0
    total_shuffle_read = 0.0
    total_shuffle_write = 0.0

    for executor in (executor_summary or {}).values():
        total_task_time += float(getattr(executor, "task_time", 0) or 0)
        total_failed_tasks += int(getattr(executor, "failed_tasks", 0) or 0)
        total_succeeded_tasks += int(getattr(executor, "succeeded_tasks", 0) or 0)
        total_spill += float(getattr(executor, "memory_bytes_spilled", 0) or 0)
        total_shuffle_read += float(getattr(executor, "shuffle_read", 0) or 0)
        total_shuffle_write += float(getattr(executor, "shuffle_write", 0) or 0)

    return {
        "executor_count": len(executor_summary or {}),
        "total_task_time": total_task_time,
        "failed_tasks": total_failed_tasks,
        "succeeded_tasks": total_succeeded_tasks,
        "memory_spilled_bytes": total_spill,
        "shuffle_read_bytes": total_shuffle_read,
        "shuffle_write_bytes": total_shuffle_write,
    }


def _build_executor_analysis(stage1, stage2) -> Dict[str, Any]:
    """Build executor comparison analysis for a pair of stages."""

    metrics1 = _summarize_executor_metrics(getattr(stage1, "executor_summary", None))
    metrics2 = _summarize_executor_metrics(getattr(stage2, "executor_summary", None))

    comparative = {
        "task_time_ratio": _calculate_safe_ratio(
            metrics1["total_task_time"], metrics2["total_task_time"]
        ),
        "failed_task_ratio": _calculate_safe_ratio(
            metrics1["failed_tasks"], metrics2["failed_tasks"]
        ),
        "memory_spill_ratio": _calculate_safe_ratio(
            metrics1["memory_spilled_bytes"], metrics2["memory_spilled_bytes"]
        ),
    }

    insights = []
    if metrics1["executor_count"] and metrics2["executor_count"]:
        if metrics2["total_task_time"] > metrics1["total_task_time"]:
            insights.append("app2 executors spent more time per stage compared to app1")
        elif metrics1["total_task_time"] > metrics2["total_task_time"]:
            insights.append("app1 executors spent more time per stage compared to app2")

    recommendations = []
    if metrics2["memory_spilled_bytes"] > metrics1["memory_spilled_bytes"] * 2:
        recommendations.append(
            {
                "type": "executor_memory",
                "priority": "medium",
                "issue": "App2 stage executors spill significantly more memory",
                "suggestion": "Consider increasing executor memory or optimizing data skew",
            }
        )

    return {
        "app1_executor_metrics": metrics1,
        "app2_executor_metrics": metrics2,
        "comparative_analysis": comparative,
        "insights": insights,
        "recommendations": recommendations,
    }


# Helper functions for compare_app_performance refactoring
def _create_app_fetch_error_response(
    app_id1: str,
    app_id2: str,
    failed_app_id: str,
    error_msg: str,
    top_n: int,
    similarity_threshold: float,
    app1_name: str = "Unknown",
) -> Dict[str, Any]:
    """Create standardized error response for application fetch failures."""
    return {
        "schema_version": 1,
        "applications": {
            "app1": {
                "id": app_id1,
                "name": app1_name if failed_app_id != app_id1 else "Unknown",
                "error": f"Failed to fetch: {error_msg}"
                if failed_app_id == app_id1
                else None,
            },
            "app2": {
                "id": app_id2,
                "name": "Unknown",
                "error": f"Failed to fetch: {error_msg}"
                if failed_app_id == app_id2
                else None,
            },
        },
        "aggregated_overview": {
            "error": f"Failed to fetch application {failed_app_id}: {error_msg}"
        },
        "stage_deep_dive": {
            "error": f"Failed to fetch application {failed_app_id}: {error_msg}",
            "applications": {
                "app1": {
                    "id": app_id1,
                    "name": app1_name if failed_app_id != app_id1 else "Unknown",
                },
                "app2": {"id": app_id2, "name": "Unknown"},
            },
            "top_stage_differences": [],
            "analysis_parameters": {
                "requested_top_n": top_n,
                "similarity_threshold": similarity_threshold,
                "available_stages_app1": 0,
                "available_stages_app2": 0,
                "matched_stages": 0,
            },
            "stage_summary": {
                "matched_stages": 0,
                "total_time_difference_seconds": 0.0,
                "average_time_difference_seconds": 0.0,
                "max_time_difference_seconds": 0.0,
            },
        },
        "error": f"Failed to fetch application {failed_app_id}: {error_msg}",
        "recommendations": [],
        "key_recommendations": [],
    }


def _fetch_applications_safely(
    app_id1: str,
    app_id2: str,
    server: Optional[str],
    top_n: int,
    similarity_threshold: float,
) -> tuple[Any, Any, Optional[Dict[str, Any]]]:
    """
    Safely fetch both applications with error handling.

    Returns:
        tuple: (app1, app2, error_response) where error_response is None on success
    """
    # Fetch first application
    try:
        app1 = fetcher_tools.fetch_app(app_id1, server)
    except Exception as e:
        error_response = _create_app_fetch_error_response(
            app_id1, app_id2, app_id1, str(e), top_n, similarity_threshold
        )
        return None, None, error_response

    # Fetch second application
    try:
        app2 = fetcher_tools.fetch_app(app_id2, server)
    except Exception as e:
        error_response = _create_app_fetch_error_response(
            app_id1,
            app_id2,
            app_id2,
            str(e),
            top_n,
            similarity_threshold,
            app1_name=getattr(app1, "name", "Unknown"),
        )
        return app1, None, error_response

    return app1, app2, None


def _build_aggregated_overview(
    app_id1: str, app_id2: str, server: Optional[str], significance_threshold: float
) -> Dict[str, Any]:
    """Build the aggregated application overview section."""
    # PHASE 1: AGGREGATED APPLICATION OVERVIEW
    # Use specialized comparison tools for aggregated overview with hardcoded defaults
    try:
        executor_comparison = compare_app_executors(
            app_id1,
            app_id2,
            server,
            significance_threshold=significance_threshold,
            show_only_significant=True,
        )
    except Exception as e:
        executor_comparison = {"error": f"Failed to get executor comparison: {str(e)}"}

    try:
        stage_comparison = compare_app_stages_aggregated(
            app_id1,
            app_id2,
            server,
            significance_threshold=significance_threshold,
            show_only_significant=True,
        )
    except Exception as e:
        stage_comparison = {"error": f"Failed to get stage comparison: {str(e)}"}

    # Create streamlined aggregated overview using specialized tools
    return {
        "application_summary": {},
        "job_performance": {},
        "stage_metrics": stage_comparison,
        "executor_performance": executor_comparison,
    }


def _analyze_stage_deep_dive(
    app_id1: str,
    app_id2: str,
    server: Optional[str],
    top_n: int,
    similarity_threshold: float,
    app1: Any,
    app2: Any,
) -> Dict[str, Any]:
    """Perform stage-level deep dive analysis."""
    # PHASE 2: STAGE-LEVEL DEEP DIVE ANALYSIS
    # Use the new find_top_stage_differences tool for stage analysis
    try:
        stage_analysis = find_top_stage_differences(
            app_id1, app_id2, server, top_n, similarity_threshold=similarity_threshold
        )
    except Exception as e:
        stage_analysis = {
            "error": f"Failed to analyze stage differences: {str(e)}",
            "applications": {
                "app1": {"id": app_id1, "name": getattr(app1, "name", "Unknown")},
                "app2": {"id": app_id2, "name": getattr(app2, "name", "Unknown")},
            },
            "top_stage_differences": [],
            "analysis_parameters": {
                "requested_top_n": top_n,
                "similarity_threshold": 0.6,
                "available_stages_app1": 0,
                "available_stages_app2": 0,
                "matched_stages": 0,
            },
            "stage_summary": {
                "matched_stages": 0,
                "total_time_difference_seconds": 0.0,
                "average_time_difference_seconds": 0.0,
                "max_time_difference_seconds": 0.0,
            },
        }

    return stage_analysis
