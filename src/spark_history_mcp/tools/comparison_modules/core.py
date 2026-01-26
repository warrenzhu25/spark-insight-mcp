"""
Core comparison tools for MCP server.

This module contains the main application comparison functions including
comprehensive performance comparison and summary comparison.
"""

from typing import Any, Dict, Optional

from ...core.app import mcp
from .. import fetchers as fetcher_tools
from ..application import get_app_summary as _get_app_summary_impl
from ..common import get_config, resolve_legacy_tool
from ..recommendations import (
    apply_rules as apply_rec_rules,
)
from ..recommendations import (
    dedupe as dedupe_recs,
)
from ..recommendations import (
    default_rules as default_rec_rules,
)
from ..recommendations import (
    prioritize as prioritize_recs,
)
from ..schema import CompareAppPerformanceOutput, validate_output
from .utils import (
    resolve_client,
    sort_comparison_data,
)


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
    resolve_client(server)

    # Get application info via fetchers to populate caches used downstream
    try:
        app1 = fetcher_tools.fetch_app(app_id1, server)
    except Exception as e:
        return _create_error_response(
            app_id1, app_id2, str(e), "app1", top_n, similarity_threshold
        )

    try:
        app2 = fetcher_tools.fetch_app(app_id2, server)
    except Exception as e:
        return _create_error_response(
            app_id1,
            app_id2,
            str(e),
            "app2",
            top_n,
            similarity_threshold,
            app1_name=getattr(app1, "name", "Unknown"),
        )

    # Import specialized comparison tools to avoid circular imports
    from .environment import compare_app_stages_aggregated
    from .executors import compare_app_executors
    from .stages import find_top_stage_differences

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
    aggregated_overview = {
        "application_summary": {},
        "job_performance": {},
        "stage_metrics": stage_comparison,
        "executor_performance": executor_comparison,
    }

    # PHASE 2: STAGE-LEVEL DEEP DIVE ANALYSIS
    # Use the new find_top_stage_differences tool for stage analysis
    try:
        stage_analysis = find_top_stage_differences(
            app_id1, app_id2, server, top_n, similarity_threshold=similarity_threshold
        )
    except Exception as e:
        stage_analysis = _create_stage_analysis_error(
            app1, app2, app_id1, app_id2, str(e), top_n
        )

    # Generate basic recommendations
    basic_recommendations = _generate_basic_recommendations(app1, app2)

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
    recommendations = basic_recommendations[:]

    # Apply default rule set (resource allocation, large stage diffs)
    rule_ctx = {
        "app1": app1,
        "app2": app2,
        "detailed_comparisons": detailed_comparisons,
    }
    recommendations.extend(apply_rec_rules(rule_ctx, default_rec_rules()))

    # Extract recommendations from specialized comparison tools
    if (
        aggregated_overview["executor_performance"]
        and isinstance(aggregated_overview["executor_performance"], dict)
        and "recommendations" in aggregated_overview["executor_performance"]
    ):
        recommendations.extend(
            aggregated_overview["executor_performance"]["recommendations"]
        )

    if (
        aggregated_overview["stage_metrics"]
        and isinstance(aggregated_overview["stage_metrics"], dict)
        and "recommendations" in aggregated_overview["stage_metrics"]
    ):
        recommendations.extend(aggregated_overview["stage_metrics"]["recommendations"])

    # Remove duplicates and prioritize
    unique_recommendations = dedupe_recs(recommendations)
    sorted_recommendations = prioritize_recs(unique_recommendations)

    # Filter for key recommendations (top 5, medium priority or higher)
    priority_weights = {"critical": 5, "high": 4, "medium": 3, "low": 2, "info": 1}
    filtered_recommendations = [
        rec
        for rec in sorted_recommendations[:5]
        if priority_weights.get(rec.get("priority", "low"), 1) >= 3
    ]

    # APP SUMMARY COMPARISON
    try:
        app_summary_diff = compare_app_summaries(
            app_id1, app_id2, server, significance_threshold
        )
    except Exception as e:
        app_summary_diff = {"error": f"Failed to get app summary comparison: {str(e)}"}

    # ENVIRONMENT COMPARISON
    try:
        from .utils import _compare_environments

        app1_env = fetcher_tools.fetch_env(app_id1, server)
        app2_env = fetcher_tools.fetch_env(app_id2, server)
        environment_comparison = _compare_environments(
            app1_env, app2_env, filter_auto_generated=True
        )
    except Exception as e:
        environment_comparison = {"error": f"Failed to compare environments: {str(e)}"}

    result = {
        "schema_version": 1,
        "applications": {
            "app1": {"id": app_id1, "name": app1.name},
            "app2": {"id": app_id2, "name": app2.name},
        },
        "aggregated_overview": aggregated_overview,
        "stage_deep_dive": stage_analysis,
        "app_summary_diff": app_summary_diff,
        "environment_comparison": environment_comparison,
        "recommendations": sorted_recommendations,
        "key_recommendations": filtered_recommendations,
    }

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
    # Validate server parameter (client resolution handled by tool calls)
    _ = server

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

    # Calculate percentage changes
    diff = {}
    filtered_app1_metrics = {}
    filtered_app2_metrics = {}

    for metric_name in app1_metrics:
        if metric_name in app2_metrics and metric_name != "application_id":
            change_str = _calculate_percentage_change(
                app1_metrics[metric_name], app2_metrics[metric_name]
            )
            change_value = _extract_percentage_value(change_str)

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

    return sort_comparison_data(result, sort_key="change")


def _create_error_response(
    app_id1: str,
    app_id2: str,
    error_msg: str,
    failed_app: str,
    top_n: int,
    similarity_threshold: float,
    app1_name: str = "Unknown",
) -> Dict[str, Any]:
    """Create standardized error response for app comparison failures."""
    apps_info = {
        "app1": {"id": app_id1, "name": app1_name},
        "app2": {"id": app_id2, "name": "Unknown"},
    }

    if failed_app == "app1":
        apps_info["app1"]["error"] = f"Failed to fetch: {error_msg}"
    else:
        apps_info["app2"]["error"] = f"Failed to fetch: {error_msg}"

    return {
        "schema_version": 1,
        "applications": apps_info,
        "aggregated_overview": {
            "error": f"Failed to fetch application {app_id1 if failed_app == 'app1' else app_id2}: {error_msg}"
        },
        "stage_deep_dive": {
            "error": f"Failed to fetch application {app_id1 if failed_app == 'app1' else app_id2}: {error_msg}",
            "applications": apps_info,
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
        "error": error_msg,
        "recommendations": [],
        "key_recommendations": [],
    }


def _create_stage_analysis_error(
    app1, app2, app_id1: str, app_id2: str, error_msg: str, top_n: int
) -> Dict[str, Any]:
    """Create error response for stage analysis failures."""
    return {
        "error": f"Failed to analyze stage differences: {error_msg}",
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


def _generate_basic_recommendations(app1, app2) -> list:
    """Generate basic resource allocation recommendations."""
    basic_recommendations = []

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

    return basic_recommendations


def _calculate_percentage_change(val1: float, val2: float) -> str:
    """Calculate percentage change from val1 to val2."""
    if val1 == 0:
        return "N/A" if val2 == 0 else "+∞"
    change = ((val2 - val1) / val1) * 100
    return f"{change:+.1f}%"


def _extract_percentage_value(change_str: str) -> float:
    """Extract numeric percentage value from change string."""
    if change_str in ["N/A", "+∞", "-∞"]:
        return 0.0
    try:
        return abs(float(change_str.replace("+", "").replace("%", "")))
    except (ValueError, AttributeError):
        return 0.0
