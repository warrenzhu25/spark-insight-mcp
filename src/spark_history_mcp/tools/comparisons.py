"""
Application comparison tools for MCP server.

This module contains tools for comparing performance metrics, resource allocation,
and configurations between different Spark applications.
"""

from typing import Any, Dict, Optional

from spark_history_mcp.core.app import mcp


def get_client_or_default(ctx, server_name: Optional[str] = None):
    """
    Get a client by server name or the default client if no name is provided.

    Args:
        ctx: The MCP context
        server_name: Optional server name

    Returns:
        SparkRestClient: The requested client or default client

    Raises:
        ValueError: If no client is found
    """
    clients = ctx.request_context.lifespan_context.clients
    default_client = ctx.request_context.lifespan_context.default_client

    if server_name:
        client = clients.get(server_name)
        if client:
            return client

    if default_client:
        return default_client

    raise ValueError(
        "No Spark client found. Please specify a valid server name or set a default server."
    )


@mcp.tool()
def compare_app_performance(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None,
    top_n: int = 3,
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
    # Import comparison tools from other modules to avoid circular imports
    from spark_history_mcp.tools.application import get_application
    from spark_history_mcp.tools.executors import get_executor_summary

    # Get basic application info
    app1 = get_application(app_id1, server)
    app2 = get_application(app_id2, server)

    # Get executor summaries for comparison
    exec_summary1 = get_executor_summary(app_id1, server)
    exec_summary2 = get_executor_summary(app_id2, server)

    # Basic application comparison
    applications = {
        "app1": {
            "id": app_id1,
            "name": app1.name,
            "user": app1.user,
            "start_time": app1.start_time,
            "end_time": app1.end_time,
            "duration": app1.duration,
        },
        "app2": {
            "id": app_id2,
            "name": app2.name,
            "user": app2.user,
            "start_time": app2.start_time,
            "end_time": app2.end_time,
            "duration": app2.duration,
        },
    }

    # Executor efficiency comparison
    executor_comparison = _compare_executor_metrics(exec_summary1, exec_summary2)

    # Performance comparison structure
    performance_comparison = {
        "executors": executor_comparison,
        "stages": {
            "note": "Stage-level comparison requires implementing stage matching logic",
            "top_stage_differences": [],
        },
    }

    # Application summary differences
    app_summary_diff = _calculate_app_summary_differences(app1, app2, exec_summary1, exec_summary2)

    # Environment comparison (simplified)
    environment_comparison = {
        "note": "Environment comparison requires implementing configuration analysis",
        "configuration_differences": {},
    }

    # Generate recommendations
    recommendations = _generate_performance_recommendations(
        app1, app2, exec_summary1, exec_summary2, executor_comparison
    )

    return {
        "applications": applications,
        "performance_comparison": performance_comparison,
        "app_summary_diff": app_summary_diff,
        "environment_comparison": environment_comparison,
        "key_recommendations": recommendations,
        "metadata": {
            "comparison_timestamp": "2025-09-28T09:00:00Z",
            "analysis_version": "refactored_tools_v1",
        },
    }


def _compare_executor_metrics(exec1: Dict[str, Any], exec2: Dict[str, Any]) -> Dict[str, Any]:
    """Compare executor metrics between two applications."""
    def safe_ratio(val1, val2):
        return val2 / val1 if val1 > 0 else float('inf') if val2 > 0 else 1.0

    return {
        "total_executors": {
            "app1": exec1["total_executors"],
            "app2": exec2["total_executors"],
            "ratio": safe_ratio(exec1["total_executors"], exec2["total_executors"]),
        },
        "completed_tasks": {
            "app1": exec1["completed_tasks"],
            "app2": exec2["completed_tasks"],
            "ratio": safe_ratio(exec1["completed_tasks"], exec2["completed_tasks"]),
        },
        "failed_tasks": {
            "app1": exec1["failed_tasks"],
            "app2": exec2["failed_tasks"],
            "ratio": safe_ratio(exec1["failed_tasks"], exec2["failed_tasks"]),
        },
        "total_gc_time": {
            "app1": exec1["total_gc_time"],
            "app2": exec2["total_gc_time"],
            "ratio": safe_ratio(exec1["total_gc_time"], exec2["total_gc_time"]),
        },
        "memory_used": {
            "app1": exec1["memory_used"],
            "app2": exec2["memory_used"],
            "ratio": safe_ratio(exec1["memory_used"], exec2["memory_used"]),
        },
    }


def _calculate_app_summary_differences(app1, app2, exec1, exec2) -> Dict[str, Any]:
    """Calculate high-level application summary differences."""
    def pct_change(old_val, new_val):
        if old_val == 0:
            return float('inf') if new_val > 0 else 0
        return ((new_val - old_val) / old_val) * 100

    duration1 = app1.duration.total_seconds() if app1.duration else 0
    duration2 = app2.duration.total_seconds() if app2.duration else 0

    return {
        "duration_seconds": {
            "app1": duration1,
            "app2": duration2,
            "pct_change": pct_change(duration1, duration2),
        },
        "total_executors": {
            "app1": exec1["total_executors"],
            "app2": exec2["total_executors"],
            "pct_change": pct_change(exec1["total_executors"], exec2["total_executors"]),
        },
        "completed_tasks": {
            "app1": exec1["completed_tasks"],
            "app2": exec2["completed_tasks"],
            "pct_change": pct_change(exec1["completed_tasks"], exec2["completed_tasks"]),
        },
        "memory_used": {
            "app1": exec1["memory_used"],
            "app2": exec2["memory_used"],
            "pct_change": pct_change(exec1["memory_used"], exec2["memory_used"]),
        },
    }


def _generate_performance_recommendations(app1, app2, exec1, exec2, executor_comparison) -> list:
    """Generate performance improvement recommendations."""
    recommendations = []

    # Duration-based recommendations
    duration1 = app1.duration.total_seconds() if app1.duration else 0
    duration2 = app2.duration.total_seconds() if app2.duration else 0

    if duration2 > duration1 * 1.5:
        recommendations.append({
            "priority": "high",
            "category": "performance",
            "message": f"App2 took {(duration2 - duration1) / 60:.1f} minutes longer than App1. "
                      "Consider optimizing resource allocation or data processing logic."
        })

    # Executor efficiency recommendations
    if executor_comparison["failed_tasks"]["ratio"] > 2.0:
        recommendations.append({
            "priority": "critical",
            "category": "reliability",
            "message": f"App2 has {executor_comparison['failed_tasks']['ratio']:.1f}x more failed tasks. "
                      "Investigate executor stability and resource configuration."
        })

    # GC recommendations
    if executor_comparison["total_gc_time"]["ratio"] > 1.5:
        recommendations.append({
            "priority": "medium",
            "category": "memory",
            "message": f"App2 has {executor_comparison['total_gc_time']['ratio']:.1f}x more GC time. "
                      "Consider increasing executor memory or optimizing memory usage."
        })

    # Resource utilization recommendations
    if executor_comparison["total_executors"]["ratio"] > 2.0:
        recommendations.append({
            "priority": "medium",
            "category": "scaling",
            "message": f"App2 used {executor_comparison['total_executors']['ratio']:.1f}x more executors. "
                      "Evaluate if the increased resources provided proportional performance benefits."
        })

    return recommendations[:5]  # Return top 5 recommendations


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
    # Import here to avoid circular imports
    from spark_history_mcp.tools.application import get_app_summary

    app1_summary = get_app_summary(app_id1, server)
    app2_summary = get_app_summary(app_id2, server)

    # Calculate differences
    diff = {}
    for key in app1_summary.keys():
        if key in app2_summary and isinstance(app1_summary[key], (int, float)):
            old_val = app1_summary[key]
            new_val = app2_summary[key]

            if old_val != 0:
                pct_change = ((new_val - old_val) / old_val) * 100
                if abs(pct_change) >= significance_threshold * 100:  # Convert threshold to percentage
                    diff[key] = {
                        "app1": old_val,
                        "app2": new_val,
                        "pct_change": pct_change,
                    }

    return {
        "app1_summary": app1_summary,
        "app2_summary": app2_summary,
        "diff": diff,
        "metadata": {
            "significance_threshold": significance_threshold,
            "total_metrics_compared": len(app1_summary),
            "significant_differences": len(diff),
        },
    }


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
    # Import here to avoid circular imports
    from spark_history_mcp.tools.application import get_application
    from spark_history_mcp.tools.jobs_stages import list_stages

    app1 = get_application(app_id1, server)
    app2 = get_application(app_id2, server)

    stages1 = list_stages(app_id1, server)
    stages2 = list_stages(app_id2, server)

    # Simple stage matching by name similarity (simplified implementation)
    stage_differences = []

    for stage1 in stages1:
        if not stage1.completion_time or not stage1.submission_time:
            continue

        duration1 = (stage1.completion_time - stage1.submission_time).total_seconds()

        # Find best matching stage in app2
        best_match = None
        best_similarity = 0

        for stage2 in stages2:
            if not stage2.completion_time or not stage2.submission_time:
                continue

            # Simple name similarity check
            name1 = stage1.name or f"Stage {stage1.stage_id}"
            name2 = stage2.name or f"Stage {stage2.stage_id}"

            # Basic similarity: check if names contain similar keywords
            similarity = _calculate_name_similarity(name1, name2)

            if similarity > best_similarity and similarity >= similarity_threshold:
                best_similarity = similarity
                best_match = stage2

        if best_match:
            duration2 = (best_match.completion_time - best_match.submission_time).total_seconds()
            time_diff = abs(duration2 - duration1)

            stage_differences.append({
                "stage1": {
                    "stage_id": stage1.stage_id,
                    "name": stage1.name,
                    "duration_seconds": duration1,
                    "num_tasks": stage1.num_tasks,
                },
                "stage2": {
                    "stage_id": best_match.stage_id,
                    "name": best_match.name,
                    "duration_seconds": duration2,
                    "num_tasks": best_match.num_tasks,
                },
                "time_difference_seconds": time_diff,
                "similarity_score": best_similarity,
            })

    # Sort by time difference and return top N
    stage_differences.sort(key=lambda x: x["time_difference_seconds"], reverse=True)

    return {
        "applications": {
            "app1": {"id": app_id1, "name": app1.name},
            "app2": {"id": app_id2, "name": app2.name},
        },
        "top_stage_differences": stage_differences[:top_n],
        "metadata": {
            "total_stages_app1": len(stages1),
            "total_stages_app2": len(stages2),
            "matched_stages": len(stage_differences),
            "similarity_threshold": similarity_threshold,
        },
    }


def _calculate_name_similarity(name1: str, name2: str) -> float:
    """Calculate similarity between two stage names."""
    if not name1 or not name2:
        return 0.0

    # Simple similarity based on common words
    words1 = set(name1.lower().split())
    words2 = set(name2.lower().split())

    if not words1 or not words2:
        return 0.0

    common_words = words1.intersection(words2)
    total_words = words1.union(words2)

    return len(common_words) / len(total_words) if total_words else 0.0