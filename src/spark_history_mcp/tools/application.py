"""
Application-level Spark tools for MCP server.

This module contains tools for retrieving and analyzing Spark application
metadata, environment configuration, and high-level application insights.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from ..core.app import mcp
from ..models.spark_types import ApplicationInfo, StageStatus
from .analysis import (
    analyze_auto_scaling,
    analyze_failed_tasks,
    analyze_shuffle_skew,
)
from .common import get_client_or_default
from .fetchers import fetch_app


@mcp.tool()
def get_application(app_id: str, server: Optional[str] = None) -> ApplicationInfo:
    """
    Get detailed information about a specific Spark application.

    Retrieves comprehensive information about a Spark application including its
    status, resource usage, duration, and attempt details.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        ApplicationInfo object containing application details
    """
    # Use shared fetcher to avoid duplicating client resolution and enable caching
    return fetch_app(app_id=app_id, server=server)


@mcp.tool()
def list_applications(
    server: Optional[str] = None,
    status: Optional[list[str]] = None,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    min_end_date: Optional[str] = None,
    max_end_date: Optional[str] = None,
    limit: Optional[int] = None,
    app_name: Optional[str] = None,
    search_type: str = "contains",
) -> list:
    """
    Get a list of all Spark applications with optional filtering.

    Retrieves applications from the Spark History Server with support for
    filtering by status, date ranges, name patterns, and result limits.

    Args:
        server: Optional server name to use (uses default if not specified)
        status: Optional list of application status values to filter by (e.g., ["COMPLETED", "RUNNING"])
        min_date: Minimum start date (yyyy-MM-dd'T'HH:mm:ss.SSSz or yyyy-MM-dd)
        max_date: Maximum start date
        min_end_date: Minimum end date
        max_end_date: Maximum end date
        limit: Maximum number of applications to return
        app_name: Optional application name or pattern to filter by
        search_type: Type of name search - "exact", "contains", or "regex" (default: "contains")

    Returns:
        List of ApplicationInfo objects for matching applications

    Raises:
        ValueError: If search_type is not one of "exact", "contains", or "regex"
        re.error: If search_type is "regex" and app_name is not a valid regex pattern
    """
    import re

    valid_search_types = ["exact", "contains", "regex"]
    if search_type not in valid_search_types:
        raise ValueError(
            f"search_type must be one of {valid_search_types}, got: {search_type}"
        )

    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get applications from the server with existing filters
    applications = client.list_applications(
        status=status,
        min_date=min_date,
        max_date=max_date,
        min_end_date=min_end_date,
        max_end_date=max_end_date,
        limit=limit,
    )

    # If no name filtering is requested, return as-is
    if not app_name:
        return applications

    # Filter applications by name based on search type
    matching_apps = []

    for app in applications:
        app_display_name = app.name if app.name else ""

        try:
            if search_type == "exact":
                if app_display_name == app_name:
                    matching_apps.append(app)
            elif search_type == "contains":
                if app_name.lower() in app_display_name.lower():
                    matching_apps.append(app)
            elif search_type == "regex":
                if re.search(app_name, app_display_name, re.IGNORECASE):
                    matching_apps.append(app)
        except re.error as e:
            # Re-raise regex errors with more context
            raise re.error(f"Invalid regex pattern '{app_name}': {str(e)}") from e

    return matching_apps


@mcp.tool()
def get_environment(app_id: str, server: Optional[str] = None):
    """
    Get the comprehensive Spark runtime configuration for a Spark application.

    Details including JVM information, Spark properties, system properties,
    classpath entries, and environment variables.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        ApplicationEnvironmentInfo object containing environment details
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    return client.get_environment(app_id=app_id)


@mcp.tool()
def get_application_insights(
    app_id: str,
    server: Optional[str] = None,
    include_auto_scaling: bool = True,
    include_shuffle_skew: bool = True,
    include_failed_tasks: bool = True,
) -> Dict[str, Any]:
    """
    Get comprehensive SparkInsight-style analysis for an application.

    Runs multiple analysis tools to provide a complete performance and
    optimization overview of a Spark application, similar to SparkInsight's
    comprehensive analysis approach.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        include_auto_scaling: Whether to include auto-scaling analysis (default: True)
        include_shuffle_skew: Whether to include shuffle skew analysis (default: True)
        include_failed_tasks: Whether to include failed task analysis (default: True)

    Returns:
        Dictionary containing comprehensive application insights
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get basic application info
    app = client.get_application(app_id)

    insights = {
        "application_id": app_id,
        "application_name": app.name,
        "analysis_timestamp": datetime.now().isoformat(),
        "analysis_type": "Comprehensive SparkInsight Analysis",
        "analyses": {},
    }

    # Run requested analyses
    if include_auto_scaling:
        try:
            insights["analyses"]["auto_scaling"] = analyze_auto_scaling(app_id, server)
        except Exception as e:
            insights["analyses"]["auto_scaling"] = {"error": str(e)}

    if include_shuffle_skew:
        try:
            insights["analyses"]["shuffle_skew"] = analyze_shuffle_skew(app_id, server)
        except Exception as e:
            insights["analyses"]["shuffle_skew"] = {"error": str(e)}

    if include_failed_tasks:
        try:
            insights["analyses"]["failed_tasks"] = analyze_failed_tasks(app_id, server)
        except Exception as e:
            insights["analyses"]["failed_tasks"] = {"error": str(e)}

    # Aggregate recommendations from all analyses
    all_recommendations = []
    critical_issues = []

    for analysis_name, analysis_result in insights["analyses"].items():
        if "recommendations" in analysis_result and analysis_result["recommendations"]:
            recommendations = analysis_result["recommendations"]

            # Handle both list and dict recommendation formats defensively
            if isinstance(recommendations, list):
                # Standard format: list of recommendation dictionaries
                for rec in recommendations:
                    if isinstance(rec, dict):
                        # Create a copy to avoid modifying the original
                        rec_copy = rec.copy()
                        rec_copy["source_analysis"] = analysis_name
                        all_recommendations.append(rec_copy)
                        if rec_copy.get("priority") == "critical":
                            critical_issues.append(rec_copy)
            elif isinstance(recommendations, dict):
                # Legacy format: nested dictionary (convert to list format)
                for key, value in recommendations.items():
                    if isinstance(value, dict):
                        rec_copy = value.copy()
                        rec_copy["source_analysis"] = analysis_name
                        rec_copy["recommendation_type"] = key
                        rec_copy.setdefault("type", "configuration")
                        rec_copy.setdefault("priority", "medium")
                        rec_copy.setdefault(
                            "issue", f"Configuration optimization for {key}"
                        )
                        rec_copy.setdefault(
                            "suggestion",
                            f"Update {key} configuration based on analysis",
                        )
                        all_recommendations.append(rec_copy)
                        if rec_copy.get("priority") == "critical":
                            critical_issues.append(rec_copy)
            else:
                # Handle unexpected formats gracefully
                # Note: Unexpected recommendations format will be skipped gracefully
                pass

    # Sort recommendations by priority
    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    all_recommendations.sort(
        key=lambda x: priority_order.get(x.get("priority", "low"), 3)
    )

    insights["summary"] = {
        "total_analyses_run": len(
            [a for a in insights["analyses"].values() if "error" not in a]
        ),
        "total_recommendations": len(all_recommendations),
        "critical_issues": len(critical_issues),
        "high_priority_recommendations": len(
            [r for r in all_recommendations if r.get("priority") == "high"]
        ),
        "overall_health": "critical"
        if critical_issues
        else "good"
        if len(all_recommendations) < 3
        else "needs_attention",
    }

    insights["recommendations"] = all_recommendations

    return insights


@mcp.tool()
def get_app_summary(app_id: str, server: Optional[str] = None) -> Dict[str, Any]:
    """
    Get a comprehensive application performance summary with key metrics.

    Provides a focused summary of Spark application performance including timing,
    resource utilization, data processing, and efficiency metrics. Based on the
    sparkInsight AppSummaryAnalyzer but simplified for clarity.

    Args:
        app_id: The Spark application ID to analyze
        server: Optional server name (uses default if not specified)

    Returns:
        Dictionary containing application performance summary
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    try:
        # Get application data
        app = client.get_application(app_id)
        stages = client.list_stages(app_id=app_id, with_summaries=True)
        executors = client.list_all_executors(app_id=app_id)

        if not app.attempts:
            return {"error": "No application attempts found", "application_id": app_id}

        attempt = app.attempts[-1]  # Latest attempt

        # Calculate timing metrics
        total_runtime_ms = attempt.duration or 0
        total_runtime_min = total_runtime_ms / (1000 * 60)

        # Calculate executor metrics
        total_executor_runtime_ms = sum(
            stage.executor_run_time or 0 for stage in stages
        )
        total_executor_runtime_min = total_executor_runtime_ms / (1000 * 60)

        total_executor_cpu_time_ns = sum(
            stage.executor_cpu_time or 0 for stage in stages
        )
        total_executor_cpu_time_min = total_executor_cpu_time_ns / (
            1000 * 1000 * 1000 * 60
        )  # Convert from nanoseconds to minutes

        total_gc_time_ms = sum(
            getattr(stage, "jvm_gc_time", 0) or 0 for stage in stages
        )
        total_gc_time_min = total_gc_time_ms / (1000 * 60)

        # Calculate executor utilization
        if attempt.end_time and attempt.start_time:
            app_end_time_ms = attempt.end_time.timestamp() * 1000
        else:
            app_end_time_ms = None

        total_executor_time_ms = 0
        executor_cores = app.cores_per_executor or 1

        for executor in executors:
            if executor.add_time:
                add_time_ms = executor.add_time.timestamp() * 1000
                if executor.remove_time:
                    remove_time_ms = executor.remove_time.timestamp() * 1000
                elif app_end_time_ms:
                    remove_time_ms = app_end_time_ms
                else:
                    continue
                total_executor_time_ms += remove_time_ms - add_time_ms

        executor_utilization = 0.0
        if total_executor_time_ms > 0:
            executor_utilization = (
                total_executor_runtime_ms / (total_executor_time_ms * executor_cores)
            ) * 100

        # Calculate data processing metrics
        total_input_bytes = sum(
            getattr(stage, "input_bytes", 0) or 0 for stage in stages
        )
        total_input_gb = total_input_bytes / (1024 * 1024 * 1024)

        total_output_bytes = sum(
            getattr(stage, "output_bytes", 0) or 0 for stage in stages
        )
        total_output_gb = total_output_bytes / (1024 * 1024 * 1024)

        total_shuffle_read_bytes = sum(
            getattr(stage, "shuffle_read_bytes", 0) or 0 for stage in stages
        )
        total_shuffle_read_gb = total_shuffle_read_bytes / (1024 * 1024 * 1024)

        total_shuffle_write_bytes = sum(
            getattr(stage, "shuffle_write_bytes", 0) or 0 for stage in stages
        )
        total_shuffle_write_gb = total_shuffle_write_bytes / (1024 * 1024 * 1024)

        total_memory_spilled_bytes = sum(
            getattr(stage, "memory_bytes_spilled", 0) or 0 for stage in stages
        )
        total_memory_spilled_gb = total_memory_spilled_bytes / (1024 * 1024 * 1024)

        total_disk_spilled_bytes = sum(
            getattr(stage, "disk_bytes_spilled", 0) or 0 for stage in stages
        )
        total_disk_spilled_gb = total_disk_spilled_bytes / (1024 * 1024 * 1024)

        # Calculate performance metrics (values are in nanoseconds from Spark)
        total_shuffle_fetch_wait_time_ns = 0
        total_shuffle_write_time_ns = 0
        total_failed_tasks = sum(
            getattr(stage, "num_failed_tasks", 0) or 0 for stage in stages
        )

        for stage in stages:
            # Get shuffle timing from task metrics if available
            if (
                hasattr(stage, "task_metrics_distributions")
                and stage.task_metrics_distributions
            ):
                dist = stage.task_metrics_distributions

                # Shuffle fetch wait time
                if (
                    dist.shuffle_read_metrics
                    and dist.shuffle_read_metrics.fetch_wait_time
                    and len(dist.shuffle_read_metrics.fetch_wait_time) >= 3
                ):
                    # Use median value approximation
                    median_fetch_wait = dist.shuffle_read_metrics.fetch_wait_time[2]
                    num_tasks = stage.num_tasks or 0
                    total_shuffle_fetch_wait_time_ns += median_fetch_wait * num_tasks

                # Shuffle write time
                if (
                    dist.shuffle_write_metrics
                    and dist.shuffle_write_metrics.write_time
                    and len(dist.shuffle_write_metrics.write_time) >= 3
                ):
                    # Use median value approximation
                    median_write_time = dist.shuffle_write_metrics.write_time[2]
                    num_tasks = stage.num_tasks or 0
                    total_shuffle_write_time_ns += median_write_time * num_tasks

        shuffle_fetch_wait_min = total_shuffle_fetch_wait_time_ns / (
            1000 * 1000 * 1000 * 60
        )  # Convert from nanoseconds
        shuffle_write_time_min = total_shuffle_write_time_ns / (
            1000 * 1000 * 1000 * 60
        )  # Convert from nanoseconds

        # Build summary
        summary = {
            "application_id": app_id,
            "application_name": app.name,
            "analysis_timestamp": datetime.now().isoformat(),
            # Time metrics (improved for clarity)
            "application_duration_minutes": round(total_runtime_min, 2),
            "total_executor_runtime_minutes": round(total_executor_runtime_min, 2),
            "executor_cpu_time_minutes": round(total_executor_cpu_time_min, 2),
            "jvm_gc_time_minutes": round(total_gc_time_min, 2),
            "executor_utilization_percent": round(executor_utilization, 2),
            # Data processing metrics
            "input_data_size_gb": round(total_input_gb, 3),
            "output_data_size_gb": round(total_output_gb, 3),
            "shuffle_read_size_gb": round(total_shuffle_read_gb, 3),
            "shuffle_write_size_gb": round(total_shuffle_write_gb, 3),
            "memory_spilled_gb": round(total_memory_spilled_gb, 3),
            "disk_spilled_gb": round(total_disk_spilled_gb, 3),
            # Performance metrics
            "shuffle_read_wait_time_minutes": round(shuffle_fetch_wait_min, 2),
            "shuffle_write_time_minutes": round(shuffle_write_time_min, 2),
            "failed_tasks": total_failed_tasks,
            # Additional context
            "total_stages": len(stages),
            "completed_stages": len(
                [s for s in stages if s.status == StageStatus.COMPLETE]
            ),
            "failed_stages": len([s for s in stages if s.status == StageStatus.FAILED]),
        }

        return summary

    except Exception as e:
        return {
            "error": f"Failed to generate app summary: {str(e)}",
            "application_id": app_id,
        }
