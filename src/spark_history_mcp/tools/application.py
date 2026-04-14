"""
Application-level Spark tools for MCP server.

This module contains tools for retrieving and analyzing Spark application
metadata, environment configuration, and high-level application insights.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from ..core.app import mcp
from . import common
from .analysis import (
    analyze_auto_scaling,
    analyze_failed_tasks,
    analyze_shuffle_skew,
)
from .common import compact_output
from .fetchers import fetch_app, fetch_env, fetch_executors, fetch_stages
from .metrics import summarize_app
from .recommendations import compact_recommendation


@mcp.tool()
def get_application(
    app_id: str, server: Optional[str] = None, compact: Optional[bool] = None
) -> Any:
    """
    Get detailed information about a specific Spark application.

    Retrieves comprehensive information about a Spark application including its
    status, resource usage, duration, and attempt details.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        compact: Whether to return a compact summary (default: True)

    Returns:
        ApplicationInfo object containing application details (or compact summary)
    """
    # Use shared fetcher to avoid duplicating client resolution and enable caching
    app = fetch_app(app_id=app_id, server=server)
    return compact_output(app, compact)


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
    compact: Optional[bool] = None,
) -> Any:
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
        compact: Whether to return a compact summary (default: True)

    Returns:
        List of ApplicationInfo objects (or compact summary list)

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
    client = common.get_client_or_default(ctx, server)

    # When name filtering is requested, fetch all apps first so the name
    # filter runs over the full set, then apply the limit afterwards.
    api_limit = limit if not app_name else None

    # Get applications from the server with existing filters
    applications = client.list_applications(
        status=status,
        min_date=min_date,
        max_date=max_date,
        min_end_date=min_end_date,
        max_end_date=max_end_date,
        limit=api_limit,
    )

    # If no name filtering is requested, return as-is
    if not app_name:
        return compact_output(applications, compact)

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

    # Apply limit after name filtering
    if limit and len(matching_apps) > limit:
        matching_apps = matching_apps[:limit]

    return compact_output(matching_apps, compact)


@mcp.tool()
def get_environment(
    app_id: str, server: Optional[str] = None, compact: Optional[bool] = None
) -> Any:
    """
    Get the comprehensive Spark runtime configuration for a Spark application.

    Details including JVM information, Spark properties, system properties,
    classpath entries, and environment variables.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        compact: Whether to return a compact summary (default: True)

    Returns:
        ApplicationEnvironmentInfo object containing environment details (or compact summary)
    """
    environment = fetch_env(app_id=app_id, server=server)
    return compact_output(environment, compact)


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
    # Get basic application info
    app = fetch_app(app_id=app_id, server=server)

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

    cfg = common.get_config()
    if cfg.compact_tool_output:
        # Compact: summarize each sub-analysis instead of full results
        compact_analyses = {}
        for name, result in insights["analyses"].items():
            if "error" in result:
                compact_analyses[name] = {"status": "error", "error": result["error"]}
            else:
                compact_analyses[name] = {
                    "status": "ok",
                    "recommendation_count": len(result.get("recommendations", [])),
                }
        insights["analyses"] = compact_analyses

        # Compact recommendations: top N with minimal fields
        limit = cfg.compact_recommendations_limit
        insights["recommendations"] = [
            compact_recommendation(r) for r in all_recommendations[:limit]
        ]
        if len(all_recommendations) > limit:
            insights["_recommendations_truncated"] = {
                "total": len(all_recommendations),
                "returned": limit,
            }
    else:
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
    try:
        app = fetch_app(app_id=app_id, server=server)
        stages = fetch_stages(app_id=app_id, server=server, with_summaries=True)
        executors = fetch_executors(app_id=app_id, server=server)
        return summarize_app(app, stages, executors, app_id=app_id)
    except Exception as e:
        return {
            "error": f"Failed to generate app summary: {str(e)}",
            "application_id": app_id,
        }
