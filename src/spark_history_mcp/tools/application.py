"""
Application-level Spark tools for MCP server.

This module contains tools for retrieving and analyzing Spark application
metadata, environment configuration, and high-level application insights.
"""

import re
from typing import Optional

from spark_history_mcp.core.app import mcp
from spark_history_mcp.models.spark_types import ApplicationInfo
from spark_history_mcp.tools.fetchers import fetch_app
from spark_history_mcp.tools.metrics import summarize_app


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

    # Validate search_type
    valid_search_types = ["exact", "contains", "regex"]
    if search_type not in valid_search_types:
        raise ValueError(
            f"search_type must be one of {valid_search_types}, got: {search_type}"
        )

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
    include_executor_utilization: bool = True,
):
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
        include_executor_utilization: Whether to include executor utilization analysis (default: True)

    Returns:
        Dictionary containing comprehensive application insights
    """
    # Import here to avoid circular imports
    from spark_history_mcp.tools.analysis import (
        analyze_auto_scaling,
        analyze_failed_tasks,
        analyze_shuffle_skew,
        get_job_bottlenecks,
    )
    from spark_history_mcp.tools.executors import analyze_executor_utilization

    insights = {}

    # Application overview
    app = get_application(app_id, server)
    insights["application"] = {
        "id": app.id,
        "name": app.name,
        "user": app.user,
        "start_time": app.start_time,
        "end_time": app.end_time,
        "duration": app.duration,
    }

    # Performance bottlenecks
    insights["bottlenecks"] = get_job_bottlenecks(app_id, server)

    # Optional analyses
    if include_auto_scaling:
        insights["auto_scaling"] = analyze_auto_scaling(app_id, server)

    if include_shuffle_skew:
        insights["shuffle_skew"] = analyze_shuffle_skew(app_id, server)

    if include_failed_tasks:
        insights["failed_tasks"] = analyze_failed_tasks(app_id, server)

    if include_executor_utilization:
        insights["executor_utilization"] = analyze_executor_utilization(app_id, server)

    return insights


@mcp.tool()
def get_app_summary(app_id: str, server: Optional[str] = None):
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
    return summarize_app(app_id=app_id, server=server)