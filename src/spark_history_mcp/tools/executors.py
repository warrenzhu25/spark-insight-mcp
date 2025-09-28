"""
Executor and resource analysis tools for MCP server.

This module contains tools for retrieving and analyzing Spark executor
information, resource allocation, and utilization patterns.
"""

from datetime import timedelta
from typing import Any, Dict, Optional

from spark_history_mcp.core.app import mcp
from spark_history_mcp.tools.fetchers import fetch_executors


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
def list_executors(
    app_id: str, server: Optional[str] = None, include_inactive: bool = False
):
    """
    Get executor information for a Spark application.

    Retrieves a list of executors (active by default) for the specified Spark application
    with their resource allocation, task statistics, and performance metrics.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        include_inactive: Whether to include inactive executors (default: False)

    Returns:
        List of ExecutorSummary objects containing executor information
    """
    if include_inactive:
        return fetch_executors(app_id=app_id, server=server, include_inactive=True)
    # Fallback to client active-only API if needed; otherwise reuse full list and filter
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)
    try:
        return client.list_executors(app_id=app_id)
    except Exception:
        # If active-only is not available, filter from all
        return [e for e in fetch_executors(app_id=app_id, server=server) if getattr(e, 'is_active', False)]


@mcp.tool()
def get_executor(app_id: str, executor_id: str, server: Optional[str] = None):
    """
    Get information about a specific executor.

    Retrieves detailed information about a single executor including resource allocation,
    task statistics, memory usage, and performance metrics.

    Args:
        app_id: The Spark application ID
        executor_id: The executor ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        ExecutorSummary object containing executor details or None if not found
    """
    # Get all executors and find the one with matching ID
    executors = fetch_executors(app_id=app_id, server=server)

    for executor in executors:
        if executor.id == executor_id:
            return executor

    return None


@mcp.tool()
def get_executor_summary(app_id: str, server: Optional[str] = None):
    """
    Aggregates metrics across all executors for a Spark application.

    Retrieves all executors (active and inactive) and calculates summary statistics
    including memory usage, disk usage, task counts, and performance metrics.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing aggregated executor metrics
    """
    executors = fetch_executors(app_id=app_id, server=server)

    summary = {
        "total_executors": len(executors),
        "active_executors": sum(1 for e in executors if e.is_active),
        "memory_used": 0,
        "disk_used": 0,
        "completed_tasks": 0,
        "failed_tasks": 0,
        "total_duration": 0,
        "total_gc_time": 0,
        "total_input_bytes": 0,
        "total_shuffle_read": 0,
        "total_shuffle_write": 0,
    }

    # Aggregate metrics from all executors
    for executor in executors:
        summary["memory_used"] += (
            executor.memory_metrics.used_on_heap_storage_memory
            + executor.memory_metrics.used_off_heap_storage_memory
        )
        summary["disk_used"] += executor.disk_used
        summary["completed_tasks"] += executor.completed_tasks
        summary["failed_tasks"] += executor.failed_tasks
        summary["total_duration"] += executor.total_duration
        summary["total_gc_time"] += executor.total_gc_time
        summary["total_input_bytes"] += executor.total_input_bytes
        summary["total_shuffle_read"] += executor.total_shuffle_read
        summary["total_shuffle_write"] += executor.total_shuffle_write

    return summary


@mcp.tool()
def get_resource_usage_timeline(
    app_id: str, server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get resource usage timeline for a Spark application.

    Provides a chronological view of resource allocation and usage patterns
    including executor additions/removals and stage execution overlap.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing timeline of resource usage
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get application info
    app = client.get_application(app_id)

    # Get all executors
    executors = client.list_all_executors(app_id=app_id)

    # Get stages
    stages = client.list_stages(app_id=app_id)

    # Create timeline events
    timeline_events = []

    # Add executor events
    for executor in executors:
        if executor.add_time:
            timeline_events.append(
                {
                    "timestamp": executor.add_time,
                    "type": "executor_add",
                    "executor_id": executor.id,
                    "cores": executor.total_cores,
                    "memory_mb": executor.max_memory / (1024 * 1024)
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
                    "reason": executor.remove_reason,
                }
            )

    # Add stage events
    for stage in stages:
        if stage.submission_time:
            timeline_events.append(
                {
                    "timestamp": stage.submission_time,
                    "type": "stage_start",
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "task_count": stage.num_tasks,
                }
            )

        if stage.completion_time:
            timeline_events.append(
                {
                    "timestamp": stage.completion_time,
                    "type": "stage_end",
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "status": stage.status,
                    "duration_seconds": (
                        stage.completion_time - stage.submission_time
                    ).total_seconds()
                    if stage.submission_time
                    else 0,
                }
            )

    # Sort events by timestamp
    timeline_events.sort(key=lambda x: x["timestamp"])

    # Calculate resource utilization over time
    utilization_summary = {
        "peak_executors": max(
            len([e for e in executors if e.add_time]) if executors else 0, 1
        ),
        "total_core_hours": 0,
        "avg_executor_count": 0,
        "executor_lifetime_stats": {},
    }

    # Calculate core hours and other metrics
    total_executor_time = 0
    for executor in executors:
        if executor.add_time:
            remove_time = executor.remove_time or app.end_time
            if remove_time:
                duration_hours = (remove_time - executor.add_time).total_seconds() / 3600
                core_hours = duration_hours * executor.total_cores
                utilization_summary["total_core_hours"] += core_hours
                total_executor_time += duration_hours

    if executors:
        utilization_summary["avg_executor_count"] = (
            total_executor_time / len(executors) if executors else 0
        )

    return {
        "application_id": app_id,
        "timeline_events": timeline_events,
        "utilization_summary": utilization_summary,
        "analysis": {
            "executor_count": len(executors),
            "stage_count": len(stages),
            "event_count": len(timeline_events),
        },
    }


@mcp.tool()
def analyze_executor_utilization(
    app_id: str, server: Optional[str] = None, interval_minutes: int = 1
) -> Dict[str, Any]:
    """
    Analyze executor utilization patterns over time.

    Tracks executor allocation and usage throughout the application lifecycle
    to identify periods of over/under-provisioning and optimization opportunities.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        interval_minutes: Time interval for analysis in minutes (default: 1)

    Returns:
        Dictionary containing executor utilization analysis
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    app = client.get_application(app_id)
    executors = client.list_all_executors(app_id=app_id)

    if not app.attempts:
        return {"error": "No application attempts found", "application_id": app_id}

    start_time = app.attempts[0].start_time
    end_time = app.attempts[0].end_time

    if not start_time or not end_time:
        return {
            "error": "Application start/end times not available",
            "application_id": app_id,
        }

    # Create time intervals
    duration = end_time - start_time
    total_minutes = int(duration.total_seconds() / 60)
    intervals = []

    for minute in range(0, total_minutes + 1, interval_minutes):
        interval_time = start_time + timedelta(minutes=minute)

        # Count active executors at this time
        active_executors = 0
        total_cores = 0
        total_memory_mb = 0

        for executor in executors:
            add_time = executor.add_time
            remove_time = executor.remove_time or end_time

            if add_time <= interval_time < remove_time:
                active_executors += 1
                total_cores += executor.total_cores
                if executor.max_memory:
                    total_memory_mb += executor.max_memory / (1024 * 1024)

        intervals.append(
            {
                "minute": minute,
                "timestamp": interval_time.isoformat(),
                "active_executors": active_executors,
                "total_cores": total_cores,
                "total_memory_mb": int(total_memory_mb),
            }
        )

    # Merge consecutive intervals with same executor count
    merged_intervals = []
    if intervals:
        current_start = intervals[0]["minute"]
        current_count = intervals[0]["active_executors"]
        current_cores = intervals[0]["total_cores"]
        current_memory = intervals[0]["total_memory_mb"]

        for i in range(1, len(intervals)):
            if intervals[i]["active_executors"] != current_count:
                # End current interval
                merged_intervals.append(
                    {
                        "start_minute": current_start,
                        "end_minute": intervals[i - 1]["minute"],
                        "duration_minutes": intervals[i - 1]["minute"] - current_start + interval_minutes,
                        "active_executors": current_count,
                        "total_cores": current_cores,
                        "total_memory_mb": current_memory,
                    }
                )

                # Start new interval
                current_start = intervals[i]["minute"]
                current_count = intervals[i]["active_executors"]
                current_cores = intervals[i]["total_cores"]
                current_memory = intervals[i]["total_memory_mb"]

        # Add final interval
        merged_intervals.append(
            {
                "start_minute": current_start,
                "end_minute": intervals[-1]["minute"],
                "duration_minutes": intervals[-1]["minute"] - current_start + interval_minutes,
                "active_executors": current_count,
                "total_cores": current_cores,
                "total_memory_mb": current_memory,
            }
        )

    # Calculate utilization statistics
    stats = {
        "total_intervals": len(merged_intervals),
        "avg_executors": sum(interval["active_executors"] for interval in merged_intervals) / len(merged_intervals)
        if merged_intervals
        else 0,
        "max_executors": max(interval["active_executors"] for interval in merged_intervals)
        if merged_intervals
        else 0,
        "min_executors": min(interval["active_executors"] for interval in merged_intervals)
        if merged_intervals
        else 0,
    }

    # Identify periods of low/high utilization
    recommendations = []
    if merged_intervals:
        zero_executor_periods = [
            interval for interval in merged_intervals if interval["active_executors"] == 0
        ]
        if zero_executor_periods:
            total_idle_time = sum(p["duration_minutes"] for p in zero_executor_periods)
            recommendations.append(
                f"Found {len(zero_executor_periods)} periods with no active executors "
                f"totaling {total_idle_time} minutes of idle time"
            )

        high_executor_periods = [
            interval for interval in merged_intervals
            if interval["active_executors"] > stats["avg_executors"] * 1.5
        ]
        if high_executor_periods:
            recommendations.append(
                f"Found {len(high_executor_periods)} periods with high executor usage "
                f"(>150% of average)"
            )

    return {
        "application_id": app_id,
        "analysis_period": {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_minutes": total_minutes,
            "interval_minutes": interval_minutes,
        },
        "utilization_intervals": merged_intervals,
        "statistics": stats,
        "recommendations": recommendations,
    }