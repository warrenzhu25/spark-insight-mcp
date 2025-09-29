"""
Executor and resource analysis tools for MCP server.

This module contains tools for retrieving and analyzing Spark executor
information, resource allocation, and utilization patterns.
"""

import statistics
from datetime import timedelta
from typing import Any, Dict, Optional

from ..core.app import mcp
from .common import get_client_or_default
from .fetchers import fetch_executors


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
    active_executors = 0
    total_cores = 0
    total_memory = 0

    resource_timeline = []

    for event in timeline_events:
        if event["type"] == "executor_add":
            active_executors += 1
            total_cores += event["cores"]
            total_memory += event["memory_mb"]
        elif event["type"] == "executor_remove":
            active_executors -= 1
            # Note: We don't have cores/memory info in remove events

        resource_timeline.append(
            {
                "timestamp": event["timestamp"],
                "active_executors": active_executors,
                "total_cores": total_cores,
                "total_memory_mb": total_memory,
                "event": event,
            }
        )

    return {
        "application_id": app_id,
        "application_name": app.name,
        "summary": {
            "total_events": len(timeline_events),
            "executor_additions": len(
                [e for e in timeline_events if e["type"] == "executor_add"]
            ),
            "executor_removals": len(
                [e for e in timeline_events if e["type"] == "executor_remove"]
            ),
            "stage_executions": len(
                [e for e in timeline_events if e["type"] == "stage_start"]
            ),
            "peak_executors": max(
                [r["active_executors"] for r in resource_timeline] + [0]
            ),
            "peak_cores": max([r["total_cores"] for r in resource_timeline] + [0]),
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
                time_range = (
                    f"{current_start}"
                    if current_start == intervals[i - 1]["minute"]
                    else f"{current_start}-{intervals[i - 1]['minute']}"
                )
                merged_intervals.append(
                    {
                        "time_range_minutes": time_range,
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
        time_range = (
            f"{current_start}"
            if current_start == intervals[-1]["minute"]
            else f"{current_start}-{intervals[-1]['minute']}"
        )
        merged_intervals.append(
            {
                "time_range_minutes": time_range,
                "active_executors": current_count,
                "total_cores": current_cores,
                "total_memory_mb": current_memory,
            }
        )

    # Calculate utilization metrics
    executor_counts = [interval["active_executors"] for interval in intervals]
    peak_executors = max(executor_counts) if executor_counts else 0
    avg_executors = statistics.mean(executor_counts) if executor_counts else 0
    min_executors = min(executor_counts) if executor_counts else 0

    # Calculate efficiency metrics
    total_executor_minutes = sum(interval["active_executors"] for interval in intervals)
    utilization_efficiency = (
        (avg_executors / peak_executors * 100) if peak_executors > 0 else 0
    )

    recommendations = []
    if utilization_efficiency < 70:
        recommendations.append(
            {
                "type": "resource_efficiency",
                "priority": "medium",
                "issue": f"Low executor utilization efficiency ({utilization_efficiency:.1f}%)",
                "suggestion": "Consider optimizing dynamic allocation settings or job scheduling",
            }
        )

    if peak_executors > avg_executors * 2:
        recommendations.append(
            {
                "type": "resource_planning",
                "priority": "medium",
                "issue": "High variance in executor demand",
                "suggestion": "Review workload patterns and consider more aggressive scaling policies",
            }
        )

    return {
        "application_id": app_id,
        "analysis_type": "Executor Utilization Analysis",
        "timeline": merged_intervals,
        "summary": {
            "peak_executors": peak_executors,
            "average_executors": round(avg_executors, 1),
            "minimum_executors": min_executors,
            "total_duration_minutes": total_minutes,
            "utilization_efficiency_percent": round(utilization_efficiency, 1),
            "total_executor_minutes": total_executor_minutes,
        },
        "recommendations": recommendations,
    }
