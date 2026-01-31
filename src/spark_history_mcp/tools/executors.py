"""
Executor and resource analysis tools for MCP server.

This module contains tools for retrieving and analyzing Spark executor
information, resource allocation, and utilization patterns.
"""

import statistics
from datetime import timedelta
from typing import Any, Dict, Optional

from ..core.app import mcp
from .common import (
    DEFAULT_INTERVAL_MINUTES,
    MAX_INTERVALS,
    compact_output,
    get_client_or_default,
)
from .fetchers import fetch_app, fetch_executors, fetch_stages
from .timelines import build_app_executor_timeline


@mcp.tool()
def list_executors(
    app_id: str,
    server: Optional[str] = None,
    include_inactive: bool = False,
    compact: Optional[bool] = None,
) -> Any:
    """
    Get executor information for a Spark application.

    Retrieves a list of executors (active by default) for the specified Spark application
    with their resource allocation, task statistics, and performance metrics.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        include_inactive: Whether to include inactive executors (default: False)
        compact: Whether to return a compact summary (default: True)

    Returns:
        List of ExecutorSummary objects containing executor information (or compact summary list)
    """
    if include_inactive:
        executors = fetch_executors(app_id=app_id, server=server, include_inactive=True)
        return compact_output(executors, compact)
    # Fallback to client active-only API if needed; otherwise reuse full list and filter
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)
    try:
        executors = client.list_executors(app_id=app_id)
        return compact_output(executors, compact)
    except Exception:
        # If active-only is not available, filter from all
        executors = [
            e
            for e in fetch_executors(app_id=app_id, server=server)
            if getattr(e, "is_active", False)
        ]
        return compact_output(executors, compact)


@mcp.tool()
def get_executor(
    app_id: str,
    executor_id: str,
    server: Optional[str] = None,
    compact: Optional[bool] = None,
) -> Any:
    """
    Get information about a specific executor.

    Retrieves detailed information about a single executor including resource allocation,
    task statistics, memory usage, and performance metrics.

    Args:
        app_id: The Spark application ID
        executor_id: The executor ID
        server: Optional server name to use (uses default if not specified)
        compact: Whether to return a compact summary (default: True)

    Returns:
        ExecutorSummary object containing executor details (or compact summary) or None if not found
    """
    # Get all executors and find the one with matching ID
    executors = fetch_executors(app_id=app_id, server=server)

    for executor in executors:
        if executor.id == executor_id:
            return compact_output(executor, compact)

    return None


@mcp.tool()
def get_executor_summary(app_id: str, server: Optional[str] = None):
    """
    Aggregates metrics across all executors for a Spark application.

    Retrieves all executors (active and inactive) and calculates summary statistics
    including memory usage, disk usage, task counts, performance metrics, and
    utilization efficiency.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing aggregated executor metrics and utilization efficiency
    """
    executors = fetch_executors(app_id=app_id, server=server)
    app = fetch_app(app_id=app_id, server=server)

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

    # Calculate utilization efficiency if app has timing information
    if app.attempts and app.attempts[0].start_time and app.attempts[0].end_time:
        start_time = app.attempts[0].start_time
        end_time = app.attempts[0].end_time
        duration = end_time - start_time
        total_minutes = int(duration.total_seconds() / 60)

        # Sample executor counts at 1-minute intervals
        executor_counts = []
        for minute in range(0, total_minutes + 1):
            interval_time = start_time + timedelta(minutes=minute)
            active_count = 0

            for executor in executors:
                add_time = getattr(executor, "add_time", None)
                remove_time = getattr(executor, "remove_time", None) or end_time

                if add_time and add_time <= interval_time < remove_time:
                    active_count += 1

            executor_counts.append(active_count)

        # Calculate efficiency metrics
        if executor_counts:
            peak_executors = max(executor_counts)
            avg_executors = statistics.mean(executor_counts)
            utilization_efficiency = (
                (avg_executors / peak_executors * 100) if peak_executors > 0 else 0
            )

            summary["peak_executors"] = peak_executors
            summary["average_executors"] = round(avg_executors, 1)
            summary["utilization_efficiency_percent"] = round(utilization_efficiency, 1)

        # Calculate executor utilization (task time vs available executor time)
        app_end_time_ms = end_time.timestamp() * 1000
        total_executor_time_ms = 0
        executor_cores = app.cores_per_executor or 1

        for executor in executors:
            add_time = getattr(executor, "add_time", None)
            if add_time:
                add_time_ms = add_time.timestamp() * 1000
                remove_time = getattr(executor, "remove_time", None)
                if remove_time:
                    remove_time_ms = remove_time.timestamp() * 1000
                else:
                    remove_time_ms = app_end_time_ms
                total_executor_time_ms += remove_time_ms - add_time_ms

        if total_executor_time_ms > 0:
            # total_duration is already in milliseconds from Spark API
            total_task_time_ms = summary["total_duration"]
            executor_utilization = (
                total_task_time_ms / (total_executor_time_ms * executor_cores)
            ) * 100
            summary["executor_utilization_percent"] = round(executor_utilization, 2)

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
    # Get application info
    app = fetch_app(app_id=app_id, server=server)

    # Get all executors
    executors = fetch_executors(app_id=app_id, server=server)

    # Get stages
    stages = fetch_stages(app_id=app_id, server=server)

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
def get_app_executor_timeline(
    app_id: str,
    server: Optional[str] = None,
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
    max_intervals: int = MAX_INTERVALS,
) -> Dict[str, Any]:
    """
    Get interval-based executor timeline for a Spark application.

    Provides a timeline of active executor counts at regular intervals throughout
    the application lifecycle. This is useful for analyzing resource allocation
    patterns and comparing applications.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        interval_minutes: Time interval in minutes for timeline sampling (default: 1)
        max_intervals: Maximum number of intervals to return (default: 10000)

    Returns:
        Dictionary containing app info, simplified timeline with active executor counts,
        and summary statistics
    """
    # Get application info
    app = fetch_app(app_id=app_id, server=server)

    # Get all executors
    executors = fetch_executors(app_id=app_id, server=server)

    # Get stages
    stages = fetch_stages(app_id=app_id, server=server)

    # Build full timeline using helper
    result = build_app_executor_timeline(
        app=app,
        executors=executors,
        stages=stages,
        interval_minutes=interval_minutes,
        max_intervals=max_intervals,
    )

    if not result:
        return {
            "error": "Unable to build timeline - application has no timing information",
            "application_id": app_id,
        }

    # Simplify timeline to only include active executor count
    simplified_timeline = [
        {
            "interval_start": entry["interval_start"],
            "interval_end": entry["interval_end"],
            "active_executor_count": entry["active_executor_count"],
        }
        for entry in result["timeline"]
    ]

    return {
        "app_info": result["app_info"],
        "timeline": simplified_timeline,
        "summary": result["summary"],
    }
