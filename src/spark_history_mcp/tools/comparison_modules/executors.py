"""
Executor-focused comparison tools for MCP server.

This module contains tools for comparing executor performance, timeline patterns,
and resource utilization between Spark applications.
"""

from datetime import timedelta
from typing import Any, Dict, Optional

from ...core.app import mcp
from .. import executors as executor_tools
from .. import fetchers as fetcher_tools
from .utils import calculate_safe_ratio, filter_significant_metrics, resolve_client


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
    client = resolve_client(server)

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

        # Build timelines for both applications
        timeline1 = _build_app_executor_timeline(
            app1, executors1, stages1, app_id1, interval_minutes
        )
        timeline2 = _build_app_executor_timeline(
            app2, executors2, stages2, app_id2, interval_minutes
        )

        if not timeline1 or not timeline2:
            return {
                "error": "Failed to build executor timelines",
                "applications": {
                    "app1": {"id": app_id1, "timeline_built": bool(timeline1)},
                    "app2": {"id": app_id2, "timeline_built": bool(timeline2)},
                },
            }

        # Compare timelines and create analysis
        comparison = _analyze_timeline_differences(timeline1, timeline2, app1, app2)

        return {
            "applications": {
                "app1": {"id": app_id1, "name": app1.name},
                "app2": {"id": app_id2, "name": app2.name},
            },
            "timeline_comparison": comparison,
            "analysis_parameters": {
                "interval_minutes": interval_minutes,
                "app1_duration_minutes": timeline1.get("total_duration_minutes", 0),
                "app2_duration_minutes": timeline2.get("total_duration_minutes", 0),
            },
        }

    except Exception as e:
        return {"error": f"Timeline comparison failed: {str(e)}"}


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
    client = resolve_client(server)

    try:
        # Get stage information
        stage1 = client.get_stage_attempt(app_id1, stage_id1, 0)
        stage2 = client.get_stage_attempt(app_id2, stage_id2, 0)

        # Get executor information for both applications
        executors1 = client.list_all_executors(app_id=app_id1)
        executors2 = client.list_all_executors(app_id=app_id2)

        # Build stage-specific timelines
        timeline1 = _build_stage_executor_timeline(stage1, executors1, interval_minutes)
        timeline2 = _build_stage_executor_timeline(stage2, executors2, interval_minutes)

        if not timeline1 or not timeline2:
            return {
                "error": "Failed to build stage executor timelines",
                "stages": {
                    "stage1": {
                        "app_id": app_id1,
                        "stage_id": stage_id1,
                        "timeline_built": bool(timeline1),
                    },
                    "stage2": {
                        "app_id": app_id2,
                        "stage_id": stage_id2,
                        "timeline_built": bool(timeline2),
                    },
                },
            }

        # Compare stage timelines
        comparison = _compare_stage_timelines(timeline1, timeline2, stage1, stage2)

        return {
            "app1_info": {
                "app_id": app_id1,
                "stage_details": {
                    "stage_id": stage_id1,
                    "name": stage1.name,
                    "duration_minutes": timeline1.get("stage_duration_minutes", 0),
                },
            },
            "app2_info": {
                "app_id": app_id2,
                "stage_details": {
                    "stage_id": stage_id2,
                    "name": stage2.name,
                    "duration_minutes": timeline2.get("stage_duration_minutes", 0),
                },
            },
            "comparison_config": {
                "interval_minutes": interval_minutes,
            },
            "timeline_comparison": [comparison],  # Make it a list for consistency
            "summary": {
                "merged_intervals": 1,
                "intervals_with_executor_differences": 1
                if timeline1.get("active_executors", 0)
                != timeline2.get("active_executors", 0)
                else 0,
                "max_executor_count_difference": abs(
                    timeline1.get("active_executors", 0)
                    - timeline2.get("active_executors", 0)
                ),
            },
        }

    except Exception as e:
        return {"error": f"Stage timeline comparison failed: {str(e)}"}


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
        exec1 = executor_tools.get_executor_summary(app_id1, server)
        exec2 = executor_tools.get_executor_summary(app_id2, server)

        # Get applications for basic info
        app1 = fetcher_tools.fetch_app(app_id1, server)
        app2 = fetcher_tools.fetch_app(app_id2, server)

        # Build basic executor efficiency metrics
        efficiency_metrics = _build_executor_efficiency_metrics(exec1, exec2)

        # Apply significance filtering if requested
        if show_only_significant:
            efficiency_metrics = filter_significant_metrics(
                efficiency_metrics, significance_threshold, show_only_significant
            )

        # Generate executor-specific recommendations
        recommendations = _generate_executor_recommendations(exec1, exec2, app1, app2)

        return {
            "applications": {
                "app1": {"id": app_id1, "name": getattr(app1, "name", "Unknown")},
                "app2": {"id": app_id2, "name": getattr(app2, "name", "Unknown")},
            },
            "executor_summary_comparison": {
                "app1_summary": exec1,
                "app2_summary": exec2,
                "efficiency_ratios": efficiency_metrics,
            },
            "filtering_info": {
                "significance_threshold": significance_threshold,
                "show_only_significant": show_only_significant,
            },
            "recommendations": recommendations,
        }

    except Exception as e:
        return {"error": f"Executor comparison failed: {str(e)}"}


def _build_app_executor_timeline(
    app, executors, stages, app_id: str, interval_minutes: int
) -> Optional[Dict[str, Any]]:
    """Build timeline for an application's executor usage."""
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

    # Add stage events for context
    for stage in stages:
        if stage.submission_time:
            timeline_events.append(
                {
                    "timestamp": stage.submission_time,
                    "type": "stage_start",
                    "stage_id": stage.stage_id,
                    "stage_name": stage.name,
                }
            )

        if stage.completion_time:
            timeline_events.append(
                {
                    "timestamp": stage.completion_time,
                    "type": "stage_end",
                    "stage_id": stage.stage_id,
                    "stage_name": stage.name,
                }
            )

    # Sort events by timestamp
    timeline_events.sort(key=lambda x: x["timestamp"])

    # Build interval-based analysis
    intervals = _create_timeline_intervals(start_time, end_time, interval_minutes)
    interval_analysis = _analyze_intervals(intervals, timeline_events)

    total_duration = (end_time - start_time).total_seconds() / 60  # minutes

    return {
        "app_id": app_id,
        "total_duration_minutes": total_duration,
        "timeline_events": timeline_events,
        "interval_analysis": interval_analysis,
        "executor_stats": _calculate_executor_stats(executors),
    }


def _build_stage_executor_timeline(
    stage, executors, interval_minutes: int
) -> Optional[Dict[str, Any]]:
    """Build timeline for a specific stage's executor usage."""
    if not stage.submission_time:
        return None

    start_time = stage.submission_time
    end_time = stage.completion_time or (
        start_time + timedelta(hours=1)
    )  # Default if running

    # Filter executors that were active during stage time
    active_executors = []
    for executor in executors:
        if executor.add_time and executor.add_time <= end_time:
            if not executor.remove_time or executor.remove_time >= start_time:
                active_executors.append(executor)

    # Create stage-specific intervals
    intervals = _create_timeline_intervals(start_time, end_time, interval_minutes)

    stage_duration = (end_time - start_time).total_seconds() / 60  # minutes

    return {
        "stage_duration_minutes": stage_duration,
        "active_executors": len(active_executors),
        "interval_analysis": _analyze_stage_intervals(
            intervals, active_executors, start_time, end_time
        ),
    }


def _create_timeline_intervals(start_time, end_time, interval_minutes: int) -> list:
    """Create time intervals for analysis."""
    intervals = []
    current = start_time
    interval_delta = timedelta(minutes=interval_minutes)

    while current < end_time:
        next_time = min(current + interval_delta, end_time)
        intervals.append(
            {
                "start": current,
                "end": next_time,
                "duration_minutes": (next_time - current).total_seconds() / 60,
            }
        )
        current = next_time

    return intervals


def _analyze_intervals(intervals, timeline_events) -> list:
    """Analyze timeline intervals for executor usage patterns."""
    analysis = []
    current_executors = set()
    current_cores = 0
    current_memory_mb = 0
    active_stages = set()

    event_idx = 0

    for interval in intervals:
        # Process events that occur in this interval
        while (
            event_idx < len(timeline_events)
            and timeline_events[event_idx]["timestamp"] <= interval["end"]
        ):
            event = timeline_events[event_idx]

            if event["type"] == "executor_add":
                current_executors.add(event["executor_id"])
                current_cores += event.get("cores", 0)
                current_memory_mb += event.get("memory_mb", 0)
            elif event["type"] == "executor_remove":
                if event["executor_id"] in current_executors:
                    current_executors.remove(event["executor_id"])
                    # Note: We don't track individual executor resources for removal
            elif event["type"] == "stage_start":
                active_stages.add(event["stage_id"])
            elif event["type"] == "stage_end":
                active_stages.discard(event["stage_id"])

            event_idx += 1

        analysis.append(
            {
                "interval": interval,
                "executor_count": len(current_executors),
                "total_cores": current_cores,
                "total_memory_mb": current_memory_mb,
                "active_stages": len(active_stages),
            }
        )

    return analysis


def _analyze_stage_intervals(intervals, active_executors, start_time, end_time) -> list:
    """Analyze intervals for stage-specific executor usage."""
    analysis = []

    for interval in intervals:
        # Count executors active during this interval
        active_count = 0
        total_cores = 0
        total_memory = 0

        for executor in active_executors:
            # Check if executor was active during this interval
            exec_start = (
                max(executor.add_time, start_time) if executor.add_time else start_time
            )
            exec_end = (
                min(executor.remove_time, end_time)
                if executor.remove_time
                else end_time
            )

            if exec_start <= interval["end"] and exec_end >= interval["start"]:
                active_count += 1
                total_cores += executor.total_cores or 0
                total_memory += (
                    (executor.max_memory / (1024 * 1024)) if executor.max_memory else 0
                )

        analysis.append(
            {
                "interval": interval,
                "active_executor_count": active_count,
                "total_cores": total_cores,
                "total_memory_mb": total_memory,
            }
        )

    return analysis


def _analyze_timeline_differences(timeline1, timeline2, app1, app2) -> Dict[str, Any]:
    """Analyze differences between two application timelines."""
    # Compare peak resource usage
    max_executors1 = max(
        (interval["executor_count"] for interval in timeline1["interval_analysis"]),
        default=0,
    )
    max_executors2 = max(
        (interval["executor_count"] for interval in timeline2["interval_analysis"]),
        default=0,
    )

    max_cores1 = max(
        (interval["total_cores"] for interval in timeline1["interval_analysis"]),
        default=0,
    )
    max_cores2 = max(
        (interval["total_cores"] for interval in timeline2["interval_analysis"]),
        default=0,
    )

    # Calculate utilization efficiency
    total_intervals1 = len(timeline1["interval_analysis"])
    total_intervals2 = len(timeline2["interval_analysis"])

    avg_utilization1 = sum(
        interval["executor_count"] for interval in timeline1["interval_analysis"]
    ) / max(total_intervals1, 1)
    avg_utilization2 = sum(
        interval["executor_count"] for interval in timeline2["interval_analysis"]
    ) / max(total_intervals2, 1)

    return {
        "resource_peak_comparison": {
            "max_executors": {
                "app1": max_executors1,
                "app2": max_executors2,
                "ratio": calculate_safe_ratio(max_executors1, max_executors2),
            },
            "max_cores": {
                "app1": max_cores1,
                "app2": max_cores2,
                "ratio": calculate_safe_ratio(max_cores1, max_cores2),
            },
        },
        "utilization_efficiency": {
            "avg_executor_utilization": {
                "app1": avg_utilization1,
                "app2": avg_utilization2,
                "ratio": calculate_safe_ratio(avg_utilization1, avg_utilization2),
            },
        },
        "duration_comparison": {
            "app1_duration_minutes": timeline1["total_duration_minutes"],
            "app2_duration_minutes": timeline2["total_duration_minutes"],
            "duration_ratio": calculate_safe_ratio(
                timeline1["total_duration_minutes"], timeline2["total_duration_minutes"]
            ),
        },
    }


def _compare_stage_timelines(timeline1, timeline2, stage1, stage2) -> Dict[str, Any]:
    """Compare stage-specific timelines."""
    return {
        "stage_duration_comparison": {
            "stage1_minutes": timeline1["stage_duration_minutes"],
            "stage2_minutes": timeline2["stage_duration_minutes"],
            "ratio": calculate_safe_ratio(
                timeline1["stage_duration_minutes"], timeline2["stage_duration_minutes"]
            ),
        },
        "executor_allocation": {
            "stage1_max_executors": timeline1["active_executors"],
            "stage2_max_executors": timeline2["active_executors"],
            "ratio": calculate_safe_ratio(
                timeline1["active_executors"], timeline2["active_executors"]
            ),
        },
    }


def _calculate_executor_stats(executors) -> Dict[str, Any]:
    """Calculate basic executor statistics."""
    if not executors:
        return {"total_executors": 0, "total_cores": 0, "total_memory_gb": 0}

    total_cores = sum(executor.total_cores or 0 for executor in executors)
    total_memory = sum(
        (executor.max_memory or 0) / (1024 * 1024 * 1024) for executor in executors
    )

    return {
        "total_executors": len(executors),
        "total_cores": total_cores,
        "total_memory_gb": total_memory,
    }


def _build_executor_efficiency_metrics(exec1, exec2) -> Dict[str, Any]:
    """Build executor efficiency comparison metrics."""
    metrics = {}

    # Calculate efficiency ratios for key metrics
    efficiency_fields = [
        ("total_duration", "Total Duration"),
        ("total_cores", "Total Cores"),
        ("total_executor_time", "Total Executor Time"),
        ("total_gc_time", "Total GC Time"),
        ("active_executors", "Active Executors"),
    ]

    for field, _label in efficiency_fields:
        val1 = exec1.get(field, 0)
        val2 = exec2.get(field, 0)
        if val1 or val2:
            ratio = calculate_safe_ratio(val1, val2)
            percent_change = ((val2 - val1) / max(abs(val1), 1)) * 100

            metrics[f"{field}_ratio"] = ratio
            metrics[f"{field}_percent_change"] = percent_change

    return metrics


def _generate_executor_recommendations(exec1, exec2, app1, app2) -> list:
    """Generate executor-specific optimization recommendations."""
    recommendations = []

    # GC pressure comparison
    gc_ratio1 = exec1.get("total_gc_time", 0) / max(
        exec1.get("total_executor_time", 1), 1
    )
    gc_ratio2 = exec2.get("total_gc_time", 0) / max(
        exec2.get("total_executor_time", 1), 1
    )

    if gc_ratio1 > 0.2 or gc_ratio2 > 0.2:
        higher_gc_app = "app1" if gc_ratio1 > gc_ratio2 else "app2"
        recommendations.append(
            {
                "type": "memory_management",
                "priority": "high",
                "issue": f"High GC pressure detected in {higher_gc_app} ({max(gc_ratio1, gc_ratio2):.1%})",
                "suggestion": "Consider increasing executor memory or reducing memory-intensive operations",
            }
        )

    # Executor count efficiency
    exec_count1 = exec1.get("active_executors", 0)
    exec_count2 = exec2.get("active_executors", 0)

    if (
        abs(exec_count1 - exec_count2) > max(exec_count1, exec_count2) * 0.5
    ):  # >50% difference
        recommendations.append(
            {
                "type": "resource_allocation",
                "priority": "medium",
                "issue": f"Significant executor count difference ({exec_count1} vs {exec_count2})",
                "suggestion": "Review executor allocation strategy for optimal resource utilization",
            }
        )

    return recommendations
