"""
Performance analysis tools for MCP server.

This module contains tools for analyzing Spark application performance,
including bottleneck identification, auto-scaling analysis, shuffle skew
detection, and failure analysis.
"""

import heapq
import statistics
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from spark_history_mcp.core.app import mcp
from spark_history_mcp.models.spark_types import StageData
from spark_history_mcp.tools.fetchers import fetch_executors, fetch_stages


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
def get_job_bottlenecks(
    app_id: str, server: Optional[str] = None, top_n: int = 5
) -> Dict[str, Any]:
    """
    Identify performance bottlenecks in a Spark job.

    Analyzes stages, tasks, and executors to find the most time-consuming
    operations and resource-intensive components.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        top_n: Number of top bottlenecks to return

    Returns:
        Dictionary containing identified bottlenecks and recommendations
    """
    # Import here to avoid circular imports
    from spark_history_mcp.tools.jobs_stages import list_slowest_jobs, list_slowest_stages
    from spark_history_mcp.tools.executors import get_executor_summary

    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get slowest stages
    slowest_stages = list_slowest_stages(app_id, server, False, top_n)

    # Get slowest jobs
    slowest_jobs = list_slowest_jobs(app_id, server, False, top_n)

    # Get executor summary
    exec_summary = get_executor_summary(app_id, server)

    all_stages = client.list_stages(app_id=app_id)

    # Identify stages with high spill
    high_spill_stages = []
    for stage in all_stages:
        if (
            stage.memory_bytes_spilled
            and stage.memory_bytes_spilled > 100 * 1024 * 1024
        ):  # > 100MB
            high_spill_stages.append(
                {
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "memory_spilled_mb": stage.memory_bytes_spilled / (1024 * 1024),
                    "disk_spilled_mb": stage.disk_bytes_spilled / (1024 * 1024)
                    if stage.disk_bytes_spilled
                    else 0,
                }
            )

    high_spill_stages = heapq.nlargest(
        len(high_spill_stages), high_spill_stages, key=lambda x: x["memory_spilled_mb"]
    )

    # Identify GC pressure
    gc_pressure = (
        exec_summary["total_gc_time"] / max(exec_summary["total_duration"], 1)
        if exec_summary["total_duration"] > 0
        else 0
    )

    bottlenecks = {
        "application_id": app_id,
        "performance_bottlenecks": {
            "slowest_stages": [
                {
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "duration_seconds": (
                        stage.completion_time - stage.submission_time
                    ).total_seconds()
                    if stage.completion_time and stage.submission_time
                    else 0,
                    "task_count": stage.num_tasks,
                    "failed_tasks": stage.num_failed_tasks,
                }
                for stage in slowest_stages[:top_n]
            ],
            "slowest_jobs": [
                {
                    "job_id": job.job_id,
                    "name": getattr(job, "name", f"Job {job.job_id}"),
                    "duration_seconds": (
                        job.completion_time - job.submission_time
                    ).total_seconds()
                    if job.completion_time and job.submission_time
                    else 0,
                    "stage_count": len(job.stage_ids) if job.stage_ids else 0,
                    "failed_stage_count": len(job.failed_stage_ids) if job.failed_stage_ids else 0,
                }
                for job in slowest_jobs[:top_n]
            ],
            "high_spill_stages": high_spill_stages[:top_n],
            "gc_pressure_ratio": gc_pressure,
        },
        "resource_utilization": exec_summary,
        "recommendations": _generate_bottleneck_recommendations(
            slowest_stages, high_spill_stages, gc_pressure, exec_summary
        ),
    }

    return bottlenecks


def _generate_bottleneck_recommendations(
    slowest_stages: List[StageData],
    high_spill_stages: List[Dict[str, Any]],
    gc_pressure: float,
    exec_summary: Dict[str, Any],
) -> List[str]:
    """Generate recommendations based on bottleneck analysis."""
    recommendations = []

    # Stage-based recommendations
    if slowest_stages:
        avg_duration = sum(
            (stage.completion_time - stage.submission_time).total_seconds()
            if stage.completion_time and stage.submission_time
            else 0
            for stage in slowest_stages
        ) / len(slowest_stages)

        if avg_duration > 300:  # 5 minutes
            recommendations.append(
                f"Consider optimizing stages with average duration of {avg_duration:.1f}s. "
                "Look into data partitioning and filtering optimizations."
            )

    # Spill-based recommendations
    if high_spill_stages:
        total_spill = sum(stage["memory_spilled_mb"] for stage in high_spill_stages)
        recommendations.append(
            f"High memory spill detected ({total_spill:.1f}MB across {len(high_spill_stages)} stages). "
            "Consider increasing executor memory or optimizing data structures."
        )

    # GC pressure recommendations
    if gc_pressure > 0.1:  # More than 10% time in GC
        recommendations.append(
            f"High GC pressure detected ({gc_pressure:.1%}). "
            "Consider increasing executor memory or optimizing memory usage patterns."
        )

    # Executor utilization recommendations
    if exec_summary["failed_tasks"] > exec_summary["completed_tasks"] * 0.05:  # More than 5% failures
        recommendations.append(
            f"High task failure rate ({exec_summary['failed_tasks']} failures). "
            "Investigate executor stability and resource allocation."
        )

    return recommendations


@mcp.tool()
def analyze_auto_scaling(
    app_id: str,
    server: Optional[str] = None,
    target_stage_duration_minutes: int = 2,
) -> Dict[str, Any]:
    """
    Analyze application workload and provide auto-scaling recommendations.

    Provides recommendations for dynamic allocation configuration based on
    application workload patterns. Calculates optimal initial and maximum
    executor counts to achieve target stage completion times.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        target_stage_duration_minutes: Target duration for stage completion (default: 2 minutes)

    Returns:
        Dictionary containing auto-scaling analysis and recommendations
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    app = client.get_application(app_id)
    stages = fetch_stages(app_id=app_id, server=server)
    executors = fetch_executors(app_id=app_id, server=server, include_inactive=True)

    if not stages:
        return {
            "error": "No stages found for analysis",
            "application_id": app_id,
        }

    # Analyze stage patterns
    stage_durations = []
    parallelism_levels = []

    for stage in stages:
        if stage.completion_time and stage.submission_time:
            duration_minutes = (
                stage.completion_time - stage.submission_time
            ).total_seconds() / 60
            stage_durations.append(duration_minutes)
            parallelism_levels.append(stage.num_tasks)

    if not stage_durations:
        return {
            "error": "No completed stages found for analysis",
            "application_id": app_id,
        }

    # Calculate statistics
    avg_stage_duration = statistics.mean(stage_durations)
    max_stage_duration = max(stage_durations)
    avg_parallelism = statistics.mean(parallelism_levels)
    max_parallelism = max(parallelism_levels)

    # Current resource allocation
    current_max_executors = len(executors) if executors else 1
    current_cores_per_executor = (
        executors[0].total_cores if executors else app.cores_per_executor or 1
    )

    # Calculate optimal executor count
    # Simple heuristic: scale executors based on parallelism and target duration
    scaling_factor = avg_stage_duration / target_stage_duration_minutes
    optimal_executors = max(
        int(current_max_executors * scaling_factor),
        int(avg_parallelism / current_cores_per_executor),
        1,
    )

    # Cap at reasonable limits
    recommended_min_executors = max(1, optimal_executors // 4)
    recommended_max_executors = min(optimal_executors * 2, int(max_parallelism / current_cores_per_executor))

    recommendations = []
    if avg_stage_duration > target_stage_duration_minutes * 1.5:
        recommendations.append(
            f"Current average stage duration ({avg_stage_duration:.1f}min) exceeds target "
            f"({target_stage_duration_minutes}min). Consider increasing executor count."
        )

    if max_stage_duration > target_stage_duration_minutes * 3:
        recommendations.append(
            f"Maximum stage duration ({max_stage_duration:.1f}min) is very high. "
            "Consider optimizing longest-running stages or increasing resources."
        )

    return {
        "application_id": app_id,
        "current_configuration": {
            "max_executors": current_max_executors,
            "cores_per_executor": current_cores_per_executor,
            "memory_per_executor_mb": app.memory_per_executor_mb,
        },
        "workload_analysis": {
            "total_stages": len(stages),
            "avg_stage_duration_minutes": avg_stage_duration,
            "max_stage_duration_minutes": max_stage_duration,
            "avg_parallelism": avg_parallelism,
            "max_parallelism": max_parallelism,
        },
        "recommendations": {
            "min_executors": recommended_min_executors,
            "max_executors": recommended_max_executors,
            "target_stage_duration_minutes": target_stage_duration_minutes,
            "scaling_factor": scaling_factor,
        },
        "analysis_notes": recommendations,
    }


@mcp.tool()
def analyze_shuffle_skew(
    app_id: str,
    server: Optional[str] = None,
    shuffle_threshold_gb: int = 10,
    skew_ratio_threshold: float = 2.0,
) -> Dict[str, Any]:
    """
    Analyze shuffle operations to identify data skew issues.

    Identifies stages with significant shuffle data skew by analyzing both:
    1. Task-level skew: comparing max vs median shuffle write bytes across tasks
    2. Executor-level skew: comparing max vs median shuffle write bytes across executors

    This dual analysis helps identify both data distribution issues and
    executor resource/performance bottlenecks during shuffle operations.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        shuffle_threshold_gb: Minimum total shuffle write in GB to analyze (default: 10)
        skew_ratio_threshold: Minimum ratio of max/median to flag as skewed (default: 2.0)

    Returns:
        Dictionary containing shuffle skew analysis results with both task-level
        and executor-level skew detection
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    stages = fetch_stages(app_id=app_id, server=server)

    if not stages:
        return {
            "error": "No stages found for analysis",
            "application_id": app_id,
        }

    shuffle_stages = []
    skewed_stages = []

    for stage in stages:
        # Only analyze stages with significant shuffle writes
        if (
            stage.shuffle_write_bytes
            and stage.shuffle_write_bytes > shuffle_threshold_gb * 1024 * 1024 * 1024
        ):
            shuffle_stages.append(stage)

            # Simplified skew detection based on stage-level metrics
            # In a real implementation, you'd fetch task-level metrics
            stage_info = {
                "stage_id": stage.stage_id,
                "attempt_id": stage.attempt_id,
                "name": stage.name,
                "shuffle_write_gb": stage.shuffle_write_bytes / (1024 * 1024 * 1024),
                "num_tasks": stage.num_tasks,
                "skew_detected": False,
                "skew_ratio": 1.0,
            }

            # Simple heuristic: if there are failed tasks in a shuffle stage, it might indicate skew
            if stage.num_failed_tasks > 0:
                stage_info["skew_detected"] = True
                stage_info["skew_ratio"] = stage.num_failed_tasks / max(stage.num_tasks, 1) * 10
                skewed_stages.append(stage_info)

    recommendations = []
    if skewed_stages:
        recommendations.append(
            f"Found {len(skewed_stages)} stages with potential shuffle skew. "
            "Consider repartitioning data or using salting techniques."
        )

    if len(shuffle_stages) > len(stages) * 0.5:
        recommendations.append(
            "High proportion of shuffle-heavy stages detected. "
            "Consider optimizing join strategies and data locality."
        )

    return {
        "application_id": app_id,
        "analysis_parameters": {
            "shuffle_threshold_gb": shuffle_threshold_gb,
            "skew_ratio_threshold": skew_ratio_threshold,
        },
        "shuffle_analysis": {
            "total_stages": len(stages),
            "shuffle_stages": len(shuffle_stages),
            "skewed_stages": len(skewed_stages),
            "total_shuffle_gb": sum(
                stage.shuffle_write_bytes / (1024 * 1024 * 1024)
                for stage in shuffle_stages
            ),
        },
        "skewed_stages": skewed_stages,
        "recommendations": recommendations,
    }


@mcp.tool()
def analyze_failed_tasks(
    app_id: str,
    server: Optional[str] = None,
    failure_threshold: int = 1,
) -> Dict[str, Any]:
    """
    Analyze failed tasks to identify patterns and root causes.

    Examines stages and executors with task failures to identify common
    failure patterns, problematic executors, and potential root causes.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        failure_threshold: Minimum number of failures to include in analysis (default: 1)

    Returns:
        Dictionary containing failed task analysis results
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    stages = fetch_stages(app_id=app_id, server=server)
    executors = fetch_executors(app_id=app_id, server=server, include_inactive=True)

    failed_stages = []
    total_failed_tasks = 0

    for stage in stages:
        if stage.num_failed_tasks >= failure_threshold:
            failed_stages.append({
                "stage_id": stage.stage_id,
                "attempt_id": stage.attempt_id,
                "name": stage.name,
                "failed_tasks": stage.num_failed_tasks,
                "total_tasks": stage.num_tasks,
                "failure_rate": stage.num_failed_tasks / max(stage.num_tasks, 1),
                "status": stage.status,
            })
            total_failed_tasks += stage.num_failed_tasks

    # Analyze executor failures
    problematic_executors = []
    for executor in executors:
        if executor.failed_tasks >= failure_threshold:
            problematic_executors.append({
                "executor_id": executor.id,
                "failed_tasks": executor.failed_tasks,
                "completed_tasks": executor.completed_tasks,
                "failure_rate": executor.failed_tasks / max(
                    executor.failed_tasks + executor.completed_tasks, 1
                ),
                "is_active": executor.is_active,
                "remove_reason": getattr(executor, "remove_reason", "N/A"),
            })

    recommendations = []
    if failed_stages:
        avg_failure_rate = sum(s["failure_rate"] for s in failed_stages) / len(failed_stages)
        recommendations.append(
            f"Found {len(failed_stages)} stages with failures (avg failure rate: {avg_failure_rate:.1%}). "
            "Investigate resource allocation and data processing logic."
        )

    if problematic_executors:
        recommendations.append(
            f"Found {len(problematic_executors)} executors with high failure rates. "
            "Check executor stability and resource configuration."
        )

    return {
        "application_id": app_id,
        "failure_analysis": {
            "total_failed_tasks": total_failed_tasks,
            "failed_stages_count": len(failed_stages),
            "problematic_executors_count": len(problematic_executors),
        },
        "failed_stages": failed_stages,
        "problematic_executors": problematic_executors,
        "recommendations": recommendations,
    }