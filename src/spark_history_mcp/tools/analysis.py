"""
Performance analysis tools for MCP server.

This module contains tools for analyzing Spark application performance,
including bottleneck identification, auto-scaling analysis, shuffle skew
detection, and failure analysis.
"""

import heapq
import statistics
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from spark_history_mcp.core.app import mcp
from spark_history_mcp.tools.common import get_client_or_default
from spark_history_mcp.tools.executors import get_executor_summary
from spark_history_mcp.tools.jobs_stages import list_slowest_jobs, list_slowest_stages


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
                    "name": job.name,
                    "duration_seconds": (
                        job.completion_time - job.submission_time
                    ).total_seconds()
                    if job.completion_time and job.submission_time
                    else 0,
                    "failed_tasks": job.num_failed_tasks,
                    "status": job.status,
                }
                for job in slowest_jobs[:top_n]
            ],
        },
        "resource_bottlenecks": {
            "memory_spill_stages": high_spill_stages[:top_n],
            "gc_pressure_ratio": gc_pressure,
            "executor_utilization": {
                "total_executors": exec_summary["total_executors"],
                "active_executors": exec_summary["active_executors"],
                "utilization_ratio": exec_summary["active_executors"]
                / max(exec_summary["total_executors"], 1),
            },
        },
        "recommendations": [],
    }

    # Generate recommendations
    if gc_pressure > 0.1:  # More than 10% time in GC
        bottlenecks["recommendations"].append(
            {
                "type": "memory",
                "priority": "high",
                "issue": f"High GC pressure ({gc_pressure:.1%})",
                "suggestion": "Consider increasing executor memory or reducing memory usage",
            }
        )

    if high_spill_stages:
        bottlenecks["recommendations"].append(
            {
                "type": "memory",
                "priority": "high",
                "issue": f"Memory spilling detected in {len(high_spill_stages)} stages",
                "suggestion": "Increase executor memory or optimize data partitioning",
            }
        )

    if exec_summary["failed_tasks"] > 0:
        bottlenecks["recommendations"].append(
            {
                "type": "reliability",
                "priority": "medium",
                "issue": f"{exec_summary['failed_tasks']} failed tasks",
                "suggestion": "Investigate task failures and consider increasing task retry settings",
            }
        )

    return bottlenecks

@mcp.tool()
def analyze_auto_scaling(
    app_id: str, server: Optional[str] = None, target_stage_duration_minutes: int = 2
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

    # Get application data
    app = client.get_application(app_id)
    environment = client.get_environment(app_id)
    stages = client.list_stages(app_id=app_id)

    if not stages:
        return {"error": "No stages found for application", "application_id": app_id}

    target_duration_ms = target_stage_duration_minutes * 60 * 1000

    # Get app start time and analyze initial stages (first 2 minutes)
    app_start = app.attempts[0].start_time if app.attempts else datetime.now()
    initial_window = app_start + timedelta(minutes=2)

    # Filter stages that were running in the first 2 minutes
    initial_stages = [
        s
        for s in stages
        if s.submission_time
        and s.completion_time
        and s.submission_time <= initial_window
        and s.completion_time >= initial_window
    ]

    # Calculate recommended initial executors
    initial_executor_demand = 0
    for stage in initial_stages:
        if stage.executor_run_time and stage.num_tasks:
            # Estimate executors needed to complete stage in target duration
            executors_needed = (
                min(stage.executor_run_time / target_duration_ms, stage.num_tasks) / 4
            )  # Conservative scaling factor
            initial_executor_demand += executors_needed

    recommended_initial = max(2, int(initial_executor_demand))

    # Calculate maximum executors needed during peak load
    # Create timeline of executor demand
    stage_events = []
    for stage in stages:
        if (
            stage.submission_time
            and stage.completion_time
            and stage.executor_run_time
            and stage.num_tasks
        ):
            executors_needed = int(
                min(stage.executor_run_time / target_duration_ms, stage.num_tasks) / 4
            )
            stage_events.append((stage.submission_time, executors_needed))
            stage_events.append((stage.completion_time, -executors_needed))

    # Sort events and calculate peak demand
    stage_events.sort(key=lambda x: x[0])
    current_demand = 0
    max_demand = 2

    for _timestamp, demand_change in stage_events:
        current_demand += demand_change
        max_demand = max(max_demand, current_demand)

    recommended_max = max(recommended_initial, max_demand)

    # Get current configuration
    spark_props = (
        {k: v for k, v in environment.spark_properties}
        if environment.spark_properties
        else {}
    )
    current_initial = spark_props.get(
        "spark.dynamicAllocation.initialExecutors", "Not set"
    )
    current_max = spark_props.get("spark.dynamicAllocation.maxExecutors", "Not set")

    # Generate recommendations as a list to match other analysis functions
    recommendations = []

    if not current_initial.isdigit() or recommended_initial != int(current_initial):
        recommendations.append(
            {
                "type": "auto_scaling",
                "priority": "medium",
                "issue": f"Initial executors could be optimized (current: {current_initial})",
                "suggestion": f"Set spark.dynamicAllocation.initialExecutors to {recommended_initial}",
                "configuration": {
                    "parameter": "spark.dynamicAllocation.initialExecutors",
                    "current_value": current_initial,
                    "recommended_value": str(recommended_initial),
                    "description": "Based on stages running in first 2 minutes",
                },
            }
        )

    if not current_max.isdigit() or recommended_max != int(current_max):
        recommendations.append(
            {
                "type": "auto_scaling",
                "priority": "medium",
                "issue": f"Max executors could be optimized (current: {current_max})",
                "suggestion": f"Set spark.dynamicAllocation.maxExecutors to {recommended_max}",
                "configuration": {
                    "parameter": "spark.dynamicAllocation.maxExecutors",
                    "current_value": current_max,
                    "recommended_value": str(recommended_max),
                    "description": "Based on peak concurrent stage demand",
                },
            }
        )

    return {
        "application_id": app_id,
        "analysis_type": "Auto-scaling Configuration",
        "target_stage_duration_minutes": target_stage_duration_minutes,
        "recommendations": recommendations,
        "analysis_details": {
            "total_stages": len(stages),
            "initial_stages_analyzed": len(initial_stages),
            "peak_concurrent_demand": max_demand,
            "calculation_method": f"Aims to complete stages in {target_stage_duration_minutes} minutes",
            "configuration_analysis": {
                "initial_executors": {
                    "current": current_initial,
                    "recommended": str(recommended_initial),
                    "description": "Based on stages running in first 2 minutes",
                },
                "max_executors": {
                    "current": current_max,
                    "recommended": str(recommended_max),
                    "description": "Based on peak concurrent stage demand",
                },
            },
        },
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

    # Try to get stages with summaries, fallback to basic stages if validation fails
    try:
        stages = client.list_stages(app_id=app_id, with_summaries=True)
    except Exception as e:
        if "executorMetricsDistributions.peakMemoryMetrics.quantiles" in str(e):
            # Known issue with executor metrics distributions - use stages without summaries
            stages = client.list_stages(app_id=app_id, with_summaries=False)
        else:
            raise e
    shuffle_threshold_bytes = shuffle_threshold_gb * 1024 * 1024 * 1024

    skewed_stages = []

    for stage in stages:
        # Check if stage has significant shuffle write
        if (
            not stage.shuffle_write_bytes
            or stage.shuffle_write_bytes < shuffle_threshold_bytes
        ):
            continue

        stage_skew_info = {
            "stage_id": stage.stage_id,
            "attempt_id": stage.attempt_id,
            "name": stage.name,
            "total_shuffle_write_gb": round(
                stage.shuffle_write_bytes / (1024 * 1024 * 1024), 2
            ),
            "task_count": stage.num_tasks,
            "failed_tasks": stage.num_failed_tasks,
            "task_skew": None,
            "executor_skew": None,
        }

        # Check task-level skew
        try:
            task_summary = stage.task_metrics_distributions

            if task_summary.shuffle_write_bytes:
                # Extract quantiles (typically [min, 25th, 50th, 75th, max])
                quantiles = task_summary.shuffle_write_bytes
                if len(quantiles) >= 5:
                    median = quantiles[2]  # 50th percentile
                    max_val = quantiles[4]  # 100th percentile (max)

                    if median > 0:
                        task_skew_ratio = max_val / median
                        stage_skew_info["task_skew"] = {
                            "skew_ratio": round(task_skew_ratio, 2),
                            "max_shuffle_write_mb": round(max_val / (1024 * 1024), 2),
                            "median_shuffle_write_mb": round(median / (1024 * 1024), 2),
                            "is_skewed": task_skew_ratio > skew_ratio_threshold,
                        }
        except Exception:
            # Skip task-level analysis if it fails
            pass

        # Check executor-level skew using stage.executor_metrics_distributions
        if (
            stage.executor_metrics_distributions
            and stage.executor_metrics_distributions.shuffle_write
        ):
            executor_quantiles = stage.executor_metrics_distributions.shuffle_write
            if len(executor_quantiles) >= 5:
                exec_median = executor_quantiles[2]  # 50th percentile
                exec_max = executor_quantiles[4]  # 100th percentile (max)

                if exec_median > 0:
                    exec_skew_ratio = exec_max / exec_median
                    stage_skew_info["executor_skew"] = {
                        "skew_ratio": round(exec_skew_ratio, 2),
                        "max_executor_shuffle_write_mb": round(
                            exec_max / (1024 * 1024), 2
                        ),
                        "median_executor_shuffle_write_mb": round(
                            exec_median / (1024 * 1024), 2
                        ),
                        "is_skewed": exec_skew_ratio > skew_ratio_threshold,
                    }

        # Add stage to skewed list if either task or executor skew is detected
        is_task_skewed = (
            stage_skew_info["task_skew"] and stage_skew_info["task_skew"]["is_skewed"]
        )
        is_executor_skewed = (
            stage_skew_info["executor_skew"]
            and stage_skew_info["executor_skew"]["is_skewed"]
        )

        if is_task_skewed or is_executor_skewed:
            # Calculate overall skew ratio for sorting (use higher of the two)
            task_ratio = (
                stage_skew_info["task_skew"]["skew_ratio"]
                if stage_skew_info["task_skew"]
                else 0
            )
            exec_ratio = (
                stage_skew_info["executor_skew"]["skew_ratio"]
                if stage_skew_info["executor_skew"]
                else 0
            )
            stage_skew_info["max_skew_ratio"] = max(task_ratio, exec_ratio)
            skewed_stages.append(stage_skew_info)

    # Sort by max skew ratio (highest first)
    skewed_stages.sort(key=lambda x: x["max_skew_ratio"], reverse=True)

    recommendations = []
    if skewed_stages:
        task_skewed_count = sum(
            1 for s in skewed_stages if s["task_skew"] and s["task_skew"]["is_skewed"]
        )
        executor_skewed_count = sum(
            1
            for s in skewed_stages
            if s["executor_skew"] and s["executor_skew"]["is_skewed"]
        )

        if task_skewed_count > 0:
            recommendations.append(
                {
                    "type": "data_partitioning",
                    "priority": "high",
                    "issue": f"Found {task_skewed_count} stages with task-level shuffle skew",
                    "suggestion": "Consider repartitioning data by key distribution or using salting techniques",
                }
            )

        if executor_skewed_count > 0:
            recommendations.append(
                {
                    "type": "resource_allocation",
                    "priority": "high",
                    "issue": f"Found {executor_skewed_count} stages with executor-level shuffle skew",
                    "suggestion": "Check executor resource allocation, network issues, or host-specific problems",
                }
            )

        max_skew = max(s["max_skew_ratio"] for s in skewed_stages)
        if max_skew > 10:
            recommendations.append(
                {
                    "type": "performance",
                    "priority": "critical",
                    "issue": f"Extreme skew detected (ratio: {max_skew})",
                    "suggestion": "Investigate data distribution and consider custom partitioning strategies",
                }
            )

    return {
        "application_id": app_id,
        "analysis_type": "Shuffle Skew Analysis",
        "parameters": {
            "shuffle_threshold_gb": shuffle_threshold_gb,
            "skew_ratio_threshold": skew_ratio_threshold,
        },
        "skewed_stages": skewed_stages,
        "summary": {
            "total_stages_analyzed": len(
                [
                    s
                    for s in stages
                    if s.shuffle_write_bytes
                    and s.shuffle_write_bytes >= shuffle_threshold_bytes
                ]
            ),
            "skewed_stages_count": len(skewed_stages),
            "task_skewed_count": sum(
                1
                for s in skewed_stages
                if s["task_skew"] and s["task_skew"]["is_skewed"]
            ),
            "executor_skewed_count": sum(
                1
                for s in skewed_stages
                if s["executor_skew"] and s["executor_skew"]["is_skewed"]
            ),
            "max_skew_ratio": max([s["max_skew_ratio"] for s in skewed_stages])
            if skewed_stages
            else 0,
        },
        "recommendations": recommendations,
    }

@mcp.tool()
def analyze_failed_tasks(
    app_id: str, server: Optional[str] = None, failure_threshold: int = 1
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

    stages = client.list_stages(app_id=app_id)
    executors = client.list_all_executors(app_id=app_id)

    # Analyze stages with failures
    failed_stages = []
    total_failed_tasks = 0

    for stage in stages:
        if stage.num_failed_tasks and stage.num_failed_tasks >= failure_threshold:
            failed_stages.append(
                {
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "failed_tasks": stage.num_failed_tasks,
                    "total_tasks": stage.num_tasks,
                    "failure_rate": round(
                        stage.num_failed_tasks / max(stage.num_tasks, 1) * 100, 2
                    ),
                    "status": stage.status,
                }
            )
            total_failed_tasks += stage.num_failed_tasks

    # Analyze executor failures
    problematic_executors = []
    for executor in executors:
        if executor.failed_tasks and executor.failed_tasks >= failure_threshold:
            problematic_executors.append(
                {
                    "executor_id": executor.id,
                    "host": executor.host,
                    "failed_tasks": executor.failed_tasks,
                    "completed_tasks": executor.completed_tasks,
                    "failure_rate": round(
                        executor.failed_tasks
                        / max(executor.failed_tasks + executor.completed_tasks, 1)
                        * 100,
                        2,
                    ),
                    "remove_reason": executor.remove_reason,
                    "is_active": executor.is_active,
                }
            )

    # Sort by failure counts
    failed_stages.sort(key=lambda x: x["failed_tasks"], reverse=True)
    problematic_executors.sort(key=lambda x: x["failed_tasks"], reverse=True)

    # Generate recommendations
    recommendations = []

    if failed_stages:
        avg_failure_rate = statistics.mean([s["failure_rate"] for s in failed_stages])
        recommendations.append(
            {
                "type": "reliability",
                "priority": "high" if avg_failure_rate > 10 else "medium",
                "issue": f"Task failures detected in {len(failed_stages)} stages (avg failure rate: {avg_failure_rate:.1f}%)",
                "suggestion": "Investigate task failure logs and consider increasing task retry settings",
            }
        )

    if problematic_executors:
        # Check for host-specific issues
        host_failures = {}
        for executor in problematic_executors:
            host = executor["host"]
            host_failures[host] = host_failures.get(host, 0) + executor["failed_tasks"]

        max_host_failures = max(host_failures.values()) if host_failures else 0
        if max_host_failures > total_failed_tasks * 0.5:
            recommendations.append(
                {
                    "type": "infrastructure",
                    "priority": "high",
                    "issue": "High concentration of failures on specific hosts",
                    "suggestion": "Check infrastructure health and consider blacklisting problematic nodes",
                }
            )

    return {
        "application_id": app_id,
        "analysis_type": "Failed Task Analysis",
        "parameters": {"failure_threshold": failure_threshold},
        "failed_stages": failed_stages,
        "problematic_executors": problematic_executors,
        "summary": {
            "total_failed_tasks": total_failed_tasks,
            "stages_with_failures": len(failed_stages),
            "executors_with_failures": len(problematic_executors),
            "overall_failure_impact": "high"
            if total_failed_tasks > 100
            else "medium"
            if total_failed_tasks > 10
            else "low",
        },
        "recommendations": recommendations,
    }
