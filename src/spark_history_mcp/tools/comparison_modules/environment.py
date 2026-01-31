"""
Environment and resource comparison tools for MCP server.

This module contains tools for comparing resource allocation, job-level metrics,
aggregated stage metrics, and application configurations between Spark applications.
"""

from typing import Any, Dict, Optional

from ...core.app import mcp
from .. import fetchers as fetcher_tools
from .utils import (
    calculate_safe_ratio,
    filter_significant_metrics,
    sort_comparison_data,
)


@mcp.tool()
def compare_app_resources(
    app_id1: str, app_id2: str, server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare resource allocation and configuration between two Spark applications.

    Focuses specifically on resource allocation patterns, executor configuration,
    and resource utilization efficiency without getting into detailed performance metrics.

    Args:
        app_id1: First Spark application ID (baseline)
        app_id2: Second Spark application ID (comparison target)
        server: Optional Spark History Server name

    Returns:
        Dict containing resource allocation comparison, efficiency ratios, and recommendations
    """
    try:
        # Get application info
        app1 = fetcher_tools.fetch_app(app_id1, server)
        app2 = fetcher_tools.fetch_app(app_id2, server)

        app1_info = _get_basic_app_info(app1)
        app2_info = _get_basic_app_info(app2)

        # Calculate resource ratios and comparisons
        resource_comparison = {}

        if app1_info["cores_granted"] and app2_info["cores_granted"]:
            resource_comparison["cores_granted_ratio"] = (
                app2_info["cores_granted"] / app1_info["cores_granted"]
            )

        if app1_info["max_cores"] and app2_info["max_cores"]:
            resource_comparison["max_cores_ratio"] = (
                app2_info["max_cores"] / app1_info["max_cores"]
            )

        if app1_info["memory_per_executor_mb"] and app2_info["memory_per_executor_mb"]:
            resource_comparison["memory_per_executor_ratio"] = (
                app2_info["memory_per_executor_mb"]
                / app1_info["memory_per_executor_mb"]
            )

        if app1_info["max_executors"] and app2_info["max_executors"]:
            resource_comparison["max_executors_ratio"] = (
                app2_info["max_executors"] / app1_info["max_executors"]
            )

        # Generate resource-specific recommendations
        recommendations = _generate_resource_recommendations(resource_comparison)

        result = {
            "applications": {"app1": app1_info, "app2": app2_info},
            "resource_comparison": resource_comparison,
            "recommendations": recommendations,
        }

        return sort_comparison_data(result, sort_key="ratio")

    except Exception as e:
        return {
            "error": f"Failed to compare app resources: {str(e)}",
            "applications": {"app1": {"id": app_id1}, "app2": {"id": app_id2}},
        }


@mcp.tool()
def compare_app_jobs(
    app_id1: str, app_id2: str, server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare job-level performance metrics between two Spark applications.

    Focuses specifically on job counts, durations, success rates, and job-level
    parallelism patterns without detailed stage or executor analysis.

    Args:
        app_id1: First Spark application ID (baseline)
        app_id2: Second Spark application ID (comparison target)
        server: Optional Spark History Server name

    Returns:
        Dict containing job performance comparison, timing analysis, and recommendations
    """
    try:
        # Get jobs for both applications
        jobs1 = fetcher_tools.fetch_jobs(app_id=app_id1, server=server)
        jobs2 = fetcher_tools.fetch_jobs(app_id=app_id2, server=server)

        # Get application info
        app1 = fetcher_tools.fetch_app(app_id1, server)
        app2 = fetcher_tools.fetch_app(app_id2, server)

        # Calculate job statistics
        job_stats1 = _calculate_job_stats(jobs1)
        job_stats2 = _calculate_job_stats(jobs2)

        # Calculate job performance ratios
        job_comparison = {}
        for stat in job_stats1:
            if stat in job_stats2:
                ratio = calculate_safe_ratio(job_stats1[stat], job_stats2[stat])
                job_comparison[f"{stat}_ratio"] = ratio

        # Generate job-specific recommendations
        recommendations = _generate_job_recommendations(
            job_stats1, job_stats2, job_comparison
        )

        return {
            "applications": {
                "app1": {"id": app_id1, "name": getattr(app1, "name", "Unknown")},
                "app2": {"id": app_id2, "name": getattr(app2, "name", "Unknown")},
            },
            "job_statistics": {
                "app1": job_stats1,
                "app2": job_stats2,
            },
            "job_performance_comparison": job_comparison,
            "recommendations": recommendations,
        }

    except Exception as e:
        return {"error": f"Failed to compare jobs: {str(e)}"}


@mcp.tool()
def compare_app_stages_aggregated(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None,
    significance_threshold: float = 0.1,
    show_only_significant: bool = True,
) -> Dict[str, Any]:
    """
    Compare aggregated stage-level metrics between two Spark applications.

    Focuses on overall stage performance patterns, I/O volumes, shuffle operations,
    and data processing efficiency without individual stage-by-stage analysis.

    Args:
        app_id1: First Spark application ID (baseline)
        app_id2: Second Spark application ID (comparison target)
        server: Optional Spark History Server name
        significance_threshold: Minimum difference threshold to show metric (default: 0.1)
        show_only_significant: When True, filter out metrics below significance threshold (default: True)

    Returns:
        Dict containing aggregated stage comparison, I/O analysis, and recommendations
    """
    try:
        # Get stages and application info
        stages1 = fetcher_tools.fetch_stages(app_id=app_id1, server=server)
        stages2 = fetcher_tools.fetch_stages(app_id=app_id2, server=server)
        app1 = fetcher_tools.fetch_app(app_id1, server)
        app2 = fetcher_tools.fetch_app(app_id2, server)

        # Calculate aggregated stage metrics
        agg_metrics1 = _calculate_aggregated_stage_metrics(stages1)
        agg_metrics2 = _calculate_aggregated_stage_metrics(stages2)

        # Calculate stage performance ratios
        stage_comparison = {}
        for metric in agg_metrics1:
            if metric in agg_metrics2:
                ratio = calculate_safe_ratio(agg_metrics1[metric], agg_metrics2[metric])
                percent_change = (
                    (agg_metrics2[metric] - agg_metrics1[metric])
                    / max(abs(agg_metrics1[metric]), 1)
                ) * 100
                stage_comparison[f"{metric}_ratio"] = ratio
                stage_comparison[f"{metric}_percent_change"] = percent_change

        # Apply significance filtering if requested
        if show_only_significant:
            stage_comparison = filter_significant_metrics(
                stage_comparison, significance_threshold, show_only_significant
            )

        # Generate stage-specific recommendations
        recommendations = _generate_aggregated_stage_recommendations(
            agg_metrics1, agg_metrics2, stage_comparison
        )

        return {
            "applications": {
                "app1": {"id": app_id1, "name": getattr(app1, "name", "Unknown")},
                "app2": {"id": app_id2, "name": getattr(app2, "name", "Unknown")},
            },
            "aggregated_stage_metrics": {
                "app1": agg_metrics1,
                "app2": agg_metrics2,
            },
            "stage_performance_comparison": stage_comparison,
            "filtering_info": {
                "significance_threshold": significance_threshold,
                "show_only_significant": show_only_significant,
            },
            "recommendations": recommendations,
        }

    except Exception as e:
        return {"error": f"Failed to compare aggregated stages: {str(e)}"}


def _get_basic_app_info(app) -> Dict[str, Any]:
    """Extract basic resource information from application."""
    info = {
        "id": app.id,
        "name": app.name,
        "cores_granted": app.cores_granted,
        "max_cores": app.max_cores,
        "memory_per_executor_mb": app.memory_per_executor_mb,
        "max_executors": app.max_executors,
        "cores_per_executor": app.cores_per_executor,
    }

    # Add attempt information if available
    if app.attempts:
        attempt = app.attempts[-1]  # Latest attempt
        info.update(
            {
                "duration_ms": getattr(attempt, "duration", 0),
                "start_time": getattr(attempt, "start_time", None),
                "end_time": getattr(attempt, "end_time", None),
            }
        )

    return info


def _generate_resource_recommendations(resource_comparison: Dict[str, Any]) -> list:
    """Generate resource allocation recommendations."""
    recommendations = []

    # Cores analysis
    if resource_comparison.get("cores_granted_ratio", 1) > 2:
        recommendations.append(
            {
                "type": "resource_scaling",
                "priority": "medium",
                "issue": f"App2 uses {resource_comparison['cores_granted_ratio']:.1f}x more cores than App1",
                "suggestion": "Consider if App2 needs this level of CPU resources or if App1 is under-provisioned",
            }
        )
    elif resource_comparison.get("cores_granted_ratio", 1) < 0.5:
        recommendations.append(
            {
                "type": "resource_scaling",
                "priority": "high",
                "issue": f"App2 uses {resource_comparison['cores_granted_ratio']:.1f}x fewer cores than App1",
                "suggestion": "App2 may be CPU-constrained - consider increasing core allocation",
            }
        )

    # Memory analysis
    if resource_comparison.get("memory_per_executor_ratio", 1) > 2:
        recommendations.append(
            {
                "type": "memory_allocation",
                "priority": "medium",
                "issue": f"App2 allocates {resource_comparison['memory_per_executor_ratio']:.1f}x more memory per executor",
                "suggestion": "Verify if App2's workload requires this memory or if it's over-provisioned",
            }
        )
    elif resource_comparison.get("memory_per_executor_ratio", 1) < 0.5:
        recommendations.append(
            {
                "type": "memory_allocation",
                "priority": "high",
                "issue": f"App2 has {resource_comparison['memory_per_executor_ratio']:.1f}x less memory per executor",
                "suggestion": "App2 may experience memory pressure - consider increasing executor memory",
            }
        )

    return recommendations


def _calculate_job_stats(jobs) -> Dict[str, Any]:
    """Calculate job statistics from job list."""
    if not jobs:
        return {
            "total_jobs": 0,
            "completed_jobs": 0,
            "failed_jobs": 0,
            "running_jobs": 0,
            "avg_job_duration_ms": 0,
            "max_job_duration_ms": 0,
            "success_rate": 0.0,
        }

    completed_jobs = [j for j in jobs if j.status == "SUCCEEDED"]
    failed_jobs = [j for j in jobs if j.status == "FAILED"]
    running_jobs = [j for j in jobs if j.status == "RUNNING"]

    # Calculate durations for completed jobs
    durations = []
    for job in completed_jobs:
        if job.completion_time and job.submission_time:
            duration = (
                job.completion_time - job.submission_time
            ).total_seconds() * 1000  # ms
            durations.append(duration)

    avg_duration = sum(durations) / len(durations) if durations else 0
    max_duration = max(durations) if durations else 0
    success_rate = len(completed_jobs) / len(jobs) if jobs else 0.0

    return {
        "total_jobs": len(jobs),
        "completed_jobs": len(completed_jobs),
        "failed_jobs": len(failed_jobs),
        "running_jobs": len(running_jobs),
        "avg_job_duration_ms": avg_duration,
        "max_job_duration_ms": max_duration,
        "success_rate": success_rate,
    }


def _calculate_aggregated_stage_metrics(stages) -> Dict[str, Any]:
    """Calculate aggregated metrics across all stages."""
    if not stages:
        return {
            "total_stages": 0,
            "total_input_bytes": 0,
            "total_output_bytes": 0,
            "total_shuffle_read_bytes": 0,
            "total_shuffle_write_bytes": 0,
            "total_memory_spilled_bytes": 0,
            "total_disk_spilled_bytes": 0,
            "total_executor_run_time_ms": 0,
            "total_executor_cpu_time_ns": 0,
            "total_gc_time_ms": 0,
            "avg_stage_duration_ms": 0,
            "total_tasks": 0,
            "total_failed_tasks": 0,
        }

    total_input = sum(getattr(s, "input_bytes", 0) or 0 for s in stages)
    total_output = sum(getattr(s, "output_bytes", 0) or 0 for s in stages)
    total_shuffle_read = sum(getattr(s, "shuffle_read_bytes", 0) or 0 for s in stages)
    total_shuffle_write = sum(getattr(s, "shuffle_write_bytes", 0) or 0 for s in stages)
    total_memory_spilled = sum(
        getattr(s, "memory_bytes_spilled", 0) or 0 for s in stages
    )
    total_disk_spilled = sum(getattr(s, "disk_bytes_spilled", 0) or 0 for s in stages)
    total_executor_runtime = sum(
        getattr(s, "executor_run_time", 0) or 0 for s in stages
    )
    total_executor_cpu_time = sum(
        getattr(s, "executor_cpu_time", 0) or 0 for s in stages
    )
    total_gc_time = sum(getattr(s, "jvm_gc_time", 0) or 0 for s in stages)
    total_tasks = sum(getattr(s, "num_tasks", 0) or 0 for s in stages)
    total_failed_tasks = sum(getattr(s, "num_failed_tasks", 0) or 0 for s in stages)

    # Calculate average stage duration
    durations = []
    for stage in stages:
        if stage.completion_time and stage.submission_time:
            duration = (
                stage.completion_time - stage.submission_time
            ).total_seconds() * 1000  # ms
            durations.append(duration)

    avg_duration = sum(durations) / len(durations) if durations else 0

    return {
        "total_stages": len(stages),
        "total_input_bytes": total_input,
        "total_output_bytes": total_output,
        "total_shuffle_read_bytes": total_shuffle_read,
        "total_shuffle_write_bytes": total_shuffle_write,
        "total_memory_spilled_bytes": total_memory_spilled,
        "total_disk_spilled_bytes": total_disk_spilled,
        "total_executor_run_time_ms": total_executor_runtime,
        "total_executor_cpu_time_ns": total_executor_cpu_time,
        "total_gc_time_ms": total_gc_time,
        "avg_stage_duration_ms": avg_duration,
        "total_tasks": total_tasks,
        "total_failed_tasks": total_failed_tasks,
    }


def _generate_job_recommendations(job_stats1, job_stats2, job_comparison) -> list:
    """Generate job-level recommendations."""
    recommendations = []

    # Success rate comparison
    if job_stats1["success_rate"] < 0.9 or job_stats2["success_rate"] < 0.9:
        lower_app = (
            "app1"
            if job_stats1["success_rate"] < job_stats2["success_rate"]
            else "app2"
        )
        lower_rate = min(job_stats1["success_rate"], job_stats2["success_rate"])
        recommendations.append(
            {
                "type": "reliability",
                "priority": "high",
                "issue": f"Low job success rate in {lower_app} ({lower_rate:.1%})",
                "suggestion": "Investigate job failures and improve error handling or resource allocation",
            }
        )

    # Job duration comparison
    if job_comparison.get("avg_job_duration_ms_ratio", 1) > 2:
        recommendations.append(
            {
                "type": "performance",
                "priority": "medium",
                "issue": f"App2 jobs take {job_comparison['avg_job_duration_ms_ratio']:.1f}x longer than App1",
                "suggestion": "Analyze App2's job performance - may need optimization or more resources",
            }
        )

    return recommendations


def _generate_aggregated_stage_recommendations(
    agg_metrics1, agg_metrics2, stage_comparison
) -> list:
    """Generate recommendations based on aggregated stage metrics."""
    recommendations = []

    # Spill analysis
    spill_ratio = stage_comparison.get("total_memory_spilled_bytes_ratio", 1)
    if spill_ratio > 2:
        recommendations.append(
            {
                "type": "memory_optimization",
                "priority": "high",
                "issue": f"App2 has {spill_ratio:.1f}x more memory spill than App1",
                "suggestion": "Increase executor memory or optimize data structures to reduce spilling",
            }
        )

    # Shuffle analysis
    shuffle_ratio = stage_comparison.get("total_shuffle_read_bytes_ratio", 1)
    if shuffle_ratio > 2:
        recommendations.append(
            {
                "type": "shuffle_optimization",
                "priority": "medium",
                "issue": f"App2 has {shuffle_ratio:.1f}x more shuffle data than App1",
                "suggestion": "Review partitioning strategy to reduce shuffle overhead",
            }
        )

    # GC pressure analysis
    gc_ratio = stage_comparison.get("total_gc_time_ms_ratio", 1)
    if gc_ratio > 2:
        recommendations.append(
            {
                "type": "gc_optimization",
                "priority": "high",
                "issue": f"App2 has {gc_ratio:.1f}x more GC time than App1",
                "suggestion": "Increase executor memory or tune GC settings to reduce garbage collection overhead",
            }
        )

    return recommendations
