import heapq
import statistics
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from spark_history_mcp.core.app import mcp
from spark_history_mcp.models.mcp_types import (
    JobSummary,
    SqlQuerySummary,
)
from spark_history_mcp.models.spark_types import (
    ApplicationInfo,
    ExecutionData,
    JobData,
    JobExecutionStatus,
    SQLExecutionStatus,
    StageData,
    StageStatus,
    TaskMetricDistributions,
)


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
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    return client.get_application(app_id)


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
        raise ValueError(f"search_type must be one of {valid_search_types}, got: {search_type}")

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
def list_jobs(
    app_id: str, server: Optional[str] = None, status: Optional[list[str]] = None
) -> list:
    """
    Get a list of all jobs for a Spark application.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        status: Optional list of job status values to filter by

    Returns:
        List of JobData objects for the application
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Convert string status values to JobExecutionStatus enum if provided
    job_statuses = None
    if status:
        job_statuses = [JobExecutionStatus.from_string(s) for s in status]

    return client.list_jobs(app_id=app_id, status=job_statuses)


@mcp.tool()
def list_slowest_jobs(
    app_id: str,
    server: Optional[str] = None,
    include_running: bool = False,
    n: int = 5,
) -> List[JobData]:
    """
    Get the N slowest jobs for a Spark application.

    Retrieves all jobs for the application and returns the ones with the longest duration.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        include_running: Whether to include running jobs in the search
        n: Number of slowest jobs to return (default: 5)

    Returns:
        List of JobData objects for the slowest jobs, or empty list if no jobs found
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get all jobs
    jobs = client.list_jobs(app_id=app_id)

    if not jobs:
        return []

    # Filter out running jobs if not included
    if not include_running:
        jobs = [job for job in jobs if job.status != JobExecutionStatus.RUNNING.value]

    if not jobs:
        return []

    def get_job_duration(job):
        if job.completion_time and job.submission_time:
            return (job.completion_time - job.submission_time).total_seconds()
        return 0

    return heapq.nlargest(n, jobs, key=get_job_duration)


@mcp.tool()
def list_stages(
    app_id: str,
    server: Optional[str] = None,
    status: Optional[list[str]] = None,
    with_summaries: bool = False,
) -> list:
    """
    Get a list of all stages for a Spark application.

    Retrieves information about stages in a Spark application with options to filter
    by status and include additional details and summary metrics.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        status: Optional list of stage status values to filter by
        with_summaries: Whether to include summary metrics in the response

    Returns:
        List of StageData objects for the application
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Convert string status values to StageStatus enum if provided
    stage_statuses = None
    if status:
        stage_statuses = [StageStatus.from_string(s) for s in status]

    return client.list_stages(
        app_id=app_id,
        status=stage_statuses,
        with_summaries=with_summaries,
    )


@mcp.tool()
def list_slowest_stages(
    app_id: str,
    server: Optional[str] = None,
    include_running: bool = False,
    n: int = 5,
) -> List[StageData]:
    """
    Get the N slowest stages for a Spark application.

    Retrieves all stages for the application and returns the ones with the longest duration.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        include_running: Whether to include running stages in the search
        n: Number of slowest stages to return (default: 5)

    Returns:
        List of StageData objects for the slowest stages, or empty list if no stages found
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    stages = client.list_stages(app_id=app_id)

    # Filter out running stages if not included. This avoids using the `details` param which can significantly slow down the execution time
    if not include_running:
        stages = [stage for stage in stages if stage.status != "RUNNING"]

    if not stages:
        return []

    def get_stage_duration(stage: StageData):
        if stage.completion_time and stage.first_task_launched_time:
            return (
                stage.completion_time - stage.first_task_launched_time
            ).total_seconds()
        return 0

    return heapq.nlargest(n, stages, key=get_stage_duration)


@mcp.tool()
def get_stage(
    app_id: str,
    stage_id: int,
    attempt_id: Optional[int] = None,
    server: Optional[str] = None,
    with_summaries: bool = False,
) -> StageData:
    """
    Get information about a specific stage.

    Args:
        app_id: The Spark application ID
        stage_id: The stage ID
        attempt_id: Optional stage attempt ID (if not provided, returns the latest attempt)
        server: Optional server name to use (uses default if not specified)
        with_summaries: Whether to include summary metrics

    Returns:
        StageData object containing stage information
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    if attempt_id is not None:
        # Get specific attempt
        stage_data = client.get_stage_attempt(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=attempt_id,
            details=False,
            with_summaries=with_summaries,
        )
    else:
        # Get all attempts and use the latest one
        stages = client.list_stage_attempts(
            app_id=app_id,
            stage_id=stage_id,
            details=False,
            with_summaries=with_summaries,
        )

        if not stages:
            raise ValueError(f"No stage found with ID {stage_id}")

        # If multiple attempts exist, get the one with the highest attempt_id
        if isinstance(stages, list):
            stage_data = max(stages, key=lambda s: s.attempt_id)
        else:
            stage_data = stages

    # If summaries were requested but metrics distributions are missing, fetch them separately
    if with_summaries and (
        not hasattr(stage_data, "task_metrics_distributions")
        or stage_data.task_metrics_distributions is None
    ):
        task_summary = client.get_stage_task_summary(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=stage_data.attempt_id,
        )
        stage_data.task_metrics_distributions = task_summary

    return stage_data


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
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    if include_inactive:
        return client.list_all_executors(app_id=app_id)
    else:
        return client.list_executors(app_id=app_id)


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
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get all executors and find the one with matching ID
    executors = client.list_all_executors(app_id=app_id)

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
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    executors = client.list_all_executors(app_id=app_id)

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
def compare_job_environments(
    app_id1: str, app_id2: str, server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare Spark environment configurations between two jobs.

    Identifies differences in Spark properties, JVM settings, system properties,
    and other configuration parameters between two Spark applications.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing configuration differences and similarities
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    env1 = client.get_environment(app_id=app_id1)
    env2 = client.get_environment(app_id=app_id2)

    def props_to_dict(props):
        return {k: v for k, v in props} if props else {}

    spark_props1 = props_to_dict(env1.spark_properties)
    spark_props2 = props_to_dict(env2.spark_properties)

    system_props1 = props_to_dict(env1.system_properties)
    system_props2 = props_to_dict(env2.system_properties)

    comparison = {
        "applications": {"app1": app_id1, "app2": app_id2},
        "runtime_comparison": {
            "app1": {
                "java_version": env1.runtime.java_version,
                "java_home": env1.runtime.java_home,
                "scala_version": env1.runtime.scala_version,
            },
            "app2": {
                "java_version": env2.runtime.java_version,
                "java_home": env2.runtime.java_home,
                "scala_version": env2.runtime.scala_version,
            },
        },
        "spark_properties": {
            "common": {
                k: {"app1": v, "app2": spark_props2.get(k)}
                for k, v in spark_props1.items()
                if k in spark_props2 and v == spark_props2[k]
            },
            "different": {
                k: {"app1": v, "app2": spark_props2.get(k, "NOT_SET")}
                for k, v in spark_props1.items()
                if k in spark_props2 and v != spark_props2[k]
            },
            "only_in_app1": {
                k: v for k, v in spark_props1.items() if k not in spark_props2
            },
            "only_in_app2": {
                k: v for k, v in spark_props2.items() if k not in spark_props1
            },
        },
        "system_properties": {
            "key_differences": {
                k: {
                    "app1": system_props1.get(k, "NOT_SET"),
                    "app2": system_props2.get(k, "NOT_SET"),
                }
                for k in [
                    "java.version",
                    "java.runtime.version",
                    "os.name",
                    "os.version",
                    "user.timezone",
                    "file.encoding",
                ]
                if system_props1.get(k) != system_props2.get(k)
            }
        },
    }

    return comparison


@mcp.tool()
def compare_job_performance(
    app_id1: str, app_id2: str, server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare performance metrics between two Spark jobs.

    **DEPRECATED**: This function is deprecated in favor of `compare_app_performance` which provides
    both high-level aggregated analysis and detailed stage-by-stage comparison in a single comprehensive tool.
    Please use `compare_app_performance` for new implementations.

    Analyzes execution times, resource usage, task distribution, and other
    performance indicators to identify differences between jobs.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing detailed performance comparison
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get application info
    app1 = client.get_application(app_id1)
    app2 = client.get_application(app_id2)

    # Get executor summaries
    exec_summary1 = get_executor_summary(app_id1, server)
    exec_summary2 = get_executor_summary(app_id2, server)

    # Get job data
    jobs1 = client.list_jobs(app_id=app_id1)
    jobs2 = client.list_jobs(app_id=app_id2)

    # Calculate job duration statistics
    def calc_job_stats(jobs):
        if not jobs:
            return {"count": 0, "total_duration": 0, "avg_duration": 0}

        completed_jobs = [j for j in jobs if j.completion_time and j.submission_time]
        if not completed_jobs:
            return {"count": len(jobs), "total_duration": 0, "avg_duration": 0}

        durations = [
            (j.completion_time - j.submission_time).total_seconds()
            for j in completed_jobs
        ]

        return {
            "count": len(jobs),
            "completed_count": len(completed_jobs),
            "total_duration": sum(durations),
            "avg_duration": sum(durations) / len(durations),
            "min_duration": min(durations),
            "max_duration": max(durations),
        }

    job_stats1 = calc_job_stats(jobs1)
    job_stats2 = calc_job_stats(jobs2)

    comparison = {
        "applications": {
            "app1": {"id": app_id1, "name": app1.name},
            "app2": {"id": app_id2, "name": app2.name},
        },
        "resource_allocation": {
            "app1": {
                "cores_granted": app1.cores_granted,
                "max_cores": app1.max_cores,
                "cores_per_executor": app1.cores_per_executor,
                "memory_per_executor_mb": app1.memory_per_executor_mb,
            },
            "app2": {
                "cores_granted": app2.cores_granted,
                "max_cores": app2.max_cores,
                "cores_per_executor": app2.cores_per_executor,
                "memory_per_executor_mb": app2.memory_per_executor_mb,
            },
        },
        "executor_metrics": {
            "app1": exec_summary1,
            "app2": exec_summary2,
            "comparison": {
                "executor_count_ratio": exec_summary2["total_executors"]
                / max(exec_summary1["total_executors"], 1),
                "memory_usage_ratio": exec_summary2["memory_used"]
                / max(exec_summary1["memory_used"], 1),
                "task_completion_ratio": exec_summary2["completed_tasks"]
                / max(exec_summary1["completed_tasks"], 1),
                "gc_time_ratio": exec_summary2["total_gc_time"]
                / max(exec_summary1["total_gc_time"], 1),
            },
        },
        "job_performance": {
            "app1": job_stats1,
            "app2": job_stats2,
            "comparison": {
                "job_count_ratio": job_stats2["count"] / max(job_stats1["count"], 1),
                "avg_duration_ratio": job_stats2["avg_duration"]
                / max(job_stats1["avg_duration"], 1)
                if job_stats1["avg_duration"] > 0
                else 0,
                "total_duration_ratio": job_stats2["total_duration"]
                / max(job_stats1["total_duration"], 1)
                if job_stats1["total_duration"] > 0
                else 0,
            },
        },
    }

    return comparison


@mcp.tool()
def compare_sql_execution_plans(
    app_id1: str,
    app_id2: str,
    execution_id1: Optional[int] = None,
    execution_id2: Optional[int] = None,
    server: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compare SQL execution plans between two Spark jobs.

    Analyzes the logical and physical plans, identifies differences in operations,
    and compares execution metrics between SQL queries.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        execution_id1: Optional specific execution ID for first app (uses longest if not specified)
        execution_id2: Optional specific execution ID for second app (uses longest if not specified)
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing SQL execution plan comparison
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get SQL executions for both applications
    sql_execs1 = client.get_sql_list(
        app_id=app_id1, details=True, plan_description=True
    )
    sql_execs2 = client.get_sql_list(
        app_id=app_id2, details=True, plan_description=True
    )

    # If specific execution IDs not provided, use the longest running ones
    if execution_id1 is None and sql_execs1:
        execution_id1 = max(sql_execs1, key=lambda x: x.duration or 0).id
    if execution_id2 is None and sql_execs2:
        execution_id2 = max(sql_execs2, key=lambda x: x.duration or 0).id

    if execution_id1 is None or execution_id2 is None:
        return {
            "error": "No SQL executions found in one or both applications",
            "app1_sql_count": len(sql_execs1),
            "app2_sql_count": len(sql_execs2),
        }

    # Get specific execution details
    exec1 = client.get_sql_execution(
        app_id1, execution_id1, details=True, plan_description=True
    )
    exec2 = client.get_sql_execution(
        app_id2, execution_id2, details=True, plan_description=True
    )

    # Analyze nodes and operations
    def analyze_nodes(execution):
        node_types = {}
        for node in execution.nodes:
            node_type = node.node_name
            if node_type not in node_types:
                node_types[node_type] = 0
            node_types[node_type] += 1
        return node_types

    nodes1 = analyze_nodes(exec1)
    nodes2 = analyze_nodes(exec2)

    all_node_types = set(nodes1.keys()) | set(nodes2.keys())

    comparison = {
        "applications": {"app1": app_id1, "app2": app_id2},
        "executions": {
            "app1": {
                "execution_id": execution_id1,
                "duration": exec1.duration,
                "status": exec1.status,
                "node_count": len(exec1.nodes),
                "edge_count": len(exec1.edges),
            },
            "app2": {
                "execution_id": execution_id2,
                "duration": exec2.duration,
                "status": exec2.status,
                "node_count": len(exec2.nodes),
                "edge_count": len(exec2.edges),
            },
        },
        "plan_structure": {
            "node_type_comparison": {
                node_type: {
                    "app1_count": nodes1.get(node_type, 0),
                    "app2_count": nodes2.get(node_type, 0),
                }
                for node_type in sorted(all_node_types)
            },
            "complexity_metrics": {
                "node_count_ratio": len(exec2.nodes) / max(len(exec1.nodes), 1),
                "edge_count_ratio": len(exec2.edges) / max(len(exec1.edges), 1),
                "duration_ratio": (exec2.duration or 0) / max(exec1.duration or 1, 1),
            },
        },
        "job_associations": {
            "app1": {
                "running_jobs": exec1.running_job_ids,
                "success_jobs": exec1.success_job_ids,
                "failed_jobs": exec1.failed_job_ids,
            },
            "app2": {
                "running_jobs": exec2.running_job_ids,
                "success_jobs": exec2.success_job_ids,
                "failed_jobs": exec2.failed_job_ids,
            },
        },
    }

    return comparison


@mcp.tool()
def get_stage_task_summary(
    app_id: str,
    stage_id: int,
    attempt_id: int = 0,
    server: Optional[str] = None,
) -> TaskMetricDistributions:
    """
    Get a summary of task metrics for a specific stage.

    Retrieves statistical distributions of task metrics for a stage, including
    execution times, memory usage, I/O metrics, and shuffle metrics.

    Args:
        app_id: The Spark application ID
        stage_id: The stage ID
        attempt_id: The stage attempt ID (default: 0)
        server: Optional server name to use (uses default if not specified)

    Returns:
        TaskMetricDistributions object containing metric distributions
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    return client.get_stage_task_summary(
        app_id=app_id, stage_id=stage_id, attempt_id=attempt_id
    )


def truncate_plan_description(plan_desc: str, max_length: int) -> str:
    """
    Truncate plan description while preserving structure.

    Args:
        plan_desc: The plan description to truncate
        max_length: Maximum length in characters

    Returns:
        Truncated plan description with indicator if truncated
    """
    if not plan_desc or len(plan_desc) <= max_length:
        return plan_desc

    # Try to truncate at a logical boundary (end of a line)
    truncated = plan_desc[:max_length]
    last_newline = truncated.rfind("\n")

    # If we can preserve most content by truncating at newline, do so
    if last_newline > max_length * 0.8:
        truncated = truncated[:last_newline]

    return truncated + "\n... [truncated]"


@mcp.tool()
def list_slowest_sql_queries(
    app_id: str,
    server: Optional[str] = None,
    attempt_id: Optional[str] = None,
    top_n: int = 1,
    page_size: int = 100,
    include_running: bool = False,
    include_plan_description: bool = True,
    plan_description_max_length: int = 2000,
) -> List[SqlQuerySummary]:
    """
    Get the N slowest SQL queries for a Spark application.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        attempt_id: Optional attempt ID
        top_n: Number of slowest queries to return (default: 1)
        page_size: Number of executions to fetch per page (default: 100)
        include_running: Whether to include running queries (default: False)
        include_plan_description: Whether to include execution plans (default: True)
        plan_description_max_length: Max characters for plan description (default: 1500)

    Returns:
        List of SqlQuerySummary objects for the slowest queries
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    all_executions: List[ExecutionData] = []
    offset = 0

    # Fetch all pages of SQL executions
    while True:
        executions: List[ExecutionData] = client.get_sql_list(
            app_id=app_id,
            attempt_id=attempt_id,
            details=True,
            plan_description=True,
            offset=offset,
            length=page_size,
        )

        if not executions:
            break

        all_executions.extend(executions)
        offset += page_size

        # If we got fewer executions than the page size, we've reached the end
        if len(executions) < page_size:
            break

    # Filter out running queries if not included
    if not include_running:
        all_executions = [
            e for e in all_executions if e.status != SQLExecutionStatus.RUNNING.value
        ]

    # Get the top N slowest executions
    slowest_executions = heapq.nlargest(top_n, all_executions, key=lambda e: e.duration)

    # Create simplified results without additional API calls. Raw object is too verbose.
    simplified_results = []
    for execution in slowest_executions:
        job_summary = JobSummary(
            success_job_ids=execution.success_job_ids,
            failed_job_ids=execution.failed_job_ids,
            running_job_ids=execution.running_job_ids,
        )

        # Handle plan description based on include_plan_description flag
        plan_description = ""
        if include_plan_description and execution.plan_description:
            plan_description = truncate_plan_description(
                execution.plan_description, plan_description_max_length
            )

        query_summary = SqlQuerySummary(
            id=execution.id,
            duration=execution.duration,
            description=execution.description,
            status=execution.status,
            submission_time=execution.submission_time.isoformat()
            if execution.submission_time
            else None,
            plan_description=plan_description,
            job_summary=job_summary,
        )

        simplified_results.append(query_summary)

    return simplified_results


@mcp.tool()
def get_stage_dependency_from_sql_plan(
    app_id: str,
    execution_id: Optional[int] = None,
    server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get stage dependency information from SQL execution plan.

    Analyzes SQL execution data to extract stage dependencies and relationships
    by examining job-to-stage mappings and stage execution patterns.

    Args:
        app_id: The Spark application ID
        execution_id: Optional specific execution ID (uses longest if not specified)
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing stage dependency analysis including:
        - stage_dependencies: Graph of stage parent/child relationships
        - execution_timeline: Chronological stage execution order
        - critical_path: Stages on the critical execution path
        - stage_job_mapping: Mapping between stages and jobs
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    try:
        # Get SQL execution data
        if execution_id is None:
            # Get the longest running SQL query if no execution_id specified
            sql_executions = client.get_sql_list(app_id, details=True, plan_description=True)
            if not sql_executions:
                return {
                    "error": "No SQL executions found for application",
                    "app_id": app_id,
                    "stage_dependencies": {},
                    "execution_timeline": [],
                    "critical_path": [],
                    "stage_job_mapping": {}
                }

            # Find the longest duration execution
            execution = max(sql_executions, key=lambda x: x.duration or 0)
            execution_id = execution.id
        else:
            execution = client.get_sql_execution(app_id, execution_id, details=True, plan_description=True)

        # Get all job IDs from the execution
        all_job_ids: set[int] = set()
        all_job_ids.update(execution.running_job_ids or [])
        all_job_ids.update(execution.success_job_ids or [])
        all_job_ids.update(execution.failed_job_ids or [])

        if not all_job_ids:
            return {
                "error": "No jobs found for SQL execution",
                "app_id": app_id,
                "execution_id": execution_id,
                "stage_dependencies": {},
                "execution_timeline": [],
                "critical_path": [],
                "stage_job_mapping": {}
            }

        # Get job details and collect stage IDs
        jobs = client.list_jobs(app_id)
        relevant_jobs = [job for job in jobs if job.job_id in all_job_ids]

        stage_job_mapping: dict[int, list[dict[str, Any]]] = {}
        all_stage_ids: set[int] = set()

        for job in relevant_jobs:
            if job.stage_ids:
                for stage_id in job.stage_ids:
                    all_stage_ids.add(stage_id)
                    if stage_id not in stage_job_mapping:
                        stage_job_mapping[stage_id] = []
                    stage_job_mapping[stage_id].append({
                        "job_id": job.job_id,
                        "job_name": job.name,
                        "job_status": job.status,
                        "submission_time": job.submission_time.isoformat() if job.submission_time else None,
                        "completion_time": job.completion_time.isoformat() if job.completion_time else None
                    })

        # Get stage details
        stages = client.list_stages(app_id, with_summaries=False)
        relevant_stages = [stage for stage in stages if stage.stage_id in all_stage_ids]

        # Build stage dependency graph based on submission/completion times
        stage_dependencies = {}
        execution_timeline = []

        # Create stage info with timing
        stage_info = {}
        for stage in relevant_stages:
            stage_id = stage.stage_id
            stage_info[stage_id] = {
                "stage_id": stage_id,
                "stage_name": stage.name,
                "status": stage.status,
                "num_tasks": stage.num_tasks,
                "submission_time": stage.submission_time.isoformat() if stage.submission_time else None,
                "completion_time": stage.completion_time.isoformat() if stage.completion_time else None,
                "duration_ms": None,
                "attempt_id": stage.attempt_id
            }

            # Calculate duration
            if stage.submission_time and stage.completion_time:
                duration = (stage.completion_time - stage.submission_time).total_seconds() * 1000
                stage_info[stage_id]["duration_ms"] = int(duration)

        # Sort stages by submission time for timeline
        timeline_stages = [s for s in relevant_stages if s.submission_time]
        timeline_stages.sort(key=lambda s: s.submission_time)

        execution_timeline = [
            {
                "stage_id": stage.stage_id,
                "stage_name": stage.name,
                "submission_time": stage.submission_time.isoformat(),
                "completion_time": stage.completion_time.isoformat() if stage.completion_time else None,
                "status": stage.status
            }
            for stage in timeline_stages
        ]

        # Infer dependencies based on timing and job relationships
        for i, stage in enumerate(timeline_stages):
            stage_id = stage.stage_id
            stage_dependencies[stage_id] = {
                "parents": [],
                "children": [],
                "stage_info": stage_info.get(stage_id, {})
            }

            # Find potential parent stages (completed before this stage started)
            for j in range(i):
                prev_stage = timeline_stages[j]
                if (prev_stage.completion_time and
                    prev_stage.completion_time <= stage.submission_time):

                    # Check if they're in related jobs (more likely to be dependencies)
                    prev_jobs = set(job["job_id"] for job in stage_job_mapping.get(prev_stage.stage_id, []))
                    curr_jobs = set(job["job_id"] for job in stage_job_mapping.get(stage_id, []))

                    # If they share jobs or are sequential, likely dependency
                    if prev_jobs.intersection(curr_jobs) or abs(j - i) <= 2:
                        stage_dependencies[stage_id]["parents"].append({
                            "stage_id": prev_stage.stage_id,
                            "stage_name": prev_stage.name,
                            "relationship_type": "timing_based"
                        })

                        # Add child relationship to parent
                        if prev_stage.stage_id not in stage_dependencies:
                            stage_dependencies[prev_stage.stage_id] = {
                                "parents": [],
                                "children": [],
                                "stage_info": stage_info.get(prev_stage.stage_id, {})
                            }

                        stage_dependencies[prev_stage.stage_id]["children"].append({
                            "stage_id": stage_id,
                            "stage_name": stage.name,
                            "relationship_type": "timing_based"
                        })

        # Identify critical path (longest duration sequence)
        critical_path = []
        if timeline_stages:
            # Simple heuristic: stages with longest individual duration
            stages_by_duration = [(s.stage_id, stage_info[s.stage_id].get("duration_ms", 0))
                                 for s in timeline_stages if s.stage_id in stage_info]
            stages_by_duration.sort(key=lambda x: x[1], reverse=True)

            # Take top stages that represent significant portion of execution
            total_duration = sum(d for _, d in stages_by_duration)
            critical_duration = 0

            for stage_id, duration in stages_by_duration:
                critical_path.append({
                    "stage_id": stage_id,
                    "stage_name": stage_info[stage_id]["stage_name"],
                    "duration_ms": duration,
                    "percentage_of_total": (duration / total_duration * 100) if total_duration > 0 else 0
                })
                critical_duration += duration

                # Include stages that make up ~80% of execution time
                if critical_duration / total_duration >= 0.8:
                    break

        return {
            "app_id": app_id,
            "execution_id": execution_id,
            "sql_description": execution.description,
            "execution_status": execution.status,
            "total_jobs": len(relevant_jobs),
            "total_stages": len(relevant_stages),
            "stage_dependencies": stage_dependencies,
            "execution_timeline": execution_timeline,
            "critical_path": critical_path,
            "stage_job_mapping": stage_job_mapping,
            "analysis_metadata": {
                "dependency_inference": "timing_based",
                "stages_analyzed": len(relevant_stages),
                "jobs_analyzed": len(relevant_jobs)
            }
        }

    except Exception as e:
        return {
            "error": f"Failed to analyze stage dependencies: {str(e)}",
            "app_id": app_id,
            "execution_id": execution_id,
            "stage_dependencies": {},
            "execution_timeline": [],
            "critical_path": [],
            "stage_job_mapping": {}
        }


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


# SparkInsight Analysis Tools

@mcp.tool()
def analyze_auto_scaling(
    app_id: str,
    server: Optional[str] = None,
    target_stage_duration_minutes: int = 2
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
        s for s in stages
        if s.submission_time and s.completion_time and
        s.submission_time <= initial_window and s.completion_time >= initial_window
    ]

    # Calculate recommended initial executors
    initial_executor_demand = 0
    for stage in initial_stages:
        if stage.executor_run_time and stage.num_tasks:
            # Estimate executors needed to complete stage in target duration
            executors_needed = min(
                stage.executor_run_time / target_duration_ms,
                stage.num_tasks
            ) / 4  # Conservative scaling factor
            initial_executor_demand += executors_needed

    recommended_initial = max(2, int(initial_executor_demand))

    # Calculate maximum executors needed during peak load
    # Create timeline of executor demand
    stage_events = []
    for stage in stages:
        if stage.submission_time and stage.completion_time and stage.executor_run_time and stage.num_tasks:
            executors_needed = int(min(
                stage.executor_run_time / target_duration_ms,
                stage.num_tasks
            ) / 4)
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
    spark_props = {k: v for k, v in environment.spark_properties} if environment.spark_properties else {}
    current_initial = spark_props.get("spark.dynamicAllocation.initialExecutors", "Not set")
    current_max = spark_props.get("spark.dynamicAllocation.maxExecutors", "Not set")

    # Generate recommendations as a list to match other analysis functions
    recommendations = []

    if not current_initial.isdigit() or recommended_initial != int(current_initial):
        recommendations.append({
            "type": "auto_scaling",
            "priority": "medium",
            "issue": f"Initial executors could be optimized (current: {current_initial})",
            "suggestion": f"Set spark.dynamicAllocation.initialExecutors to {recommended_initial}",
            "configuration": {
                "parameter": "spark.dynamicAllocation.initialExecutors",
                "current_value": current_initial,
                "recommended_value": str(recommended_initial),
                "description": "Based on stages running in first 2 minutes"
            }
        })

    if not current_max.isdigit() or recommended_max != int(current_max):
        recommendations.append({
            "type": "auto_scaling",
            "priority": "medium",
            "issue": f"Max executors could be optimized (current: {current_max})",
            "suggestion": f"Set spark.dynamicAllocation.maxExecutors to {recommended_max}",
            "configuration": {
                "parameter": "spark.dynamicAllocation.maxExecutors",
                "current_value": current_max,
                "recommended_value": str(recommended_max),
                "description": "Based on peak concurrent stage demand"
            }
        })

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
                    "description": "Based on stages running in first 2 minutes"
                },
                "max_executors": {
                    "current": current_max,
                    "recommended": str(recommended_max),
                    "description": "Based on peak concurrent stage demand"
                }
            }
        }
    }


@mcp.tool()
def analyze_shuffle_skew(
    app_id: str,
    server: Optional[str] = None,
    shuffle_threshold_gb: int = 10,
    skew_ratio_threshold: float = 2.0
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
        if not stage.shuffle_write_bytes or stage.shuffle_write_bytes < shuffle_threshold_bytes:
            continue

        stage_skew_info = {
            "stage_id": stage.stage_id,
            "attempt_id": stage.attempt_id,
            "name": stage.name,
            "total_shuffle_write_gb": round(stage.shuffle_write_bytes / (1024 * 1024 * 1024), 2),
            "task_count": stage.num_tasks,
            "failed_tasks": stage.num_failed_tasks,
            "task_skew": None,
            "executor_skew": None
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
                            "is_skewed": task_skew_ratio > skew_ratio_threshold
                        }
        except Exception:
            # Skip task-level analysis if it fails
            pass

        # Check executor-level skew using stage.executor_metrics_distributions
        if stage.executor_metrics_distributions and stage.executor_metrics_distributions.shuffle_write:
            executor_quantiles = stage.executor_metrics_distributions.shuffle_write
            if len(executor_quantiles) >= 5:
                exec_median = executor_quantiles[2]  # 50th percentile
                exec_max = executor_quantiles[4]  # 100th percentile (max)

                if exec_median > 0:
                    exec_skew_ratio = exec_max / exec_median
                    stage_skew_info["executor_skew"] = {
                        "skew_ratio": round(exec_skew_ratio, 2),
                        "max_executor_shuffle_write_mb": round(exec_max / (1024 * 1024), 2),
                        "median_executor_shuffle_write_mb": round(exec_median / (1024 * 1024), 2),
                        "is_skewed": exec_skew_ratio > skew_ratio_threshold
                    }

        # Add stage to skewed list if either task or executor skew is detected
        is_task_skewed = stage_skew_info["task_skew"] and stage_skew_info["task_skew"]["is_skewed"]
        is_executor_skewed = stage_skew_info["executor_skew"] and stage_skew_info["executor_skew"]["is_skewed"]

        if is_task_skewed or is_executor_skewed:
            # Calculate overall skew ratio for sorting (use higher of the two)
            task_ratio = stage_skew_info["task_skew"]["skew_ratio"] if stage_skew_info["task_skew"] else 0
            exec_ratio = stage_skew_info["executor_skew"]["skew_ratio"] if stage_skew_info["executor_skew"] else 0
            stage_skew_info["max_skew_ratio"] = max(task_ratio, exec_ratio)
            skewed_stages.append(stage_skew_info)

    # Sort by max skew ratio (highest first)
    skewed_stages.sort(key=lambda x: x["max_skew_ratio"], reverse=True)

    recommendations = []
    if skewed_stages:
        task_skewed_count = sum(1 for s in skewed_stages if s["task_skew"] and s["task_skew"]["is_skewed"])
        executor_skewed_count = sum(1 for s in skewed_stages if s["executor_skew"] and s["executor_skew"]["is_skewed"])

        if task_skewed_count > 0:
            recommendations.append({
                "type": "data_partitioning",
                "priority": "high",
                "issue": f"Found {task_skewed_count} stages with task-level shuffle skew",
                "suggestion": "Consider repartitioning data by key distribution or using salting techniques"
            })

        if executor_skewed_count > 0:
            recommendations.append({
                "type": "resource_allocation",
                "priority": "high",
                "issue": f"Found {executor_skewed_count} stages with executor-level shuffle skew",
                "suggestion": "Check executor resource allocation, network issues, or host-specific problems"
            })

        max_skew = max(s["max_skew_ratio"] for s in skewed_stages)
        if max_skew > 10:
            recommendations.append({
                "type": "performance",
                "priority": "critical",
                "issue": f"Extreme skew detected (ratio: {max_skew})",
                "suggestion": "Investigate data distribution and consider custom partitioning strategies"
            })

    return {
        "application_id": app_id,
        "analysis_type": "Shuffle Skew Analysis",
        "parameters": {
            "shuffle_threshold_gb": shuffle_threshold_gb,
            "skew_ratio_threshold": skew_ratio_threshold
        },
        "skewed_stages": skewed_stages,
        "summary": {
            "total_stages_analyzed": len([s for s in stages if s.shuffle_write_bytes and s.shuffle_write_bytes >= shuffle_threshold_bytes]),
            "skewed_stages_count": len(skewed_stages),
            "task_skewed_count": sum(1 for s in skewed_stages if s["task_skew"] and s["task_skew"]["is_skewed"]),
            "executor_skewed_count": sum(1 for s in skewed_stages if s["executor_skew"] and s["executor_skew"]["is_skewed"]),
            "max_skew_ratio": max([s["max_skew_ratio"] for s in skewed_stages]) if skewed_stages else 0
        },
        "recommendations": recommendations
    }


@mcp.tool()
def analyze_failed_tasks(
    app_id: str,
    server: Optional[str] = None,
    failure_threshold: int = 1
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
            failed_stages.append({
                "stage_id": stage.stage_id,
                "attempt_id": stage.attempt_id,
                "name": stage.name,
                "failed_tasks": stage.num_failed_tasks,
                "total_tasks": stage.num_tasks,
                "failure_rate": round(stage.num_failed_tasks / max(stage.num_tasks, 1) * 100, 2),
                "status": stage.status
            })
            total_failed_tasks += stage.num_failed_tasks

    # Analyze executor failures
    problematic_executors = []
    for executor in executors:
        if executor.failed_tasks and executor.failed_tasks >= failure_threshold:
            problematic_executors.append({
                "executor_id": executor.id,
                "host": executor.host,
                "failed_tasks": executor.failed_tasks,
                "completed_tasks": executor.completed_tasks,
                "failure_rate": round(executor.failed_tasks / max(executor.failed_tasks + executor.completed_tasks, 1) * 100, 2),
                "remove_reason": executor.remove_reason,
                "is_active": executor.is_active
            })

    # Sort by failure counts
    failed_stages.sort(key=lambda x: x["failed_tasks"], reverse=True)
    problematic_executors.sort(key=lambda x: x["failed_tasks"], reverse=True)

    # Generate recommendations
    recommendations = []

    if failed_stages:
        avg_failure_rate = statistics.mean([s["failure_rate"] for s in failed_stages])
        recommendations.append({
            "type": "reliability",
            "priority": "high" if avg_failure_rate > 10 else "medium",
            "issue": f"Task failures detected in {len(failed_stages)} stages (avg failure rate: {avg_failure_rate:.1f}%)",
            "suggestion": "Investigate task failure logs and consider increasing task retry settings"
        })

    if problematic_executors:
        # Check for host-specific issues
        host_failures = {}
        for executor in problematic_executors:
            host = executor["host"]
            host_failures[host] = host_failures.get(host, 0) + executor["failed_tasks"]

        max_host_failures = max(host_failures.values()) if host_failures else 0
        if max_host_failures > total_failed_tasks * 0.5:
            recommendations.append({
                "type": "infrastructure",
                "priority": "high",
                "issue": "High concentration of failures on specific hosts",
                "suggestion": "Check infrastructure health and consider blacklisting problematic nodes"
            })

    return {
        "application_id": app_id,
        "analysis_type": "Failed Task Analysis",
        "parameters": {
            "failure_threshold": failure_threshold
        },
        "failed_stages": failed_stages,
        "problematic_executors": problematic_executors,
        "summary": {
            "total_failed_tasks": total_failed_tasks,
            "stages_with_failures": len(failed_stages),
            "executors_with_failures": len(problematic_executors),
            "overall_failure_impact": "high" if total_failed_tasks > 100 else "medium" if total_failed_tasks > 10 else "low"
        },
        "recommendations": recommendations
    }


@mcp.tool()
def analyze_executor_utilization(
    app_id: str,
    server: Optional[str] = None,
    interval_minutes: int = 1
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
        return {"error": "Application start/end times not available", "application_id": app_id}

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

        intervals.append({
            "minute": minute,
            "timestamp": interval_time.isoformat(),
            "active_executors": active_executors,
            "total_cores": total_cores,
            "total_memory_mb": int(total_memory_mb)
        })

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
                time_range = f"{current_start}" if current_start == intervals[i-1]["minute"] else f"{current_start}-{intervals[i-1]['minute']}"
                merged_intervals.append({
                    "time_range_minutes": time_range,
                    "active_executors": current_count,
                    "total_cores": current_cores,
                    "total_memory_mb": current_memory
                })

                # Start new interval
                current_start = intervals[i]["minute"]
                current_count = intervals[i]["active_executors"]
                current_cores = intervals[i]["total_cores"]
                current_memory = intervals[i]["total_memory_mb"]

        # Add final interval
        time_range = f"{current_start}" if current_start == intervals[-1]["minute"] else f"{current_start}-{intervals[-1]['minute']}"
        merged_intervals.append({
            "time_range_minutes": time_range,
            "active_executors": current_count,
            "total_cores": current_cores,
            "total_memory_mb": current_memory
        })

    # Calculate utilization metrics
    executor_counts = [interval["active_executors"] for interval in intervals]
    peak_executors = max(executor_counts) if executor_counts else 0
    avg_executors = statistics.mean(executor_counts) if executor_counts else 0
    min_executors = min(executor_counts) if executor_counts else 0

    # Calculate efficiency metrics
    total_executor_minutes = sum(interval["active_executors"] for interval in intervals)
    utilization_efficiency = (avg_executors / peak_executors * 100) if peak_executors > 0 else 0

    recommendations = []
    if utilization_efficiency < 70:
        recommendations.append({
            "type": "resource_efficiency",
            "priority": "medium",
            "issue": f"Low executor utilization efficiency ({utilization_efficiency:.1f}%)",
            "suggestion": "Consider optimizing dynamic allocation settings or job scheduling"
        })

    if peak_executors > avg_executors * 2:
        recommendations.append({
            "type": "resource_planning",
            "priority": "medium",
            "issue": "High variance in executor demand",
            "suggestion": "Review workload patterns and consider more aggressive scaling policies"
        })

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
            "total_executor_minutes": total_executor_minutes
        },
        "recommendations": recommendations
    }


@mcp.tool()
def get_application_insights(
    app_id: str,
    server: Optional[str] = None,
    include_auto_scaling: bool = True,
    include_shuffle_skew: bool = True,
    include_failed_tasks: bool = True,
    include_executor_utilization: bool = True
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
        include_executor_utilization: Whether to include executor utilization analysis (default: True)

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
        "analyses": {}
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

    if include_executor_utilization:
        try:
            insights["analyses"]["executor_utilization"] = analyze_executor_utilization(app_id, server)
        except Exception as e:
            insights["analyses"]["executor_utilization"] = {"error": str(e)}

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
                        rec_copy.setdefault("issue", f"Configuration optimization for {key}")
                        rec_copy.setdefault("suggestion", f"Update {key} configuration based on analysis")
                        all_recommendations.append(rec_copy)
                        if rec_copy.get("priority") == "critical":
                            critical_issues.append(rec_copy)
            else:
                # Handle unexpected formats gracefully
# Note: Unexpected recommendations format will be skipped gracefully
                pass

    # Sort recommendations by priority
    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    all_recommendations.sort(key=lambda x: priority_order.get(x.get("priority", "low"), 3))

    insights["summary"] = {
        "total_analyses_run": len([a for a in insights["analyses"].values() if "error" not in a]),
        "total_recommendations": len(all_recommendations),
        "critical_issues": len(critical_issues),
        "high_priority_recommendations": len([r for r in all_recommendations if r.get("priority") == "high"]),
        "overall_health": "critical" if critical_issues else "good" if len(all_recommendations) < 3 else "needs_attention"
    }

    insights["recommendations"] = all_recommendations

    return insights


def _calculate_stage_similarity(stage1_name: str, stage2_name: str) -> float:
    """
    Calculate similarity between two stage names using SequenceMatcher.

    Args:
        stage1_name: Name of the first stage
        stage2_name: Name of the second stage

    Returns:
        Float between 0 and 1 representing similarity
    """
    return SequenceMatcher(None, stage1_name.lower(), stage2_name.lower()).ratio()


def _find_matching_stages(stages1: List[StageData], stages2: List[StageData], similarity_threshold: float = 0.6) -> List[Tuple[StageData, StageData, float]]:
    """
    Find matching stages between two applications based on stage name similarity.

    Args:
        stages1: Stages from first application
        stages2: Stages from second application
        similarity_threshold: Minimum similarity threshold for matching

    Returns:
        List of tuples containing (stage1, stage2, similarity_score)
    """
    matches = []
    used_stages2 = set()

    for stage1 in stages1:
        best_match = None
        best_similarity = 0

        for i, stage2 in enumerate(stages2):
            if i in used_stages2:
                continue

            similarity = _calculate_stage_similarity(stage1.name, stage2.name)

            if similarity > best_similarity and similarity >= similarity_threshold:
                best_similarity = similarity
                best_match = (stage2, i)

        if best_match:
            matches.append((stage1, best_match[0], best_similarity))
            used_stages2.add(best_match[1])

    return matches


def _calculate_stage_duration(stage: StageData) -> float:
    """
    Calculate stage duration in seconds.

    Args:
        stage: StageData object

    Returns:
        Duration in seconds, 0 if times are not available
    """
    if stage.completion_time and stage.submission_time:
        return (stage.completion_time - stage.submission_time).total_seconds()
    return 0


def _calculate_aggregated_stage_metrics(stages: List[StageData]) -> Dict[str, Any]:
    """
    Calculate aggregated metrics across all stages.

    Args:
        stages: List of StageData objects

    Returns:
        Dictionary containing aggregated stage metrics
    """
    if not stages:
        return {
            "total_stages": 0,
            "completed_stages": 0,
            "failed_stages": 0,
            "total_stage_duration": 0,
            "total_executor_run_time": 0,
            "total_memory_spilled": 0,
            "total_disk_spilled": 0,
            "total_shuffle_read_bytes": 0,
            "total_shuffle_write_bytes": 0,
            "total_shuffle_fetch_wait_time": 0,
            "total_shuffle_remote_reqs_duration": 0,
            "total_input_bytes": 0,
            "total_output_bytes": 0,
            "total_tasks": 0,
            "total_failed_tasks": 0
        }

    total_stage_duration = 0
    completed_stages = 0
    failed_stages = 0

    # Aggregate metrics
    metrics = {
        "total_stages": len(stages),
        "total_executor_run_time": 0,
        "total_memory_spilled": 0,
        "total_disk_spilled": 0,
        "total_shuffle_read_bytes": 0,
        "total_shuffle_write_bytes": 0,
        "total_shuffle_fetch_wait_time": 0,
        "total_shuffle_remote_reqs_duration": 0,
        "total_input_bytes": 0,
        "total_output_bytes": 0,
        "total_tasks": 0,
        "total_failed_tasks": 0
    }

    for stage in stages:
        # Calculate stage duration
        if stage.completion_time and stage.submission_time:
            stage_duration = (stage.completion_time - stage.submission_time).total_seconds()
            total_stage_duration += stage_duration
            completed_stages += 1

        # Check if stage failed
        if stage.status and stage.status.upper() == "FAILED":
            failed_stages += 1

        # Aggregate numeric metrics (handle None values)
        metrics["total_executor_run_time"] += stage.executor_run_time or 0
        metrics["total_memory_spilled"] += stage.memory_bytes_spilled or 0
        metrics["total_disk_spilled"] += stage.disk_bytes_spilled or 0
        metrics["total_shuffle_read_bytes"] += stage.shuffle_read_bytes or 0
        metrics["total_shuffle_write_bytes"] += stage.shuffle_write_bytes or 0
        metrics["total_input_bytes"] += stage.input_bytes or 0
        metrics["total_output_bytes"] += stage.output_bytes or 0
        metrics["total_tasks"] += stage.num_tasks or 0
        metrics["total_failed_tasks"] += stage.num_failed_tasks or 0

        # Aggregate shuffle timing metrics from task distributions
        if hasattr(stage, 'task_metrics_distributions') and stage.task_metrics_distributions:
            dist = stage.task_metrics_distributions

            # Aggregate fetch wait time (use median values to avoid outlier skew)
            if (dist.shuffle_read_metrics and
                dist.shuffle_read_metrics.fetch_wait_time and
                len(dist.shuffle_read_metrics.fetch_wait_time) >= 5):
                # Use median value (index 2) multiplied by number of tasks for approximation
                median_fetch_wait = dist.shuffle_read_metrics.fetch_wait_time[2]
                metrics["total_shuffle_fetch_wait_time"] += median_fetch_wait * (stage.num_tasks or 1)

            # Aggregate remote requests duration (use median values)
            if (dist.shuffle_read_metrics and
                dist.shuffle_read_metrics.remote_reqs_duration and
                len(dist.shuffle_read_metrics.remote_reqs_duration) >= 5):
                # Use median value (index 2) multiplied by number of tasks for approximation
                median_remote_reqs = dist.shuffle_read_metrics.remote_reqs_duration[2]
                metrics["total_shuffle_remote_reqs_duration"] += median_remote_reqs * (stage.num_tasks or 1)

    metrics.update({
        "total_stage_duration": total_stage_duration,
        "completed_stages": completed_stages,
        "failed_stages": failed_stages,
        "avg_stage_duration": total_stage_duration / max(completed_stages, 1)
    })

    return metrics


def _compare_environments(client, app_id1: str, app_id2: str) -> Dict[str, Any]:
    """
    Compare Spark environment configurations between two applications.
    Extracted from compare_job_environments with performance impact analysis added.
    """
    env1 = client.get_environment(app_id=app_id1)
    env2 = client.get_environment(app_id=app_id2)

    def props_to_dict(props):
        return {k: v for k, v in props} if props else {}

    spark_props1 = props_to_dict(env1.spark_properties)
    spark_props2 = props_to_dict(env2.spark_properties)
    system_props1 = props_to_dict(env1.system_properties)
    system_props2 = props_to_dict(env2.system_properties)

    # Performance-critical configuration analysis
    performance_impact_analysis = []

    # Memory-related configs
    memory_configs = [
        "spark.executor.memory", "spark.executor.memoryFraction", "spark.storage.memoryFraction",
        "spark.shuffle.memoryFraction", "spark.driver.memory", "spark.driver.maxResultSize"
    ]

    # Resource allocation configs
    resource_configs = [
        "spark.executor.cores", "spark.executor.instances", "spark.dynamicAllocation.enabled",
        "spark.dynamicAllocation.minExecutors", "spark.dynamicAllocation.maxExecutors"
    ]

    # Performance tuning configs
    performance_configs = [
        "spark.serializer", "spark.sql.adaptive.enabled", "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.shuffle.compress", "spark.io.compression.codec", "spark.sql.execution.arrow.pyspark.enabled"
    ]

    all_perf_configs = memory_configs + resource_configs + performance_configs

    for config in all_perf_configs:
        val1 = spark_props1.get(config)
        val2 = spark_props2.get(config)

        if val1 != val2:
            impact = _analyze_config_performance_impact(config, val1, val2)
            if impact:
                performance_impact_analysis.append({
                    "property": config,
                    "app1_value": val1 or "NOT_SET",
                    "app2_value": val2 or "NOT_SET",
                    "category": impact["category"],
                    "likely_impact": impact["description"],
                    "severity": impact["severity"]
                })

    return {
        "spark_properties": {
            "different": {
                k: {"app1": v, "app2": spark_props2.get(k, "NOT_SET")}
                for k, v in spark_props1.items()
                if k in spark_props2 and v != spark_props2[k]
            },
            "app1_only": {
                k: v for k, v in spark_props1.items() if k not in spark_props2
            },
            "app2_only": {
                k: v for k, v in spark_props2.items() if k not in spark_props1
            },
            "performance_impact_analysis": performance_impact_analysis
        },
        "runtime_environment": {
            "app1": {
                "java_version": env1.runtime.java_version,
                "java_home": env1.runtime.java_home,
                "scala_version": env1.runtime.scala_version,
            },
            "app2": {
                "java_version": env2.runtime.java_version,
                "java_home": env2.runtime.java_home,
                "scala_version": env2.runtime.scala_version,
            },
            "differences": [
                {"property": "java_version", "app1": env1.runtime.java_version, "app2": env2.runtime.java_version}
                for prop in ["java_version", "scala_version"]
                if getattr(env1.runtime, prop, None) != getattr(env2.runtime, prop, None)
            ]
        },
        "system_properties": {
            "key_differences": {
                k: {
                    "app1": system_props1.get(k, "NOT_SET"),
                    "app2": system_props2.get(k, "NOT_SET"),
                }
                for k in [
                    "java.version", "java.runtime.version", "os.name",
                    "os.version", "user.timezone", "file.encoding"
                ]
                if system_props1.get(k) != system_props2.get(k)
            }
        }
    }


def _analyze_config_performance_impact(config: str, val1, val2) -> Optional[Dict[str, str]]:
    """Analyze the potential performance impact of configuration differences"""
    if val1 == val2:
        return None

    # Memory-related configurations
    if "memory" in config.lower():
        if config == "spark.executor.memory":
            return {
                "category": "memory_allocation",
                "description": "Different executor memory allocation may affect task performance and spill behavior",
                "severity": "high" if abs(hash(str(val1)) - hash(str(val2))) > 1000000 else "medium"
            }
        elif "fraction" in config.lower():
            return {
                "category": "memory_management",
                "description": "Memory fraction differences can impact caching and shuffle performance",
                "severity": "medium"
            }

    # Resource allocation configurations
    elif config in ["spark.executor.cores", "spark.executor.instances"]:
        return {
            "category": "resource_allocation",
            "description": f"Different {config.split('.')[-1]} allocation affects parallelism and resource utilization",
            "severity": "high"
        }

    elif "dynamicAllocation" in config:
        return {
            "category": "auto_scaling",
            "description": "Dynamic allocation settings impact resource scaling behavior",
            "severity": "medium"
        }

    # Performance tuning configurations
    elif config == "spark.serializer":
        return {
            "category": "serialization",
            "description": f"Different serializers ({val1} vs {val2}) can significantly impact performance",
            "severity": "high" if "kryo" in str(val1).lower() or "kryo" in str(val2).lower() else "medium"
        }

    elif "adaptive" in config.lower():
        return {
            "category": "query_optimization",
            "description": "Adaptive query execution settings affect SQL optimization",
            "severity": "medium"
        }

    elif "compress" in config.lower() or "codec" in config.lower():
        return {
            "category": "compression",
            "description": "Compression settings impact I/O performance and storage efficiency",
            "severity": "low"
        }

    return None


def _compare_sql_execution_plans(client, app_id1: str, app_id2: str) -> Dict[str, Any]:
    """Compare SQL execution plans between two Spark applications."""
    try:
        # Get SQL queries for both applications (check if method exists)
        if not hasattr(client, 'list_sql_queries'):
            return {
                "sql_analysis": "not_supported",
                "message": "SQL query listing not supported by client"
            }

        sql_queries1 = client.list_sql_queries(app_id1)
        sql_queries2 = client.list_sql_queries(app_id2)

        if not sql_queries1 and not sql_queries2:
            return {
                "sql_analysis": "no_sql_queries",
                "message": "No SQL queries found in either application"
            }

        # SQL query analysis
        sql_comparison = {
            "app1": {
                "query_count": len(sql_queries1),
                "total_duration_ms": sum(q.duration_ms or 0 for q in sql_queries1),
                "avg_duration_ms": sum(q.duration_ms or 0 for q in sql_queries1) / max(len(sql_queries1), 1),
                "failed_queries": sum(1 for q in sql_queries1 if q.status == "FAILED"),
            },
            "app2": {
                "query_count": len(sql_queries2),
                "total_duration_ms": sum(q.duration_ms or 0 for q in sql_queries2),
                "avg_duration_ms": sum(q.duration_ms or 0 for q in sql_queries2) / max(len(sql_queries2), 1),
                "failed_queries": sum(1 for q in sql_queries2 if q.status == "FAILED"),
            }
        }

        # Calculate comparison ratios
        comparison_ratios = {}
        if sql_comparison["app1"]["query_count"] > 0 and sql_comparison["app2"]["query_count"] > 0:
            comparison_ratios = {
                "query_count_ratio": sql_comparison["app2"]["query_count"] / sql_comparison["app1"]["query_count"],
                "avg_duration_ratio": sql_comparison["app2"]["avg_duration_ms"] / max(sql_comparison["app1"]["avg_duration_ms"], 1),
                "total_duration_ratio": sql_comparison["app2"]["total_duration_ms"] / max(sql_comparison["app1"]["total_duration_ms"], 1),
            }

        # Execution plan analysis (simplified)
        plan_analysis = {
            "app1_plan_count": 0,
            "app2_plan_count": 0,
            "common_plan_patterns": [],
            "plan_differences": []
        }

        # Try to get detailed execution plans for top queries
        top_queries1 = sorted(sql_queries1, key=lambda x: x.duration_ms or 0, reverse=True)[:3]
        top_queries2 = sorted(sql_queries2, key=lambda x: x.duration_ms or 0, reverse=True)[:3]

        plan_details1 = []
        plan_details2 = []

        for query in top_queries1:
            try:
                plan = client.get_sql_query_execution_plan(app_id1, query.execution_id)
                if plan:
                    plan_details1.append({
                        "execution_id": query.execution_id,
                        "duration_ms": query.duration_ms,
                        "plan_nodes_count": len(plan.get('nodes', [])) if isinstance(plan, dict) else 0
                    })
            except:
                pass

        for query in top_queries2:
            try:
                plan = client.get_sql_query_execution_plan(app_id2, query.execution_id)
                if plan:
                    plan_details2.append({
                        "execution_id": query.execution_id,
                        "duration_ms": query.duration_ms,
                        "plan_nodes_count": len(plan.get('nodes', [])) if isinstance(plan, dict) else 0
                    })
            except:
                pass

        plan_analysis["app1_plan_count"] = len(plan_details1)
        plan_analysis["app2_plan_count"] = len(plan_details2)

        # SQL performance recommendations
        sql_recommendations = []

        if sql_comparison["app1"]["query_count"] > 0 or sql_comparison["app2"]["query_count"] > 0:
            # Query performance analysis
            if comparison_ratios.get("avg_duration_ratio", 1) > 1.5:
                sql_recommendations.append({
                    "type": "sql_performance",
                    "priority": "high",
                    "issue": f"App2 SQL queries are {comparison_ratios['avg_duration_ratio']:.1f}x slower on average",
                    "suggestion": "Review SQL query optimization, indexing strategies, and adaptive query execution settings"
                })

            # Query failure analysis
            if sql_comparison["app1"]["failed_queries"] > 0 or sql_comparison["app2"]["failed_queries"] > 0:
                sql_recommendations.append({
                    "type": "sql_reliability",
                    "priority": "medium",
                    "issue": f"SQL query failures detected (App1: {sql_comparison['app1']['failed_queries']}, App2: {sql_comparison['app2']['failed_queries']})",
                    "suggestion": "Investigate failed SQL queries and review data quality or schema issues"
                })

        return {
            "sql_query_comparison": sql_comparison,
            "comparison_ratios": comparison_ratios,
            "execution_plan_analysis": plan_analysis,
            "sql_recommendations": sql_recommendations,
            "top_queries_analyzed": {
                "app1_count": len(plan_details1),
                "app2_count": len(plan_details2)
            }
        }

    except Exception as e:
        return {
            "sql_analysis": "error",
            "error_message": f"Error analyzing SQL execution plans: {str(e)}",
            "sql_recommendations": []
        }


def _gather_basic_insights(client, app_id1: str, app_id2: str) -> Dict[str, Any]:
    """Gather basic insights using simplified analysis (avoiding MCP tool calls)."""
    insights = {
        "shuffle_analysis": {},
        "failure_analysis": {},
        "utilization_analysis": {}
    }

    try:
        # Get stages for both applications for basic shuffle analysis
        try:
            stages1 = client.list_stages(app_id=app_id1, with_summaries=True)
            stages2 = client.list_stages(app_id=app_id2, with_summaries=True)
        except Exception:
            try:
                stages1 = client.list_stages(app_id=app_id1, with_summaries=False)
                stages2 = client.list_stages(app_id=app_id2, with_summaries=False)
            except Exception as e:
                insights["shuffle_analysis"]["error"] = f"Could not get stages: {str(e)}"
                stages1 = stages2 = []

        if stages1 and stages2:
            # Simple shuffle analysis
            total_shuffle_read1 = sum(getattr(s, 'shuffle_read_bytes', 0) for s in stages1)
            total_shuffle_read2 = sum(getattr(s, 'shuffle_read_bytes', 0) for s in stages2)
            total_shuffle_write1 = sum(getattr(s, 'shuffle_write_bytes', 0) for s in stages1)
            total_shuffle_write2 = sum(getattr(s, 'shuffle_write_bytes', 0) for s in stages2)

            insights["shuffle_analysis"] = {
                "app1": {
                    "total_shuffle_read_gb": total_shuffle_read1 / (1024**3),
                    "total_shuffle_write_gb": total_shuffle_write1 / (1024**3),
                    "shuffle_stages_count": sum(1 for s in stages1 if getattr(s, 'shuffle_read_bytes', 0) > 0 or getattr(s, 'shuffle_write_bytes', 0) > 0)
                },
                "app2": {
                    "total_shuffle_read_gb": total_shuffle_read2 / (1024**3),
                    "total_shuffle_write_gb": total_shuffle_write2 / (1024**3),
                    "shuffle_stages_count": sum(1 for s in stages2 if getattr(s, 'shuffle_read_bytes', 0) > 0 or getattr(s, 'shuffle_write_bytes', 0) > 0)
                },
                "comparison": {
                    "shuffle_read_ratio": total_shuffle_read2 / max(total_shuffle_read1, 1),
                    "shuffle_write_ratio": total_shuffle_write2 / max(total_shuffle_write1, 1),
                    "shuffle_volume_ratio": (total_shuffle_read2 + total_shuffle_write2) / max(total_shuffle_read1 + total_shuffle_write1, 1)
                }
            }

        # Simple failure analysis using stage data
        try:
            failed_tasks1 = sum(getattr(s, 'num_failed_tasks', 0) for s in stages1)
            failed_tasks2 = sum(getattr(s, 'num_failed_tasks', 0) for s in stages2)
            total_tasks1 = sum(getattr(s, 'num_complete_tasks', 0) + getattr(s, 'num_failed_tasks', 0) for s in stages1)
            total_tasks2 = sum(getattr(s, 'num_complete_tasks', 0) + getattr(s, 'num_failed_tasks', 0) for s in stages2)

            failure_rate1 = (failed_tasks1 / max(total_tasks1, 1)) * 100
            failure_rate2 = (failed_tasks2 / max(total_tasks2, 1)) * 100

            insights["failure_analysis"] = {
                "app1": {
                    "total_failed_tasks": failed_tasks1,
                    "failure_rate": failure_rate1,
                    "failed_stages_count": sum(1 for s in stages1 if getattr(s, 'num_failed_tasks', 0) > 0)
                },
                "app2": {
                    "total_failed_tasks": failed_tasks2,
                    "failure_rate": failure_rate2,
                    "failed_stages_count": sum(1 for s in stages2 if getattr(s, 'num_failed_tasks', 0) > 0)
                },
                "comparison": {
                    "failure_rate_improvement": failure_rate1 - failure_rate2,
                    "reliability_change": "improved" if failure_rate2 < failure_rate1 else
                                         "degraded" if failure_rate2 > failure_rate1 else "unchanged"
                }
            }
        except Exception as e:
            insights["failure_analysis"]["error"] = f"Error analyzing failures: {str(e)}"

        # Simple utilization analysis using executor data
        try:
            executors1 = client.list_executors(app_id1, include_inactive=True)
            executors2 = client.list_executors(app_id2, include_inactive=True)

            # Calculate basic utilization metrics
            active_executors1 = sum(1 for e in executors1 if e.is_active)
            active_executors2 = sum(1 for e in executors2 if e.is_active)

            insights["utilization_analysis"] = {
                "app1": {
                    "total_executors": len(executors1),
                    "active_executors": active_executors1,
                    "executor_efficiency": active_executors1 / max(len(executors1), 1) * 100
                },
                "app2": {
                    "total_executors": len(executors2),
                    "active_executors": active_executors2,
                    "executor_efficiency": active_executors2 / max(len(executors2), 1) * 100
                },
                "comparison": {
                    "executor_count_ratio": len(executors2) / max(len(executors1), 1),
                    "efficiency_change": (active_executors2 / max(len(executors2), 1) * 100) - (active_executors1 / max(len(executors1), 1) * 100)
                }
            }
        except Exception as e:
            insights["utilization_analysis"]["error"] = f"Error analyzing utilization: {str(e)}"

    except Exception as e:
        return {"error": f"Error gathering basic insights: {str(e)}"}

    # Generate insights-based recommendations
    insights_recommendations = []

    # Shuffle-based recommendations
    shuffle_data = insights.get("shuffle_analysis", {})
    if not shuffle_data.get("error"):
        shuffle_volume_ratio = shuffle_data.get("comparison", {}).get("shuffle_volume_ratio", 1)
        if shuffle_volume_ratio > 2:
            insights_recommendations.append({
                "type": "shuffle_volume",
                "priority": "medium",
                "issue": f"App2 has {shuffle_volume_ratio:.1f}x more shuffle data than App1",
                "suggestion": "Consider broadcast joins, pre-aggregation, or data layout optimizations"
            })

    # Failure-based recommendations
    failure_data = insights.get("failure_analysis", {})
    if not failure_data.get("error"):
        reliability_change = failure_data.get("comparison", {}).get("reliability_change")
        if reliability_change == "degraded":
            insights_recommendations.append({
                "type": "reliability",
                "priority": "high",
                "issue": "App2 has higher task failure rate than App1",
                "suggestion": "Investigate resource allocation, memory settings, and error patterns"
            })

    # Utilization-based recommendations
    util_data = insights.get("utilization_analysis", {})
    if not util_data.get("error"):
        efficiency_change = util_data.get("comparison", {}).get("efficiency_change", 0)
        if efficiency_change < -10:  # Efficiency decreased by more than 10%
            insights_recommendations.append({
                "type": "resource_efficiency",
                "priority": "medium",
                "issue": f"App2 resource efficiency decreased by {abs(efficiency_change):.1f}%",
                "suggestion": "Review executor allocation, memory settings, and parallelism configuration"
            })

    insights["recommendations"] = insights_recommendations
    return insights


def _generate_cross_dimensional_recommendations(
    environment_comparison: Dict[str, Any],
    sql_plans_comparison: Dict[str, Any],
    basic_insights: Dict[str, Any],
    aggregated_overview: Dict[str, Any]
) -> List[Dict[str, str]]:
    """Generate enhanced recommendations by analyzing patterns across all dimensions."""
    cross_recommendations = []

    # Analyze cross-dimensional patterns
    config_differences = environment_comparison.get("configuration_differences", {})
    performance_impacts = environment_comparison.get("performance_impact_analysis", {})
    sql_metrics = sql_plans_comparison.get("sql_query_comparison", {})
    shuffle_analysis = basic_insights.get("shuffle_analysis", {})
    failure_analysis = basic_insights.get("failure_analysis", {})
    utilization_analysis = basic_insights.get("utilization_analysis", {})

    # Pattern 1: Memory configuration + Memory spill + Utilization
    memory_configs = [k for k in config_differences.keys() if "memory" in k.lower()]
    stage_metrics = aggregated_overview.get("aggregated_stage_comparison", {})
    memory_spill_ratio = stage_metrics.get("comparison", {}).get("memory_spill_ratio", 1)

    if memory_configs and memory_spill_ratio > 2:
        memory_util_change = utilization_analysis.get("comparison", {}).get("memory_utilization_improvement", 0)
        if memory_util_change < 0:
            cross_recommendations.append({
                "type": "cross_dimensional_memory",
                "priority": "critical",
                "issue": "Memory configuration differences correlate with increased memory spill and decreased utilization",
                "suggestion": f"Memory configs changed: {', '.join(memory_configs[:3])}. Increase executor memory or optimize memory fractions to reduce {memory_spill_ratio:.1f}x spill increase"
            })

    # Pattern 2: Serializer + Shuffle performance + Network utilization
    serializer_changed = any("serializer" in k.lower() for k in config_differences.keys())
    shuffle_volume_ratio = shuffle_analysis.get("comparison", {}).get("shuffle_volume_ratio", 1)

    if serializer_changed and shuffle_volume_ratio > 1.5:
        cross_recommendations.append({
            "type": "cross_dimensional_serialization",
            "priority": "high",
            "issue": f"Serializer configuration change coincides with {shuffle_volume_ratio:.1f}x increase in shuffle data",
            "suggestion": "Review serializer efficiency (e.g., Kryo vs Java) and its impact on shuffle operations and network utilization"
        })

    # Pattern 3: SQL query performance + Stage performance correlation
    sql_avg_duration_ratio = sql_plans_comparison.get("comparison_ratios", {}).get("avg_duration_ratio", 1)
    stage_duration_ratio = stage_metrics.get("comparison", {}).get("duration_ratio", 1)

    if sql_avg_duration_ratio > 1.3 and stage_duration_ratio > 1.3:
        cross_recommendations.append({
            "type": "cross_dimensional_performance",
            "priority": "high",
            "issue": f"Both SQL queries ({sql_avg_duration_ratio:.1f}x) and stages ({stage_duration_ratio:.1f}x) are significantly slower in App2",
            "suggestion": "Performance degradation spans both SQL optimization and stage execution. Check adaptive query execution settings and review overall resource allocation"
        })

    # Pattern 4: Dynamic allocation + Executor utilization patterns
    dynamic_allocation_configs = [k for k in config_differences.keys() if "dynamic" in k.lower()]
    efficiency_change = utilization_analysis.get("comparison", {}).get("efficiency_change", 0)
    underutil_diff = (utilization_analysis.get("app2", {}).get("underutilized_executors", 0) -
                     utilization_analysis.get("app1", {}).get("underutilized_executors", 0))

    if dynamic_allocation_configs and efficiency_change < -5 and underutil_diff > 0:
        cross_recommendations.append({
            "type": "cross_dimensional_scaling",
            "priority": "medium",
            "issue": f"Dynamic allocation changes led to {abs(efficiency_change):.1f}% efficiency decrease and {underutil_diff} more underutilized executors",
            "suggestion": f"Review dynamic allocation parameters: {', '.join(dynamic_allocation_configs)}. Consider adjusting min/max executors and scaling thresholds"
        })

    # Pattern 5: Resource allocation + Failure rate correlation
    core_configs = [k for k in config_differences.keys() if "cores" in k.lower() or "executor" in k.lower()]
    reliability_change = failure_analysis.get("comparison", {}).get("reliability_change")

    if core_configs and reliability_change == "degraded":
        failure_rate_change = failure_analysis.get("comparison", {}).get("failure_rate_improvement", 0)
        cross_recommendations.append({
            "type": "cross_dimensional_reliability",
            "priority": "high",
            "issue": f"Resource allocation changes coincide with reliability degradation (failure rate increased by {abs(failure_rate_change):.2f}%)",
            "suggestion": f"Resource configs changed: {', '.join(core_configs[:2])}. Ensure adequate resources per executor and review task timeout settings"
        })

    # Pattern 6: Compression + I/O patterns + Disk utilization
    compression_configs = [k for k in config_differences.keys() if "compress" in k.lower() or "codec" in k.lower()]
    input_output_changes = (
        stage_metrics.get("comparison", {}).get("input_ratio", 1) +
        stage_metrics.get("comparison", {}).get("output_ratio", 1)
    ) / 2

    if compression_configs and input_output_changes > 1.3:
        cross_recommendations.append({
            "type": "cross_dimensional_io",
            "priority": "low",
            "issue": f"Compression configuration changes with {input_output_changes:.1f}x increase in I/O operations",
            "suggestion": "Review compression codec efficiency and its impact on I/O performance. Consider codec benchmarking for your data patterns"
        })

    # Pattern 7: Holistic performance degradation pattern
    total_degradation_indicators = sum([
        1 if sql_avg_duration_ratio > 1.2 else 0,
        1 if stage_duration_ratio > 1.2 else 0,
        1 if efficiency_change < -5 else 0,
        1 if reliability_change == "degraded" else 0,
        1 if memory_spill_ratio > 1.5 else 0
    ])

    if total_degradation_indicators >= 3:
        cross_recommendations.append({
            "type": "holistic_performance_review",
            "priority": "critical",
            "issue": f"Multiple performance dimensions degraded ({total_degradation_indicators}/5 indicators)",
            "suggestion": "Comprehensive performance review needed. Consider reverting to App1 configuration as baseline and making incremental changes with performance validation"
        })

    return cross_recommendations


def _get_basic_app_info(app) -> Dict[str, Any]:
    """Extract basic application information for comparison"""
    return {
        "id": app.id,
        "name": app.name,
        "cores_granted": getattr(app, 'cores_granted', None),
        "max_cores": getattr(app, 'max_cores', None),
        "cores_per_executor": getattr(app, 'cores_per_executor', None),
        "memory_per_executor_mb": getattr(app, 'memory_per_executor_mb', None),
        "max_executors": getattr(app, 'max_executors', None),
    }


def _calculate_job_stats(jobs) -> Dict[str, Any]:
    """
    Calculate job duration statistics.
    Extracted from compare_job_performance for reuse.

    Args:
        jobs: List of job data

    Returns:
        Dictionary containing job statistics
    """
    if not jobs:
        return {"count": 0, "total_duration": 0, "avg_duration": 0}

    completed_jobs = [j for j in jobs if j.completion_time and j.submission_time]
    if not completed_jobs:
        return {"count": len(jobs), "total_duration": 0, "avg_duration": 0}

    durations = [
        (j.completion_time - j.submission_time).total_seconds()
        for j in completed_jobs
    ]

    return {
        "count": len(jobs),
        "completed_count": len(completed_jobs),
        "total_duration": sum(durations),
        "avg_duration": sum(durations) / len(durations),
        "min_duration": min(durations),
        "max_duration": max(durations),
    }


def _analyze_executor_performance_patterns(
    executor_summary1: Dict[str, Any],
    executor_summary2: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Analyze executor performance patterns and generate comparative insights.

    Args:
        executor_summary1: Executor summary from first application
        executor_summary2: Executor summary from second application

    Returns:
        Dictionary containing executor performance analysis and insights
    """
    if not executor_summary1 and not executor_summary2:
        return {"analysis": "No executor data available for comparison"}

    def analyze_executor_group(executors: Dict[str, Any]) -> Dict[str, Any]:
        if not executors:
            return {
                "total_executors": 0,
                "avg_task_time": 0,
                "avg_failed_tasks": 0,
                "avg_succeeded_tasks": 0,
                "total_memory_spilled": 0,
                "total_shuffle_read": 0,
                "total_shuffle_write": 0,
                "executors_with_failures": 0,
                "executors_with_spill": 0
            }

        task_times = []
        failed_tasks = []
        succeeded_tasks = []
        memory_spilled = []
        shuffle_reads = []
        shuffle_writes = []

        executors_with_failures = 0
        executors_with_spill = 0

        # Check if this is already aggregated data from get_executor_summary
        if isinstance(executors, dict) and "total_executors" in executors:
            # This is aggregated summary data
            return {
                "total_executors": executors.get("total_executors", 0),
                "avg_task_time": executors.get("total_duration", 0) / max(executors.get("completed_tasks", 1), 1),
                "avg_failed_tasks": executors.get("failed_tasks", 0) / max(executors.get("total_executors", 1), 1),
                "avg_succeeded_tasks": executors.get("completed_tasks", 0) / max(executors.get("total_executors", 1), 1),
                "total_memory_spilled": executors.get("memory_used", 0),
                "total_shuffle_read": executors.get("total_shuffle_read", 0),
                "total_shuffle_write": executors.get("total_shuffle_write", 0),
                "executors_with_failures": 1 if executors.get("failed_tasks", 0) > 0 else 0,
                "executors_with_spill": 1 if executors.get("memory_used", 0) > 0 else 0,
                "task_efficiency": executors.get("completed_tasks", 0) / max(executors.get("completed_tasks", 0) + executors.get("failed_tasks", 0), 1)
            }

        # This is individual executor data (original format)
        for executor_id, metrics in executors.items():
            # Safely access attributes with getattr or direct dict access
            if hasattr(metrics, 'task_time') and metrics.task_time:
                task_times.append(metrics.task_time)
            elif isinstance(metrics, dict) and metrics.get('task_time'):
                task_times.append(metrics['task_time'])

            if hasattr(metrics, 'failed_tasks') and metrics.failed_tasks:
                failed_tasks.append(metrics.failed_tasks)
                if metrics.failed_tasks > 0:
                    executors_with_failures += 1
            elif isinstance(metrics, dict) and metrics.get('failed_tasks'):
                failed_tasks.append(metrics['failed_tasks'])
                if metrics['failed_tasks'] > 0:
                    executors_with_failures += 1

            if hasattr(metrics, 'succeeded_tasks') and metrics.succeeded_tasks:
                succeeded_tasks.append(metrics.succeeded_tasks)
            elif isinstance(metrics, dict) and metrics.get('succeeded_tasks'):
                succeeded_tasks.append(metrics['succeeded_tasks'])

            if hasattr(metrics, 'memory_bytes_spilled') and metrics.memory_bytes_spilled:
                memory_spilled.append(metrics.memory_bytes_spilled)
                if metrics.memory_bytes_spilled > 0:
                    executors_with_spill += 1
            elif isinstance(metrics, dict) and metrics.get('memory_bytes_spilled'):
                memory_spilled.append(metrics['memory_bytes_spilled'])
                if metrics['memory_bytes_spilled'] > 0:
                    executors_with_spill += 1

            if hasattr(metrics, 'shuffle_read') and metrics.shuffle_read:
                shuffle_reads.append(metrics.shuffle_read)
            elif isinstance(metrics, dict) and metrics.get('shuffle_read'):
                shuffle_reads.append(metrics['shuffle_read'])

            if hasattr(metrics, 'shuffle_write') and metrics.shuffle_write:
                shuffle_writes.append(metrics.shuffle_write)
            elif isinstance(metrics, dict) and metrics.get('shuffle_write'):
                shuffle_writes.append(metrics['shuffle_write'])

        return {
            "total_executors": len(executors),
            "avg_task_time": statistics.mean(task_times) if task_times else 0,
            "avg_failed_tasks": statistics.mean(failed_tasks) if failed_tasks else 0,
            "avg_succeeded_tasks": statistics.mean(succeeded_tasks) if succeeded_tasks else 0,
            "total_memory_spilled": sum(memory_spilled),
            "total_shuffle_read": sum(shuffle_reads),
            "total_shuffle_write": sum(shuffle_writes),
            "executors_with_failures": executors_with_failures,
            "executors_with_spill": executors_with_spill,
            "task_efficiency": statistics.mean([s / (s + f) for s, f in zip(succeeded_tasks, failed_tasks, strict=False) if (s + f) > 0]) if succeeded_tasks and failed_tasks else 1.0
        }

    app1_analysis = analyze_executor_group(executor_summary1)
    app2_analysis = analyze_executor_group(executor_summary2)

    # Calculate comparison metrics
    comparison_analysis = {}

    if app1_analysis["total_executors"] > 0 and app2_analysis["total_executors"] > 0:
        comparison_analysis = {
            "executor_count_ratio": app2_analysis["total_executors"] / app1_analysis["total_executors"],
            "task_time_ratio": app2_analysis["avg_task_time"] / max(app1_analysis["avg_task_time"], 1),
            "failure_rate_ratio": app2_analysis["avg_failed_tasks"] / max(app1_analysis["avg_failed_tasks"], 1),
            "memory_spill_ratio": app2_analysis["total_memory_spilled"] / max(app1_analysis["total_memory_spilled"], 1),
            "shuffle_efficiency_comparison": {
                "read_ratio": app2_analysis["total_shuffle_read"] / max(app1_analysis["total_shuffle_read"], 1),
                "write_ratio": app2_analysis["total_shuffle_write"] / max(app1_analysis["total_shuffle_write"], 1)
            },
            "reliability_comparison": {
                "app1_failure_percentage": (app1_analysis["executors_with_failures"] / max(app1_analysis["total_executors"], 1)) * 100,
                "app2_failure_percentage": (app2_analysis["executors_with_failures"] / max(app2_analysis["total_executors"], 1)) * 100
            }
        }

    # Generate insights and recommendations
    insights = []
    recommendations = []

    if comparison_analysis:
        # Task efficiency insights
        if comparison_analysis["task_time_ratio"] > 1.5:
            insights.append("App2 executors are significantly slower in task execution")
            recommendations.append("Investigate executor resource allocation and task distribution in App2")
        elif comparison_analysis["task_time_ratio"] < 0.67:
            insights.append("App2 executors are significantly faster in task execution")

        # Memory efficiency insights
        if comparison_analysis["memory_spill_ratio"] > 2.0:
            insights.append(f"App2 has {comparison_analysis['memory_spill_ratio']:.1f}x more memory spill per executor")
            recommendations.append("Consider increasing executor memory allocation in App2")

        # Reliability insights
        app1_failure_pct = comparison_analysis["reliability_comparison"]["app1_failure_percentage"]
        app2_failure_pct = comparison_analysis["reliability_comparison"]["app2_failure_percentage"]

        if app2_failure_pct > app1_failure_pct * 2:
            insights.append(f"App2 has {app2_failure_pct:.1f}% executors with failures vs {app1_failure_pct:.1f}% in App1")
            recommendations.append("Investigate infrastructure issues affecting App2 executors")

    return {
        "app1_executor_metrics": app1_analysis,
        "app2_executor_metrics": app2_analysis,
        "comparative_analysis": comparison_analysis,
        "insights": insights,
        "recommendations": recommendations
    }


def get_stage_summary(
    client, app_id: str, stage: StageData
) -> Optional[Dict[str, Any]]:
    """
    Get stage task summary with graceful fallback.

    Args:
        client: Spark REST client
        app_id: Application ID
        stage: StageData object

    Returns:
        Stage summary dict or None if unavailable
    """
    # First try: get detailed task summary via API
    try:
        task_summary = stage.task

        return {
            "quantiles": task_summary.quantiles,
            "duration": task_summary.duration,
            "executor_run_time": task_summary.executor_run_time,
            "executor_cpu_time": task_summary.executor_cpu_time,
            "jvm_gc_time": task_summary.jvm_gc_time,
            "memory_bytes_spilled": task_summary.memory_bytes_spilled,
            "disk_bytes_spilled": task_summary.disk_bytes_spilled,
            "shuffle_read_bytes": task_summary.shuffle_read_metrics.read_bytes if task_summary.shuffle_read_metrics else None,
            "shuffle_write_bytes": task_summary.shuffle_write_bytes,
            "input_bytes": task_summary.input_metrics.bytes_read if task_summary.input_metrics else None,
            "output_bytes": task_summary.output_metrics.bytes_written if task_summary.output_metrics else None
        }
    except Exception:
        # Second try: use basic stage data as fallback
        try:
            return {
                "stage_id": stage.stage_id,
                "stage_name": stage.name,
                "status": stage.status,
                "num_tasks": stage.num_tasks,
                "num_failed_tasks": stage.num_failed_tasks,
                "executor_run_time": stage.executor_run_time,
                "memory_bytes_spilled": getattr(stage, 'memory_bytes_spilled', None),
                "disk_bytes_spilled": getattr(stage, 'disk_bytes_spilled', None),
                "shuffle_read_bytes": getattr(stage, 'shuffle_read_bytes', None),
                "shuffle_write_bytes": getattr(stage, 'shuffle_write_bytes', None),
                "input_bytes": getattr(stage, 'input_bytes', None),
                "output_bytes": getattr(stage, 'output_bytes', None),
                "fallback_note": "Using basic stage data - detailed task metrics unavailable"
            }
        except Exception:
            return None


@mcp.tool()
def compare_app_resources(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None
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
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    try:
        # Get application info
        app1 = client.get_application(app_id1)
        app2 = client.get_application(app_id2)

        app1_info = _get_basic_app_info(app1)
        app2_info = _get_basic_app_info(app2)

        # Calculate resource ratios and comparisons
        resource_comparison = {}

        if app1_info["cores_granted"] and app2_info["cores_granted"]:
            resource_comparison["cores_granted_ratio"] = app2_info["cores_granted"] / app1_info["cores_granted"]

        if app1_info["max_cores"] and app2_info["max_cores"]:
            resource_comparison["max_cores_ratio"] = app2_info["max_cores"] / app1_info["max_cores"]

        if app1_info["memory_per_executor_mb"] and app2_info["memory_per_executor_mb"]:
            resource_comparison["memory_per_executor_ratio"] = app2_info["memory_per_executor_mb"] / app1_info["memory_per_executor_mb"]

        if app1_info["max_executors"] and app2_info["max_executors"]:
            resource_comparison["max_executors_ratio"] = app2_info["max_executors"] / app1_info["max_executors"]

        # Generate resource-specific recommendations
        recommendations = []

        # Cores analysis
        if resource_comparison.get("cores_granted_ratio", 1) > 2:
            recommendations.append({
                "type": "resource_scaling",
                "priority": "medium",
                "issue": f"App2 uses {resource_comparison['cores_granted_ratio']:.1f}x more cores than App1",
                "suggestion": "Consider if App2 needs this level of CPU resources or if App1 is under-provisioned"
            })
        elif resource_comparison.get("cores_granted_ratio", 1) < 0.5:
            recommendations.append({
                "type": "resource_scaling",
                "priority": "high",
                "issue": f"App2 uses {resource_comparison['cores_granted_ratio']:.1f}x fewer cores than App1",
                "suggestion": "App2 may be CPU-constrained - consider increasing core allocation"
            })

        # Memory analysis
        if resource_comparison.get("memory_per_executor_ratio", 1) > 2:
            recommendations.append({
                "type": "memory_allocation",
                "priority": "medium",
                "issue": f"App2 allocates {resource_comparison['memory_per_executor_ratio']:.1f}x more memory per executor",
                "suggestion": "Verify if App2's workload requires this memory or if it's over-provisioned"
            })
        elif resource_comparison.get("memory_per_executor_ratio", 1) < 0.5:
            recommendations.append({
                "type": "memory_allocation",
                "priority": "high",
                "issue": f"App2 has {resource_comparison['memory_per_executor_ratio']:.1f}x less memory per executor",
                "suggestion": "App2 may experience memory pressure - consider increasing executor memory"
            })

        return {
            "applications": {
                "app1": app1_info,
                "app2": app2_info
            },
            "resource_comparison": resource_comparison,
            "recommendations": recommendations
        }

    except Exception as e:
        return {
            "error": f"Failed to compare app resources: {str(e)}",
            "applications": {
                "app1": {"id": app_id1},
                "app2": {"id": app_id2}
            }
        }


@mcp.tool()
def compare_app_executors(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare executor-level performance metrics between two Spark applications.

    Focuses specifically on executor utilization, memory usage, GC performance,
    and task completion patterns without detailed stage-by-stage analysis.

    Args:
        app_id1: First Spark application ID (baseline)
        app_id2: Second Spark application ID (comparison target)
        server: Optional Spark History Server name

    Returns:
        Dict containing executor performance comparison, efficiency ratios, and recommendations
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    try:
        # Get executor summaries for both applications
        exec_summary1 = get_executor_summary(app_id1, server)
        exec_summary2 = get_executor_summary(app_id2, server)

        if not exec_summary1 or not exec_summary2:
            return {
                "error": "Could not retrieve executor summaries for one or both applications",
                "applications": {
                    "app1": {"id": app_id1, "executor_summary": exec_summary1 is not None},
                    "app2": {"id": app_id2, "executor_summary": exec_summary2 is not None}
                }
            }

        # Calculate executor performance ratios
        executor_comparison = {
            "executor_count_ratio": exec_summary2.get("total_executors", 0) / max(exec_summary1.get("total_executors", 1), 1),
            "memory_usage_ratio": exec_summary2.get("memory_used", 0) / max(exec_summary1.get("memory_used", 1), 1),
            "task_completion_ratio": exec_summary2.get("completed_tasks", 0) / max(exec_summary1.get("completed_tasks", 1), 1),
            "gc_time_ratio": exec_summary2.get("total_gc_time", 0) / max(exec_summary1.get("total_gc_time", 1), 1),
            "active_tasks_ratio": exec_summary2.get("active_tasks", 0) / max(exec_summary1.get("active_tasks", 1), 1),
        }

        # Calculate efficiency metrics
        efficiency_metrics = {}

        # Task completion efficiency (tasks per executor)
        if exec_summary1.get("total_executors", 0) > 0:
            efficiency_metrics["app1_tasks_per_executor"] = exec_summary1.get("completed_tasks", 0) / exec_summary1["total_executors"]
        if exec_summary2.get("total_executors", 0) > 0:
            efficiency_metrics["app2_tasks_per_executor"] = exec_summary2.get("completed_tasks", 0) / exec_summary2["total_executors"]

        # Memory utilization efficiency
        if exec_summary1.get("memory_used", 0) > 0 and exec_summary1.get("completed_tasks", 0) > 0:
            efficiency_metrics["app1_tasks_per_mb"] = exec_summary1["completed_tasks"] / (exec_summary1["memory_used"] / (1024 * 1024))
        if exec_summary2.get("memory_used", 0) > 0 and exec_summary2.get("completed_tasks", 0) > 0:
            efficiency_metrics["app2_tasks_per_mb"] = exec_summary2["completed_tasks"] / (exec_summary2["memory_used"] / (1024 * 1024))

        # Generate executor-specific recommendations
        recommendations = []

        # Executor scaling analysis
        if executor_comparison["executor_count_ratio"] > 1.5:
            recommendations.append({
                "type": "executor_scaling",
                "priority": "medium",
                "issue": f"App2 uses {executor_comparison['executor_count_ratio']:.1f}x more executors than App1",
                "suggestion": "Evaluate if App2 needs this level of parallelism or if resources can be optimized"
            })
        elif executor_comparison["executor_count_ratio"] < 0.7:
            recommendations.append({
                "type": "executor_scaling",
                "priority": "high",
                "issue": f"App2 uses {executor_comparison['executor_count_ratio']:.1f}x fewer executors than App1",
                "suggestion": "App2 may benefit from increased parallelism - consider scaling up executors"
            })

        # Memory efficiency analysis
        if executor_comparison["memory_usage_ratio"] > 2.0:
            recommendations.append({
                "type": "memory_efficiency",
                "priority": "medium",
                "issue": f"App2 uses {executor_comparison['memory_usage_ratio']:.1f}x more memory than App1",
                "suggestion": "Review App2's memory usage patterns - may indicate inefficient data structures or caching"
            })

        # GC performance analysis
        if executor_comparison["gc_time_ratio"] > 2.0:
            recommendations.append({
                "type": "gc_performance",
                "priority": "high",
                "issue": f"App2 has {executor_comparison['gc_time_ratio']:.1f}x more GC time than App1",
                "suggestion": "App2 experiencing memory pressure - consider increasing executor memory or optimizing data structures"
            })

        # Task efficiency analysis
        if efficiency_metrics.get("app1_tasks_per_executor", 0) > 0 and efficiency_metrics.get("app2_tasks_per_executor", 0) > 0:
            task_efficiency_ratio = efficiency_metrics["app2_tasks_per_executor"] / efficiency_metrics["app1_tasks_per_executor"]
            if task_efficiency_ratio < 0.5:
                recommendations.append({
                    "type": "task_efficiency",
                    "priority": "medium",
                    "issue": f"App2 processes {task_efficiency_ratio:.1f}x fewer tasks per executor than App1",
                    "suggestion": "App2's executors may be underutilized - check for data skew or resource bottlenecks"
                })

        return {
            "applications": {
                "app1": {
                    "id": app_id1,
                    "executor_metrics": exec_summary1
                },
                "app2": {
                    "id": app_id2,
                    "executor_metrics": exec_summary2
                }
            },
            "executor_comparison": executor_comparison,
            "efficiency_metrics": efficiency_metrics,
            "recommendations": recommendations
        }

    except Exception as e:
        return {
            "error": f"Failed to compare executor performance: {str(e)}",
            "applications": {
                "app1": {"id": app_id1},
                "app2": {"id": app_id2}
            }
        }


@mcp.tool()
def compare_app_jobs(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None
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
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    try:
        # Get job data for both applications
        jobs1 = client.list_jobs(app_id=app_id1)
        jobs2 = client.list_jobs(app_id=app_id2)

        # Calculate job statistics
        job_stats1 = _calculate_job_stats(jobs1)
        job_stats2 = _calculate_job_stats(jobs2)

        # Calculate job performance ratios
        job_comparison = {
            "job_count_ratio": job_stats2["count"] / max(job_stats1["count"], 1),
            "avg_duration_ratio": job_stats2["avg_duration"] / max(job_stats1["avg_duration"], 1) if job_stats1["avg_duration"] > 0 else 0,
            "total_duration_ratio": job_stats2["total_duration"] / max(job_stats1["total_duration"], 1) if job_stats1["total_duration"] > 0 else 0,
            "completion_rate_ratio": (job_stats2["completed_count"] / max(job_stats2["count"], 1)) / max((job_stats1["completed_count"] / max(job_stats1["count"], 1)), 0.01),
        }

        # Job success rate analysis
        job1_success_rate = job_stats1["completed_count"] / max(job_stats1["count"], 1)
        job2_success_rate = job_stats2["completed_count"] / max(job_stats2["count"], 1)

        # Job timing analysis
        timing_analysis = {}
        if job_stats1["avg_duration"] > 0 and job_stats2["avg_duration"] > 0:
            timing_analysis["avg_duration_difference_seconds"] = job_stats2["avg_duration"] - job_stats1["avg_duration"]
            timing_analysis["avg_duration_improvement_percent"] = ((job_stats1["avg_duration"] - job_stats2["avg_duration"]) / job_stats1["avg_duration"]) * 100

        # Generate job-specific recommendations
        recommendations = []

        # Job count analysis
        if job_comparison["job_count_ratio"] > 2.0:
            recommendations.append({
                "type": "job_complexity",
                "priority": "medium",
                "issue": f"App2 has {job_comparison['job_count_ratio']:.1f}x more jobs than App1",
                "suggestion": "App2 may have more complex workflow or different job decomposition strategy"
            })

        # Duration performance analysis
        if job_comparison["avg_duration_ratio"] > 1.5:
            recommendations.append({
                "type": "job_performance",
                "priority": "high",
                "issue": f"App2 jobs are {job_comparison['avg_duration_ratio']:.1f}x slower on average than App1",
                "suggestion": "Investigate job-level performance bottlenecks in App2 - may need optimization or resource scaling"
            })
        elif job_comparison["avg_duration_ratio"] < 0.7:
            recommendations.append({
                "type": "job_performance",
                "priority": "low",
                "issue": f"App2 jobs are {1/job_comparison['avg_duration_ratio']:.1f}x faster than App1",
                "suggestion": "App2 shows better job-level performance - consider applying similar optimizations to App1"
            })

        # Success rate analysis
        if job2_success_rate < job1_success_rate - 0.1:  # More than 10% difference
            recommendations.append({
                "type": "job_reliability",
                "priority": "high",
                "issue": f"App2 has {(job1_success_rate - job2_success_rate)*100:.1f}% lower job success rate",
                "suggestion": "App2 experiencing more job failures - investigate error patterns and resource issues"
            })

        # Total execution time analysis
        if job_comparison["total_duration_ratio"] > 2.0:
            recommendations.append({
                "type": "overall_efficiency",
                "priority": "medium",
                "issue": f"App2 takes {job_comparison['total_duration_ratio']:.1f}x longer total execution time",
                "suggestion": "App2 may benefit from better parallelization or resource optimization"
            })

        return {
            "applications": {
                "app1": {
                    "id": app_id1,
                    "job_stats": job_stats1,
                    "success_rate": job1_success_rate
                },
                "app2": {
                    "id": app_id2,
                    "job_stats": job_stats2,
                    "success_rate": job2_success_rate
                }
            },
            "job_comparison": job_comparison,
            "timing_analysis": timing_analysis,
            "recommendations": recommendations
        }

    except Exception as e:
        return {
            "error": f"Failed to compare job performance: {str(e)}",
            "applications": {
                "app1": {"id": app_id1},
                "app2": {"id": app_id2}
            }
        }


@mcp.tool()
def compare_app_stages_aggregated(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare aggregated stage-level metrics between two Spark applications.

    Focuses on overall stage performance patterns, I/O volumes, shuffle operations,
    and data processing efficiency without individual stage-by-stage analysis.

    Args:
        app_id1: First Spark application ID (baseline)
        app_id2: Second Spark application ID (comparison target)
        server: Optional Spark History Server name

    Returns:
        Dict containing aggregated stage comparison, I/O analysis, and recommendations
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    try:
        # Get stages from both applications - try with summaries first, fallback if needed
        try:
            stages1 = client.list_stages(app_id=app_id1, with_summaries=True)
            stages2 = client.list_stages(app_id=app_id2, with_summaries=True)
        except Exception as e:
            if "executorMetricsDistributions.peakMemoryMetrics.quantiles" in str(e):
                stages1 = client.list_stages(app_id=app_id1, with_summaries=False)
                stages2 = client.list_stages(app_id=app_id2, with_summaries=False)
            else:
                raise e

        if not stages1 or not stages2:
            return {
                "error": "No stages found in one or both applications",
                "applications": {
                    "app1": {"id": app_id1, "stage_count": len(stages1) if stages1 else 0},
                    "app2": {"id": app_id2, "stage_count": len(stages2) if stages2 else 0}
                }
            }

        # Calculate aggregated stage metrics
        stage_metrics1 = _calculate_aggregated_stage_metrics(stages1)
        stage_metrics2 = _calculate_aggregated_stage_metrics(stages2)

        # Calculate stage performance ratios
        stage_comparison = {
            "stage_count_ratio": stage_metrics2["total_stages"] / max(stage_metrics1["total_stages"], 1),
            "duration_ratio": stage_metrics2["total_stage_duration"] / max(stage_metrics1["total_stage_duration"], 1),
            "executor_runtime_ratio": stage_metrics2["total_executor_run_time"] / max(stage_metrics1["total_executor_run_time"], 1),
            "memory_spill_ratio": stage_metrics2["total_memory_spilled"] / max(stage_metrics1["total_memory_spilled"], 1) if stage_metrics1["total_memory_spilled"] > 0 else 0,
            "shuffle_read_ratio": stage_metrics2["total_shuffle_read_bytes"] / max(stage_metrics1["total_shuffle_read_bytes"], 1) if stage_metrics1["total_shuffle_read_bytes"] > 0 else 0,
            "shuffle_write_ratio": stage_metrics2["total_shuffle_write_bytes"] / max(stage_metrics1["total_shuffle_write_bytes"], 1) if stage_metrics1["total_shuffle_write_bytes"] > 0 else 0,
            "input_ratio": stage_metrics2["total_input_bytes"] / max(stage_metrics1["total_input_bytes"], 1) if stage_metrics1["total_input_bytes"] > 0 else 0,
            "output_ratio": stage_metrics2["total_output_bytes"] / max(stage_metrics1["total_output_bytes"], 1) if stage_metrics1["total_output_bytes"] > 0 else 0,
            "task_failure_ratio": stage_metrics2["total_failed_tasks"] / max(stage_metrics1["total_failed_tasks"], 1) if stage_metrics1["total_failed_tasks"] > 0 else 0,
        }

        # Data processing efficiency analysis
        efficiency_analysis = {}

        # Tasks per stage efficiency
        if stage_metrics1["total_stages"] > 0:
            efficiency_analysis["app1_avg_tasks_per_stage"] = stage_metrics1["total_tasks"] / stage_metrics1["total_stages"]
        if stage_metrics2["total_stages"] > 0:
            efficiency_analysis["app2_avg_tasks_per_stage"] = stage_metrics2["total_tasks"] / stage_metrics2["total_stages"]

        # Data throughput analysis (bytes processed per second)
        if stage_metrics1["total_stage_duration"] > 0:
            efficiency_analysis["app1_input_throughput_bps"] = stage_metrics1["total_input_bytes"] / stage_metrics1["total_stage_duration"]
            efficiency_analysis["app1_output_throughput_bps"] = stage_metrics1["total_output_bytes"] / stage_metrics1["total_stage_duration"]

        if stage_metrics2["total_stage_duration"] > 0:
            efficiency_analysis["app2_input_throughput_bps"] = stage_metrics2["total_input_bytes"] / stage_metrics2["total_stage_duration"]
            efficiency_analysis["app2_output_throughput_bps"] = stage_metrics2["total_output_bytes"] / stage_metrics2["total_stage_duration"]

        # Generate stage-specific recommendations
        recommendations = []

        # Stage complexity analysis
        if stage_comparison["stage_count_ratio"] > 1.5:
            recommendations.append({
                "type": "stage_complexity",
                "priority": "medium",
                "issue": f"App2 has {stage_comparison['stage_count_ratio']:.1f}x more stages than App1",
                "suggestion": "App2 has more complex execution plan - may indicate different algorithm or less optimized query planning"
            })

        # Performance analysis
        if stage_comparison["duration_ratio"] > 1.5:
            recommendations.append({
                "type": "stage_performance",
                "priority": "high",
                "issue": f"App2 stages take {stage_comparison['duration_ratio']:.1f}x longer total time than App1",
                "suggestion": "App2 experiencing stage-level performance issues - investigate resource allocation or data skew"
            })

        # Memory spill analysis
        if stage_comparison["memory_spill_ratio"] > 2.0:
            recommendations.append({
                "type": "memory_pressure",
                "priority": "high",
                "issue": f"App2 has {stage_comparison['memory_spill_ratio']:.1f}x more memory spill than App1",
                "suggestion": "App2 experiencing memory pressure - increase executor memory or optimize data structures"
            })

        # Shuffle efficiency analysis
        if stage_comparison["shuffle_read_ratio"] > 2.0 or stage_comparison["shuffle_write_ratio"] > 2.0:
            recommendations.append({
                "type": "shuffle_efficiency",
                "priority": "medium",
                "issue": f"App2 has significantly more shuffle operations (read: {stage_comparison['shuffle_read_ratio']:.1f}x, write: {stage_comparison['shuffle_write_ratio']:.1f}x)",
                "suggestion": "App2 may have data skew or inefficient partitioning - consider repartitioning strategies"
            })

        # Task failure analysis
        if stage_comparison["task_failure_ratio"] > 2.0:
            recommendations.append({
                "type": "reliability",
                "priority": "high",
                "issue": f"App2 has {stage_comparison['task_failure_ratio']:.1f}x more task failures than App1",
                "suggestion": "App2 experiencing reliability issues - investigate infrastructure or data quality problems"
            })

        # Throughput efficiency analysis
        if (efficiency_analysis.get("app1_input_throughput_bps", 0) > 0 and
            efficiency_analysis.get("app2_input_throughput_bps", 0) > 0):
            throughput_ratio = efficiency_analysis["app2_input_throughput_bps"] / efficiency_analysis["app1_input_throughput_bps"]
            if throughput_ratio < 0.5:
                recommendations.append({
                    "type": "throughput_efficiency",
                    "priority": "medium",
                    "issue": f"App2 has {throughput_ratio:.1f}x lower input processing throughput than App1",
                    "suggestion": "App2's data processing efficiency is lower - check for I/O bottlenecks or resource constraints"
                })

        return {
            "applications": {
                "app1": {
                    "id": app_id1,
                    "stage_metrics": stage_metrics1
                },
                "app2": {
                    "id": app_id2,
                    "stage_metrics": stage_metrics2
                }
            },
            "stage_comparison": stage_comparison,
            "efficiency_analysis": efficiency_analysis,
            "recommendations": recommendations
        }

    except Exception as e:
        return {
            "error": f"Failed to compare aggregated stage performance: {str(e)}",
            "applications": {
                "app1": {"id": app_id1},
                "app2": {"id": app_id2}
            }
        }


@mcp.tool()
def compare_app_performance(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None,
    top_n: int = 3,
    similarity_threshold: float = 0.6,
    include_raw_data: bool = False
) -> Dict[str, Any]:
    """
    Comprehensive performance comparison between two Spark applications.

    Provides both high-level aggregated analysis and detailed stage-by-stage comparison.
    First analyzes overall application metrics (resources, jobs, executors, aggregated stages),
    then identifies the top N stages with the most significant time differences for deep-dive analysis.

    This merged analysis combines the capabilities of compare_job_performance and stage-level
    comparison to provide comprehensive performance insights and optimization recommendations.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server: Optional server name to use (uses default if not specified)
        top_n: Number of top stage differences to return for detailed analysis (default: 3)
        similarity_threshold: Minimum similarity for stage name matching (default: 0.6)
        include_raw_data: Include full raw metrics in output for debugging (default: False)

    Returns:
        Dictionary containing:
        - aggregated_overview: Application-level executor and stage metrics
        - stage_deep_dive: Top N stages with most time difference and detailed comparisons
        - recommendations: Enhanced recommendations covering both application and stage levels

        When include_raw_data=False (default): Streamlined output with processed comparisons only
        When include_raw_data=True: Includes additional raw metrics for detailed investigation
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    # Get application info
    app1 = client.get_application(app_id1)
    app2 = client.get_application(app_id2)

    stages1 = client.list_stages(app_id=app_id1, with_summaries=True)
    stages2 = client.list_stages(app_id=app_id2, with_summaries=True)

    if not stages1 or not stages2:
        return {
            "error": "No stages found in one or both applications",
            "applications": {
                "app1": {"id": app_id1, "name": app1.name, "stage_count": len(stages1)},
                "app2": {"id": app_id2, "name": app2.name, "stage_count": len(stages2)}
            }
        }

    # PHASE 1: AGGREGATED APPLICATION OVERVIEW
    # Use specialized comparison tools for aggregated overview
    try:
        executor_comparison = compare_app_executors(app_id1, app_id2, server)
    except Exception as e:
        executor_comparison = {"error": f"Failed to get executor comparison: {str(e)}"}

    try:
        stage_comparison = compare_app_stages_aggregated(app_id1, app_id2, server)
    except Exception as e:
        stage_comparison = {"error": f"Failed to get stage comparison: {str(e)}"}

    # Create streamlined aggregated overview using specialized tools
    aggregated_overview = {
        "executor_comparison": executor_comparison,
        "stage_comparison": stage_comparison
    }

    # Find matching stages between applications
    stage_matches = _find_matching_stages(stages1, stages2, similarity_threshold)

    if not stage_matches:
        return {
            "error": f"No matching stages found between applications (similarity threshold: {similarity_threshold})",
            "applications": {
                "app1": {"id": app_id1, "name": app1.name, "stage_count": len(stages1)},
                "app2": {"id": app_id2, "name": app2.name, "stage_count": len(stages2)}
            },
            "suggestion": "Try lowering the similarity_threshold parameter or check that applications are performing similar operations"
        }

    # Calculate time differences for matching stages
    stage_differences = []

    for stage1, stage2, similarity in stage_matches:
        duration1 = _calculate_stage_duration(stage1)
        duration2 = _calculate_stage_duration(stage2)

        if duration1 > 0 and duration2 > 0:
            time_diff = abs(duration2 - duration1)
            time_diff_percent = (time_diff / max(duration1, duration2)) * 100

            stage_differences.append({
                "stage1": stage1,
                "stage2": stage2,
                "similarity": similarity,
                "duration1": duration1,
                "duration2": duration2,
                "time_difference_seconds": time_diff,
                "time_difference_percent": time_diff_percent,
                "slower_app": "app1" if duration1 > duration2 else "app2"
            })

    if not stage_differences:
        return {
            "error": "No stages with calculable durations found",
            "applications": {
                "app1": {"id": app_id1, "name": app1.name},
                "app2": {"id": app_id2, "name": app2.name}
            },
            "matched_stages": len(stage_matches)
        }

    # Sort by time difference and get top N
    top_differences = sorted(
        stage_differences,
        key=lambda x: x["time_difference_seconds"],
        reverse=True
    )[:top_n]

    # Get detailed summaries for top different stages
    detailed_comparisons = []

    for diff in top_differences:
        stage1, stage2 = diff["stage1"], diff["stage2"]

        # Use compare_stages tool for detailed stage comparison
        try:
            detailed_stage_comparison = compare_stages(
                app_id1=app_id1,
                app_id2=app_id2,
                stage_id1=stage1.stage_id,
                stage_id2=stage2.stage_id,
                server=server,
                significance_threshold=0.2
            )
        except Exception as e:
            # Fallback to basic comparison if compare_stages fails
            detailed_stage_comparison = {
                "error": f"Failed to get detailed comparison: {str(e)}",
                "basic_info": {
                    "stage1": {"id": stage1.stage_id, "name": stage1.name},
                    "stage2": {"id": stage2.stage_id, "name": stage2.name}
                }
            }

        # Extract executor summaries with improved fallback logic
        def get_executor_summary_for_stage(stage, app_id):
            """Get executor summary for a stage with fallback options"""
            # First try: stage already has executor_summary (when with_summaries=True worked)
            if hasattr(stage, 'executor_summary') and stage.executor_summary:
                return stage.executor_summary

            # Second try: use get_executor_summary tool for the application
            # Note: This gives application-level executor summary, not stage-specific
            try:
                app_executor_summary = get_executor_summary(app_id, server)
                return app_executor_summary if app_executor_summary else {}
            except Exception:
                return {}

        executor_summary1 = get_executor_summary_for_stage(stage1, app_id1)
        executor_summary2 = get_executor_summary_for_stage(stage2, app_id2)

        # Build stage comparison with optional raw data
        stage_comparison = {
            "stage_name": stage1.name,
            "similarity_score": diff["similarity"],
            "app1_stage": {
                "stage_id": stage1.stage_id,
                "name": stage1.name,
                "status": stage1.status,
                "duration_seconds": diff["duration1"]
            },
            "app2_stage": {
                "stage_id": stage2.stage_id,
                "name": stage2.name,
                "status": stage2.status,
                "duration_seconds": diff["duration2"]
            },
            "time_difference": {
                "absolute_seconds": diff["time_difference_seconds"],
                "percentage": diff["time_difference_percent"],
                "slower_application": diff["slower_app"]
            },
            "detailed_stage_comparison": detailed_stage_comparison,
            "executor_analysis": _analyze_executor_performance_patterns(
                executor_summary1, executor_summary2
            )
        }

        # Conditionally add raw data for debugging/investigation
        if include_raw_data:
            stage_comparison["raw_data"] = {
                "app1_stage_metrics": {
                    "num_tasks": stage1.num_tasks,
                    "num_failed_tasks": stage1.num_failed_tasks,
                    "executor_run_time_ms": stage1.executor_run_time,
                    "memory_spilled_bytes": stage1.memory_bytes_spilled,
                    "disk_spilled_bytes": stage1.disk_bytes_spilled,
                    "shuffle_read_bytes": stage1.shuffle_read_bytes,
                    "shuffle_write_bytes": stage1.shuffle_write_bytes,
                    "input_bytes": stage1.input_bytes,
                    "output_bytes": stage1.output_bytes
                },
                "app2_stage_metrics": {
                    "num_tasks": stage2.num_tasks,
                    "num_failed_tasks": stage2.num_failed_tasks,
                    "executor_run_time_ms": stage2.executor_run_time,
                    "memory_spilled_bytes": stage2.memory_bytes_spilled,
                    "disk_spilled_bytes": stage2.disk_bytes_spilled,
                    "shuffle_read_bytes": stage2.shuffle_read_bytes,
                    "shuffle_write_bytes": stage2.shuffle_write_bytes,
                    "input_bytes": stage2.input_bytes,
                    "output_bytes": stage2.output_bytes
                }
            }

        detailed_comparisons.append(stage_comparison)

    # Generate summary statistics
    total_time_diff = sum(d["time_difference_seconds"] for d in top_differences)
    avg_time_diff = total_time_diff / len(top_differences) if top_differences else 0
    max_time_diff = max(d["time_difference_seconds"] for d in top_differences) if top_differences else 0

    # Enhanced recommendations combining both application and stage-level insights
    recommendations = []

    # APPLICATION-LEVEL RECOMMENDATIONS
    # Resource allocation differences
    if app1.cores_granted and app2.cores_granted:
        core_ratio = app2.cores_granted / app1.cores_granted
        if core_ratio > 1.5 or core_ratio < 0.67:  # >50% difference
            slower_app = "app1" if core_ratio > 1.5 else "app2"
            faster_app = "app2" if core_ratio > 1.5 else "app1"
            recommendations.append({
                "type": "resource_allocation",
                "priority": "medium",
                "issue": f"Significant core allocation difference (ratio: {core_ratio:.2f})",
                "suggestion": f"Consider equalizing core allocation - {slower_app} has fewer cores than {faster_app}"
            })

    # Memory allocation differences
    if app1.memory_per_executor_mb and app2.memory_per_executor_mb:
        memory_ratio = app2.memory_per_executor_mb / app1.memory_per_executor_mb
        if memory_ratio > 1.5 or memory_ratio < 0.67:  # >50% difference
            recommendations.append({
                "type": "resource_allocation",
                "priority": "medium",
                "issue": f"Significant memory per executor difference (ratio: {memory_ratio:.2f})",
                "suggestion": "Review memory allocation settings between applications"
            })

    # Extract recommendations from specialized comparison tools
    # Executor efficiency recommendations
    if (aggregated_overview["executor_comparison"] and
        isinstance(aggregated_overview["executor_comparison"], dict) and
        "recommendations" in aggregated_overview["executor_comparison"]):
        recommendations.extend(aggregated_overview["executor_comparison"]["recommendations"])

    # Stage-level aggregated recommendations
    if (aggregated_overview["stage_comparison"] and
        isinstance(aggregated_overview["stage_comparison"], dict) and
        "recommendations" in aggregated_overview["stage_comparison"]):
        recommendations.extend(aggregated_overview["stage_comparison"]["recommendations"])

    # STAGE-LEVEL RECOMMENDATIONS (existing logic)
    # Check for stages with large time differences
    large_diff_threshold = 60  # seconds
    large_diff_stages = [d for d in top_differences if d["time_difference_seconds"] > large_diff_threshold]

    if large_diff_stages:
        recommendations.append({
            "type": "stage_performance",
            "priority": "high",
            "issue": f"Found {len(large_diff_stages)} stages with >60s time difference",
            "suggestion": f"Investigate {'app1' if large_diff_stages[0]['slower_app'] == 'app1' else 'app2'} for potential performance issues in specific stages"
        })

    # Check for memory spilling differences at stage level
    spill_diff_stages = []
    for comp in detailed_comparisons:
        app1_spill = comp["app1_stage"].get("memory_spilled_bytes", 0) or 0
        app2_spill = comp["app2_stage"].get("memory_spilled_bytes", 0) or 0

        if abs(app1_spill - app2_spill) > 100 * 1024 * 1024:  # >100MB difference
            spill_diff_stages.append(comp)

    if spill_diff_stages:
        recommendations.append({
            "type": "stage_memory",
            "priority": "medium",
            "issue": f"Found {len(spill_diff_stages)} stages with significant memory spill differences",
            "suggestion": "Check memory allocation and partitioning strategies for specific stages"
        })

    # Environment and configuration comparison
    environment_comparison = _compare_environments(client, app_id1, app_id2)

    # SQL execution plans comparison
    sql_plans_comparison = _compare_sql_execution_plans(client, app_id1, app_id2)

    # Merge SQL recommendations with existing recommendations
    if sql_plans_comparison.get("sql_recommendations"):
        recommendations.extend(sql_plans_comparison["sql_recommendations"])

    # Sort recommendations by priority
    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    recommendations.sort(key=lambda x: priority_order.get(x.get("priority", "low"), 3))

    return {
        "applications": {
            "app1": {"id": app_id1, "name": app1.name},
            "app2": {"id": app_id2, "name": app2.name}
        },
        "aggregated_overview": aggregated_overview,
        "environment_comparison": environment_comparison,
        "sql_execution_plans": sql_plans_comparison,
        "stage_deep_dive": {
            "analysis_parameters": {
                "top_n": top_n,
                "similarity_threshold": similarity_threshold,
                "total_stages_app1": len(stages1),
                "total_stages_app2": len(stages2),
                "matched_stages": len(stage_matches)
            },
            "top_stage_differences": detailed_comparisons,
            "stage_summary": {
                "total_time_difference_seconds": total_time_diff,
                "average_time_difference_seconds": avg_time_diff,
                "maximum_time_difference_seconds": max_time_diff,
                "stages_analyzed": len(top_differences)
            }
        },
        "recommendations": recommendations
    }


@mcp.tool()
def compare_stages(
    app_id1: str,
    app_id2: str,
    stage_id1: int,
    stage_id2: int,
    server: Optional[str] = None,
    significance_threshold: float = 0.2
) -> Dict[str, Any]:
    """
    Compare specific stages between two Spark applications.

    Focuses on median and max values from distributions, showing only metrics
    with significant differences to reduce noise and highlight actionable insights.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        stage_id1: Stage ID from first application
        stage_id2: Stage ID from second application
        server: Optional server name to use (uses default if not specified)
        significance_threshold: Minimum difference threshold to include metric (default: 0.2)

    Returns:
        Dictionary containing stage comparison with significant differences only
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    try:
        # Get stage data with summaries
        stage1 = client.get_stage_attempt(
            app_id=app_id1,
            stage_id=stage_id1,
            attempt_id=0,
            details=False,
            with_summaries=True
        )
        stage2 = client.get_stage_attempt(
            app_id=app_id2,
            stage_id=stage_id2,
            attempt_id=0,
            details=False,
            with_summaries=True
        )

        # Get task metric distributions
        try:
            task_dist1 = client.get_stage_task_summary(
                app_id=app_id1, stage_id=stage_id1, attempt_id=0
            )
            stage1.task_metrics_distributions = task_dist1
        except Exception:
            pass

        try:
            task_dist2 = client.get_stage_task_summary(
                app_id=app_id2, stage_id=stage_id2, attempt_id=0
            )
            stage2.task_metrics_distributions = task_dist2
        except Exception:
            pass

    except Exception as e:
        return {
            "error": f"Failed to retrieve stage data: {str(e)}",
            "stages": {
                "stage1": {"app_id": app_id1, "stage_id": stage_id1},
                "stage2": {"app_id": app_id2, "stage_id": stage_id2}
            }
        }

    # Build comparison result
    result = {
        "stage_comparison": {
            "stage1": {
                "app_id": app_id1,
                "stage_id": stage_id1,
                "name": stage1.name,
                "status": stage1.status
            },
            "stage2": {
                "app_id": app_id2,
                "stage_id": stage_id2,
                "name": stage2.name,
                "status": stage2.status
            }
        },
        "significant_differences": {},
        "summary": {
            "significance_threshold": significance_threshold,
            "total_differences_found": 0
        }
    }

    # Helper function to calculate significance and format comparison
    def calculate_difference(val1: float, val2: float, metric_name: str) -> Optional[Dict[str, Any]]:
        if val1 == 0 and val2 == 0:
            return None

        # Avoid division by zero
        denominator = max(abs(val1), abs(val2), 1)
        diff_ratio = abs(val1 - val2) / denominator

        if diff_ratio >= significance_threshold:
            change_pct = ((val2 - val1) / max(abs(val1), 1)) * 100
            return {
                "stage1": val1,
                "stage2": val2,
                "change": f"{change_pct:+.1f}%",
                "significance": diff_ratio
            }
        return None

    # Compare stage-level metrics
    stage_metrics = {}

    # Duration comparison
    duration1 = 0
    duration2 = 0
    if stage1.completion_time and stage1.first_task_launched_time:
        duration1 = (stage1.completion_time - stage1.first_task_launched_time).total_seconds()
    if stage2.completion_time and stage2.first_task_launched_time:
        duration2 = (stage2.completion_time - stage2.first_task_launched_time).total_seconds()

    duration_diff = calculate_difference(duration1, duration2, "duration")
    if duration_diff:
        stage_metrics["duration_seconds"] = duration_diff

    # Task count comparisons
    task_metrics = [
        ("num_tasks", stage1.num_tasks or 0, stage2.num_tasks or 0),
        ("num_failed_tasks", stage1.num_failed_tasks or 0, stage2.num_failed_tasks or 0),
        ("memory_bytes_spilled", stage1.memory_bytes_spilled or 0, stage2.memory_bytes_spilled or 0),
        ("disk_bytes_spilled", stage1.disk_bytes_spilled or 0, stage2.disk_bytes_spilled or 0),
        ("input_bytes", stage1.input_bytes or 0, stage2.input_bytes or 0),
        ("output_bytes", stage1.output_bytes or 0, stage2.output_bytes or 0),
        ("shuffle_read_bytes", stage1.shuffle_read_bytes or 0, stage2.shuffle_read_bytes or 0),
        ("shuffle_write_bytes", stage1.shuffle_write_bytes or 0, stage2.shuffle_write_bytes or 0)
    ]

    for metric_name, val1, val2 in task_metrics:
        diff = calculate_difference(val1, val2, metric_name)
        if diff:
            stage_metrics[metric_name] = diff

    if stage_metrics:
        result["significant_differences"]["stage_metrics"] = stage_metrics

    # Compare task-level distributions (median and max)
    task_distributions = {}

    if (stage1.task_metrics_distributions and stage2.task_metrics_distributions):
        dist1 = stage1.task_metrics_distributions
        dist2 = stage2.task_metrics_distributions

        # Task distribution metrics to compare
        task_dist_metrics = [
            ("duration", dist1.duration, dist2.duration),
            ("executor_run_time", dist1.executor_run_time, dist2.executor_run_time),
            ("peak_execution_memory", dist1.peak_execution_memory, dist2.peak_execution_memory),
            ("memory_bytes_spilled", dist1.memory_bytes_spilled, dist2.memory_bytes_spilled),
            ("disk_bytes_spilled", dist1.disk_bytes_spilled, dist2.disk_bytes_spilled)
        ]

        for metric_name, vals1, vals2 in task_dist_metrics:
            if vals1 and vals2 and len(vals1) >= 5 and len(vals2) >= 5:
                metric_comparison = {}

                # Compare median (50th percentile - index 2)
                median_diff = calculate_difference(vals1[2], vals2[2], f"{metric_name}_median")
                if median_diff:
                    metric_comparison["median"] = median_diff

                # Compare max (100th percentile - index 4)
                max_diff = calculate_difference(vals1[4], vals2[4], f"{metric_name}_max")
                if max_diff:
                    metric_comparison["max"] = max_diff

                if metric_comparison:
                    task_distributions[metric_name] = metric_comparison

        # Shuffle metrics from nested objects
        if (dist1.shuffle_read_metrics and dist2.shuffle_read_metrics and
            dist1.shuffle_read_metrics.read_bytes and dist2.shuffle_read_metrics.read_bytes and
            len(dist1.shuffle_read_metrics.read_bytes) >= 5 and len(dist2.shuffle_read_metrics.read_bytes) >= 5):

            read_comparison = {}
            median_diff = calculate_difference(
                dist1.shuffle_read_metrics.read_bytes[2],
                dist2.shuffle_read_metrics.read_bytes[2],
                "shuffle_read_median"
            )
            if median_diff:
                read_comparison["median"] = median_diff

            max_diff = calculate_difference(
                dist1.shuffle_read_metrics.read_bytes[4],
                dist2.shuffle_read_metrics.read_bytes[4],
                "shuffle_read_max"
            )
            if max_diff:
                read_comparison["max"] = max_diff

            if read_comparison:
                task_distributions["shuffle_read_bytes"] = read_comparison

        # Add fetch_wait_time comparison
        if (dist1.shuffle_read_metrics and dist2.shuffle_read_metrics and
            dist1.shuffle_read_metrics.fetch_wait_time and dist2.shuffle_read_metrics.fetch_wait_time and
            len(dist1.shuffle_read_metrics.fetch_wait_time) >= 5 and len(dist2.shuffle_read_metrics.fetch_wait_time) >= 5):

            fetch_wait_comparison = {}
            median_diff = calculate_difference(
                dist1.shuffle_read_metrics.fetch_wait_time[2],
                dist2.shuffle_read_metrics.fetch_wait_time[2],
                "fetch_wait_time_median"
            )
            if median_diff:
                fetch_wait_comparison["median"] = median_diff

            max_diff = calculate_difference(
                dist1.shuffle_read_metrics.fetch_wait_time[4],
                dist2.shuffle_read_metrics.fetch_wait_time[4],
                "fetch_wait_time_max"
            )
            if max_diff:
                fetch_wait_comparison["max"] = max_diff

            if fetch_wait_comparison:
                task_distributions["shuffle_fetch_wait_time"] = fetch_wait_comparison

        # Add remote_reqs_duration comparison
        if (dist1.shuffle_read_metrics and dist2.shuffle_read_metrics and
            dist1.shuffle_read_metrics.remote_reqs_duration and dist2.shuffle_read_metrics.remote_reqs_duration and
            len(dist1.shuffle_read_metrics.remote_reqs_duration) >= 5 and len(dist2.shuffle_read_metrics.remote_reqs_duration) >= 5):

            remote_reqs_comparison = {}
            median_diff = calculate_difference(
                dist1.shuffle_read_metrics.remote_reqs_duration[2],
                dist2.shuffle_read_metrics.remote_reqs_duration[2],
                "remote_reqs_duration_median"
            )
            if median_diff:
                remote_reqs_comparison["median"] = median_diff

            max_diff = calculate_difference(
                dist1.shuffle_read_metrics.remote_reqs_duration[4],
                dist2.shuffle_read_metrics.remote_reqs_duration[4],
                "remote_reqs_duration_max"
            )
            if max_diff:
                remote_reqs_comparison["max"] = max_diff

            if remote_reqs_comparison:
                task_distributions["shuffle_remote_reqs_duration"] = remote_reqs_comparison

        if (dist1.shuffle_write_metrics and dist2.shuffle_write_metrics and
            dist1.shuffle_write_metrics.write_bytes and dist2.shuffle_write_metrics.write_bytes and
            len(dist1.shuffle_write_metrics.write_bytes) >= 5 and len(dist2.shuffle_write_metrics.write_bytes) >= 5):

            write_comparison = {}
            median_diff = calculate_difference(
                dist1.shuffle_write_metrics.write_bytes[2],
                dist2.shuffle_write_metrics.write_bytes[2],
                "shuffle_write_median"
            )
            if median_diff:
                write_comparison["median"] = median_diff

            max_diff = calculate_difference(
                dist1.shuffle_write_metrics.write_bytes[4],
                dist2.shuffle_write_metrics.write_bytes[4],
                "shuffle_write_max"
            )
            if max_diff:
                write_comparison["max"] = max_diff

            if write_comparison:
                task_distributions["shuffle_write_bytes"] = write_comparison

    if task_distributions:
        result["significant_differences"]["task_distributions"] = task_distributions

    # Compare executor-level distributions (median and max)
    executor_distributions = {}

    if (stage1.executor_metrics_distributions and stage2.executor_metrics_distributions):
        exec_dist1 = stage1.executor_metrics_distributions
        exec_dist2 = stage2.executor_metrics_distributions

        # Executor distribution metrics to compare
        exec_dist_metrics = [
            ("task_time", exec_dist1.task_time, exec_dist2.task_time),
            ("shuffle_write", exec_dist1.shuffle_write, exec_dist2.shuffle_write),
            ("memory_bytes_spilled", exec_dist1.memory_bytes_spilled, exec_dist2.memory_bytes_spilled),
            ("disk_bytes_spilled", exec_dist1.disk_bytes_spilled, exec_dist2.disk_bytes_spilled)
        ]

        for metric_name, vals1, vals2 in exec_dist_metrics:
            if vals1 and vals2 and len(vals1) >= 5 and len(vals2) >= 5:
                metric_comparison = {}

                # Compare median (index 2)
                median_diff = calculate_difference(vals1[2], vals2[2], f"{metric_name}_median")
                if median_diff:
                    metric_comparison["median"] = median_diff

                # Compare max (index 4)
                max_diff = calculate_difference(vals1[4], vals2[4], f"{metric_name}_max")
                if max_diff:
                    metric_comparison["max"] = max_diff

                if metric_comparison:
                    executor_distributions[metric_name] = metric_comparison

    if executor_distributions:
        result["significant_differences"]["executor_distributions"] = executor_distributions

    # Collect all differences with significance scores for top 5 filtering
    all_differences = []

    # Collect stage-level metrics
    if stage_metrics:
        for metric_name, metric_data in stage_metrics.items():
            all_differences.append({
                "category": "stage_metrics",
                "metric_name": metric_name,
                "full_name": f"stage_metrics.{metric_name}",
                "significance": metric_data["significance"],
                "data": metric_data
            })

    # Collect task distribution metrics
    if task_distributions:
        for metric_name, metric_data in task_distributions.items():
            if isinstance(metric_data, dict):
                for sub_metric, sub_data in metric_data.items():  # median/max
                    all_differences.append({
                        "category": "task_distributions",
                        "metric_name": metric_name,
                        "sub_metric": sub_metric,
                        "full_name": f"task_distributions.{metric_name}.{sub_metric}",
                        "significance": sub_data["significance"],
                        "data": sub_data
                    })

    # Collect executor distribution metrics
    if executor_distributions:
        for metric_name, metric_data in executor_distributions.items():
            if isinstance(metric_data, dict):
                for sub_metric, sub_data in metric_data.items():  # median/max
                    all_differences.append({
                        "category": "executor_distributions",
                        "metric_name": metric_name,
                        "sub_metric": sub_metric,
                        "full_name": f"executor_distributions.{metric_name}.{sub_metric}",
                        "significance": sub_data["significance"],
                        "data": sub_data
                    })

    # Sort by significance (highest first) and take top 5
    total_diffs = len(all_differences)
    all_differences.sort(key=lambda x: x["significance"], reverse=True)
    top_differences = all_differences[:5]

    # Rebuild the significant_differences structure with only top 5
    filtered_significant_differences = {}

    for diff in top_differences:
        category = diff["category"]
        if category not in filtered_significant_differences:
            filtered_significant_differences[category] = {}

        if category == "stage_metrics":
            # Stage metrics are single-level
            filtered_significant_differences[category][diff["metric_name"]] = diff["data"]
        else:
            # Task and executor distributions have sub-metrics (median/max)
            metric_name = diff["metric_name"]
            if metric_name not in filtered_significant_differences[category]:
                filtered_significant_differences[category][metric_name] = {}
            filtered_significant_differences[category][metric_name][diff["sub_metric"]] = diff["data"]

    result["significant_differences"] = filtered_significant_differences
    result["summary"]["total_differences_found"] = total_diffs
    result["summary"]["differences_shown"] = len(top_differences)

    return result


@mcp.tool()
def compare_stage_executor_timeline(
    app_id1: str,
    app_id2: str,
    stage_id1: int,
    stage_id2: int,
    server: Optional[str] = None,
    interval_minutes: int = 1
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
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    try:
        # Get stage information for both applications
        stage1 = client.get_stage_attempt(
            app_id=app_id1,
            stage_id=stage_id1,
            attempt_id=0,
            details=False,
            with_summaries=False
        )
        stage2 = client.get_stage_attempt(
            app_id=app_id2,
            stage_id=stage_id2,
            attempt_id=0,
            details=False,
            with_summaries=False
        )

        # Get executors for both applications
        executors1 = client.list_all_executors(app_id=app_id1)
        executors2 = client.list_all_executors(app_id=app_id2)

        # Helper function to build timeline for a stage
        def build_stage_executor_timeline(stage, executors, app_id):
            if not stage.submission_time:
                return {
                    "error": f"Stage {stage.stage_id} has no submission time",
                    "stage_info": {
                        "stage_id": stage.stage_id,
                        "attempt_id": getattr(stage, 'attempt_id', 0),
                        "name": getattr(stage, 'name', 'unknown'),
                        "submission_time": None,
                        "completion_time": None,
                        "duration_seconds": 0
                    },
                    "timeline": []
                }

            stage_start = stage.submission_time
            stage_end = stage.completion_time or stage_start + timedelta(hours=24)  # Default to 24h if not completed

            # Sanity check: ensure stage_end is after stage_start
            if stage_end <= stage_start:
                stage_end = stage_start + timedelta(minutes=interval_minutes)

            # Create timeline intervals
            timeline = []
            current_time = stage_start
            max_intervals = 10000  # Safety limit to prevent excessive memory usage

            interval_count = 0
            while current_time < stage_end and interval_count < max_intervals:
                interval_end = current_time + timedelta(minutes=interval_minutes)

                # Ensure we don't go past the stage end, but break if we're at the end
                if interval_end >= stage_end:
                    interval_end = stage_end

                # Find active executors during this interval
                active_executors = []
                total_cores = 0
                total_memory = 0

                for executor in executors:
                    # Check if executor was active during this interval
                    executor_start = executor.add_time or stage_start
                    executor_end = executor.remove_time or stage_end

                    if (executor_start <= interval_end and
                        executor_end >= current_time):
                        active_executors.append({
                            "id": executor.id,
                            "host_port": executor.host_port,
                            "cores": executor.total_cores or 0,
                            "memory_mb": (executor.max_memory / (1024 * 1024)) if executor.max_memory else 0
                        })
                        total_cores += executor.total_cores or 0
                        total_memory += (executor.max_memory / (1024 * 1024)) if executor.max_memory else 0

                timeline.append({
                    "timestamp": current_time.isoformat(),
                    "interval_start": current_time.isoformat(),
                    "interval_end": interval_end.isoformat(),
                    "active_executor_count": len(active_executors),
                    "total_cores": total_cores,
                    "total_memory_mb": total_memory,
                    "active_executors": active_executors
                })

                # Break if we've reached the stage end to prevent infinite loop
                if interval_end >= stage_end:
                    break

                current_time = interval_end
                interval_count += 1

            # Add warning if we hit the interval limit
            if interval_count >= max_intervals:
                timeline.append({
                    "warning": f"Timeline truncated at {max_intervals} intervals to prevent excessive memory usage",
                    "stage_duration_hours": (stage_end - stage_start).total_seconds() / 3600,
                    "interval_minutes": interval_minutes
                })

            return {
                "stage_info": {
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "submission_time": stage_start.isoformat() if stage_start else None,
                    "completion_time": stage_end.isoformat() if stage.completion_time else None,
                    "duration_seconds": (stage_end - stage_start).total_seconds() if stage_start else 0
                },
                "timeline": timeline
            }

        # Build timelines for both stages
        timeline1 = build_stage_executor_timeline(stage1, executors1, app_id1)
        timeline2 = build_stage_executor_timeline(stage2, executors2, app_id2)

        # Compare timelines and find significant differences
        comparison_data = []
        min_length = min(len(timeline1["timeline"]), len(timeline2["timeline"]))

        for i in range(min_length):
            interval1 = timeline1["timeline"][i]
            interval2 = timeline2["timeline"][i]

            executor_diff = interval2["active_executor_count"] - interval1["active_executor_count"]
            cores_diff = interval2["total_cores"] - interval1["total_cores"]
            memory_diff = interval2["total_memory_mb"] - interval1["total_memory_mb"]

            comparison_data.append({
                "interval": i + 1,
                "timestamp_range": f"{interval1['interval_start']} to {interval1['interval_end']}",
                "app1": {
                    "executor_count": interval1["active_executor_count"],
                    "total_cores": interval1["total_cores"],
                    "total_memory_mb": interval1["total_memory_mb"]
                },
                "app2": {
                    "executor_count": interval2["active_executor_count"],
                    "total_cores": interval2["total_cores"],
                    "total_memory_mb": interval2["total_memory_mb"]
                },
                "differences": {
                    "executor_count_diff": executor_diff,
                    "cores_diff": cores_diff,
                    "memory_mb_diff": memory_diff
                }
            })

        # Calculate summary statistics
        total_intervals = len(comparison_data)
        intervals_with_executor_diff = sum(1 for c in comparison_data if c["differences"]["executor_count_diff"] != 0)
        max_executor_diff = max((abs(c["differences"]["executor_count_diff"]) for c in comparison_data), default=0)

        return {
            "app1_info": {
                "app_id": app_id1,
                "stage_details": timeline1["stage_info"]
            },
            "app2_info": {
                "app_id": app_id2,
                "stage_details": timeline2["stage_info"]
            },
            "comparison_config": {
                "interval_minutes": interval_minutes,
                "total_intervals_compared": total_intervals
            },
            "timeline_comparison": comparison_data,
            "summary": {
                "total_intervals": total_intervals,
                "intervals_with_executor_differences": intervals_with_executor_diff,
                "max_executor_count_difference": max_executor_diff,
                "stages_overlap": timeline1["stage_info"]["completion_time"] is not None and timeline2["stage_info"]["completion_time"] is not None
            }
        }

    except Exception as e:
        return {
            "error": f"Failed to compare stage executor timelines: {str(e)}",
            "app1_id": app_id1,
            "app2_id": app_id2,
            "stage_ids": [stage_id1, stage_id2]
        }


@mcp.tool()
def compare_app_executor_timeline(
    app_id1: str,
    app_id2: str,
    server: Optional[str] = None,
    interval_minutes: int = 1
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
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    try:
        # Get application information for both apps
        app1 = client.get_application(app_id1)
        app2 = client.get_application(app_id2)

        if not app1.attempts or not app2.attempts:
            return {
                "error": "One or both applications have no attempts",
                "applications": {
                    "app1": {"id": app_id1, "has_attempts": bool(app1.attempts)},
                    "app2": {"id": app_id2, "has_attempts": bool(app2.attempts)}
                }
            }

        # Get executors for both applications
        executors1 = client.list_all_executors(app_id=app_id1)
        executors2 = client.list_all_executors(app_id=app_id2)

        # Get stages for both applications to track active stages
        stages1 = client.list_stages(app_id=app_id1)
        stages2 = client.list_stages(app_id=app_id2)

        def build_app_executor_timeline(app, executors, stages, app_id):
            """Build timeline for an application"""
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
                    timeline_events.append({
                        "timestamp": executor.add_time,
                        "type": "executor_add",
                        "executor_id": executor.id,
                        "cores": executor.total_cores or 0,
                        "memory_mb": (executor.max_memory / (1024 * 1024)) if executor.max_memory else 0
                    })

                if executor.remove_time:
                    timeline_events.append({
                        "timestamp": executor.remove_time,
                        "type": "executor_remove",
                        "executor_id": executor.id
                    })

            # Add stage events for tracking active stages
            for stage in stages:
                if stage.submission_time:
                    timeline_events.append({
                        "timestamp": stage.submission_time,
                        "type": "stage_start",
                        "stage_id": stage.stage_id,
                        "name": stage.name
                    })

                if stage.completion_time:
                    timeline_events.append({
                        "timestamp": stage.completion_time,
                        "type": "stage_end",
                        "stage_id": stage.stage_id
                    })

            # Sort events by timestamp
            timeline_events.sort(key=lambda x: x["timestamp"])

            # Build interval-based timeline
            timeline = []
            current_time = start_time
            max_intervals = 10000  # Safety limit

            interval_count = 0
            while current_time < end_time and interval_count < max_intervals:
                interval_end = current_time + timedelta(minutes=interval_minutes)

                if interval_end > end_time:
                    interval_end = end_time

                # Calculate active resources at this interval
                active_executor_count = 0
                total_cores = 0
                total_memory_mb = 0
                active_stages = set()

                # Count active executors
                for executor in executors:
                    executor_start = executor.add_time or start_time
                    executor_end = executor.remove_time or end_time

                    if (executor_start <= interval_end and executor_end >= current_time):
                        active_executor_count += 1
                        total_cores += executor.total_cores or 0
                        total_memory_mb += (executor.max_memory / (1024 * 1024)) if executor.max_memory else 0

                # Count active stages
                for stage in stages:
                    stage_start = stage.submission_time
                    stage_end = stage.completion_time or end_time

                    if (stage_start and stage_start <= interval_end and stage_end >= current_time):
                        active_stages.add(stage.stage_id)

                timeline.append({
                    "interval_start": current_time.isoformat(),
                    "interval_end": interval_end.isoformat(),
                    "active_executor_count": active_executor_count,
                    "total_cores": total_cores,
                    "total_memory_mb": total_memory_mb,
                    "active_stages_count": len(active_stages)
                })

                current_time = interval_end
                interval_count += 1

            return {
                "app_info": {
                    "app_id": app_id,
                    "name": app.name,
                    "start_time": start_time.isoformat() if start_time else None,
                    "end_time": end_time.isoformat() if app.attempts[0].end_time else None,
                    "duration_seconds": (end_time - start_time).total_seconds() if start_time else 0
                },
                "timeline": timeline,
                "summary": {
                    "total_executors": len(executors),
                    "total_stages": len(stages),
                    "peak_executor_count": max((interval["active_executor_count"] for interval in timeline), default=0),
                    "avg_executor_count": sum(interval["active_executor_count"] for interval in timeline) / len(timeline) if timeline else 0,
                    "peak_cores": max((interval["total_cores"] for interval in timeline), default=0),
                    "peak_memory_mb": max((interval["total_memory_mb"] for interval in timeline), default=0)
                }
            }

        # Build timelines for both applications
        timeline1 = build_app_executor_timeline(app1, executors1, stages1, app_id1)
        timeline2 = build_app_executor_timeline(app2, executors2, stages2, app_id2)

        if not timeline1 or not timeline2:
            return {
                "error": "Could not build timeline for one or both applications",
                "applications": {
                    "app1": {"id": app_id1, "timeline_built": timeline1 is not None},
                    "app2": {"id": app_id2, "timeline_built": timeline2 is not None}
                }
            }

        # Compare timelines interval by interval
        comparison_data = []
        min_length = min(len(timeline1["timeline"]), len(timeline2["timeline"]))

        total_executor_diff = 0
        total_cores_diff = 0
        total_memory_diff = 0
        intervals_with_differences = 0

        for i in range(min_length):
            interval1 = timeline1["timeline"][i]
            interval2 = timeline2["timeline"][i]

            executor_diff = interval2["active_executor_count"] - interval1["active_executor_count"]
            cores_diff = interval2["total_cores"] - interval1["total_cores"]
            memory_diff = interval2["total_memory_mb"] - interval1["total_memory_mb"]
            stages_diff = interval2["active_stages_count"] - interval1["active_stages_count"]

            if executor_diff != 0 or cores_diff != 0 or memory_diff != 0:
                intervals_with_differences += 1

            total_executor_diff += abs(executor_diff)
            total_cores_diff += abs(cores_diff)
            total_memory_diff += abs(memory_diff)

            comparison_data.append({
                "interval": i + 1,
                "timestamp_range": f"{interval1['interval_start']} to {interval1['interval_end']}",
                "app1": {
                    "executor_count": interval1["active_executor_count"],
                    "total_cores": interval1["total_cores"],
                    "total_memory_mb": interval1["total_memory_mb"],
                    "active_stages": interval1["active_stages_count"]
                },
                "app2": {
                    "executor_count": interval2["active_executor_count"],
                    "total_cores": interval2["total_cores"],
                    "total_memory_mb": interval2["total_memory_mb"],
                    "active_stages": interval2["active_stages_count"]
                },
                "differences": {
                    "executor_count_diff": executor_diff,
                    "cores_diff": cores_diff,
                    "memory_mb_diff": memory_diff,
                    "stages_diff": stages_diff
                }
            })

        # Calculate efficiency metrics
        def calculate_efficiency_metrics(timeline_data):
            timeline = timeline_data["timeline"]
            if not timeline:
                return {}

            total_intervals = len(timeline)
            non_zero_intervals = [t for t in timeline if t["active_executor_count"] > 0]

            if not non_zero_intervals:
                return {"avg_utilization": 0, "efficiency_score": 0}

            avg_utilization = sum(t["active_executor_count"] for t in non_zero_intervals) / len(non_zero_intervals)
            peak_count = max(t["active_executor_count"] for t in timeline)

            # Simple efficiency score: how close to peak utilization on average
            efficiency_score = (avg_utilization / peak_count) if peak_count > 0 else 0

            return {
                "avg_utilization": avg_utilization,
                "efficiency_score": efficiency_score,
                "resource_waste_intervals": sum(1 for t in timeline if t["active_executor_count"] < avg_utilization * 0.5)
            }

        efficiency1 = calculate_efficiency_metrics(timeline1)
        efficiency2 = calculate_efficiency_metrics(timeline2)

        # Generate recommendations
        recommendations = []

        app1_peak = timeline1["summary"]["peak_executor_count"]
        app2_peak = timeline2["summary"]["peak_executor_count"]
        app1_avg = timeline1["summary"]["avg_executor_count"]
        app2_avg = timeline2["summary"]["avg_executor_count"]

        if app2_avg > app1_avg * 1.2:
            recommendations.append({
                "type": "resource_allocation",
                "priority": "medium",
                "issue": f"App2 uses {((app2_avg/app1_avg - 1) * 100):.0f}% more executors on average",
                "suggestion": "Consider if App2's higher resource allocation provides proportional performance benefits"
            })

        if efficiency2.get("efficiency_score", 0) > efficiency1.get("efficiency_score", 0) * 1.1:
            recommendations.append({
                "type": "efficiency",
                "priority": "high",
                "issue": "App2 shows significantly better executor utilization efficiency",
                "suggestion": "Apply App2's resource allocation pattern to App1 for better efficiency"
            })

        if timeline1["app_info"]["duration_seconds"] > timeline2["app_info"]["duration_seconds"] * 1.2:
            time_savings = timeline1["app_info"]["duration_seconds"] - timeline2["app_info"]["duration_seconds"]
            recommendations.append({
                "type": "performance",
                "priority": "high",
                "issue": f"App1 takes {time_savings:.0f}s longer to complete",
                "suggestion": "Analyze App2's parallelization and resource allocation strategy"
            })

        return {
            "app1_info": timeline1["app_info"],
            "app2_info": timeline2["app_info"],
            "comparison_config": {
                "interval_minutes": interval_minutes,
                "total_intervals_compared": min_length,
                "analysis_type": "App-Level Executor Timeline Comparison"
            },
            "timeline_comparison": comparison_data,
            "resource_efficiency": {
                "app1": {
                    **timeline1["summary"],
                    **efficiency1
                },
                "app2": {
                    **timeline2["summary"],
                    **efficiency2
                }
            },
            "summary": {
                "total_intervals": min_length,
                "intervals_with_differences": intervals_with_differences,
                "avg_executor_count_difference": total_executor_diff / min_length if min_length > 0 else 0,
                "max_executor_count_difference": max((abs(c["differences"]["executor_count_diff"]) for c in comparison_data), default=0),
                "app2_more_efficient": efficiency2.get("efficiency_score", 0) > efficiency1.get("efficiency_score", 0),
                "performance_improvement": {
                    "time_difference_seconds": timeline1["app_info"]["duration_seconds"] - timeline2["app_info"]["duration_seconds"],
                    "efficiency_improvement_ratio": (efficiency2.get("efficiency_score", 0) / efficiency1.get("efficiency_score", 1)) if efficiency1.get("efficiency_score", 0) > 0 else 1
                }
            },
            "recommendations": recommendations,
            "key_differences": {
                "peak_executor_difference": app2_peak - app1_peak,
                "avg_executor_difference": app2_avg - app1_avg,
                "duration_difference_seconds": timeline2["app_info"]["duration_seconds"] - timeline1["app_info"]["duration_seconds"]
            }
        }

    except Exception as e:
        return {
            "error": f"Failed to compare app executor timelines: {str(e)}",
            "app1_id": app_id1,
            "app2_id": app_id2
        }
