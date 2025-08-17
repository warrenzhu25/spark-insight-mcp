import heapq
import statistics
from datetime import datetime, timedelta
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
    quantiles: str = "0.05,0.25,0.5,0.75,0.95",
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
        quantiles: Comma-separated list of quantiles to use for summary metrics

    Returns:
        TaskMetricDistributions object containing metric distributions
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    return client.get_stage_task_summary(
        app_id=app_id, stage_id=stage_id, attempt_id=attempt_id, quantiles=quantiles
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
    
    for timestamp, demand_change in stage_events:
        current_demand += demand_change
        max_demand = max(max_demand, current_demand)
    
    recommended_max = max(recommended_initial, max_demand)
    
    # Get current configuration
    spark_props = {k: v for k, v in environment.spark_properties} if environment.spark_properties else {}
    current_initial = spark_props.get("spark.dynamicAllocation.initialExecutors", "Not set")
    current_max = spark_props.get("spark.dynamicAllocation.maxExecutors", "Not set")
    
    return {
        "application_id": app_id,
        "analysis_type": "Auto-scaling Configuration",
        "target_stage_duration_minutes": target_stage_duration_minutes,
        "recommendations": {
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
        },
        "analysis_details": {
            "total_stages": len(stages),
            "initial_stages_analyzed": len(initial_stages),
            "peak_concurrent_demand": max_demand,
            "calculation_method": f"Aims to complete stages in {target_stage_duration_minutes} minutes"
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
    
    Identifies stages with significant shuffle data skew by comparing
    maximum vs median shuffle write bytes across tasks. Helps identify
    performance bottlenecks caused by uneven data distribution.
    
    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        shuffle_threshold_gb: Minimum total shuffle write in GB to analyze (default: 10)
        skew_ratio_threshold: Minimum ratio of max/median to flag as skewed (default: 2.0)
        
    Returns:
        Dictionary containing shuffle skew analysis results
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)
    
    stages = client.list_stages(app_id=app_id, with_summaries=True)
    shuffle_threshold_bytes = shuffle_threshold_gb * 1024 * 1024 * 1024
    
    skewed_stages = []
    
    for stage in stages:
        # Check if stage has significant shuffle write
        if not stage.shuffle_write_bytes or stage.shuffle_write_bytes < shuffle_threshold_bytes:
            continue
            
        # Get task metric distributions for this stage
        try:
            task_summary = client.get_stage_task_summary(
                app_id=app_id,
                stage_id=stage.stage_id,
                attempt_id=stage.attempt_id
            )
            
            if task_summary.shuffle_write_bytes:
                # Extract quantiles (typically [min, 25th, 50th, 75th, max])
                quantiles = task_summary.shuffle_write_bytes
                if len(quantiles) >= 5:
                    median = quantiles[2]  # 50th percentile
                    max_val = quantiles[4]  # 100th percentile (max)
                    
                    if median > 0:
                        skew_ratio = max_val / median
                        if skew_ratio > skew_ratio_threshold:
                            skewed_stages.append({
                                "stage_id": stage.stage_id,
                                "attempt_id": stage.attempt_id,
                                "name": stage.name,
                                "skew_ratio": round(skew_ratio, 2),
                                "max_shuffle_write_mb": round(max_val / (1024 * 1024), 2),
                                "median_shuffle_write_mb": round(median / (1024 * 1024), 2),
                                "total_shuffle_write_gb": round(stage.shuffle_write_bytes / (1024 * 1024 * 1024), 2),
                                "task_count": stage.num_tasks,
                                "failed_tasks": stage.num_failed_tasks
                            })
        except Exception as e:
            # Skip stages where we can't get task summaries
            continue
    
    # Sort by skew ratio (highest first)
    skewed_stages.sort(key=lambda x: x["skew_ratio"], reverse=True)
    
    recommendations = []
    if skewed_stages:
        recommendations.append({
            "type": "data_partitioning",
            "priority": "high",
            "issue": f"Found {len(skewed_stages)} stages with shuffle skew",
            "suggestion": "Consider repartitioning data by key distribution or using salting techniques"
        })
        
        max_skew = max(s["skew_ratio"] for s in skewed_stages)
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
            "max_skew_ratio": max([s["skew_ratio"] for s in skewed_stages]) if skewed_stages else 0
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
        if "recommendations" in analysis_result:
            for rec in analysis_result["recommendations"]:
                rec["source_analysis"] = analysis_name
                all_recommendations.append(rec)
                if rec.get("priority") == "critical":
                    critical_issues.append(rec)
    
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
