"""
Job and Stage analysis tools for MCP server.

This module contains tools for retrieving and analyzing Spark jobs and stages,
including performance metrics, SQL queries, and stage dependencies.
"""

import heapq
from typing import Any, List, Optional

from ..core.app import mcp
from ..models.mcp_types import JobSummary, SqlQuerySummary
from ..models.spark_types import (
    ExecutionData,
    JobExecutionStatus,
    SQLExecutionStatus,
    StageData,
    TaskMetricDistributions,
)
from .common import compact_output, get_client_or_default, get_config
from .fetchers import (
    fetch_jobs,
    fetch_stage_attempt,
    fetch_stage_attempts,
    fetch_stage_task_summary,
    fetch_stages,
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
def list_jobs(
    app_id: str,
    server: Optional[str] = None,
    status: Optional[list[str]] = None,
    compact: Optional[bool] = None,
) -> Any:
    """
    Get a list of all jobs for a Spark application.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        status: Optional list of job status values to filter by
        compact: Whether to return a compact summary (default: True)

    Returns:
        List of JobData objects for the application (or compact summary list)
    """
    # Delegate to fetchers (centralized enum conversion and optional caching)
    jobs = fetch_jobs(app_id=app_id, server=server, status=status)
    return compact_output(jobs, compact)


def _find_slowest_jobs(
    app_id: str,
    server: Optional[str] = None,
    include_running: bool = False,
    n: int = 5,
    compact: Optional[bool] = None,
) -> Any:
    """Internal helper: Get the N slowest jobs for a Spark application."""
    cfg = get_config()
    jobs = fetch_jobs(app_id=app_id, server=server)

    if not jobs:
        return []

    if not include_running and not cfg.include_running_defaults:
        jobs = [job for job in jobs if job.status != JobExecutionStatus.RUNNING.value]

    if not jobs:
        return []

    def get_job_duration(job):
        if job.completion_time and job.submission_time:
            return (job.completion_time - job.submission_time).total_seconds()
        return 0

    slowest = heapq.nlargest(n, jobs, key=get_job_duration)
    return compact_output(slowest, compact)


@mcp.tool()
def list_stages(
    app_id: str,
    server: Optional[str] = None,
    status: Optional[list[str]] = None,
    with_summaries: bool = False,
    compact: Optional[bool] = None,
) -> Any:
    """
    Get a list of all stages for a Spark application.

    Retrieves information about stages in a Spark application with options to filter
    by status and include additional details and summary metrics.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        status: Optional list of stage status values to filter by
        with_summaries: Whether to include summary metrics in the response
        compact: Whether to return a compact summary (default: True)

    Returns:
        List of StageData objects for the application (or compact summary list)
    """
    # Delegate to fetchers (centralized enum conversion and optional caching)
    stages = fetch_stages(
        app_id=app_id, server=server, status=status, with_summaries=with_summaries
    )
    return compact_output(stages, compact)


def _find_slowest_stages(
    app_id: str,
    server: Optional[str] = None,
    include_running: bool = False,
    n: int = 5,
    compact: Optional[bool] = None,
) -> Any:
    """Internal helper: Get the N slowest stages for a Spark application."""
    cfg = get_config()
    stages = fetch_stages(app_id=app_id, server=server)

    if not include_running and not cfg.include_running_defaults:
        stages = [stage for stage in stages if stage.status != "RUNNING"]

    if not stages:
        return []

    def get_stage_duration(stage: StageData):
        if stage.completion_time and stage.first_task_launched_time:
            return (
                stage.completion_time - stage.first_task_launched_time
            ).total_seconds()
        return 0

    slowest = heapq.nlargest(n, stages, key=get_stage_duration)
    return compact_output(slowest, compact)


@mcp.tool()
def get_stage(
    app_id: str,
    stage_id: int,
    attempt_id: Optional[int] = None,
    server: Optional[str] = None,
    with_summaries: bool = False,
    compact: Optional[bool] = None,
) -> Any:
    """
    Get information about a specific stage.

    Args:
        app_id: The Spark application ID
        stage_id: The stage ID
        attempt_id: Optional stage attempt ID (if not provided, returns the latest attempt)
        server: Optional server name to use (uses default if not specified)
        with_summaries: Whether to include summary metrics
        compact: Whether to return a compact summary (default: True)

    Returns:
        StageData object containing stage information (or compact summary)
    """
    if attempt_id is not None:
        stage_data = fetch_stage_attempt(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=attempt_id,
            server=server,
            with_summaries=with_summaries,
        )
    else:
        stages = fetch_stage_attempts(
            app_id=app_id,
            stage_id=stage_id,
            server=server,
            with_summaries=with_summaries,
        )
        if not stages:
            raise ValueError(f"No stage found with ID {stage_id}")
        stage_data = (
            max(stages, key=lambda s: s.attempt_id)
            if isinstance(stages, list)
            else stages
        )

    # If summaries were requested but metrics distributions are missing, fetch them separately
    if with_summaries and (
        not hasattr(stage_data, "task_metrics_distributions")
        or stage_data.task_metrics_distributions is None
    ):
        task_summary = fetch_stage_task_summary(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=stage_data.attempt_id,
            server=server,
        )
        stage_data.task_metrics_distributions = task_summary

    return compact_output(stage_data, compact)


@mcp.tool()
def get_stage_task_summary(
    app_id: str,
    stage_id: int,
    attempt_id: int = 0,
    server: Optional[str] = None,
    quantiles: Optional[str] = "0.05,0.25,0.5,0.75,0.95",
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
        quantiles: Comma-separated quantiles string to request from the server

    Returns:
        TaskMetricDistributions object containing metric distributions
    """
    return fetch_stage_task_summary(
        app_id=app_id,
        stage_id=stage_id,
        attempt_id=attempt_id,
        server=server,
        quantiles=quantiles,
    )


def _find_slowest_sql(
    app_id: str,
    server: Optional[str] = None,
    attempt_id: Optional[str] = None,
    n: int = 5,
    page_size: int = 100,
    include_running: bool = False,
    include_plan_description: bool = True,
    plan_description_max_length: int = 2000,
) -> List[SqlQuerySummary]:
    """Internal helper: Get the N slowest SQL queries for a Spark application."""
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    all_executions: List[ExecutionData] = []
    offset = 0

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

        if len(executions) < page_size:
            break

    if not include_running:
        all_executions = [
            e for e in all_executions if e.status != SQLExecutionStatus.RUNNING.value
        ]

    slowest_executions = heapq.nlargest(n, all_executions, key=lambda e: e.duration)

    simplified_results = []
    for execution in slowest_executions:
        job_summary = JobSummary(
            success_job_ids=execution.success_job_ids,
            failed_job_ids=execution.failed_job_ids,
            running_job_ids=execution.running_job_ids,
        )

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
def find_slowest(
    app_id: str,
    type: str = "stages",  # noqa: A002
    server: Optional[str] = None,
    n: int = 5,
    include_running: bool = False,
    compact: Optional[bool] = None,
) -> Any:
    """
    Find the N slowest jobs, stages, or SQL queries for a Spark application.

    A unified tool that consolidates slowest-N queries across different entity types.

    Args:
        app_id: The Spark application ID
        type: Type of entity to find - "jobs", "stages", or "sql" (default: "stages")
        server: Optional server name to use (uses default if not specified)
        n: Number of slowest items to return (default: 5)
        include_running: Whether to include running items in the search (default: False)
        compact: Whether to return a compact summary (default: True)

    Returns:
        List of the slowest items (jobs, stages, or SQL queries) based on type
    """
    entity_type = type.lower()

    if entity_type == "jobs":
        return _find_slowest_jobs(
            app_id=app_id,
            server=server,
            include_running=include_running,
            n=n,
            compact=compact,
        )
    elif entity_type == "stages":
        return _find_slowest_stages(
            app_id=app_id,
            server=server,
            include_running=include_running,
            n=n,
            compact=compact,
        )
    elif entity_type == "sql":
        return _find_slowest_sql(
            app_id=app_id,
            server=server,
            n=n,
            include_running=include_running,
        )
    else:
        raise ValueError(f"Invalid type '{type}'. Must be one of: jobs, stages, sql")
