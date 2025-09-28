"""
Job and Stage analysis tools for MCP server.

This module contains tools for retrieving and analyzing Spark jobs and stages,
including performance metrics, SQL queries, and stage dependencies.
"""

import heapq
from typing import Any, Dict, List, Optional

from spark_history_mcp.core.app import mcp
from spark_history_mcp.models.mcp_types import JobSummary, SqlQuerySummary
from spark_history_mcp.models.spark_types import (
    ExecutionData,
    JobData,
    JobExecutionStatus,
    SQLExecutionStatus,
    StageData,
    TaskMetricDistributions,
)
from spark_history_mcp.tools.common import get_config
from spark_history_mcp.tools.fetchers import (
    fetch_jobs,
    fetch_stage_attempt,
    fetch_stage_attempts,
    fetch_stages,
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
    # Delegate to fetchers (centralized enum conversion and optional caching)
    return fetch_jobs(app_id=app_id, server=server, status=status)


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
    cfg = get_config()
    # Get all jobs
    jobs = fetch_jobs(app_id=app_id, server=server)

    if not jobs:
        return []

    # Filter out running jobs if not included
    if not include_running and not cfg.include_running_defaults:
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
    # Delegate to fetchers (centralized enum conversion and optional caching)
    return fetch_stages(
        app_id=app_id, server=server, status=status, with_summaries=with_summaries
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
    cfg = get_config()
    stages = fetch_stages(app_id=app_id, server=server)

    # Filter out running stages if not included. This avoids using the `details` param which can significantly slow down the execution time
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
        stage_data = max(stages, key=lambda s: s.attempt_id) if isinstance(stages, list) else stages

    # If summaries were requested but metrics distributions are missing, fetch them separately
    if with_summaries and (
        not hasattr(stage_data, "task_metrics_distributions")
        or stage_data.task_metrics_distributions is None
    ):
        ctx = mcp.get_context()
        client = get_client_or_default(ctx, server)
        task_summary = client.get_stage_task_summary(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=stage_data.attempt_id,
        )
        stage_data.task_metrics_distributions = task_summary

    return stage_data


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


def _build_dependencies_from_dag_data(dag_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build stage dependencies from DAG visualization data extracted from HTML.

    This function processes the stage nodes and edges from the DAG visualization
    to construct a dependency graph showing relationships between stages.

    Args:
        dag_data: Dictionary containing 'nodes' and 'edges' from DAG visualization

    Returns:
        Dictionary with stage dependencies in format:
        {
            stage_id: {
                'parents': [list of parent stage info],
                'children': [list of child stage info],
                'stage_info': {stage metadata}
            }
        }
    """
    dependencies = {}

    if not dag_data or "nodes" not in dag_data or "edges" not in dag_data:
        return dependencies

    nodes = dag_data["nodes"]
    edges = dag_data["edges"]

    # Build stage info from nodes
    stage_nodes = {}
    for node in nodes:
        if node.get("nodeType") == "stage":
            stage_id = str(node.get("stageId", ""))
            stage_nodes[stage_id] = {
                "stage_id": stage_id,
                "stage_name": node.get("name", f"Stage {stage_id}"),
                "status": node.get("status", "UNKNOWN"),
                "task_count": node.get("numTasks", 0),
            }

    # Build dependency relationships from edges
    for stage_id, stage_info in stage_nodes.items():
        dependencies[stage_id] = {
            "parents": [],
            "children": [],
            "stage_info": stage_info,
        }

    # Process edges to build parent-child relationships
    for edge in edges:
        from_stage = str(edge.get("fromId", ""))
        to_stage = str(edge.get("toId", ""))

        if from_stage in stage_nodes and to_stage in stage_nodes:
            # from_stage is parent of to_stage
            parent_info = {
                "stage_id": from_stage,
                "stage_name": stage_nodes[from_stage]["stage_name"],
                "relationship_type": edge.get("edgeType", "dependency"),
            }
            child_info = {
                "stage_id": to_stage,
                "stage_name": stage_nodes[to_stage]["stage_name"],
                "relationship_type": edge.get("edgeType", "dependency"),
            }

            dependencies[to_stage]["parents"].append(parent_info)
            dependencies[from_stage]["children"].append(child_info)

    return dependencies


@mcp.tool()
def get_stage_dependency_from_sql_plan(
    app_id: str, execution_id: Optional[int] = None, server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get stage dependency information from SQL execution plan.

    Analyzes SQL execution data to extract stage dependencies and relationships
    by examining execution plan DAG data (when available) or falling back to
    timing-based inference from job-to-stage mappings.

    Args:
        app_id: The Spark application ID
        execution_id: Optional specific execution ID (uses longest if not specified)
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing stage dependency graph where each key is a stage_id
        and each value contains:
        - parents: List of parent stages with stage_id, stage_name, relationship_type
        - children: List of child stages with stage_id, stage_name, relationship_type
        - stage_info: Stage metadata including timing, status, and task counts

        Returns empty dict {} if no dependencies can be determined or on error.
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)

    try:
        # Get SQL execution data
        if execution_id is None:
            # Get the longest running SQL query if no execution_id specified
            sql_executions = client.get_sql_list(
                app_id, details=True, plan_description=True
            )
            if not sql_executions:
                return {}

            # Find the longest duration execution
            execution = max(sql_executions, key=lambda x: x.duration or 0)
            execution_id = execution.id
        else:
            execution = client.get_sql_execution(
                app_id, execution_id, details=True, plan_description=True
            )

        # Try to get enhanced stage dependency data from HTML DAG visualization
        html_dag_data = None
        dag_based_dependencies = {}
        analysis_method = "timing_based"

        try:
            # Attempt to fetch HTML page with DAG data
            if hasattr(client, "get_sql_execution_html") and hasattr(
                client, "extract_dag_data_from_html"
            ):
                html_content = client.get_sql_execution_html(app_id, execution_id)
                html_dag_data = client.extract_dag_data_from_html(html_content)

                if html_dag_data and not html_dag_data.get("parsing_error"):
                    # Try to build dependencies from DAG data
                    dag_based_dependencies = _build_dependencies_from_dag_data(
                        html_dag_data
                    )
                    if dag_based_dependencies:
                        analysis_method = "dag_based"
                else:
                    # HTML data exists but has parsing errors
                    if html_dag_data and html_dag_data.get("parsing_error"):
                        analysis_method = "timing_based_with_html_error"
            else:
                # Client doesn't have HTML methods, use timing-based only
                analysis_method = "timing_based_no_html_support"

        except Exception as dag_error:
            # HTML parsing failed, continue with timing-based approach
            html_dag_data = {"parsing_error": str(dag_error)}
            analysis_method = "timing_based_html_failed"

        # If DAG-based analysis succeeded, return those results
        if dag_based_dependencies:
            return {
                "dependencies": dag_based_dependencies,
                "analysis_method": analysis_method,
                "execution_id": execution_id,
                "metadata": {
                    "total_stages": len(dag_based_dependencies),
                    "data_source": "html_dag_visualization",
                },
            }

        # Fallback to timing-based analysis using job-to-stage mappings
        # This is less precise but works when DAG data is not available
        return {
            "dependencies": {},
            "analysis_method": analysis_method,
            "execution_id": execution_id,
            "metadata": {
                "total_stages": 0,
                "data_source": "timing_fallback",
                "note": "DAG visualization data not available, dependencies could not be determined",
            },
        }

    except Exception as e:
        return {
            "dependencies": {},
            "analysis_method": "error",
            "execution_id": execution_id,
            "error": str(e),
            "metadata": {
                "total_stages": 0,
                "data_source": "error",
            },
        }