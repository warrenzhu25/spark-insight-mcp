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
from spark_history_mcp.tools.common import get_client_or_default, get_config
from spark_history_mcp.tools.fetchers import (
    fetch_jobs,
    fetch_stage_attempt,
    fetch_stage_attempts,
    fetch_stages,
)


def _build_dependencies_from_dag_data(dag_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build stage dependencies from DAG visualization data extracted from HTML.

    Args:
        dag_data: Dictionary containing parsed DAG data from HTML

    Returns:
        Dictionary containing stage dependencies or empty dict if parsing fails
    """
    if not dag_data or not isinstance(dag_data, dict):
        return {}

    try:
        dependencies = {}

        # Extract stage references from various data sources
        stage_references = dag_data.get("stage_task_references", [])
        if not isinstance(stage_references, list):
            stage_references = []

        # Process DAG visualization data if available
        if "dagVizData" in dag_data:
            dag_viz = dag_data["dagVizData"]

            # Look for stage information in DAG nodes
            if isinstance(dag_viz, dict) and "nodes" in dag_viz and "edges" in dag_viz:
                nodes = dag_viz.get("nodes", [])
                edges = dag_viz.get("edges", [])

                if not isinstance(nodes, list) or not isinstance(edges, list):
                    return {}

                # Build node-to-stage mapping from stage references
                node_to_stage = {}
                for i, ref in enumerate(stage_references):
                    if isinstance(ref, dict) and "stage_id" in ref:
                        stage_id = ref.get("stage_id")
                        if isinstance(stage_id, int):
                            node_to_stage[i] = stage_id

                # If no stage references found, try to extract from nodes directly
                if not node_to_stage and nodes:
                    # Look for stage information in node names or metadata
                    for i, node in enumerate(nodes):
                        if isinstance(node, dict):
                            node_name = node.get("name", node.get("nodeName", ""))
                            if (
                                isinstance(node_name, str)
                                and "stage" in node_name.lower()
                            ):
                                # Try to extract stage ID from node name
                                import re

                                stage_match = re.search(
                                    r"stage\s*(\d+)", node_name, re.IGNORECASE
                                )
                                if stage_match:
                                    stage_id = int(stage_match.group(1))
                                    node_to_stage[i] = stage_id

                # Build dependencies from edges and stage mappings
                stage_to_parents = {}
                stage_to_children = {}

                for edge in edges:
                    if not isinstance(edge, dict):
                        continue

                    # Handle different edge formats
                    from_node = edge.get("from", edge.get("fromId", edge.get("source")))
                    to_node = edge.get("to", edge.get("toId", edge.get("target")))

                    if from_node is None or to_node is None:
                        continue

                    from_stage = node_to_stage.get(from_node)
                    to_stage = node_to_stage.get(to_node)

                    if (
                        from_stage is not None
                        and to_stage is not None
                        and isinstance(from_stage, int)
                        and isinstance(to_stage, int)
                        and from_stage != to_stage
                    ):
                        # Add parent relationship
                        if to_stage not in stage_to_parents:
                            stage_to_parents[to_stage] = []
                        if from_stage not in stage_to_parents[to_stage]:
                            stage_to_parents[to_stage].append(from_stage)

                        # Add child relationship
                        if from_stage not in stage_to_children:
                            stage_to_children[from_stage] = []
                        if to_stage not in stage_to_children[from_stage]:
                            stage_to_children[from_stage].append(to_stage)

                # Build final dependency structure
                all_stages = set(stage_to_parents.keys()).union(
                    set(stage_to_children.keys())
                )
                for stage_id in all_stages:
                    dependencies[stage_id] = {
                        "parents": [
                            {"stage_id": p, "relationship_type": "dag_based"}
                            for p in sorted(stage_to_parents.get(stage_id, []))
                        ],
                        "children": [
                            {"stage_id": c, "relationship_type": "dag_based"}
                            for c in sorted(stage_to_children.get(stage_id, []))
                        ],
                        "stage_info": {"stage_id": stage_id},
                    }

        # Also try to extract from timeline data if available (future enhancement)
        if "timelineData" in dag_data and not dependencies:
            timeline_data = dag_data["timelineData"]
            if isinstance(timeline_data, list):
                # Process timeline data for stage relationships
                # This would need specific implementation based on timeline structure
                pass

        return dependencies

    except (TypeError, ValueError, KeyError, AttributeError):
        # Return empty dependencies on parsing error
        return {}
    except Exception:
        # Catch any other unexpected errors
        return {}


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

        # Get all job IDs from the execution
        all_job_ids: set[int] = set()
        all_job_ids.update(execution.running_job_ids or [])
        all_job_ids.update(execution.success_job_ids or [])
        all_job_ids.update(execution.failed_job_ids or [])

        if not all_job_ids:
            return {}

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
                    stage_job_mapping[stage_id].append(
                        {
                            "job_id": job.job_id,
                            "job_name": job.name,
                            "job_status": job.status,
                            "submission_time": job.submission_time.isoformat()
                            if job.submission_time
                            else None,
                            "completion_time": job.completion_time.isoformat()
                            if job.completion_time
                            else None,
                        }
                    )

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
                "submission_time": stage.submission_time.isoformat()
                if stage.submission_time
                else None,
                "completion_time": stage.completion_time.isoformat()
                if stage.completion_time
                else None,
                "duration_ms": None,
                "attempt_id": stage.attempt_id,
            }

            # Calculate duration
            if stage.submission_time and stage.completion_time:
                duration = (
                    stage.completion_time - stage.submission_time
                ).total_seconds() * 1000
                stage_info[stage_id]["duration_ms"] = int(duration)

        # Sort stages by submission time for timeline
        timeline_stages = [s for s in relevant_stages if s.submission_time]
        timeline_stages.sort(key=lambda s: s.submission_time)

        execution_timeline = [
            {
                "stage_id": stage.stage_id,
                "stage_name": stage.name,
                "submission_time": stage.submission_time.isoformat(),
                "completion_time": stage.completion_time.isoformat()
                if stage.completion_time
                else None,
                "status": stage.status,
            }
            for stage in timeline_stages
        ]

        # Use DAG-based dependencies if available, otherwise fall back to timing-based inference
        if dag_based_dependencies:
            # Merge DAG-based dependencies with stage info
            stage_dependencies = {}
            for stage_id, dag_dep in dag_based_dependencies.items():
                stage_dependencies[stage_id] = {
                    "parents": dag_dep.get("parents", []),
                    "children": dag_dep.get("children", []),
                    "stage_info": stage_info.get(
                        stage_id, dag_dep.get("stage_info", {})
                    ),
                }

                # Enhance parent/child info with stage names if available
                for parent in stage_dependencies[stage_id]["parents"]:
                    parent_id = parent["stage_id"]
                    if parent_id in stage_info:
                        parent["stage_name"] = stage_info[parent_id].get(
                            "stage_name", f"Stage {parent_id}"
                        )

                for child in stage_dependencies[stage_id]["children"]:
                    child_id = child["stage_id"]
                    if child_id in stage_info:
                        child["stage_name"] = stage_info[child_id].get(
                            "stage_name", f"Stage {child_id}"
                        )

            # Ensure all relevant stages are included
            for stage in relevant_stages:
                if stage.stage_id not in stage_dependencies:
                    stage_dependencies[stage.stage_id] = {
                        "parents": [],
                        "children": [],
                        "stage_info": stage_info.get(stage.stage_id, {}),
                    }

        else:
            # Infer dependencies based on timing and job relationships (fallback)
            for i, stage in enumerate(timeline_stages):
                stage_id = stage.stage_id
                stage_dependencies[stage_id] = {
                    "parents": [],
                    "children": [],
                    "stage_info": stage_info.get(stage_id, {}),
                }

                # Find potential parent stages (completed before this stage started)
                for j in range(i):
                    prev_stage = timeline_stages[j]
                    if (
                        prev_stage.completion_time
                        and prev_stage.completion_time <= stage.submission_time
                    ):
                        # Check if they're in related jobs (more likely to be dependencies)
                        prev_jobs = set(
                            job["job_id"]
                            for job in stage_job_mapping.get(prev_stage.stage_id, [])
                        )
                        curr_jobs = set(
                            job["job_id"] for job in stage_job_mapping.get(stage_id, [])
                        )

                        # If they share jobs or are sequential, likely dependency
                        if prev_jobs.intersection(curr_jobs) or abs(j - i) <= 2:
                            stage_dependencies[stage_id]["parents"].append(
                                {
                                    "stage_id": prev_stage.stage_id,
                                    "stage_name": prev_stage.name,
                                    "relationship_type": "timing_based",
                                }
                            )

                            # Add child relationship to parent
                            if prev_stage.stage_id not in stage_dependencies:
                                stage_dependencies[prev_stage.stage_id] = {
                                    "parents": [],
                                    "children": [],
                                    "stage_info": stage_info.get(
                                        prev_stage.stage_id, {}
                                    ),
                                }

                            stage_dependencies[prev_stage.stage_id]["children"].append(
                                {
                                    "stage_id": stage_id,
                                    "stage_name": stage.name,
                                    "relationship_type": "timing_based",
                                }
                            )

        # Identify critical path (longest duration sequence)
        critical_path = []
        if timeline_stages:
            # Simple heuristic: stages with longest individual duration
            stages_by_duration = [
                (s.stage_id, stage_info[s.stage_id].get("duration_ms", 0))
                for s in timeline_stages
                if s.stage_id in stage_info
            ]
            stages_by_duration.sort(key=lambda x: x[1], reverse=True)

            # Take top stages that represent significant portion of execution
            total_duration = sum(d for _, d in stages_by_duration)
            critical_duration = 0

            for stage_id, duration in stages_by_duration:
                critical_path.append(
                    {
                        "stage_id": stage_id,
                        "stage_name": stage_info[stage_id]["stage_name"],
                        "duration_ms": duration,
                        "percentage_of_total": (duration / total_duration * 100)
                        if total_duration > 0
                        else 0,
                    }
                )
                critical_duration += duration

                # Include stages that make up ~80% of execution time
                if critical_duration / total_duration >= 0.8:
                    break

        # Prepare analysis metadata
        analysis_metadata = {
            "dependency_inference": analysis_method,
            "stages_analyzed": len(relevant_stages),
            "jobs_analyzed": len(relevant_jobs),
            "html_dag_available": html_dag_data is not None,
            "dag_parsing_successful": bool(dag_based_dependencies),
        }

        if html_dag_data and html_dag_data.get("parsing_error"):
            analysis_metadata["dag_parsing_error"] = html_dag_data["parsing_error"]

        if html_dag_data:
            analysis_metadata["html_data_keys"] = list(html_dag_data.keys())

        return stage_dependencies

    except Exception:
        return {}
