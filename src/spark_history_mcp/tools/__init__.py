"""MCP tools implementation for Spark History Server."""

# Import all tools to ensure MCP registration
# Application-level tools
# Analysis tools
from .analysis import (
    analyze_auto_scaling,
    analyze_failed_tasks,
    analyze_shuffle_skew,
    get_job_bottlenecks,
)
from .application import (
    get_app_summary,
    get_application,
    get_application_insights,
    get_environment,
    list_applications,
)

# Comparison tools
from .comparisons import (
    compare_app_performance,
    compare_app_summaries,
    find_top_stage_differences,
)

# Executor and resource tools
from .executors import (
    analyze_executor_utilization,
    get_executor,
    get_executor_summary,
    get_resource_usage_timeline,
    list_executors,
)

# Job and stage tools
from .jobs_stages import (
    get_stage,
    get_stage_dependency_from_sql_plan,
    get_stage_task_summary,
    list_jobs,
    list_slowest_jobs,
    list_slowest_sql_queries,
    list_slowest_stages,
    list_stages,
)

# Make tools available at package level
__all__ = [
    # Application tools
    "get_application",
    "list_applications",
    "get_environment",
    "get_application_insights",
    "get_app_summary",
    # Job and stage tools
    "list_jobs",
    "list_slowest_jobs",
    "list_stages",
    "list_slowest_stages",
    "get_stage",
    "get_stage_task_summary",
    "list_slowest_sql_queries",
    "get_stage_dependency_from_sql_plan",
    # Executor tools
    "list_executors",
    "get_executor",
    "get_executor_summary",
    "get_resource_usage_timeline",
    "analyze_executor_utilization",
    # Analysis tools
    "get_job_bottlenecks",
    "analyze_auto_scaling",
    "analyze_shuffle_skew",
    "analyze_failed_tasks",
    # Comparison tools
    "compare_app_performance",
    "compare_app_summaries",
    "find_top_stage_differences",
]
