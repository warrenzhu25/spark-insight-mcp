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

# Cleanup tools
from .cleanup import delete_event_logs

# Comparison tools (MCP-exposed)
# Comparison helpers (internal, used by CLI)
from .comparisons import (
    compare_app_environments,
    compare_app_executor_timeline,
    compare_app_executors,
    compare_app_jobs,
    compare_app_performance,
    compare_app_resources,
    compare_app_stages_aggregated,
    compare_app_summaries,
    compare_stage_executor_timeline,
    compare_stages,
    find_top_stage_differences,
)

# Executor and resource tools
from .executors import (
    get_executor,
    get_executor_summary,
    get_timeline,
    list_executors,
)

# Job and stage tools
from .jobs_stages import (
    find_slowest,
    get_stage,
    get_stage_task_summary,
    list_jobs,
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
    "list_stages",
    "find_slowest",
    "get_stage",
    "get_stage_task_summary",
    # Executor tools
    "list_executors",
    "get_executor",
    "get_executor_summary",
    "get_timeline",
    # Analysis tools
    "get_job_bottlenecks",
    "analyze_auto_scaling",
    "analyze_shuffle_skew",
    "analyze_failed_tasks",
    # Cleanup tools
    "delete_event_logs",
    # Comparison tools (MCP-exposed)
    "compare_app_performance",
    "compare_stages",
    "compare_app_environments",
    # Comparison helpers (internal, used by CLI)
    "compare_app_executor_timeline",
    "compare_app_executors",
    "compare_app_jobs",
    "compare_app_resources",
    "compare_app_stages_aggregated",
    "compare_app_summaries",
    "compare_stage_executor_timeline",
    "find_top_stage_differences",
]
