"""MCP tools implementation for Spark History Server."""

# Import all tools to ensure MCP registration
# Application-level tools
from ..core.app import mcp
from .common import get_client_or_default, get_config
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

# Comparison tools (MCP-exposed and internal helpers)
from .comparison_modules.core import (
    compare_app_performance,
    compare_app_summaries,
)
from .comparison_modules.environment import (
    compare_app_environments,
    compare_app_jobs,
    compare_app_resources,
    compare_app_stages_aggregated,
)
from .comparison_modules.executors import (
    compare_app_executor_timeline,
    compare_app_executors,
    compare_stage_executor_timeline,
)
from .comparison_modules.stages import (
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

from .comparison_modules.utils import (
    _compare_environments,
    _compare_sql_execution_plans,
)

# Make tools available at package level
__all__ = [
    # Core utilities
    "get_client_or_default",
    "get_config",
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
