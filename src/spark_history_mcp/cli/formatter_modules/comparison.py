"""
Comparison result formatting methods for Spark History Server MCP CLI.

Contains detection methods and standalone functions for formatting comparison results.
"""

from typing import Any, Dict, Optional

try:
    from rich.console import Console

    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False

from .base import registry


def is_comparison_result(data: Any) -> bool:
    """Detect if data is a comparison result structure."""
    if not isinstance(data, dict):
        return False
    # Look for key patterns that indicate this is a comparison result
    comparison_keys = {
        "applications",
        "aggregated_overview",  # Old structure
        "stage_deep_dive",  # Old structure
        "performance_comparison",  # New structure
        "app_summary_diff",  # New structure
        "key_recommendations",  # New structure
        "recommendations",
        "environment_comparison",
        "sql_execution_plans",
    }
    return len(comparison_keys.intersection(data.keys())) >= 3


def is_stage_comparison_result(data: Any) -> bool:
    """Detect if data is a stage comparison result structure."""
    if not isinstance(data, dict):
        return False
    stage_keys = {"stage_comparison", "significant_differences", "summary"}
    return len(stage_keys.intersection(data.keys())) >= 2


def is_timeline_comparison_result(data: Any) -> bool:
    """Detect if data is a timeline comparison result structure."""
    if not isinstance(data, dict):
        return False
    timeline_keys = {
        "app1_info",
        "app2_info",
        "timeline_comparison",
        "resource_efficiency",
    }
    return len(timeline_keys.intersection(data.keys())) >= 3


def is_executor_comparison_result(data: Any) -> bool:
    """Detect executor comparison results."""
    if not isinstance(data, dict):
        return False
    executor_keys = {"applications", "executor_comparison", "efficiency_metrics"}
    return len(executor_keys.intersection(data.keys())) >= 2


def is_job_comparison_result(data: Any) -> bool:
    """Detect job comparison results."""
    if not isinstance(data, dict):
        return False
    job_keys = {"applications", "job_comparison", "timing_analysis"}
    return len(job_keys.intersection(data.keys())) >= 2


def is_aggregated_stage_comparison_result(data: Any) -> bool:
    """Detect aggregated stage comparison results."""
    if not isinstance(data, dict):
        return False
    agg_keys = {
        "applications",
        "aggregated_stage_metrics",
        "stage_performance_comparison",
        "stage_comparison",
        "efficiency_analysis",
    }
    return len(agg_keys.intersection(data.keys())) >= 2


def is_resource_comparison_result(data: Any) -> bool:
    """Detect resource comparison results."""
    if not isinstance(data, dict):
        return False
    resource_keys = {"applications", "resource_comparison"}
    return len(resource_keys.intersection(data.keys())) >= 1


def is_app_summary_comparison_result(data: Any) -> bool:
    """Detect compare_app_summaries result structure."""
    if not isinstance(data, dict):
        return False
    return "app1_summary" in data and "app2_summary" in data and "diff" in data


def is_environment_comparison_result(data: Any) -> bool:
    """Detect environment comparison results."""
    if not isinstance(data, dict):
        return False
    return "spark_properties" in data and "system_properties" in data


def is_standardized_comparison_result(data: Any) -> bool:
    """Check if data is a standardized comparison result."""
    if not isinstance(data, dict) or len(data) == 0:
        return False

    # Check if at least some values are 3-tuples (left, right, percentage)
    tuple_count = 0
    for value in data.values():
        if isinstance(value, tuple) and len(value) == 3:
            tuple_count += 1

    # If more than half the values are 3-tuples, it's likely a standardized comparison
    return tuple_count >= len(data) * 0.5


def is_standardized_metrics_result(data: Any) -> bool:
    """Check if data is a standardized metrics result."""
    if not isinstance(data, dict) or len(data) == 0:
        return False

    # Check if all values are numeric-like (not tuples or complex objects)
    for value in data.values():
        if isinstance(value, tuple):
            return False  # This would be a comparison result
        if isinstance(value, str):
            return False
        if isinstance(value, (list, set)):
            return False
        if isinstance(value, dict) and len(value) > 5:
            return False  # Complex nested dict

    return True


def delegate_to_monolithic(formatter, data: Dict[str, Any], title: Optional[str] = None) -> None:
    """Delegates formatting to the monolithic implementation."""
    from ..formatters import OutputFormatter as MonolithicFormatter

    proxy = MonolithicFormatter(
        formatter.format_type, formatter.quiet, formatter.show_all_metrics
    )
    proxy.last_app_mapping = formatter.last_app_mapping

    # Determine which method to call based on data pattern
    if is_comparison_result(data):
        proxy._format_comparison_result(data, title)
    elif is_stage_comparison_result(data):
        proxy._format_stage_comparison_result(data, title)
    elif is_timeline_comparison_result(data):
        proxy._format_timeline_comparison_result(data, title)
    elif is_executor_comparison_result(data):
        proxy._format_executor_comparison_result(data, title)
    elif is_job_comparison_result(data):
        proxy._format_job_comparison_result(data, title)
    elif is_app_summary_comparison_result(data):
        proxy._format_app_summary_diff(data)
    elif is_aggregated_stage_comparison_result(data):
        proxy._format_aggregated_stage_comparison_result(data, title)
    elif is_environment_comparison_result(data):
        proxy._format_environment_comparison(data, title)
    elif is_resource_comparison_result(data):
        proxy._format_resource_comparison_result(data, title)
    elif is_standardized_comparison_result(data):
        proxy._format_standardized_comparison_result(data, title)
    elif is_standardized_metrics_result(data):
        proxy._format_standardized_metrics_result(data, title)
    else:
        # Fallback to dict formatting (should already be handled by basic.py but just in case)
        proxy._format_dict(data)

    formatter.last_app_mapping = proxy.last_app_mapping


# Register comparison patterns
# Note: Order matters if patterns overlap. registry.get_formatter returns the first match.
registry.register_pattern(is_comparison_result, delegate_to_monolithic)
registry.register_pattern(is_stage_comparison_result, delegate_to_monolithic)
registry.register_pattern(is_timeline_comparison_result, delegate_to_monolithic)
registry.register_pattern(is_executor_comparison_result, delegate_to_monolithic)
registry.register_pattern(is_job_comparison_result, delegate_to_monolithic)
registry.register_pattern(is_app_summary_comparison_result, delegate_to_monolithic)
registry.register_pattern(is_aggregated_stage_comparison_result, delegate_to_monolithic)
registry.register_pattern(is_environment_comparison_result, delegate_to_monolithic)
registry.register_pattern(is_resource_comparison_result, delegate_to_monolithic)
registry.register_pattern(is_standardized_comparison_result, delegate_to_monolithic)
registry.register_pattern(is_standardized_metrics_result, delegate_to_monolithic)
