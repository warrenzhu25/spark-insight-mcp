"""
Comparison result formatting methods for Spark History Server MCP CLI.

Contains detection methods and standalone functions for formatting comparison results.
"""

from typing import Any, Dict, List, Optional

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

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


# Group A: Core Comparison Components


def format_comparison_header(formatter, applications: Dict[str, Any]) -> None:
    """Format the applications being compared."""
    if not RICH_AVAILABLE:
        return

    if "app1" in applications and "app2" in applications:
        app1 = applications["app1"]
        app2 = applications["app2"]

        content = (
            f"[bold]App1:[/bold] {app1.get('name', app1.get('id', 'Unknown'))}\n"
        )
        content += (
            f"[bold]App2:[/bold] {app2.get('name', app2.get('id', 'Unknown'))}"
        )

        console.print(
            Panel(content, title="Performance Comparison", border_style="blue")
        )


def format_executive_summary(formatter, data: Dict[str, Any]) -> None:
    """Format key insights and summary."""
    if not RICH_AVAILABLE:
        return

    summary_items = []

    # Extract key metrics from aggregated overview or performance comparison
    overview = None
    if "aggregated_overview" in data:
        overview = data["aggregated_overview"]
    elif "performance_comparison" in data:
        overview = data["performance_comparison"]

    if overview:
        # Task completion ratio - handle both old and new structure
        executor_data = overview.get("executor_comparison") or overview.get(
            "executors", {}
        )
        if executor_data:
            exec_comp = executor_data
            if "task_completion_ratio_change" in exec_comp:
                change = exec_comp["task_completion_ratio_change"]
                summary_items.append(f"• Task completion efficiency: {change}")

    # Stage performance issues - handle both old and new structure
    stage_dive = None
    if "stage_deep_dive" in data:
        stage_dive = data["stage_deep_dive"]
    elif (
        "performance_comparison" in data
        and "stages" in data["performance_comparison"]
    ):
        stage_dive = data["performance_comparison"]["stages"]

    if stage_dive and "top_stage_differences" in stage_dive:
        differences = stage_dive["top_stage_differences"]
        if differences:
            max_diff = max(
                (
                    diff.get("time_difference", {}).get("absolute_seconds", 0)
                    for diff in differences
                ),
                default=0,
            )
            if max_diff > 60:  # More than 1 minute difference
                count = sum(
                    1
                    for diff in differences
                    if diff.get("time_difference", {}).get("absolute_seconds", 0)
                    > 60
                )
                summary_items.append(
                    f"• Found {count} stages with >60s time difference"
                )

    # Add recommendations summary - handle both old and new structure
    recommendations = data.get("recommendations") or data.get(
        "key_recommendations", []
    )
    if recommendations:
        rec_count = len(recommendations)
        if rec_count > 0:
            summary_items.append(
                f"• {rec_count} optimization recommendations available"
            )

    if summary_items:
        content = "\n".join(summary_items)
        console.print(
            Panel(content, title="Executive Summary", border_style="green")
        )


def format_performance_metrics(formatter, overview: Dict[str, Any]) -> None:
    """Format all available performance metrics dynamically."""
    if not RICH_AVAILABLE:
        return

    if "executor_comparison" not in overview and "stage_comparison" not in overview:
        return

    table = Table(title="Performance Metrics Comparison", show_lines=True)
    table.add_column("Metric", style="cyan")
    table.add_column("App1", style="blue")
    table.add_column("App2", style="blue")
    table.add_column("Change", style="magenta")

    # Dynamic executor metrics
    if "executor_comparison" in overview:
        exec_data = overview["executor_comparison"]
        if "applications" in exec_data:
            apps = exec_data["applications"]
            app1_metrics = apps.get("app1", {}).get("executor_metrics", {})
            app2_metrics = apps.get("app2", {}).get("executor_metrics", {})

            # Dynamically show key executor metrics for overview
            if formatter.show_all_metrics:
                # Show all available metrics
                all_metric_keys = set(app1_metrics.keys()) | set(
                    app2_metrics.keys()
                )
                key_executor_metrics = sorted(all_metric_keys)
            else:
                # Show only key metrics
                key_executor_metrics = [
                    "completed_tasks",
                    "total_input_bytes",
                    "total_duration",
                ]

            for metric_key in key_executor_metrics:
                if metric_key in app1_metrics and metric_key in app2_metrics:
                    app1_val = app1_metrics[metric_key]
                    app2_val = app2_metrics[metric_key]

                    display_name = formatter._get_executor_metric_display_name(
                        metric_key
                    )
                    formatter_func = formatter._get_executor_metric_formatter(metric_key)

                    app1_display = formatter_func(app1_val)
                    app2_display = formatter_func(app2_val)

                    # Get change from executor comparison analysis
                    if metric_key == "completed_tasks":
                        change = exec_data.get(
                            "task_completion_ratio_change", "N/A"
                        )
                    else:
                        # Calculate change percentage for other metrics
                        if app1_val > 0:
                            change_pct = ((app2_val - app1_val) / app1_val) * 100
                            change = (
                                f"+{change_pct:.1f}%"
                                if change_pct >= 0
                                else f"{change_pct:.1f}%"
                            )
                        else:
                            change = "N/A"

                    table.add_row(display_name, app1_display, app2_display, change)

    # Dynamic stage comparison ratios - show ALL available ratios
    if "stage_comparison" in overview:
        stage_data = overview["stage_comparison"]
        stage_comparison = stage_data.get("stage_comparison", {})

        # Dynamically iterate through all ratio change metrics
        # Sort stage comparison metrics by difference ratio (descending)
        # Metrics are already sorted by the MCP tool, use existing order
        for metric_key in stage_comparison.keys():
            if metric_key.endswith("_ratio_change"):
                change = stage_comparison[metric_key]
                display_name = formatter._get_stage_metric_display_name(metric_key)

                # Get the base ratio to calculate approximate values
                base_key = metric_key.replace("_change", "")
                ratio = stage_comparison.get(base_key, 0)

                if ratio > 0:
                    # For ratio-based metrics, show as baseline vs ratio
                    table.add_row(
                        display_name, "Baseline", f"{ratio:.1%} of App1", change
                    )

    if table.rows:
        console.print(table)


def format_top_metrics_differences(formatter, metrics: List[Dict[str, Any]]) -> None:
    """Format top application-level metric differences."""
    if not RICH_AVAILABLE or not metrics:
        return

    table = Table(title="Top Stage Metric Differences")
    table.add_column("Metric", style="cyan")
    table.add_column("App1", style="blue")
    table.add_column("App2", style="blue")
    table.add_column("Change", style="magenta")

    for item in metrics:
        metric_key = item.get("metric", "unknown")
        left_val = item.get("left")
        right_val = item.get("right")
        percent_change = item.get("percent_change", 0)

        if isinstance(left_val, (int, float)) and isinstance(
            right_val, (int, float)
        ):
            if "bytes" in str(metric_key).lower():
                left_formatted = formatter._format_bytes(left_val)
                right_formatted = formatter._format_bytes(right_val)
            elif (
                "time" in str(metric_key).lower()
                or "duration" in str(metric_key).lower()
            ):
                left_formatted = formatter._format_duration(left_val)
                right_formatted = formatter._format_duration(right_val)
            else:
                left_formatted = f"{left_val:,}"
                right_formatted = f"{right_val:,}"
        else:
            left_formatted = str(left_val) if left_val is not None else "N/A"
            right_formatted = str(right_val) if right_val is not None else "N/A"

        if percent_change > 0:
            change_formatted = f"[red]+{percent_change:.1f}%[/red]"
        elif percent_change < 0:
            change_formatted = f"[green]{percent_change:.1f}%[/green]"
        else:
            change_formatted = "0.0%"

        table.add_row(
            str(metric_key),
            left_formatted,
            right_formatted,
            change_formatted,
        )

    console.print(table)


def format_stage_differences(formatter, stage_deep_dive: Dict[str, Any]) -> None:
    """Format top stage differences in a table format."""
    if not RICH_AVAILABLE or "top_stage_differences" not in stage_deep_dive:
        return

    differences = stage_deep_dive["top_stage_differences"][:3]  # Show top 3

    if not differences:
        return

    # Create table
    table = Table(title="Stage Differences", show_lines=True)
    table.add_column("Stage", style="cyan")
    table.add_column("App1", style="blue")
    table.add_column("App2", style="blue")
    table.add_column("Diff", style="magenta")

    for diff in differences:
        stage_name = diff.get("stage_name", "Unknown Stage")
        # Truncate stage name for display
        if len(stage_name) > 25:
            stage_name = stage_name[:22] + "..."

        time_diff = diff.get("time_difference", {})
        app1_stage = diff.get("app1_stage", {})
        app2_stage = diff.get("app2_stage", {})

        # Extract stage IDs and durations
        app1_stage_id = app1_stage.get("stage_id", "N/A")
        app2_stage_id = app2_stage.get("stage_id", "N/A")
        app1_duration = app1_stage.get("duration_seconds", 0)
        app2_duration = app2_stage.get("duration_seconds", 0)

        # Format stage column with stage IDs
        stage_display = f"{stage_name} ({app1_stage_id} vs {app2_stage_id})"

        # Format durations
        app1_display = f"{app1_duration:.1f}s"
        app2_display = f"{app2_duration:.1f}s"

        # Format difference - show only percentage
        percentage = time_diff.get("percentage", 0)
        slower_app = time_diff.get("slower_application", "unknown")

        if slower_app == "app1":
            diff_display = f"[red]+{percentage:.0f}%[/red]"
        else:
            diff_display = f"[green]-{percentage:.0f}%[/green]"

        table.add_row(stage_display, app1_display, app2_display, diff_display)

    console.print(table)


def format_recommendations(formatter, recommendations: List[Dict[str, Any]]) -> None:
    """Format recommendations as a bulleted list."""
    if not RICH_AVAILABLE or not recommendations:
        return

    content = []
    for rec in recommendations:
        priority = rec.get("priority", "medium").upper()
        issue = rec.get("issue", "No description")
        suggestion = rec.get("suggestion", "No suggestion")

        priority_color = {"HIGH": "red", "MEDIUM": "yellow", "LOW": "green"}.get(
            priority, "white"
        )

        content.append(
            f"[bold {priority_color}]{priority}:[/bold {priority_color}] {issue}"
        )
        content.append(f"  → {suggestion}")
        content.append("")  # Empty line between recommendations

    if content:
        console.print(
            Panel(
                "\n".join(content[:-1]), title="Recommendations", border_style="red"
            )
        )


# Group B: Main Comparison Entry Points


def format_comparison_result(formatter, data: Dict[str, Any], title: Optional[str] = None) -> None:
    """Format comparison result data in a clean, focused way."""
    if not RICH_AVAILABLE:
        return

    # 1. Highlighted app names header with separator
    if "applications" in data:
        format_comparison_header(formatter, data["applications"])
        console.print("─" * 80)
        console.print()  # Empty line for spacing

    # 2. Performance Metrics table FIRST (overall picture)
    if "aggregated_overview" in data:
        format_performance_metrics(formatter, data["aggregated_overview"])
    elif "performance_comparison" in data:
        # Handle new structure
        format_performance_metrics(formatter, data["performance_comparison"])

    # 2b. Top application-level metric differences
    if "top_metrics_differences" in data:
        format_top_metrics_differences(formatter, data["top_metrics_differences"])

    # 3. Stage Differences table SECOND (detailed breakdown)
    if "stage_deep_dive" in data:
        format_stage_differences(formatter, data["stage_deep_dive"])
    elif (
        "performance_comparison" in data
        and "stages" in data["performance_comparison"]
    ):
        # Handle new structure - stages are now nested under performance_comparison
        format_stage_differences(formatter, data["performance_comparison"]["stages"])

    # 4. App Summary Diff table THIRD (aggregated metrics comparison)
    # Note: app_summary_diff is complex, delegating to monolithic for now via __getattr__
    app_summary_diff = data.get("app_summary_diff")
    if app_summary_diff is None:
        app_summary_diff = data.get("aggregated_overview", {}).get(
            "application_summary"
        )
    if app_summary_diff:
        formatter._format_app_summary_diff(app_summary_diff)

    # 5. Environment comparison summary
    if "environment_comparison" in data:
        formatter._format_environment_comparison(data["environment_comparison"])

    # 6. Executor timeline comparison summary
    if "timeline_comparison" in data:
        formatter._format_timeline_comparison_result(data)

    # 7. Recommendations panel LAST
    recommendations = data.get("recommendations") or data.get(
        "key_recommendations", []
    )
    if recommendations:
        format_recommendations(formatter, recommendations)


def delegate_to_monolithic(formatter, data: Dict[str, Any], title: Optional[str] = None) -> None:
    """Delegates formatting to the monolithic implementation."""
    from ..formatters import OutputFormatter as MonolithicFormatter

    proxy = MonolithicFormatter(
        formatter.format_type, formatter.quiet, formatter.show_all_metrics
    )
    proxy.last_app_mapping = formatter.last_app_mapping

    # Determine which method to call based on data pattern
    if is_comparison_result(data):
        # We already have a modular implementation for the main comparison result
        format_comparison_result(formatter, data, title)
        return
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
registry.register_pattern(is_comparison_result, format_comparison_result)
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
