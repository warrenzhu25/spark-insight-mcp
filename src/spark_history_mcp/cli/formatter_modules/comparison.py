"""
Comparison result formatting methods for Spark History Server MCP CLI.

Contains detection methods and standalone functions for formatting comparison results.
"""

from typing import Any, Dict, List, Optional

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from tabulate import tabulate

    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False

from .base import registry


def _make_comparison_table(
    title: Optional[str],
    label: str = "Metric",
    show_change: bool = True,
) -> "Table":
    """Create a consistently-formatted comparison table.

    Standard structure:
      <label> (cyan) | App 1 (default, min 10) | App 2 (default, min 10) | Change (magenta, min 8)
    """
    table = Table(title=title, show_lines=True)
    table.add_column(label, style="cyan")
    table.add_column("App 1", min_width=10)
    table.add_column("App 2", min_width=10)
    if show_change:
        table.add_column("Change", style="magenta", min_width=8)
    return table


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


# Core Comparison Components


def format_comparison_header(formatter, applications: Dict[str, Any]) -> None:
    """Format the applications being compared."""
    if not RICH_AVAILABLE:
        return

    if "app1" in applications and "app2" in applications:
        app1 = applications["app1"]
        app2 = applications["app2"]

        content = f"[bold]App1:[/bold] {app1.get('name', app1.get('id', 'Unknown'))}\n"
        content += f"[bold]App2:[/bold] {app2.get('name', app2.get('id', 'Unknown'))}"

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
        "performance_comparison" in data and "stages" in data["performance_comparison"]
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
                    if diff.get("time_difference", {}).get("absolute_seconds", 0) > 60
                )
                summary_items.append(
                    f"• Found {count} stages with >60s time difference"
                )

    # Add recommendations summary - handle both old and new structure
    recommendations = data.get("recommendations") or data.get("key_recommendations", [])
    if recommendations:
        rec_count = len(recommendations)
        if rec_count > 0:
            summary_items.append(
                f"• {rec_count} optimization recommendations available"
            )

    if summary_items:
        content = "\n".join(summary_items)
        console.print(Panel(content, title="Executive Summary", border_style="green"))


def format_performance_metrics(formatter, overview: Dict[str, Any]) -> None:
    """Format all available performance metrics dynamically."""
    if not RICH_AVAILABLE:
        return

    if "executor_comparison" not in overview and "stage_comparison" not in overview:
        return

    table = _make_comparison_table("Performance Metrics Comparison")

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
                all_metric_keys = set(app1_metrics.keys()) | set(app2_metrics.keys())
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
                    formatter_func = formatter._get_executor_metric_formatter(
                        metric_key
                    )

                    app1_display = formatter_func(app1_val)
                    app2_display = formatter_func(app2_val)

                    # Get change from executor comparison analysis
                    if metric_key == "completed_tasks":
                        change = exec_data.get("task_completion_ratio_change", "N/A")
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

    table = _make_comparison_table("Top Stage Metric Differences")

    for item in metrics:
        metric_key = item.get("metric", "unknown")
        left_val = item.get("left")
        right_val = item.get("right")
        percent_change = item.get("percent_change", 0)

        if isinstance(left_val, (int, float)) and isinstance(right_val, (int, float)):
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
    table = _make_comparison_table("Stage Differences", label="Stage")

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
            # App1 is slower → App2 improved
            diff_display = f"[green]-{percentage:.0f}%[/green]"
        else:
            # App2 is slower → App2 regressed
            diff_display = f"[red]+{percentage:.0f}%[/red]"

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
            Panel("\n".join(content[:-1]), title="Recommendations", border_style="red")
        )


# Group B: Main Comparison Entry Points


def format_comparison_result(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
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
        "performance_comparison" in data and "stages" in data["performance_comparison"]
    ):
        # Handle new structure - stages are now nested under performance_comparison
        format_stage_differences(formatter, data["performance_comparison"]["stages"])

    # 4. App Summary Diff table THIRD (aggregated metrics comparison)
    app_summary_diff = data.get("app_summary_diff")
    if app_summary_diff is None:
        app_summary_diff = data.get("aggregated_overview", {}).get(
            "application_summary"
        )
    if app_summary_diff:
        format_app_summary_diff(formatter, app_summary_diff)

    # 5. Environment comparison summary
    if "environment_comparison" in data:
        format_environment_comparison_result(formatter, data["environment_comparison"])

    # 6. Executor timeline comparison summary
    if "timeline_comparison" in data:
        format_timeline_comparison_result(formatter, data)

    # 7. Recommendations panel LAST
    recommendations = data.get("recommendations") or data.get("key_recommendations", [])
    if recommendations:
        format_recommendations(formatter, recommendations)


def format_stage_comparison_result(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format stage comparison result in a structured, readable way."""
    if not RICH_AVAILABLE:
        return

    # 1. Stage Info Header
    format_stage_info_header(formatter, data)

    # 2. Performance Metrics Table
    format_stage_performance_metrics(formatter, data)

    # 3. Summary Panel
    format_stage_summary(formatter, data)


def format_stage_info_header(formatter, data: Dict[str, Any]) -> None:
    """Format stage information header panel."""
    if not RICH_AVAILABLE:
        return

    stage_comp = data.get("stage_comparison", {})
    stage1 = stage_comp.get("stage1", {})
    stage2 = stage_comp.get("stage2", {})

    # Extract stage info
    stage_id = stage1.get("stage_id", "N/A")
    stage_name = stage1.get("name", "Unknown Stage")[:60]  # Truncate long names
    stage_status = stage1.get("status", "Unknown")

    # Get app names (shortened)
    app1_id = stage1.get("app_id", "App1")[:20] + "..."
    app2_id = stage2.get("app_id", "App2")[:20] + "..."

    content = f"[bold]Stage {stage_id}:[/bold] {stage_name}\n"
    content += f"[bold]App1:[/bold] {app1_id}\n"
    content += f"[bold]App2:[/bold] {app2_id}\n"
    content += f"[bold]Status:[/bold] {stage_status} ✓"

    console.print(
        Panel(
            content,
            title=f"Stage Comparison: {stage_id} vs {stage_id}",
            border_style="blue",
        )
    )


def format_stage_performance_metrics(formatter, data: Dict[str, Any]) -> None:
    """Format stage performance metrics in a comparison table with all available metrics."""
    if not RICH_AVAILABLE:
        return

    sig_diff = data.get("significant_differences", {})
    stage_metrics = sig_diff.get("stage_metrics", {})
    task_dist = sig_diff.get("task_distributions", {})
    exec_dist = sig_diff.get("executor_distributions", {})

    if not stage_metrics and not task_dist and not exec_dist:
        return

    table = _make_comparison_table("Performance Metrics Comparison")

    # Sort stage-level metrics by difference ratio (descending)
    # Metrics are already sorted by the MCP tool, use existing order
    for metric_key in stage_metrics.keys():
        metric_data = stage_metrics[metric_key]
        display_name = formatter._get_metric_display_name(metric_key)

        # Format values based on metric type
        if "bytes" in metric_key.lower():
            app1_val = formatter._format_bytes(metric_data.get("stage1", 0))
            app2_val = formatter._format_bytes(metric_data.get("stage2", 0))
        elif "time" in metric_key.lower() or "duration" in metric_key.lower():
            app1_val = formatter._format_duration(metric_data.get("stage1", 0))
            app2_val = formatter._format_duration(metric_data.get("stage2", 0))
        else:
            app1_val = str(metric_data.get("stage1", 0))
            app2_val = str(metric_data.get("stage2", 0))

        change = metric_data.get("change", "N/A")
        table.add_row(display_name, app1_val, app2_val, change)

    # Metrics are already sorted by the MCP tool, use existing order
    for metric_key in task_dist.keys():
        metric_data = task_dist[metric_key]
        display_name = formatter._get_metric_display_name(metric_key)

        # Use median values for primary comparison
        if "median" in metric_data:
            median_data = metric_data["median"]

            # Format values based on metric type
            if metric_key in ["shuffle_read_bytes", "shuffle_write_bytes"]:
                app1_val = formatter._format_bytes(median_data.get("stage1", 0))
                app2_val = formatter._format_bytes(median_data.get("stage2", 0))
            else:
                app1_val = formatter._format_duration(median_data.get("stage1", 0))
                app2_val = formatter._format_duration(median_data.get("stage2", 0))

            change = median_data.get("change", "N/A")
            table.add_row(f"Median {display_name}", app1_val, app2_val, change)

        # Show max values too if available
        if "max" in metric_data:
            max_data = metric_data["max"]

            # Format values based on metric type
            if metric_key in ["shuffle_read_bytes", "shuffle_write_bytes"]:
                app1_val = formatter._format_bytes(max_data.get("stage1", 0))
                app2_val = formatter._format_bytes(max_data.get("stage2", 0))
            else:
                app1_val = formatter._format_duration(max_data.get("stage1", 0))
                app2_val = formatter._format_duration(max_data.get("stage2", 0))

            change = max_data.get("change", "N/A")
            table.add_row(f"Max {display_name}", app1_val, app2_val, change)

    # Metrics are already sorted by the MCP tool, use existing order
    for metric_key in exec_dist.keys():
        metric_data = exec_dist[metric_key]
        display_name = f"Executor {formatter._get_metric_display_name(metric_key)}"

        if "median" in metric_data:
            median_data = metric_data["median"]
            app1_val = formatter._format_duration(median_data.get("stage1", 0))
            app2_val = formatter._format_duration(median_data.get("stage2", 0))
            change = median_data.get("change", "N/A")
            table.add_row(f"Median {display_name}", app1_val, app2_val, change)

    if table.rows:
        console.print(table)


def format_stage_summary(formatter, data: Dict[str, Any]) -> None:
    """Format stage comparison summary panel."""
    if not RICH_AVAILABLE:
        return

    summary = data.get("summary", {})
    sig_diff = data.get("significant_differences", {})

    summary_items = []

    # Number of differences found
    total_diffs = summary.get("total_differences_found", 0)
    if total_diffs > 0:
        summary_items.append(f"• {total_diffs} metrics compared")

    # Key performance insight
    task_dist = sig_diff.get("task_distributions", {})
    if "duration" in task_dist and "median" in task_dist["duration"]:
        change = task_dist["duration"]["median"].get("change", "")
        if change and "%" in change:
            if change.startswith("-"):
                summary_items.append(f"• App2 is {change[1:]} faster than App1")
            else:
                summary_items.append(
                    f"• App1 is {change[1:] if change.startswith('+') else change} faster than App2"
                )

    # Stage status
    stage_comp = data.get("stage_comparison", {})
    stage1 = stage_comp.get("stage1", {})
    if stage1.get("status") == "COMPLETE":
        summary_items.append("• Both stages completed successfully")

    if summary_items:
        content = "\n".join(summary_items)
        console.print(Panel(content, title="Performance Summary", border_style="green"))


def format_app_summary_diff(
    formatter, app_summary_diff: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format application summary differences in a table format."""
    if not RICH_AVAILABLE or "diff" not in app_summary_diff:
        return

    diff_data = app_summary_diff["diff"]
    app1_summary = app_summary_diff.get("app1_summary", {})
    app2_summary = app_summary_diff.get("app2_summary", {})

    # Add app names header (extract from aggregated_stage_comparison if available)
    agg_stage = app_summary_diff.get("aggregated_stage_comparison", {})
    applications = agg_stage.get("applications", {})
    if applications:
        app1_data = applications.get("app1", {})
        app2_data = applications.get("app2", {})
        app1_name = app1_data.get("name", app1_data.get("id", "App1"))
        app2_name = app2_data.get("name", app2_data.get("id", "App2"))

        console.print(f"[cyan]{app1_name}[/cyan] vs [cyan]{app2_name}[/cyan]")
        console.print("─" * 80)
        console.print()  # Empty line for spacing

    # Extract and merge unique metrics from aggregated_stage_comparison
    agg_metrics = agg_stage.get("aggregated_stage_metrics", {})
    app1_agg = agg_metrics.get("app1", {})
    app2_agg = agg_metrics.get("app2", {})
    stage_comp = agg_stage.get("stage_performance_comparison", {})

    # Merge total_tasks
    if "total_tasks" in app1_agg:
        app1_summary["total_tasks"] = app1_agg["total_tasks"]
        app2_summary["total_tasks"] = app2_agg.get("total_tasks", 0)
        if "total_tasks_percent_change" in stage_comp:
            diff_data["total_tasks_change"] = (
                f"{stage_comp['total_tasks_percent_change']:+.1f}%"
            )

    # Create table with static title
    table = _make_comparison_table("Summary Comparison")

    # Define metric display preferences for formatting
    metric_display_config = {
        # Time metrics
        "application_duration_minutes": ("App Duration (min)", "time"),
        "total_executor_runtime_minutes": ("Executor Runtime (min)", "time"),
        "executor_cpu_time_minutes": ("CPU Time (min)", "time"),
        "jvm_gc_time_minutes": ("GC Time (min)", "time"),
        "shuffle_read_wait_time_minutes": ("Shuffle Read Wait (min)", "time"),
        "shuffle_write_time_minutes": ("Shuffle Write Time (min)", "time"),
        # Size metrics (GB)
        "input_data_size_gb": ("Input Data (GB)", "size"),
        "output_data_size_gb": ("Output Data (GB)", "size"),
        "shuffle_read_size_gb": ("Shuffle Read (GB)", "size"),
        "shuffle_write_size_gb": ("Shuffle Write (GB)", "size"),
        "memory_spilled_gb": ("Memory Spilled (GB)", "size"),
        "disk_spilled_gb": ("Disk Spilled (GB)", "size"),
        # Percentage metrics
        "executor_utilization_percent": ("Executor Utilization (%)", "percent"),
        # Count metrics
        "total_stages": ("Total Stages", "count"),
        "completed_stages": ("Completed Stages", "count"),
        "failed_stages": ("Failed Stages", "count"),
        "failed_tasks": ("Failed Tasks", "count"),
        "total_tasks": ("Total Tasks", "count"),
    }

    # Use the sorted order from diff keys (already sorted by MCP tool)
    # Extract metric names from the sorted diff keys (remove '_change' suffix)
    sorted_metric_names = [
        key.replace("_change", "")
        for key in diff_data.keys()
        if key.endswith("_change")
    ]

    # Add any metrics that don't have change values (shouldn't normally happen)
    all_available_metrics = [
        key for key in app1_summary.keys() if key != "application_id"
    ]
    for metric in all_available_metrics:
        if metric not in sorted_metric_names:
            sorted_metric_names.append(metric)

    # Use the sorted order from the MCP tool
    for field_name in sorted_metric_names:
        # Get display configuration or use field name as fallback
        if field_name in metric_display_config:
            display_name, format_type = metric_display_config[field_name]
        else:
            # Auto-generate display name from field name
            display_name = field_name.replace("_", " ").title()
            format_type = "default"

        app1_val = app1_summary.get(field_name, 0)
        app2_val = app2_summary.get(field_name, 0)

        # Format values based on type
        if format_type == "size":
            app1_str = f"{app1_val:.2f}"
            app2_str = f"{app2_val:.2f}"
        elif format_type == "time":
            app1_str = f"{app1_val:.2f}"
            app2_str = f"{app2_val:.2f}"
        elif format_type == "percent":
            app1_str = f"{app1_val:.1f}"
            app2_str = f"{app2_val:.1f}"
        elif isinstance(app1_val, float):
            app1_str = f"{app1_val:.2f}"
            app2_str = f"{app2_val:.2f}"
        else:
            app1_str = str(app1_val)
            app2_str = str(app2_val)

        # Get change percentage dynamically
        change_key = f"{field_name}_change"
        change_str = diff_data.get(change_key, "N/A")

        table.add_row(display_name, app1_str, app2_str, change_str)

    console.print()  # Empty line for spacing
    console.print(table)


# Group C: Specialized Comparisons


def format_timeline_comparison_result(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format timeline comparison result in a structured, readable way."""
    if not RICH_AVAILABLE:
        return

    # Show only the timeline intervals table for compact output.
    format_timeline_overview_header(formatter, data)
    format_timeline_efficiency_panel(formatter, data)
    format_timeline_intervals_table(formatter, data)
    format_timeline_summary(formatter, data)


def format_timeline_overview_header(formatter, data: Dict[str, Any]) -> None:
    """Format timeline comparison overview header."""
    if not RICH_AVAILABLE:
        return

    app1_info = data.get("app1_info", {})
    app2_info = data.get("app2_info", {})

    # Extract app information
    app1_name = app1_info.get("name", "App1")[:40]
    app2_name = app2_info.get("name", "App2")[:40]
    app1_duration = app1_info.get("duration_seconds", 0)
    app2_duration = app2_info.get("duration_seconds", 0)

    # Format timestamps
    app1_start = (
        app1_info.get("start_time", "")[:19]
        if app1_info.get("start_time")
        else "Unknown"
    )
    app1_end = (
        app1_info.get("end_time", "")[:19] if app1_info.get("end_time") else "Unknown"
    )
    app2_start = (
        app2_info.get("start_time", "")[:19]
        if app2_info.get("start_time")
        else "Unknown"
    )
    app2_end = (
        app2_info.get("end_time", "")[:19] if app2_info.get("end_time") else "Unknown"
    )

    # Calculate performance difference
    duration_diff = app1_duration - app2_duration
    if app2_duration > 0:
        perf_pct = abs(duration_diff) / app2_duration * 100
        if duration_diff > 0:
            perf_text = f"App2 is {duration_diff:.1f}s ({perf_pct:.1f}%) faster"
        else:
            perf_text = f"App1 is {abs(duration_diff):.1f}s ({perf_pct:.1f}%) faster"
    else:
        perf_text = "Performance comparison unavailable"

    content = f"[bold]App1:[/bold] {app1_name}\n"
    content += f"Duration: {app1_duration:.1f}s ({app1_start} → {app1_end})\n\n"
    content += f"[bold]App2:[/bold] {app2_name}\n"
    content += f"Duration: {app2_duration:.1f}s ({app2_start} → {app2_end})\n\n"
    content += f"[bold]Performance:[/bold] {perf_text}"

    console.print(
        Panel(content, title="Application Timeline Comparison", border_style="blue")
    )


def format_timeline_intervals_table(formatter, data: Dict[str, Any]) -> None:
    """Format timeline intervals in a table."""
    if not RICH_AVAILABLE:
        return

    timeline_comp = data.get("timeline_comparison", [])

    if not timeline_comp:
        return

    table = Table(title="Timeline Intervals", show_lines=True)
    table.add_column("Interval", style="cyan")
    table.add_column("Time Range", style="green")
    table.add_column("App1 Executors", style="blue")
    table.add_column("App2 Executors", style="blue")
    table.add_column("Difference", style="magenta")

    for interval_data in timeline_comp:
        interval = str(interval_data.get("interval", "N/A"))
        time_range = interval_data.get("timestamp_range", "Unknown")

        # Simplify time range display
        if " to " in time_range:
            start_time = time_range.split(" to ")[0][-8:]  # Last 8 chars (HH:MM:SS)
            end_time = time_range.split(" to ")[1][-8:]
            time_display = f"{start_time} → {end_time}"
        else:
            time_display = (
                time_range[:20] + "..." if len(time_range) > 20 else time_range
            )

        app1_execs = interval_data.get("app1", {}).get("executor_count", 0)
        app2_execs = interval_data.get("app2", {}).get("executor_count", 0)
        diff = interval_data.get("differences", {}).get("executor_count_diff", 0)

        diff_display = f"+{diff}" if diff > 0 else str(diff) if diff != 0 else "Same"

        table.add_row(
            interval, time_display, str(app1_execs), str(app2_execs), diff_display
        )

    console.print(table)


def format_timeline_efficiency_panel(formatter, data: Dict[str, Any]) -> None:
    """Format resource efficiency comparison panel."""
    if not RICH_AVAILABLE:
        return

    resource_eff = data.get("resource_efficiency", {})
    app1_eff = resource_eff.get("app1", {})
    app2_eff = resource_eff.get("app2", {})

    if not app1_eff or not app2_eff:
        return

    # Extract efficiency metrics
    app1_score = app1_eff.get("efficiency_score", 0) * 100
    app2_score = app2_eff.get("efficiency_score", 0) * 100
    app1_peak = app1_eff.get("peak_executor_count", 0)
    app2_peak = app2_eff.get("peak_executor_count", 0)
    app1_avg = app1_eff.get("avg_executor_count", 0)
    app2_avg = app2_eff.get("avg_executor_count", 0)

    # Calculate efficiency difference
    eff_diff = app1_score - app2_score
    if eff_diff > 0:
        eff_text = f"App1 is {eff_diff:.1f}% more resource efficient"
    elif eff_diff < 0:
        eff_text = f"App2 is {abs(eff_diff):.1f}% more resource efficient"
    else:
        eff_text = "Both applications have similar efficiency"

    content = f"[bold]App1:[/bold] {app1_score:.1f}% efficiency ({app1_peak} peak, {app1_avg:.1f} avg executors)\n"
    content += f"[bold]App2:[/bold] {app2_score:.1f}% efficiency ({app2_peak} peak, {app2_avg:.1f} avg executors)\n\n"
    content += f"[bold]Efficiency Comparison:[/bold] {eff_text}"

    console.print(Panel(content, title="Resource Efficiency", border_style="green"))


def format_timeline_summary(formatter, data: Dict[str, Any]) -> None:
    """Format timeline comparison summary."""
    if not RICH_AVAILABLE:
        return

    summary = data.get("summary", {})
    recommendations = data.get("recommendations", [])

    summary_items = []

    # Timeline analysis info
    intervals = summary.get("original_intervals", 0)
    merged = summary.get("merged_intervals", 0)
    if intervals > 0:
        summary_items.append(
            f"• Analyzed {intervals} time intervals (merged to {merged})"
        )

    # Performance improvement
    perf_improvement = summary.get("performance_improvement", {})
    time_diff = perf_improvement.get("time_difference_seconds", 0)
    if time_diff != 0:
        if time_diff > 0:
            summary_items.append(f"• App2 completed {time_diff:.1f}s faster than App1")
        else:
            summary_items.append(
                f"• App1 completed {abs(time_diff):.1f}s faster than App2"
            )

    # Resource usage
    max_diff = summary.get("max_executor_count_difference", 0)
    if max_diff == 0:
        summary_items.append("• Both applications used identical peak executor counts")
    else:
        summary_items.append(f"• Peak executor difference: {max_diff}")

    # Recommendations count
    if recommendations:
        summary_items.append(
            f"• {len(recommendations)} optimization recommendations available"
        )

    if summary_items:
        content = "\n".join(summary_items)
        console.print(
            Panel(content, title="Timeline Analysis Summary", border_style="yellow")
        )


def format_executor_comparison_result(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format executor comparison with clean header and performance table."""
    if not RICH_AVAILABLE:
        return

    # 1. Highlighted app names header with separator
    if "applications" in data:
        app1_data = data["applications"].get("app1", {})
        app2_data = data["applications"].get("app2", {})
        app1_name = app1_data.get("name", app1_data.get("id", "App1"))
        app2_name = app2_data.get("name", app2_data.get("id", "App2"))

        console.print(f"[cyan]{app1_name}[/cyan] vs [cyan]{app2_name}[/cyan]")
        console.print("─" * 80)
        console.print()  # Empty line for spacing

    # 2. Executor Performance Table
    table = _make_comparison_table("Executor Performance Comparison")

    # Extract executor metrics
    app1_metrics = (
        data.get("applications", {}).get("app1", {}).get("executor_metrics", {})
    )
    app2_metrics = (
        data.get("applications", {}).get("app2", {}).get("executor_metrics", {})
    )

    # Dynamically show all available executor metrics, sorted by difference ratio
    all_metrics = set(app1_metrics.keys()) | set(app2_metrics.keys())

    # Metrics are already sorted by the MCP tool, use existing order
    for metric_key in all_metrics:
        app1_val = app1_metrics.get(metric_key, 0)
        app2_val = app2_metrics.get(metric_key, 0)

        if app1_val is not None and app2_val is not None:
            display_name = formatter._get_executor_metric_display_name(metric_key)
            formatter_func = formatter._get_executor_metric_formatter(metric_key)

            app1_display = formatter_func(app1_val)
            app2_display = formatter_func(app2_val)

            # Calculate change
            if metric_key == "total_executors" and app1_val == app2_val:
                change = "Same"
            elif app1_val > 0:
                change_pct = ((app2_val - app1_val) / app1_val) * 100
                if change_pct >= 0:
                    change = f"+{change_pct:.1f}%"
                else:
                    change = f"{change_pct:.1f}%"
            else:
                change = "N/A"

            table.add_row(display_name, app1_display, app2_display, change)

    # Add efficiency metrics from executor_comparison
    exec_comp = data.get("executor_comparison", {})
    if "task_completion_ratio_change" in exec_comp:
        change = exec_comp["task_completion_ratio_change"]
        table.add_row("Task Completion Efficiency", "Baseline", "Comparison", change)

    # Add efficiency ratios
    eff_ratios = data.get("efficiency_ratios", {})
    if "tasks_per_executor_ratio_change" in eff_ratios:
        change = eff_ratios["tasks_per_executor_ratio_change"]
        eff_metrics = data.get("efficiency_metrics", {})
        app1_tpe = eff_metrics.get("app1_tasks_per_executor", 0)
        app2_tpe = eff_metrics.get("app2_tasks_per_executor", 0)
        table.add_row(
            "Tasks per Executor", f"{app1_tpe:.1f}", f"{app2_tpe:.1f}", change
        )

    if table.rows:
        console.print(table)


def format_job_comparison_result(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format job comparison with timing and success metrics."""
    if not RICH_AVAILABLE:
        return

    # 1. Highlighted app names header with separator
    if "applications" in data:
        app1_data = data["applications"].get("app1", {})
        app2_data = data["applications"].get("app2", {})
        app1_name = app1_data.get("name", app1_data.get("id", "App1"))
        app2_name = app2_data.get("name", app2_data.get("id", "App2"))

        console.print(f"[cyan]{app1_name}[/cyan] vs [cyan]{app2_name}[/cyan]")
        console.print("─" * 80)
        console.print()  # Empty line for spacing

    # 2. Job Performance Table
    table = _make_comparison_table("Job Performance Comparison")

    # Extract job metrics
    app1_data = data.get("applications", {}).get("app1", {})
    app2_data = data.get("applications", {}).get("app2", {})

    app1_jobs = app1_data.get("job_stats", {})
    app2_jobs = app2_data.get("job_stats", {})

    # Success rates from application data
    app1_success = app1_data.get("success_rate", 0)
    app2_success = app2_data.get("success_rate", 0)
    if app1_success > 0 or app2_success > 0:
        table.add_row(
            "Success Rate",
            f"{app1_success:.1%}",
            f"{app2_success:.1%}",
            "Same"
            if app1_success == app2_success
            else f"{((app2_success - app1_success) * 100):.1f}%",
        )

    # Metrics are already sorted by the MCP tool, use existing order
    all_job_metrics = set(app1_jobs.keys()) | set(app2_jobs.keys())
    for metric_key in all_job_metrics:
        app1_val = app1_jobs.get(metric_key, 0)
        app2_val = app2_jobs.get(metric_key, 0)

        if (
            app1_val is not None
            and app2_val is not None
            and (app1_val > 0 or app2_val > 0)
        ):
            display_name = formatter._get_stage_metric_display_name(metric_key)

            # Format based on metric type
            if "duration" in metric_key or "time" in metric_key:
                app1_display = formatter._format_duration(app1_val)
                app2_display = formatter._format_duration(app2_val)
            elif "bytes" in metric_key or "size" in metric_key:
                app1_display = formatter._format_bytes(app1_val)
                app2_display = formatter._format_bytes(app2_val)
            else:
                app1_display = str(app1_val)
                app2_display = str(app2_val)

            # Calculate change
            if app1_val > 0:
                change_pct = ((app2_val - app1_val) / app1_val) * 100
                change = (
                    f"+{change_pct:.1f}%" if change_pct >= 0 else f"{change_pct:.1f}%"
                )
            else:
                change = "N/A"

            table.add_row(display_name, app1_display, app2_display, change)

    # Job comparison analysis metrics
    timing = data.get("timing_analysis", {})

    # Add timing analysis if available
    if timing:
        for timing_key in sorted(timing.keys()):
            timing_val = timing[timing_key]
            if timing_key.endswith("_seconds") and timing_val != 0:
                display_name = (
                    timing_key.replace("_", " ").replace("seconds", "").title()
                )
                table.add_row(
                    display_name, "Baseline", "Comparison", f"{timing_val:.1f}s"
                )
            elif timing_key.endswith("_percent") and timing_val != 0:
                display_name = (
                    timing_key.replace("_", " ").replace("percent", "").title()
                )
                table.add_row(
                    display_name, "Baseline", "Comparison", f"{timing_val:.1f}%"
                )

    if table.rows:
        console.print(table)


def format_aggregated_stage_comparison_result(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format aggregated stage metrics comprehensively."""
    if not RICH_AVAILABLE:
        return

    # 1. Highlighted app names header with separator
    if "applications" in data:
        app1_data = data["applications"].get("app1", {})
        app2_data = data["applications"].get("app2", {})
        app1_name = app1_data.get("name", app1_data.get("id", "App1"))
        app2_name = app2_data.get("name", app2_data.get("id", "App2"))

        console.print(f"[cyan]{app1_name}[/cyan] vs [cyan]{app2_name}[/cyan]")
        console.print("─" * 80)
        console.print()  # Empty line for spacing

    # 2. Aggregated Stage Metrics Table
    table = _make_comparison_table("Aggregated Stage Metrics Comparison")

    # Get raw metrics and comparison data
    agg_metrics = data.get("aggregated_stage_metrics", {})
    app1_metrics = agg_metrics.get("app1", {})
    app2_metrics = agg_metrics.get("app2", {})
    if not app1_metrics and not app2_metrics:
        app1_metrics = (
            data.get("applications", {}).get("app1", {}).get("stage_metrics", {})
        )
        app2_metrics = (
            data.get("applications", {}).get("app2", {}).get("stage_metrics", {})
        )
    stage_comp = data.get("stage_performance_comparison", {})
    if not stage_comp:
        stage_comp = data.get("stage_comparison", {})

    ratio_metric_map = {
        "stage_count_ratio": "total_stages",
        "duration_ratio": "total_stage_duration",
        "executor_runtime_ratio": "total_executor_run_time",
        "memory_spill_ratio": "total_memory_spilled",
        "shuffle_read_ratio": "total_shuffle_read_bytes",
        "shuffle_write_ratio": "total_shuffle_write_bytes",
        "input_ratio": "total_input_bytes",
        "output_ratio": "total_output_bytes",
        "task_failure_ratio": "total_failed_tasks",
    }

    # Iterate over percent_change entries in the comparison
    for metric_key in sorted(stage_comp.keys()):
        if metric_key.endswith("_percent_change"):
            # Extract base metric name
            base_metric = metric_key.replace("_percent_change", "")
            display_name = formatter._get_stage_metric_display_name(base_metric)
            change_pct = stage_comp[metric_key]

            # Get actual values from aggregated metrics
            app1_val = app1_metrics.get(base_metric, 0)
            app2_val = app2_metrics.get(base_metric, 0)

            # Format values based on metric type
            app1_display = format_stage_metric_value(formatter, base_metric, app1_val)
            app2_display = format_stage_metric_value(formatter, base_metric, app2_val)

            change_str = f"{change_pct:+.1f}%"
            table.add_row(display_name, app1_display, app2_display, change_str)
        elif metric_key.endswith("_ratio"):
            # Skip if a _percent_change entry already covers this metric
            base_for_ratio = metric_key[: -len("_ratio")]
            if f"{base_for_ratio}_percent_change" in stage_comp:
                continue
            base_metric = ratio_metric_map.get(
                metric_key, metric_key.replace("_ratio", "")
            )
            display_name = formatter._get_stage_metric_display_name(base_metric)
            ratio_val = stage_comp.get(metric_key, 0)

            app1_val = app1_metrics.get(base_metric)
            app2_val = app2_metrics.get(base_metric)
            app1_display = format_stage_metric_value(formatter, base_metric, app1_val)
            app2_display = format_stage_metric_value(formatter, base_metric, app2_val)

            change_str = "N/A"
            if isinstance(ratio_val, (int, float)):
                change_str = f"{ratio_val:.2f}x"
            if isinstance(app1_val, (int, float)) and isinstance(
                app2_val, (int, float)
            ):
                if max(abs(app1_val), 0) > 0:
                    change_pct = ((app2_val - app1_val) / max(abs(app1_val), 1)) * 100
                    change_str = f"{change_pct:+.1f}%"

            table.add_row(display_name, app1_display, app2_display, change_str)

    # If no comparison entries but we have raw metrics, show all metrics
    if not table.rows and app1_metrics and app2_metrics:
        for metric_key in sorted(app1_metrics.keys()):
            if metric_key in app2_metrics:
                display_name = formatter._get_stage_metric_display_name(
                    metric_key + "_percent_change"
                )
                app1_val = app1_metrics[metric_key]
                app2_val = app2_metrics[metric_key]
                app1_display = format_stage_metric_value(
                    formatter, metric_key, app1_val
                )
                app2_display = format_stage_metric_value(
                    formatter, metric_key, app2_val
                )
                if max(abs(app1_val), abs(app2_val)) > 0:
                    change_pct = ((app2_val - app1_val) / max(abs(app1_val), 1)) * 100
                    change_str = f"{change_pct:+.1f}%"
                else:
                    change_str = "N/A"
                table.add_row(display_name, app1_display, app2_display, change_str)

    if table.rows:
        console.print(table)


def format_stage_metric_value(formatter, metric_key: str, value) -> str:
    """Format a stage metric value as plain number (units shown in header)."""
    if value is None:
        return "N/A"
    if "bytes" in metric_key:
        # Convert bytes to GB for display
        gb_value = value / (1024 * 1024 * 1024)
        return f"{gb_value:.2f}"
    elif metric_key.endswith("_ms") or "duration_ms" in metric_key:
        # Convert ms to appropriate unit based on metric
        if "avg_stage_duration" in metric_key:
            # Show as seconds
            return f"{value / 1000:.1f}"
        else:
            # Show as minutes
            return f"{value / 60000:.2f}"
    elif metric_key.endswith("_ns"):
        # Nanoseconds - convert to minutes for CPU time
        minutes = value / (1_000_000 * 60 * 1000)
        return f"{minutes:.2f}"
    elif isinstance(value, float):
        return f"{value:,.1f}"
    else:
        return f"{value:,}"


def format_environment_comparison_result(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format environment configuration comparison."""
    if not RICH_AVAILABLE:
        return

    # Header
    if "applications" in data:
        app1_data = data["applications"].get("app1", {})
        app2_data = data["applications"].get("app2", {})
        app1_name = app1_data.get("name", app1_data.get("id", "App1"))
        app2_name = app2_data.get("name", app2_data.get("id", "App2"))
        console.print(f"[cyan]{app1_name}[/cyan] vs [cyan]{app2_name}[/cyan]")
        console.print("─" * 80)
        console.print()

    # JVM info
    jvm_info = data.get("jvm_info", {})
    if jvm_info:
        jvm_table = _make_comparison_table(
            "JVM Information", label="Property", show_change=False
        )
        for key, vals in jvm_info.items():
            if isinstance(vals, dict):
                jvm_table.add_row(
                    key,
                    str(vals.get("app1", "")),
                    str(vals.get("app2", "")),
                )
        if jvm_table.rows:
            console.print(jvm_table)
        console.print()

    # Spark properties differences
    spark_props = data.get("spark_properties", {})
    diff_props = spark_props.get("different", {})
    if diff_props:
        table = _make_comparison_table(
            "Spark Properties Differences", label="Property", show_change=False
        )
        if isinstance(diff_props, list):
            for entry in sorted(diff_props, key=lambda e: e.get("property", "")):
                table.add_row(
                    str(entry.get("property", "")),
                    str(entry.get("app1_value", "N/A")),
                    str(entry.get("app2_value", "N/A")),
                )
        else:
            for prop in sorted(diff_props.keys()):
                values = diff_props[prop]
                table.add_row(
                    prop,
                    str(values.get("app1", "N/A")),
                    str(values.get("app2", "N/A")),
                )
        console.print(table)
        total = spark_props.get(
            "total_different",
            len(diff_props) if isinstance(diff_props, (list, dict)) else 0,
        )
        if isinstance(diff_props, list) and total > len(diff_props):
            console.print(f"  ... and {total - len(diff_props)} more differences")
        console.print()

    # System properties differences
    sys_props = data.get("system_properties", {})
    sys_diff = sys_props.get("different", [])
    if sys_diff:
        table = _make_comparison_table(
            "System Properties Differences", label="Property", show_change=False
        )
        if isinstance(sys_diff, list):
            for item in sys_diff:
                table.add_row(
                    item.get("property", ""),
                    str(item.get("app1_value", "")),
                    str(item.get("app2_value", "")),
                )
        else:
            for prop in sorted(sys_diff.keys()):
                values = sys_diff[prop]
                table.add_row(
                    prop,
                    str(values.get("app1", "N/A")),
                    str(values.get("app2", "N/A")),
                )
        console.print(table)
        total = sys_props.get("total_different", len(sys_diff))
        if total > len(sys_diff):
            console.print(f"  ... and {total - len(sys_diff)} more differences")
        console.print()

    # Summary counts
    summary_parts = []
    if spark_props.get("total_different"):
        summary_parts.append(f"Spark props: {spark_props['total_different']} different")
    if spark_props.get("app1_only_count"):
        summary_parts.append(f"{spark_props['app1_only_count']} app1-only")
    if spark_props.get("app2_only_count"):
        summary_parts.append(f"{spark_props['app2_only_count']} app2-only")
    if sys_props.get("total_different"):
        summary_parts.append(f"System props: {sys_props['total_different']} different")
    if summary_parts:
        console.print("[bold]Summary:[/bold] " + " | ".join(summary_parts))


def format_resource_comparison_result(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format resource allocation comparison."""
    if not RICH_AVAILABLE:
        return

    # 1. Highlighted app names header with separator
    if "applications" in data:
        app1_data = data["applications"].get("app1", {})
        app2_data = data["applications"].get("app2", {})
        app1_name = app1_data.get("name", app1_data.get("id", "App1"))
        app2_name = app2_data.get("name", app2_data.get("id", "App2"))

        console.print(f"[cyan]{app1_name}[/cyan] vs [cyan]{app2_name}[/cyan]")
        console.print("─" * 80)
        console.print()  # Empty line for spacing

    # 2. Resource Allocation Table
    table = _make_comparison_table("Resource Allocation Comparison")

    # Extract resource metrics
    app1_data = data.get("applications", {}).get("app1", {})
    app2_data = data.get("applications", {}).get("app2", {})

    resource_metrics = [
        ("cores_granted", "Cores Granted"),
        ("max_cores", "Max Cores"),
        ("cores_per_executor", "Cores per Executor"),
        ("memory_per_executor_mb", "Memory per Executor (MB)"),
        ("max_executors", "Max Executors"),
    ]

    # Metrics are already sorted by the MCP tool, use existing order
    has_data = False
    for metric_key, display_name in resource_metrics:
        app1_val = app1_data.get(metric_key)
        app2_val = app2_data.get(metric_key)

        if app1_val is not None or app2_val is not None:
            has_data = True
            app1_display = str(app1_val) if app1_val is not None else "N/A"
            app2_display = str(app2_val) if app2_val is not None else "N/A"

            if app1_val is not None and app2_val is not None and app1_val > 0:
                change = f"{((app2_val - app1_val) / app1_val * 100):.1f}%"
            else:
                change = "N/A"

            table.add_row(display_name, app1_display, app2_display, change)

    if has_data and table.rows:
        console.print(table)
    else:
        console.print(
            "[yellow]No resource allocation data available for comparison[/yellow]"
        )


# Group D: Standardized Results


def format_standardized_metrics_result(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format standardized metrics result as a simple key-value table."""
    if not RICH_AVAILABLE:
        return

    if title:
        console.print(f"\n[bold blue]{title}[/bold blue]")

    table = Table(title="Metrics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    for key, value in data.items():
        # Format values nicely
        if isinstance(value, (int, float)):
            if "bytes" in key.lower():
                formatted_value = formatter._format_bytes(value)
            elif "time" in key.lower() or "duration" in key.lower():
                formatted_value = formatter._format_duration(value)
            else:
                formatted_value = f"{value:,}"
        elif value is None:
            formatted_value = "N/A"
        else:
            formatted_value = str(value)

        table.add_row(key, formatted_value)

    console.print(table)


def format_standardized_comparison_result(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format standardized comparison result as a comparison table."""
    if not RICH_AVAILABLE:
        return

    if title:
        console.print(f"\n[bold blue]{title}[/bold blue]")

    table = _make_comparison_table(title or "Comparison")

    for key, value in data.items():
        if isinstance(value, tuple) and len(value) == 3:
            left_val, right_val, percent_change = value

            # Format values nicely
            if isinstance(left_val, (int, float)) and isinstance(
                right_val, (int, float)
            ):
                if "bytes" in key.lower():
                    left_formatted = formatter._format_bytes(left_val)
                    right_formatted = formatter._format_bytes(right_val)
                elif "time" in key.lower() or "duration" in key.lower():
                    left_formatted = formatter._format_duration(left_val)
                    right_formatted = formatter._format_duration(right_val)
                else:
                    left_formatted = f"{left_val:,}"
                    right_formatted = f"{right_val:,}"
            else:
                left_formatted = str(left_val) if left_val is not None else "N/A"
                right_formatted = str(right_val) if right_val is not None else "N/A"

            # Format percentage change with color
            if percent_change > 0:
                change_formatted = f"[red]+{percent_change:.1f}%[/red]"
            elif percent_change < 0:
                change_formatted = f"[green]{percent_change:.1f}%[/green]"
            else:
                change_formatted = "0.0%"

            table.add_row(key, left_formatted, right_formatted, change_formatted)
        else:
            # Fallback for non-tuple values
            table.add_row(key, str(value), "N/A", "N/A")

    console.print(table)


def output_table_metrics(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format standardized metrics result as a simple table using tabulate."""
    if title:
        console.print(f"\n{title}")
        console.print("-" * len(title))

    rows = []
    for key, value in data.items():
        # Format values nicely
        if isinstance(value, (int, float)):
            if "bytes" in key.lower():
                formatted_value = formatter._format_bytes(value)
            elif "time" in key.lower() or "duration" in key.lower():
                formatted_value = formatter._format_duration(value)
            else:
                formatted_value = f"{value:,}"
        elif value is None:
            formatted_value = "N/A"
        else:
            formatted_value = str(value)

        rows.append([key, formatted_value])

    console.print(tabulate(rows, headers=["Metric", "Value"], tablefmt="grid"))


def output_table_comparison(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format standardized comparison result as a comparison table using tabulate."""
    if title:
        console.print(f"\n{title}")
        console.print("-" * len(title))

    rows = []
    for key, value in data.items():
        if isinstance(value, tuple) and len(value) == 3:
            left_val, right_val, percent_change = value

            # Format values nicely
            if isinstance(left_val, (int, float)) and isinstance(
                right_val, (int, float)
            ):
                if "bytes" in key.lower():
                    left_formatted = formatter._format_bytes(left_val)
                    right_formatted = formatter._format_bytes(right_val)
                elif "time" in key.lower() or "duration" in key.lower():
                    left_formatted = formatter._format_duration(left_val)
                    right_formatted = formatter._format_duration(right_val)
                else:
                    left_formatted = f"{left_val:,}"
                    right_formatted = f"{right_val:,}"
            else:
                left_formatted = str(left_val) if left_val is not None else "N/A"
                right_formatted = str(right_val) if right_val is not None else "N/A"

            # Format percentage change
            if percent_change > 0:
                change_formatted = f"+{percent_change:.1f}%"
            elif percent_change < 0:
                change_formatted = f"{percent_change:.1f}%"
            else:
                change_formatted = "0.0%"

            rows.append([key, left_formatted, right_formatted, change_formatted])
        else:
            # Fallback for non-tuple values
            rows.append([key, str(value), "N/A", "N/A"])

    console.print(
        tabulate(rows, headers=["Metric", "App 1", "App 2", "Change"], tablefmt="grid")
    )


# Register comparison patterns
# Note: Order matters if patterns overlap. registry.get_formatter returns the first match.
registry.register_pattern(is_comparison_result, format_comparison_result)
registry.register_pattern(is_stage_comparison_result, format_stage_comparison_result)
registry.register_pattern(
    is_timeline_comparison_result, format_timeline_comparison_result
)
registry.register_pattern(
    is_executor_comparison_result, format_executor_comparison_result
)
registry.register_pattern(is_job_comparison_result, format_job_comparison_result)
registry.register_pattern(is_app_summary_comparison_result, format_app_summary_diff)
registry.register_pattern(
    is_aggregated_stage_comparison_result, format_aggregated_stage_comparison_result
)
registry.register_pattern(
    is_environment_comparison_result, format_environment_comparison_result
)
registry.register_pattern(
    is_resource_comparison_result, format_resource_comparison_result
)
registry.register_pattern(
    is_standardized_comparison_result, format_standardized_comparison_result
)
registry.register_pattern(
    is_standardized_metrics_result, format_standardized_metrics_result
)
