"""
Basic data formatting functions for Spark History Server MCP CLI.

Contains formatting methods for applications, jobs, stages, and lists.
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

from spark_history_mcp.models.spark_types import ApplicationInfo, JobData, StageData

from .base import registry


def format_list(formatter, items: List[Any], title: Optional[str] = None) -> None:
    """Format a list of items."""
    if not RICH_AVAILABLE:
        return

    if not items:
        console.print("[dim]No items found[/dim]")
        return

    if isinstance(items[0], ApplicationInfo):
        format_application_list(formatter, items)
    elif isinstance(items[0], JobData):
        format_job_list(formatter, items)
    elif isinstance(items[0], StageData):
        format_stage_list(formatter, items)
    else:
        for i, item in enumerate(items, 1):
            console.print(f"{i}. {item}")


def format_application_list(formatter, apps: List[ApplicationInfo]) -> None:
    """Format list of applications as a rich table with numbered references."""
    # Clear and rebuild app mapping
    formatter.last_app_mapping = {}

    table = Table(title="Spark Applications", show_lines=True)
    table.add_column("#", style="dim", justify="right")
    table.add_column("Application ID", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Status", style="magenta")
    table.add_column("Duration", style="yellow")
    table.add_column("Start Time", style="blue")

    for idx, app in enumerate(apps, 1):
        # Store mapping for number references
        formatter.last_app_mapping[idx] = app.id

        attempt = app.attempts[0] if app.attempts else None
        if attempt:
            duration = f"{attempt.duration // 1000}s" if attempt.duration else "N/A"
            status = "✓ Completed" if attempt.completed else "⏳ Running"
            start_time = (
                attempt.start_time.strftime("%Y-%m-%d %H:%M")
                if attempt.start_time
                else "N/A"
            )
        else:
            duration = status = start_time = "N/A"

        table.add_row(
            str(idx), app.id, app.name or "Unnamed", status, duration, start_time
        )

    console.print(table)


def format_application(
    formatter, app: ApplicationInfo, title: Optional[str] = None
) -> None:
    """Format single application details."""
    if not RICH_AVAILABLE:
        return

    panel_content = []

    # Basic info
    panel_content.append(f"[bold]ID:[/bold] {app.id}")
    panel_content.append(f"[bold]Name:[/bold] {app.name or 'Unnamed'}")

    # Resource allocation
    if app.cores_granted:
        panel_content.append(f"[bold]Cores Granted:[/bold] {app.cores_granted}")
    if app.max_cores:
        panel_content.append(f"[bold]Max Cores:[/bold] {app.max_cores}")
    if app.memory_per_executor_mb:
        panel_content.append(
            f"[bold]Memory per Executor:[/bold] {app.memory_per_executor_mb}MB"
        )

    # Attempts
    if app.attempts:
        panel_content.append("\n[bold]Attempts:[/bold]")
        for attempt in app.attempts:
            status = "✓ Completed" if attempt.completed else "⏳ Running"
            duration = f"{attempt.duration // 1000}s" if attempt.duration else "N/A"
            panel_content.append(f"  • {status} - Duration: {duration}")
            if attempt.start_time:
                panel_content.append(
                    f"    Start: {attempt.start_time.strftime('%Y-%m-%d %H:%M:%S')}"
                )
            if attempt.end_time:
                panel_content.append(
                    f"    End: {attempt.end_time.strftime('%Y-%m-%d %H:%M:%S')}"
                )

    content = "\n".join(panel_content)
    console.print(Panel(content, title="Application Details", border_style="blue"))


def format_job(formatter, job: JobData, title: Optional[str] = None) -> None:
    """Format single job details."""
    if not RICH_AVAILABLE:
        return

    panel_content = []
    panel_content.append(f"[bold]Job ID:[/bold] {job.job_id}")
    panel_content.append(f"[bold]Name:[/bold] {job.name or 'Unnamed'}")
    panel_content.append(f"[bold]Status:[/bold] {job.status}")

    if job.submission_time:
        panel_content.append(f"[bold]Submitted:[/bold] {job.submission_time}")
    if job.completion_time:
        panel_content.append(f"[bold]Completed:[/bold] {job.completion_time}")

    content = "\n".join(panel_content)
    console.print(Panel(content, title="Job Details", border_style="green"))


def format_stage(formatter, stage: StageData, title: Optional[str] = None) -> None:
    """Format single stage details."""
    if not RICH_AVAILABLE:
        return

    panel_content = []
    panel_content.append(f"[bold]Stage ID:[/bold] {stage.stage_id}")
    panel_content.append(f"[bold]Name:[/bold] {stage.name or 'Unnamed'}")
    panel_content.append(f"[bold]Status:[/bold] {stage.status}")
    panel_content.append(f"[bold]Number of Tasks:[/bold] {stage.num_tasks}")

    content = "\n".join(panel_content)
    console.print(Panel(content, title="Stage Details", border_style="yellow"))


def format_job_list(formatter, jobs: List[JobData]) -> None:
    """Format list of jobs as a rich table."""
    if not RICH_AVAILABLE:
        return

    table = Table(title="Spark Jobs", show_lines=True)
    table.add_column("Job ID", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Status", style="magenta")
    table.add_column("Submitted", style="blue")
    table.add_column("Duration", style="yellow")

    for job in jobs:
        duration = "N/A"
        if job.submission_time and job.completion_time:
            duration = (
                f"{(job.completion_time - job.submission_time).total_seconds():.1f}s"
            )

        submitted = (
            job.submission_time.strftime("%Y-%m-%d %H:%M")
            if job.submission_time
            else "N/A"
        )
        table.add_row(
            str(job.job_id), job.name or "Unnamed", job.status, submitted, duration
        )

    console.print(table)


def format_stage_list(formatter, stages: List[StageData]) -> None:
    """Format list of stages as a rich table."""
    if not RICH_AVAILABLE:
        return

    table = Table(title="Spark Stages", show_lines=True)
    table.add_column("Stage ID", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Status", style="magenta")
    table.add_column("Tasks", style="yellow")
    table.add_column("Duration", style="blue")

    for stage in stages:
        duration = "N/A"
        if stage.submission_time and stage.completion_time:
            duration = f"{(stage.completion_time - stage.submission_time).total_seconds():.1f}s"

        table.add_row(
            str(stage.stage_id),
            stage.name or "Unnamed",
            stage.status,
            str(stage.num_tasks),
            duration,
        )

    console.print(table)


def format_dict(formatter, data: Dict[str, Any], title: Optional[str] = None) -> None:
    """Format dictionary data."""
    if not RICH_AVAILABLE:
        return

    if is_complex_dict(data):
        format_complex_dict(data)
    else:
        # Simple key-value display
        for key, value in data.items():
            console.print(f"[bold]{key}:[/bold] {value}")


def is_complex_dict(data: Dict[str, Any]) -> bool:
    """Check if dictionary has nested structures."""
    for value in data.values():
        if isinstance(value, (dict, list)) and value:
            return True
    return False


def format_complex_dict(data: Dict[str, Any]) -> None:
    """Format complex nested dictionary using Rich Tree."""
    try:
        from rich.tree import Tree

        tree = Tree("Data")
        add_dict_to_tree(tree, data)
        console.print(tree)
    except ImportError:
        # Fallback to simple formatting
        for key, value in data.items():
            console.print(f"[bold]{key}:[/bold] {str(value)[:100]}...")


def add_dict_to_tree(
    parent, data: Any, max_depth: int = 3, current_depth: int = 0
) -> None:
    """Add dictionary data to Rich tree structure."""
    if current_depth >= max_depth:
        parent.add("[dim]...(truncated)[/dim]")
        return

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)) and value:
                branch = parent.add(f"[bold]{key}[/bold]")
                add_dict_to_tree(branch, value, max_depth, current_depth + 1)
            else:
                parent.add(f"[bold]{key}:[/bold] {str(value)}")
    elif isinstance(data, list):
        for i, item in enumerate(data[:5]):  # Limit list items
            if isinstance(item, (dict, list)) and item:
                branch = parent.add(f"[cyan]{i}[/cyan]")
                add_dict_to_tree(branch, item, max_depth, current_depth + 1)
            else:
                parent.add(f"[cyan]{i}:[/cyan] {str(item)}")
        if len(data) > 5:
            parent.add(f"[dim]...and {len(data) - 5} more items[/dim]")
    else:
        parent.add(str(data))


def is_app_summary(data: Any) -> bool:
    """Detect if dictionary is an application summary."""
    if not isinstance(data, dict):
        return False
    # Look for unique keys in get_app_summary output
    summary_keys = {
        "application_duration_minutes",
        "total_executor_runtime_minutes",
        "executor_utilization_percent",
    }
    return summary_keys.issubset(data.keys())


def format_app_summary(
    formatter, data: Dict[str, Any], title: Optional[str] = None
) -> None:
    """Format application performance summary."""
    if not RICH_AVAILABLE:
        return

    # Field labels for human-readable output
    field_labels = {
        # Time metrics
        "application_duration_minutes": "Duration (Min)",
        "total_executor_runtime_minutes": "Executor Runtime (Min)",
        "executor_cpu_time_minutes": "CPU Time (Min)",
        "jvm_gc_time_minutes": "GC Time (Min)",
        "executor_utilization_percent": "Executor Utilization (%)",
        # Data processing metrics
        "input_data_size_gb": "Input Data (GB)",
        "output_data_size_gb": "Output Data (GB)",
        "shuffle_read_size_gb": "Shuffle Read (GB)",
        "shuffle_write_size_gb": "Shuffle Write (GB)",
        "memory_spilled_gb": "Memory Spilled (GB)",
        "disk_spilled_gb": "Disk Spilled (GB)",
        # Performance metrics
        "shuffle_read_wait_time_minutes": "Shuffle Read Wait (Min)",
        "shuffle_write_time_minutes": "Shuffle Write Time (Min)",
        "failed_tasks": "Failed Tasks",
        # Stage metrics
        "total_stages": "Total Stages",
        "completed_stages": "Completed Stages",
        "failed_stages": "Failed Stages",
    }

    display_data = {}
    for key, label in field_labels.items():
        if key in data:
            val = data[key]
            # Format numbers nicely
            if isinstance(val, float):
                display_data[label] = f"{val:.2f}"
            else:
                display_data[label] = str(val)

    # Use the existing format_dict to show as key-values
    for key, value in display_data.items():
        console.print(f"[bold]{key}:[/bold] {value}")


# Register basic types
registry.register_type(list, format_list)
registry.register_type(ApplicationInfo, format_application)
registry.register_type(JobData, format_job)
registry.register_type(StageData, format_stage)
registry.register_pattern(is_app_summary, format_app_summary)
registry.register_type(dict, format_dict)
