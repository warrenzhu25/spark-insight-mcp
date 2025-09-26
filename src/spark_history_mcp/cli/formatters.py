"""
Output formatters for Spark History Server MCP CLI.

Provides various formatting options for displaying Spark application data
including JSON, table, and human-readable formats.
"""

import json
from typing import Any, Dict, List, Optional

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.table import Table
    from tabulate import tabulate

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

from spark_history_mcp.models.spark_types import ApplicationInfo, JobData, StageData

if RICH_AVAILABLE:
    console = Console()


class OutputFormatter:
    """Base output formatter with multiple format options."""

    def __init__(self, format_type: str = "human", quiet: bool = False):
        self.format_type = format_type
        self.quiet = quiet

    def output(self, data: Any, title: Optional[str] = None) -> None:
        """Output data in the specified format."""
        if self.quiet and self.format_type != "json":
            return

        if self.format_type == "json":
            self._output_json(data)
        elif self.format_type == "table" and RICH_AVAILABLE:
            self._output_table(data, title)
        elif RICH_AVAILABLE:  # human
            self._output_human(data, title)
        else:
            # Fallback to simple output if Rich not available
            self._output_simple(data, title)

    def _output_json(self, data: Any) -> None:
        """Output as JSON."""
        if hasattr(data, "model_dump"):
            # Pydantic model
            output = data.model_dump()
        elif hasattr(data, "__dict__"):
            # Regular object
            output = data.__dict__
        elif isinstance(data, (list, dict)):
            output = data
        else:
            output = str(data)

        print(json.dumps(output, indent=2, default=str))

    def _output_table(self, data: Any, title: Optional[str] = None) -> None:
        """Output as table using tabulate."""
        if not RICH_AVAILABLE:
            self._output_simple(data, title)
            return

        if isinstance(data, list) and len(data) > 0:
            # List of objects - create table
            if hasattr(data[0], "model_dump"):
                # Pydantic models
                rows = [item.model_dump() for item in data]
            elif hasattr(data[0], "__dict__"):
                # Regular objects
                rows = [item.__dict__ for item in data]
            else:
                # Simple values
                rows = [{"value": item} for item in data]

            if rows:
                headers = list(rows[0].keys())
                table_data = [[row.get(h, "") for h in headers] for row in rows]
                print(tabulate(table_data, headers=headers, tablefmt="grid"))
            return

        # Single object
        if hasattr(data, "model_dump"):
            obj_data = data.model_dump()
        elif hasattr(data, "__dict__"):
            obj_data = data.__dict__
        elif isinstance(data, dict):
            obj_data = data
        else:
            print(str(data))
            return

        # Create key-value table
        rows = [[k, v] for k, v in obj_data.items()]
        print(tabulate(rows, headers=["Property", "Value"], tablefmt="grid"))

    def _output_human(self, data: Any, title: Optional[str] = None) -> None:
        """Output in human-readable format using Rich."""
        if not RICH_AVAILABLE:
            self._output_simple(data, title)
            return

        if title:
            console.print(f"\n[bold blue]{title}[/bold blue]")

        if isinstance(data, list):
            self._format_list(data)
        elif isinstance(data, ApplicationInfo):
            self._format_application(data)
        elif isinstance(data, JobData):
            self._format_job(data)
        elif isinstance(data, StageData):
            self._format_stage(data)
        elif isinstance(data, dict):
            if self._is_comparison_result(data):
                self._format_comparison_result(data, title)
            else:
                self._format_dict(data)
        else:
            console.print(str(data))

    def _output_simple(self, data: Any, title: Optional[str] = None) -> None:
        """Simple fallback output when Rich is not available."""
        if title:
            print(f"\n{title}")
            print("=" * len(title))

        if isinstance(data, list):
            for i, item in enumerate(data, 1):
                print(f"{i}. {item}")
        elif hasattr(data, "model_dump"):
            obj_data = data.model_dump()
            for k, v in obj_data.items():
                print(f"{k}: {v}")
        elif isinstance(data, dict):
            for k, v in data.items():
                print(f"{k}: {v}")
        else:
            print(str(data))

    def _format_list(self, items: List[Any]) -> None:
        """Format a list of items."""
        if not items:
            console.print("[dim]No items found[/dim]")
            return

        if isinstance(items[0], ApplicationInfo):
            self._format_application_list(items)
        elif isinstance(items[0], JobData):
            self._format_job_list(items)
        elif isinstance(items[0], StageData):
            self._format_stage_list(items)
        else:
            for i, item in enumerate(items, 1):
                console.print(f"{i}. {item}")

    def _format_application_list(self, apps: List[ApplicationInfo]) -> None:
        """Format list of applications as a rich table."""
        table = Table(title="Spark Applications")
        table.add_column("Application ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Status", style="magenta")
        table.add_column("Duration", style="yellow")
        table.add_column("Start Time", style="blue")

        for app in apps:
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

            table.add_row(app.id, app.name or "Unnamed", status, duration, start_time)

        console.print(table)

    def _format_application(self, app: ApplicationInfo) -> None:
        """Format single application details."""
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

    def _format_job(self, job: JobData) -> None:
        """Format single job details."""
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

    def _format_stage(self, stage: StageData) -> None:
        """Format single stage details."""
        panel_content = []
        panel_content.append(f"[bold]Stage ID:[/bold] {stage.stage_id}")
        panel_content.append(f"[bold]Name:[/bold] {stage.name or 'Unnamed'}")
        panel_content.append(f"[bold]Status:[/bold] {stage.status}")
        panel_content.append(f"[bold]Number of Tasks:[/bold] {stage.num_tasks}")

        content = "\n".join(panel_content)
        console.print(Panel(content, title="Stage Details", border_style="yellow"))

    def _format_job_list(self, jobs: List[JobData]) -> None:
        """Format list of jobs as a rich table."""
        table = Table(title="Spark Jobs")
        table.add_column("Job ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Status", style="magenta")
        table.add_column("Submitted", style="blue")
        table.add_column("Duration", style="yellow")

        for job in jobs:
            duration = "N/A"
            if job.submission_time and job.completion_time:
                duration = f"{(job.completion_time - job.submission_time).total_seconds():.1f}s"

            submitted = (
                job.submission_time.strftime("%Y-%m-%d %H:%M")
                if job.submission_time
                else "N/A"
            )
            table.add_row(
                str(job.job_id), job.name or "Unnamed", job.status, submitted, duration
            )

        console.print(table)

    def _format_stage_list(self, stages: List[StageData]) -> None:
        """Format list of stages as a rich table."""
        table = Table(title="Spark Stages")
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

    def _is_comparison_result(self, data: Dict[str, Any]) -> bool:
        """Detect if data is a comparison result structure."""
        # Look for key patterns that indicate this is a comparison result
        comparison_keys = {
            "applications",
            "aggregated_overview",
            "stage_deep_dive",
            "recommendations",
            "environment_comparison",
            "sql_execution_plans",
        }
        return len(comparison_keys.intersection(data.keys())) >= 3

    def _format_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format comparison result data in a structured, readable way."""
        if title:
            console.print(f"\n[bold blue]{title}[/bold blue]")

        # 1. Applications Header
        if "applications" in data:
            self._format_comparison_header(data["applications"])

        # 2. Executive Summary
        self._format_executive_summary(data)

        # 3. Top Stage Differences
        if "stage_deep_dive" in data:
            self._format_stage_differences(data["stage_deep_dive"])

        # 4. Performance Metrics
        if "aggregated_overview" in data:
            self._format_performance_metrics(data["aggregated_overview"])

        # 5. Recommendations
        if "recommendations" in data:
            self._format_recommendations(data["recommendations"])

    def _format_comparison_header(self, applications: Dict[str, Any]) -> None:
        """Format the applications being compared."""
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

    def _format_executive_summary(self, data: Dict[str, Any]) -> None:
        """Format key insights and summary."""
        summary_items = []

        # Extract key metrics from aggregated overview
        if "aggregated_overview" in data:
            overview = data["aggregated_overview"]

            # Task completion ratio
            if "executor_comparison" in overview:
                exec_comp = overview["executor_comparison"]
                if "task_completion_ratio_change" in exec_comp:
                    change = exec_comp["task_completion_ratio_change"]
                    summary_items.append(f"• Task completion efficiency: {change}")

        # Stage performance issues
        if "stage_deep_dive" in data:
            stage_dive = data["stage_deep_dive"]
            if "top_stage_differences" in stage_dive:
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
                            if diff.get("time_difference", {}).get(
                                "absolute_seconds", 0
                            )
                            > 60
                        )
                        summary_items.append(
                            f"• Found {count} stages with >60s time difference"
                        )

        # Add recommendations summary
        if "recommendations" in data:
            rec_count = len(data["recommendations"])
            if rec_count > 0:
                summary_items.append(
                    f"• {rec_count} optimization recommendations available"
                )

        if summary_items:
            content = "\n".join(summary_items)
            console.print(
                Panel(content, title="Executive Summary", border_style="green")
            )

    def _format_stage_differences(self, stage_deep_dive: Dict[str, Any]) -> None:
        """Format top stage differences in a readable way."""
        if "top_stage_differences" not in stage_deep_dive:
            return

        differences = stage_deep_dive["top_stage_differences"][:3]  # Show top 3

        for i, diff in enumerate(differences, 1):
            stage_name = diff.get("stage_name", "Unknown Stage")[
                :50
            ]  # Truncate long names
            time_diff = diff.get("time_difference", {})

            app1_stage = diff.get("app1_stage", {})
            app2_stage = diff.get("app2_stage", {})

            app1_duration = app1_stage.get("duration_seconds", 0)
            app2_duration = app2_stage.get("duration_seconds", 0)

            absolute_diff = time_diff.get("absolute_seconds", 0)
            percentage = time_diff.get("percentage", 0)
            slower_app = time_diff.get("slower_application", "unknown")

            content = f"[bold]Stage:[/bold] {stage_name}\n"
            content += f"[bold]App1:[/bold] {app1_duration:.1f}s\n"
            content += f"[bold]App2:[/bold] {app2_duration:.1f}s\n"

            if slower_app == "app1":
                content += f"[bold red]Difference:[/bold red] +{absolute_diff:.1f}s ({percentage:.1f}% slower)"
            else:
                content += f"[bold green]Difference:[/bold green] -{absolute_diff:.1f}s ({percentage:.1f}% faster)"

            title = f"Stage Difference #{i}"
            console.print(Panel(content, title=title, border_style="yellow"))

    def _format_performance_metrics(self, overview: Dict[str, Any]) -> None:
        """Format key performance metrics in a table."""
        if "executor_comparison" not in overview and "stage_comparison" not in overview:
            return

        table = Table(title="Performance Metrics Comparison")
        table.add_column("Metric", style="cyan")
        table.add_column("App1", style="blue")
        table.add_column("App2", style="blue")
        table.add_column("Change", style="magenta")

        # Executor metrics
        if "executor_comparison" in overview:
            exec_data = overview["executor_comparison"]
            if "applications" in exec_data:
                apps = exec_data["applications"]
                app1_metrics = apps.get("app1", {}).get("executor_metrics", {})
                app2_metrics = apps.get("app2", {}).get("executor_metrics", {})

                # Total tasks
                if (
                    "completed_tasks" in app1_metrics
                    and "completed_tasks" in app2_metrics
                ):
                    app1_tasks = app1_metrics["completed_tasks"]
                    app2_tasks = app2_metrics["completed_tasks"]
                    change = exec_data.get("task_completion_ratio_change", "N/A")
                    table.add_row(
                        "Total Tasks", str(app1_tasks), str(app2_tasks), change
                    )

        # Stage metrics
        if "stage_comparison" in overview:
            stage_data = overview["stage_comparison"]
            if "applications" in stage_data:
                apps = stage_data["applications"]
                app1_metrics = apps.get("app1", {}).get("stage_metrics", {})
                app2_metrics = apps.get("app2", {}).get("stage_metrics", {})

                # Input bytes
                if (
                    "total_input_bytes" in app1_metrics
                    and "total_input_bytes" in app2_metrics
                ):
                    app1_input = self._format_bytes(app1_metrics["total_input_bytes"])
                    app2_input = self._format_bytes(app2_metrics["total_input_bytes"])
                    change = stage_data.get("stage_comparison", {}).get(
                        "input_ratio_change", "N/A"
                    )
                    table.add_row("Input Data", app1_input, app2_input, change)

                # Duration
                if (
                    "total_stage_duration" in app1_metrics
                    and "total_stage_duration" in app2_metrics
                ):
                    app1_duration = f"{app1_metrics['total_stage_duration']:.1f}s"
                    app2_duration = f"{app2_metrics['total_stage_duration']:.1f}s"
                    change = stage_data.get("stage_comparison", {}).get(
                        "duration_ratio_change", "N/A"
                    )
                    table.add_row(
                        "Total Duration", app1_duration, app2_duration, change
                    )

        if table.rows:
            console.print(table)

    def _format_recommendations(self, recommendations: List[Dict[str, Any]]) -> None:
        """Format recommendations as a bulleted list."""
        if not recommendations:
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

    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes in human readable format."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f}{unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f}PB"

    def _format_dict(self, data: Dict[str, Any]) -> None:
        """Format dictionary data."""
        # For complex nested dictionaries, use a more sophisticated approach
        if self._is_complex_dict(data):
            self._format_complex_dict(data)
        else:
            # Simple key-value table for basic dictionaries
            table = Table(title="Details")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="green")

            for key, value in data.items():
                # Truncate very long values
                str_value = str(value)
                if len(str_value) > 100:
                    str_value = str_value[:97] + "..."
                table.add_row(key, str_value)

            console.print(table)

    def _is_complex_dict(self, data: Dict[str, Any]) -> bool:
        """Check if dictionary has nested complex structures."""
        for value in data.values():
            if isinstance(value, (dict, list)) and len(str(value)) > 200:
                return True
        return False

    def _format_complex_dict(self, data: Dict[str, Any]) -> None:
        """Format complex nested dictionary using Tree structure."""
        from rich.tree import Tree

        tree = Tree("Data Structure")

        for key, value in data.items():
            if isinstance(value, dict):
                branch = tree.add(f"[bold blue]{key}[/bold blue]")
                self._add_dict_to_tree(branch, value, depth=0, max_depth=2)
            elif isinstance(value, list):
                branch = tree.add(f"[bold green]{key}[/bold green] (list)")
                if value and len(value) <= 5:  # Show small lists
                    for i, item in enumerate(value):
                        branch.add(f"{i}: {str(item)[:50]}")
                elif value:
                    branch.add(f"[dim]{len(value)} items...[/dim]")
            else:
                str_value = str(value)
                if len(str_value) > 50:
                    str_value = str_value[:47] + "..."
                tree.add(f"[cyan]{key}[/cyan]: {str_value}")

        console.print(tree)

    def _add_dict_to_tree(
        self, parent, data: Dict[str, Any], depth: int, max_depth: int
    ) -> None:
        """Recursively add dictionary items to tree."""
        if depth >= max_depth:
            parent.add("[dim]...[/dim]")
            return

        for key, value in list(data.items())[:10]:  # Limit to 10 items per level
            if isinstance(value, dict):
                branch = parent.add(f"[blue]{key}[/blue]")
                self._add_dict_to_tree(branch, value, depth + 1, max_depth)
            else:
                str_value = str(value)
                if len(str_value) > 40:
                    str_value = str_value[:37] + "..."
                parent.add(f"{key}: {str_value}")


def create_progress(description: str = "Processing...") -> Progress:
    """Create a progress indicator."""
    if not RICH_AVAILABLE:
        return None

    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    )
