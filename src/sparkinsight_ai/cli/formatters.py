"""
Output formatters for SparkInsight AI CLI.

Provides various formatting options for displaying Spark application data
including JSON, table, and human-readable formats.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.panel import Panel
from rich.tree import Tree
from rich.progress import Progress, SpinnerColumn, TextColumn
from tabulate import tabulate

from sparkinsight_ai.models.spark_types import ApplicationInfo, JobData, StageData

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
        elif self.format_type == "table":
            self._output_table(data, title)
        else:  # human
            self._output_human(data, title)

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
            self._format_dict(data)
        else:
            console.print(str(data))

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
                start_time = attempt.start_time.strftime("%Y-%m-%d %H:%M") if attempt.start_time else "N/A"
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
            panel_content.append(f"[bold]Memory per Executor:[/bold] {app.memory_per_executor_mb}MB")

        # Attempts
        if app.attempts:
            panel_content.append("\n[bold]Attempts:[/bold]")
            for attempt in app.attempts:
                status = "✓ Completed" if attempt.completed else "⏳ Running"
                duration = f"{attempt.duration // 1000}s" if attempt.duration else "N/A"
                panel_content.append(f"  • {status} - Duration: {duration}")
                if attempt.start_time:
                    panel_content.append(f"    Start: {attempt.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                if attempt.end_time:
                    panel_content.append(f"    End: {attempt.end_time.strftime('%Y-%m-%d %H:%M:%S')}")

        content = "\n".join(panel_content)
        console.print(Panel(content, title="Application Details", border_style="blue"))

    def _format_job_list(self, jobs: List[JobData]) -> None:
        """Format list of jobs."""
        table = Table(title="Spark Jobs")
        table.add_column("Job ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Status", style="magenta")
        table.add_column("Duration", style="yellow")
        table.add_column("Failed Tasks", style="red")

        for job in jobs:
            duration = "N/A"
            if job.submission_time and job.completion_time:
                duration = f"{(job.completion_time - job.submission_time).total_seconds():.1f}s"

            status_icon = {"SUCCEEDED": "✓", "FAILED": "✗", "RUNNING": "⏳"}.get(job.status, "?")
            status_text = f"{status_icon} {job.status}"

            table.add_row(
                str(job.job_id),
                job.name or "Unnamed",
                status_text,
                duration,
                str(job.num_failed_tasks) if job.num_failed_tasks else "0"
            )

        console.print(table)

    def _format_job(self, job: JobData) -> None:
        """Format single job details."""
        panel_content = []
        panel_content.append(f"[bold]Job ID:[/bold] {job.job_id}")
        panel_content.append(f"[bold]Name:[/bold] {job.name or 'Unnamed'}")
        panel_content.append(f"[bold]Status:[/bold] {job.status}")

        if job.submission_time:
            panel_content.append(f"[bold]Submitted:[/bold] {job.submission_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if job.completion_time:
            panel_content.append(f"[bold]Completed:[/bold] {job.completion_time.strftime('%Y-%m-%d %H:%M:%S')}")

        if job.submission_time and job.completion_time:
            duration = (job.completion_time - job.submission_time).total_seconds()
            panel_content.append(f"[bold]Duration:[/bold] {duration:.1f}s")

        panel_content.append(f"[bold]Failed Tasks:[/bold] {job.num_failed_tasks or 0}")
        panel_content.append(f"[bold]Killed Tasks:[/bold] {job.num_killed_tasks or 0}")

        content = "\n".join(panel_content)
        console.print(Panel(content, title="Job Details", border_style="green"))

    def _format_stage_list(self, stages: List[StageData]) -> None:
        """Format list of stages."""
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

            status_icon = {"COMPLETE": "✓", "FAILED": "✗", "RUNNING": "⏳", "PENDING": "⏸"}.get(stage.status, "?")
            status_text = f"{status_icon} {stage.status}"

            table.add_row(
                f"{stage.stage_id}.{stage.attempt_id}",
                stage.name or "Unnamed",
                status_text,
                f"{stage.num_active_tasks}/{stage.num_tasks}",
                duration
            )

        console.print(table)

    def _format_stage(self, stage: StageData) -> None:
        """Format single stage details."""
        panel_content = []
        panel_content.append(f"[bold]Stage ID:[/bold] {stage.stage_id}")
        panel_content.append(f"[bold]Attempt ID:[/bold] {stage.attempt_id}")
        panel_content.append(f"[bold]Name:[/bold] {stage.name or 'Unnamed'}")
        panel_content.append(f"[bold]Status:[/bold] {stage.status}")

        # Task info
        panel_content.append(f"\n[bold]Tasks:[/bold]")
        panel_content.append(f"  Total: {stage.num_tasks}")
        panel_content.append(f"  Active: {stage.num_active_tasks}")
        panel_content.append(f"  Complete: {stage.num_complete_tasks}")
        panel_content.append(f"  Failed: {stage.num_failed_tasks}")

        # Timing
        if stage.submission_time:
            panel_content.append(f"\n[bold]Submitted:[/bold] {stage.submission_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if stage.completion_time:
            panel_content.append(f"[bold]Completed:[/bold] {stage.completion_time.strftime('%Y-%m-%d %H:%M:%S')}")

        if stage.submission_time and stage.completion_time:
            duration = (stage.completion_time - stage.submission_time).total_seconds()
            panel_content.append(f"[bold]Duration:[/bold] {duration:.1f}s")

        content = "\n".join(panel_content)
        console.print(Panel(content, title="Stage Details", border_style="yellow"))

    def _format_dict(self, data: Dict[str, Any]) -> None:
        """Format dictionary data with Rich formatting."""
        if "analysis_type" in data:
            # Analysis result
            self._format_analysis_result(data)
        else:
            # Generic dictionary
            tree = Tree("📊 Data")
            self._add_dict_to_tree(tree, data)
            console.print(tree)

    def _format_analysis_result(self, result: Dict[str, Any]) -> None:
        """Format analysis results with structured display."""
        title = result.get("analysis_type", "Analysis Result")
        console.print(f"\n[bold green]{title}[/bold green]")

        # Application info
        if "application_id" in result:
            console.print(f"[bold]Application:[/bold] {result['application_id']}")
            if "application_name" in result:
                console.print(f"[bold]Name:[/bold] {result['application_name']}")

        # Summary
        if "summary" in result:
            console.print("\n[bold blue]Summary[/bold blue]")
            summary = result["summary"]
            for key, value in summary.items():
                formatted_key = key.replace("_", " ").title()
                console.print(f"  {formatted_key}: [cyan]{value}[/cyan]")

        # Recommendations
        if "recommendations" in result and result["recommendations"]:
            console.print("\n[bold red]🚨 Recommendations[/bold red]")
            for i, rec in enumerate(result["recommendations"], 1):
                priority_color = {
                    "critical": "red",
                    "high": "orange3",
                    "medium": "yellow",
                    "low": "green"
                }.get(rec.get("priority", "low"), "white")

                console.print(f"\n{i}. [bold]{rec.get('type', 'General').replace('_', ' ').title()}[/bold]")
                console.print(f"   Priority: [{priority_color}]{rec.get('priority', 'low').upper()}[/{priority_color}]")
                console.print(f"   Issue: {rec.get('issue', 'N/A')}")
                console.print(f"   Suggestion: {rec.get('suggestion', 'N/A')}")

        # Other structured data
        for key, value in result.items():
            if key not in ["analysis_type", "application_id", "application_name", "summary", "recommendations"]:
                if isinstance(value, dict) and value:
                    console.print(f"\n[bold blue]{key.replace('_', ' ').title()}[/bold blue]")
                    self._format_nested_dict(value, indent="  ")

    def _format_nested_dict(self, data: Dict[str, Any], indent: str = "") -> None:
        """Format nested dictionary with indentation."""
        for key, value in data.items():
            formatted_key = key.replace("_", " ").title()
            if isinstance(value, dict):
                console.print(f"{indent}[bold]{formatted_key}:[/bold]")
                self._format_nested_dict(value, indent + "  ")
            elif isinstance(value, list):
                console.print(f"{indent}[bold]{formatted_key}:[/bold] {len(value)} items")
            else:
                console.print(f"{indent}{formatted_key}: [cyan]{value}[/cyan]")

    def _add_dict_to_tree(self, tree: Tree, data: Dict[str, Any], max_depth: int = 3, current_depth: int = 0) -> None:
        """Add dictionary data to Rich tree."""
        if current_depth >= max_depth:
            return

        for key, value in data.items():
            if isinstance(value, dict):
                branch = tree.add(f"📁 {key}")
                self._add_dict_to_tree(branch, value, max_depth, current_depth + 1)
            elif isinstance(value, list):
                tree.add(f"📋 {key}: {len(value)} items")
            else:
                tree.add(f"📄 {key}: {value}")


def create_progress() -> Progress:
    """Create a Rich progress bar for long operations."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True
    )