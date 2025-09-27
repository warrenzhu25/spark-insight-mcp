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
            elif self._is_stage_comparison_result(data):
                self._format_stage_comparison_result(data, title)
            elif self._is_timeline_comparison_result(data):
                self._format_timeline_comparison_result(data, title)
            elif self._is_executor_comparison_result(data):
                self._format_executor_comparison_result(data, title)
            elif self._is_job_comparison_result(data):
                self._format_job_comparison_result(data, title)
            elif self._is_aggregated_stage_comparison_result(data):
                self._format_aggregated_stage_comparison_result(data, title)
            elif self._is_resource_comparison_result(data):
                self._format_resource_comparison_result(data, title)
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
            "aggregated_overview",  # Old structure
            "stage_deep_dive",      # Old structure
            "performance_comparison",  # New structure
            "app_summary_diff",     # New structure
            "key_recommendations",     # New structure
            "recommendations",
            "environment_comparison",
            "sql_execution_plans",
        }
        return len(comparison_keys.intersection(data.keys())) >= 3

    def _is_stage_comparison_result(self, data: Dict[str, Any]) -> bool:
        """Detect if data is a stage comparison result structure."""
        stage_keys = {"stage_comparison", "significant_differences", "summary"}
        return len(stage_keys.intersection(data.keys())) >= 2

    def _is_timeline_comparison_result(self, data: Dict[str, Any]) -> bool:
        """Detect if data is a timeline comparison result structure."""
        timeline_keys = {
            "app1_info",
            "app2_info",
            "timeline_comparison",
            "resource_efficiency",
        }
        return len(timeline_keys.intersection(data.keys())) >= 3

    def _is_executor_comparison_result(self, data: Dict[str, Any]) -> bool:
        """Detect executor comparison results."""
        executor_keys = {"applications", "executor_comparison", "efficiency_metrics"}
        return len(executor_keys.intersection(data.keys())) >= 2

    def _is_job_comparison_result(self, data: Dict[str, Any]) -> bool:
        """Detect job comparison results."""
        job_keys = {"applications", "job_comparison", "timing_analysis"}
        return len(job_keys.intersection(data.keys())) >= 2

    def _is_aggregated_stage_comparison_result(self, data: Dict[str, Any]) -> bool:
        """Detect aggregated stage comparison results."""
        agg_keys = {"applications", "stage_comparison", "efficiency_analysis"}
        return len(agg_keys.intersection(data.keys())) >= 2

    def _is_resource_comparison_result(self, data: Dict[str, Any]) -> bool:
        """Detect resource comparison results."""
        resource_keys = {"applications", "resource_comparison"}
        return len(resource_keys.intersection(data.keys())) >= 1

    def _format_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format comparison result data in a clean, focused way."""
        # 1. Highlighted app names header with separator
        if "applications" in data:
            app1_data = data["applications"].get("app1", {})
            app2_data = data["applications"].get("app2", {})
            app1_name = app1_data.get("name", app1_data.get("id", "App1"))
            app2_name = app2_data.get("name", app2_data.get("id", "App2"))

            console.print(f"[cyan]{app1_name}[/cyan] vs [cyan]{app2_name}[/cyan]")
            console.print("─" * 80)
            console.print()  # Empty line for spacing

        # 2. Performance Metrics table FIRST (overall picture)
        if "aggregated_overview" in data:
            self._format_performance_metrics(data["aggregated_overview"])
        elif "performance_comparison" in data:
            # Handle new structure
            self._format_performance_metrics(data["performance_comparison"])

        # 3. Stage Differences table SECOND (detailed breakdown)
        if "stage_deep_dive" in data:
            self._format_stage_differences(data["stage_deep_dive"])
        elif "performance_comparison" in data and "stages" in data["performance_comparison"]:
            # Handle new structure - stages are now nested under performance_comparison
            self._format_stage_differences(data["performance_comparison"]["stages"])

        # 4. App Summary Diff table THIRD (aggregated metrics comparison)
        if "app_summary_diff" in data:
            self._format_app_summary_diff(data["app_summary_diff"])

    def _format_app_summary_diff(self, app_summary_diff: Dict[str, Any]) -> None:
        """Format application summary differences in a table format."""
        if "diff" not in app_summary_diff:
            return

        diff_data = app_summary_diff["diff"]
        app1_summary = app_summary_diff.get("app1_summary", {})
        app2_summary = app_summary_diff.get("app2_summary", {})

        # Create table
        table = Table(title="Application Metrics Comparison")
        table.add_column("Metric", style="cyan")
        table.add_column("App1", style="blue")
        table.add_column("App2", style="blue")
        table.add_column("Change", style="magenta")

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
        }

        # Get all metrics dynamically, excluding application_id
        all_metrics = [key for key in app1_summary.keys() if key != "application_id"]

        # Sort metrics by category for better display
        priority_order = ["application_duration_minutes", "total_executor_runtime_minutes",
                         "executor_cpu_time_minutes", "executor_utilization_percent",
                         "input_data_size_gb", "output_data_size_gb",
                         "shuffle_read_size_gb", "shuffle_write_size_gb",
                         "total_stages", "completed_stages", "failed_tasks"]

        # Sort: priority metrics first, then remaining alphabetically
        sorted_metrics = []
        for metric in priority_order:
            if metric in all_metrics:
                sorted_metrics.append(metric)
        for metric in sorted(all_metrics):
            if metric not in sorted_metrics:
                sorted_metrics.append(metric)

        for field_name in sorted_metrics:
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

        # Extract key metrics from aggregated overview or performance comparison
        overview = None
        if "aggregated_overview" in data:
            overview = data["aggregated_overview"]
        elif "performance_comparison" in data:
            overview = data["performance_comparison"]

        if overview:
            # Task completion ratio - handle both old and new structure
            executor_data = overview.get("executor_comparison") or overview.get("executors", {})
            if executor_data:
                exec_comp = executor_data
                if "task_completion_ratio_change" in exec_comp:
                    change = exec_comp["task_completion_ratio_change"]
                    summary_items.append(f"• Task completion efficiency: {change}")

        # Stage performance issues - handle both old and new structure
        stage_dive = None
        if "stage_deep_dive" in data:
            stage_dive = data["stage_deep_dive"]
        elif "performance_comparison" in data and "stages" in data["performance_comparison"]:
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
                        if diff.get("time_difference", {}).get(
                            "absolute_seconds", 0
                        )
                            > 60
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
            console.print(
                Panel(content, title="Executive Summary", border_style="green")
            )

    def _format_stage_differences(self, stage_deep_dive: Dict[str, Any]) -> None:
        """Format top stage differences in a table format."""
        if "top_stage_differences" not in stage_deep_dive:
            return

        differences = stage_deep_dive["top_stage_differences"][:3]  # Show top 3

        if not differences:
            return

        # Create table
        table = Table(title="Stage Differences")
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

    def _format_performance_metrics(self, overview: Dict[str, Any]) -> None:
        """Format all available performance metrics dynamically."""
        if "executor_comparison" not in overview and "stage_comparison" not in overview:
            return

        table = Table(title="Performance Metrics Comparison")
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
                key_executor_metrics = ["completed_tasks", "total_input_bytes", "total_duration"]

                for metric_key in key_executor_metrics:
                    if metric_key in app1_metrics and metric_key in app2_metrics:
                        app1_val = app1_metrics[metric_key]
                        app2_val = app2_metrics[metric_key]

                        display_name = self._get_executor_metric_display_name(metric_key)
                        formatter_func = self._get_executor_metric_formatter(metric_key)

                        app1_display = formatter_func(app1_val)
                        app2_display = formatter_func(app2_val)

                        # Get change from executor comparison analysis
                        if metric_key == "completed_tasks":
                            change = exec_data.get("task_completion_ratio_change", "N/A")
                        else:
                            # Calculate change percentage for other metrics
                            if app1_val > 0:
                                change_pct = ((app2_val - app1_val) / app1_val) * 100
                                change = f"+{change_pct:.1f}%" if change_pct >= 0 else f"{change_pct:.1f}%"
                            else:
                                change = "N/A"

                        table.add_row(display_name, app1_display, app2_display, change)

        # Dynamic stage comparison ratios - show ALL available ratios
        if "stage_comparison" in overview:
            stage_data = overview["stage_comparison"]
            stage_comparison = stage_data.get("stage_comparison", {})

            # Dynamically iterate through all ratio change metrics
            for metric_key in sorted(stage_comparison.keys()):
                if metric_key.endswith("_ratio_change"):
                    change = stage_comparison[metric_key]
                    display_name = self._get_stage_metric_display_name(metric_key)

                    # Get the base ratio to calculate approximate values
                    base_key = metric_key.replace("_change", "")
                    ratio = stage_comparison.get(base_key, 0)

                    if ratio > 0:
                        # For ratio-based metrics, show as baseline vs ratio
                        table.add_row(display_name, "Baseline", f"{ratio:.1%} of App1", change)

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

    def _format_stage_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format stage comparison result in a structured, readable way."""
        # 1. Stage Info Header
        self._format_stage_info_header(data)

        # 2. Performance Metrics Table
        self._format_stage_performance_metrics(data)

        # 3. Summary Panel
        self._format_stage_summary(data)

    def _format_stage_info_header(self, data: Dict[str, Any]) -> None:
        """Format stage information header panel."""
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

    def _get_metric_display_name(self, metric_key: str) -> str:
        """Get user-friendly display name for metrics."""
        metric_names = {
            "duration": "Duration",
            "executor_run_time": "Runtime",
            "shuffle_read_bytes": "Shuffle Read",
            "shuffle_fetch_wait_time": "Fetch Wait Time",
            "shuffle_remote_reqs_duration": "Remote Requests",
            "shuffle_write_bytes": "Shuffle Write",
            "task_time": "Task Time",
            "peak_execution_memory": "Peak Memory",
            "jvm_gc_time": "GC Time",
        }
        return metric_names.get(metric_key, metric_key.replace("_", " ").title())

    def _get_executor_metric_display_name(self, metric_key: str) -> str:
        """Get user-friendly display name for executor metrics."""
        executor_metric_names = {
            "total_executors": "Total Executors",
            "active_executors": "Active Executors",
            "memory_used": "Memory Used",
            "disk_used": "Disk Used",
            "completed_tasks": "Completed Tasks",
            "failed_tasks": "Failed Tasks",
            "total_duration": "Total Duration",
            "total_gc_time": "GC Time",
            "total_input_bytes": "Input Data",
            "total_shuffle_read": "Shuffle Read",
            "total_shuffle_write": "Shuffle Write",
        }
        return executor_metric_names.get(
            metric_key, metric_key.replace("_", " ").title()
        )

    def _get_executor_metric_formatter(self, metric_key: str):
        """Get appropriate formatter function for executor metrics."""
        # Time-based metrics (in milliseconds)
        if metric_key in ["total_duration", "total_gc_time"]:
            return self._format_milliseconds
        # Byte-based metrics
        elif metric_key in [
            "memory_used",
            "disk_used",
            "total_input_bytes",
            "total_shuffle_read",
            "total_shuffle_write",
        ]:
            return self._format_bytes
        # Count-based metrics (simple numbers)
        else:
            return lambda x: str(x)

    def _get_stage_metric_display_name(self, metric_key: str) -> str:
        """Get user-friendly display name for stage ratio metrics."""
        stage_metric_names = {
            "duration_ratio_change": "Total Duration",
            "executor_runtime_ratio_change": "Executor Runtime",
            "shuffle_read_ratio_change": "Shuffle Read",
            "shuffle_write_ratio_change": "Shuffle Write",
            "input_ratio_change": "Input Data",
            "output_ratio_change": "Output Data",
        }
        return stage_metric_names.get(metric_key, metric_key.replace("_", " ").title())

    def _format_stage_performance_metrics(self, data: Dict[str, Any]) -> None:
        """Format stage performance metrics in a comparison table with all available metrics."""
        sig_diff = data.get("significant_differences", {})
        task_dist = sig_diff.get("task_distributions", {})
        exec_dist = sig_diff.get("executor_distributions", {})

        if not task_dist and not exec_dist:
            return

        table = Table(title="Performance Metrics Comparison")
        table.add_column("Metric", style="cyan")
        table.add_column("App1", style="blue")
        table.add_column("App2", style="blue")
        table.add_column("Change", style="magenta")

        # Add all task distribution metrics dynamically
        for metric_key in task_dist.keys():
            metric_data = task_dist[metric_key]
            display_name = self._get_metric_display_name(metric_key)

            # Use median values for primary comparison
            if "median" in metric_data:
                median_data = metric_data["median"]

                # Format values based on metric type
                if metric_key in ["shuffle_read_bytes", "shuffle_write_bytes"]:
                    app1_val = self._format_bytes(median_data.get("stage1", 0))
                    app2_val = self._format_bytes(median_data.get("stage2", 0))
                else:
                    app1_val = self._format_milliseconds(median_data.get("stage1", 0))
                    app2_val = self._format_milliseconds(median_data.get("stage2", 0))

                change = median_data.get("change", "N/A")
                table.add_row(f"Median {display_name}", app1_val, app2_val, change)

            # Show max values too if available
            if "max" in metric_data:
                max_data = metric_data["max"]

                # Format values based on metric type
                if metric_key in ["shuffle_read_bytes", "shuffle_write_bytes"]:
                    app1_val = self._format_bytes(max_data.get("stage1", 0))
                    app2_val = self._format_bytes(max_data.get("stage2", 0))
                else:
                    app1_val = self._format_milliseconds(max_data.get("stage1", 0))
                    app2_val = self._format_milliseconds(max_data.get("stage2", 0))

                change = max_data.get("change", "N/A")
                table.add_row(f"Max {display_name}", app1_val, app2_val, change)

        # Add executor distribution metrics if available
        for metric_key in exec_dist.keys():
            metric_data = exec_dist[metric_key]
            display_name = f"Executor {self._get_metric_display_name(metric_key)}"

            if "median" in metric_data:
                median_data = metric_data["median"]
                app1_val = self._format_milliseconds(median_data.get("stage1", 0))
                app2_val = self._format_milliseconds(median_data.get("stage2", 0))
                change = median_data.get("change", "N/A")
                table.add_row(f"Median {display_name}", app1_val, app2_val, change)

        if table.rows:
            console.print(table)

    def _format_stage_summary(self, data: Dict[str, Any]) -> None:
        """Format stage comparison summary panel."""
        summary = data.get("summary", {})
        sig_diff = data.get("significant_differences", {})

        summary_items = []

        # Number of differences found
        total_diffs = summary.get("total_differences_found", 0)
        if total_diffs > 0:
            summary_items.append(
                f"• {total_diffs} significant performance differences found"
            )

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
            console.print(
                Panel(content, title="Performance Summary", border_style="green")
            )

    def _format_milliseconds(self, ms_value: float) -> str:
        """Format milliseconds to human readable duration."""
        if ms_value < 1000:
            return f"{ms_value:.0f}ms"
        else:
            seconds = ms_value / 1000
            if seconds < 60:
                return f"{seconds:.1f}s"
            else:
                minutes = seconds / 60
                return f"{minutes:.1f}m"

    def _format_timeline_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format timeline comparison result in a structured, readable way."""
        # 1. Application Overview Header
        self._format_timeline_overview_header(data)

        # 2. Timeline Intervals Table
        self._format_timeline_intervals_table(data)

        # 3. Resource Efficiency Panel
        self._format_timeline_efficiency_panel(data)

        # 4. Performance Summary
        self._format_timeline_summary(data)

    def _format_timeline_overview_header(self, data: Dict[str, Any]) -> None:
        """Format timeline comparison overview header."""
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
            app1_info.get("end_time", "")[:19]
            if app1_info.get("end_time")
            else "Unknown"
        )
        app2_start = (
            app2_info.get("start_time", "")[:19]
            if app2_info.get("start_time")
            else "Unknown"
        )
        app2_end = (
            app2_info.get("end_time", "")[:19]
            if app2_info.get("end_time")
            else "Unknown"
        )

        # Calculate performance difference
        duration_diff = app1_duration - app2_duration
        if app2_duration > 0:
            perf_pct = abs(duration_diff) / app2_duration * 100
            if duration_diff > 0:
                perf_text = f"App2 is {duration_diff:.1f}s ({perf_pct:.1f}%) faster"
            else:
                perf_text = (
                    f"App1 is {abs(duration_diff):.1f}s ({perf_pct:.1f}%) faster"
                )
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

    def _format_timeline_intervals_table(self, data: Dict[str, Any]) -> None:
        """Format timeline intervals in a table."""
        timeline_comp = data.get("timeline_comparison", [])

        if not timeline_comp:
            return

        table = Table(title="Timeline Intervals")
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

            diff_display = (
                f"+{diff}" if diff > 0 else str(diff) if diff != 0 else "Same"
            )

            table.add_row(
                interval, time_display, str(app1_execs), str(app2_execs), diff_display
            )

        console.print(table)

    def _format_timeline_efficiency_panel(self, data: Dict[str, Any]) -> None:
        """Format resource efficiency comparison panel."""
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

    def _format_timeline_summary(self, data: Dict[str, Any]) -> None:
        """Format timeline comparison summary."""
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
                summary_items.append(
                    f"• App2 completed {time_diff:.1f}s faster than App1"
                )
            else:
                summary_items.append(
                    f"• App1 completed {abs(time_diff):.1f}s faster than App2"
                )

        # Resource usage
        max_diff = summary.get("max_executor_count_difference", 0)
        if max_diff == 0:
            summary_items.append(
                "• Both applications used identical peak executor counts"
            )
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

    def _format_executor_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format executor comparison with clean header and performance table."""
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
        table = Table(title="Executor Performance Comparison")
        table.add_column("Metric", style="cyan")
        table.add_column("App1", style="blue")
        table.add_column("App2", style="blue")
        table.add_column("Change", style="magenta")

        # Extract executor metrics
        app1_metrics = (
            data.get("applications", {}).get("app1", {}).get("executor_metrics", {})
        )
        app2_metrics = (
            data.get("applications", {}).get("app2", {}).get("executor_metrics", {})
        )

        # Dynamically show all available executor metrics
        all_metrics = set(app1_metrics.keys()) | set(app2_metrics.keys())

        for metric_key in sorted(all_metrics):
            app1_val = app1_metrics.get(metric_key, 0)
            app2_val = app2_metrics.get(metric_key, 0)

            if app1_val is not None and app2_val is not None:
                display_name = self._get_executor_metric_display_name(metric_key)
                formatter_func = self._get_executor_metric_formatter(metric_key)

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
            table.add_row(
                "Task Completion Efficiency", "Baseline", "Comparison", change
            )

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

    def _format_job_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format job comparison with timing and success metrics."""
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
        table = Table(title="Job Performance Comparison")
        table.add_column("Metric", style="cyan")
        table.add_column("App1", style="blue")
        table.add_column("App2", style="blue")
        table.add_column("Change", style="magenta")

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

        # Dynamic job statistics from job_stats
        for metric_key in sorted(set(app1_jobs.keys()) | set(app2_jobs.keys())):
            app1_val = app1_jobs.get(metric_key, 0)
            app2_val = app2_jobs.get(metric_key, 0)

            if (
                app1_val is not None
                and app2_val is not None
                and (app1_val > 0 or app2_val > 0)
            ):
                display_name = self._get_stage_metric_display_name(metric_key)

                # Format based on metric type
                if "duration" in metric_key or "time" in metric_key:
                    app1_display = self._format_milliseconds(app1_val)
                    app2_display = self._format_milliseconds(app2_val)
                elif "bytes" in metric_key or "size" in metric_key:
                    app1_display = self._format_bytes(app1_val)
                    app2_display = self._format_bytes(app2_val)
                else:
                    app1_display = str(app1_val)
                    app2_display = str(app2_val)

                # Calculate change
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

    def _format_aggregated_stage_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format aggregated stage metrics comprehensively."""
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
        table = Table(title="Aggregated Stage Metrics Comparison")
        table.add_column("Metric", style="cyan")
        table.add_column("App1", style="blue")
        table.add_column("App2", style="blue")
        table.add_column("Change", style="magenta")

        # Stage comparison metrics
        stage_comp = data.get("stage_comparison", {})

        # Dynamically show all available stage comparison metrics
        for metric_key in sorted(stage_comp.keys()):
            if metric_key.endswith("_change"):
                display_name = self._get_stage_metric_display_name(metric_key)
                change = stage_comp[metric_key]

                # Extract base metric name to get the ratio
                base_key = metric_key.replace("_change", "")
                ratio = stage_comp.get(base_key, 0)

                # Calculate approximate values (simplified)
                if ratio > 0:
                    table.add_row(
                        display_name, "Baseline", f"{ratio:.1%} of App1", change
                    )

        # Efficiency analysis
        eff_analysis = data.get("efficiency_analysis", {})
        if (
            "app1_avg_tasks_per_stage" in eff_analysis
            and "app2_avg_tasks_per_stage" in eff_analysis
        ):
            app1_tasks = eff_analysis["app1_avg_tasks_per_stage"]
            app2_tasks = eff_analysis["app2_avg_tasks_per_stage"]
            change = (
                f"{((app2_tasks - app1_tasks) / app1_tasks * 100):.1f}%"
                if app1_tasks > 0
                else "N/A"
            )
            table.add_row(
                "Avg Tasks per Stage", f"{app1_tasks:.1f}", f"{app2_tasks:.1f}", change
            )

        if table.rows:
            console.print(table)

    def _format_resource_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format resource allocation comparison."""
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
        table = Table(title="Resource Allocation Comparison")
        table.add_column("Metric", style="cyan")
        table.add_column("App1", style="blue")
        table.add_column("App2", style="blue")
        table.add_column("Change", style="magenta")

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
