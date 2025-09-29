"""
Utility functions for Spark History Server MCP CLI formatters.

Contains metric formatters, display name helpers, and progress utilities.
"""

try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn

    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False


class FormatterUtilsMixin:
    """Mixin class providing formatter utility functions."""

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

    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes in human readable format."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f}{unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f}PB"


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
