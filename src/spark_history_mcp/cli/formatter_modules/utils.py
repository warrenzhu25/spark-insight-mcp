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

    def _infer_comparison_metric_unit(self, metric_key: str) -> str:
        """Infer a display-unit suffix for comparison metrics."""
        metric_key = metric_key.lower()

        if "percent" in metric_key:
            return " (%)"
        if (
            "bytes" in metric_key
            or metric_key.endswith("_gb")
            or "spilled" in metric_key
        ):
            return " (GB)"
        if (
            "time" in metric_key
            or "duration" in metric_key
            or "wait" in metric_key
            or metric_key.endswith("_ms")
            or metric_key.endswith("_minutes")
        ):
            return " (min)"
        if any(
            token in metric_key
            for token in [
                "count",
                "tasks",
                "task",
                "records",
                "record",
                "blocks",
                "block",
                "executors",
                "executor",
                "stages",
                "stage",
                "cores",
            ]
        ):
            return " (count)"
        return ""

    def _get_comparison_metric_display_name(
        self, metric_key: str, include_units: bool = False
    ) -> str:
        """Get comparison-table display names aligned across summary tables."""
        metric_names = {
            # Duration metrics
            "duration": ("App Duration", "App Duration (min)"),
            "duration_ms": ("App Duration", "App Duration (min)"),
            "application_duration_minutes": ("App Duration", "App Duration (min)"),
            "executor_run_time": ("Executor Runtime", "Executor Runtime (min)"),
            "total_executor_runtime_minutes": (
                "Executor Runtime",
                "Executor Runtime (min)",
            ),
            "executor_cpu_time": ("CPU Time", "CPU Time (min)"),
            "executor_cpu_time_minutes": ("CPU Time", "CPU Time (min)"),
            "jvm_gc_time": ("GC Time", "GC Time (min)"),
            "jvm_gc_time_minutes": ("GC Time", "GC Time (min)"),
            "shuffle_fetch_wait_time": ("Shuffle Read Wait", "Shuffle Read Wait (min)"),
            "shuffle_read_wait_time_minutes": (
                "Shuffle Read Wait",
                "Shuffle Read Wait (min)",
            ),
            "shuffle_write_time": ("Shuffle Write Time", "Shuffle Write Time (min)"),
            "shuffle_write_time_minutes": (
                "Shuffle Write Time",
                "Shuffle Write Time (min)",
            ),
            # Size metrics
            "input_bytes": ("Input Data", "Input Data (GB)"),
            "input_data_size_gb": ("Input Data", "Input Data (GB)"),
            "output_bytes": ("Output Data", "Output Data (GB)"),
            "output_data_size_gb": ("Output Data", "Output Data (GB)"),
            "shuffle_read_bytes": ("Shuffle Read", "Shuffle Read (GB)"),
            "shuffle_read_size_gb": ("Shuffle Read", "Shuffle Read (GB)"),
            "shuffle_write_bytes": ("Shuffle Write", "Shuffle Write (GB)"),
            "shuffle_write_size_gb": ("Shuffle Write", "Shuffle Write (GB)"),
            "memory_bytes_spilled": ("Memory Spilled", "Memory Spilled (GB)"),
            "memory_spilled_gb": ("Memory Spilled", "Memory Spilled (GB)"),
            "disk_bytes_spilled": ("Disk Spilled", "Disk Spilled (GB)"),
            "disk_spilled_gb": ("Disk Spilled", "Disk Spilled (GB)"),
            # Percentage metrics
            "executor_utilization_percent": (
                "Executor Utilization",
                "Executor Utilization (%)",
            ),
            # Count metrics
            "total_stages": ("Total Stages", "Total Stages"),
            "completed_stages": ("Completed Stages", "Completed Stages"),
            "failed_stages": ("Failed Stages", "Failed Stages"),
            "failed_tasks": ("Failed Tasks", "Failed Tasks"),
            "total_tasks": ("Total Tasks", "Total Tasks"),
        }
        fallback = metric_key.replace("_", " ").title()
        base_name, summary_name = metric_names.get(metric_key, (fallback, fallback))
        if include_units and summary_name == fallback:
            return f"{fallback}{self._infer_comparison_metric_unit(metric_key)}"
        return summary_name if include_units else base_name

    def _get_metric_display_name(self, metric_key: str) -> str:
        """Get user-friendly display name for metrics."""
        metric_names = {
            # Duration metrics
            "duration": "Duration",
            "duration_ms": "Duration",
            # Executor metrics
            "executor_run_time": "Runtime",
            "executor_cpu_time": "CPU Time",
            "executor_deserialize_time": "Deserialize Time",
            "executor_deserialize_cpu_time": "Deserialize CPU Time",
            # Memory metrics
            "peak_execution_memory": "Peak Memory",
            "memory_bytes_spilled": "Memory Spilled",
            "disk_bytes_spilled": "Disk Spilled",
            "result_size": "Result Size",
            "result_serialization_time": "Serialization Time",
            # GC metrics
            "jvm_gc_time": "GC Time",
            # I/O metrics
            "input_bytes": "Input",
            "input_records": "Input Records",
            "output_bytes": "Output",
            "output_records": "Output Records",
            # Shuffle read metrics
            "shuffle_read_bytes": "Shuffle Read",
            "shuffle_read_records": "Shuffle Read Records",
            "shuffle_fetch_wait_time": "Shuffle Fetch Wait",
            "shuffle_remote_bytes_read": "Shuffle Remote Read",
            "shuffle_local_bytes_read": "Shuffle Local Read",
            "shuffle_remote_blocks_fetched": "Remote Blocks Fetched",
            "shuffle_local_blocks_fetched": "Local Blocks Fetched",
            "shuffle_remote_reqs_duration": "Remote Requests",
            # Shuffle write metrics
            "shuffle_write_bytes": "Shuffle Write",
            "shuffle_write_records": "Shuffle Write Records",
            "shuffle_write_time": "Shuffle Write Time",
            # Task metrics
            "task_time": "Task Time",
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
            return self._format_duration
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
            "duration": "Duration",
            "duration_ms": "Duration",
            # Legacy ratio-change keys
            "duration_ratio_change": "Total Duration",
            "executor_runtime_ratio_change": "Executor Runtime",
            "shuffle_read_ratio_change": "Shuffle Read (GB)",
            "shuffle_write_ratio_change": "Shuffle Write (GB)",
            "input_ratio_change": "Input Data (GB)",
            "output_ratio_change": "Output Data (GB)",
            # Base metric names (used by aggregated stage comparison)
            "avg_stage_duration_minutes": "Avg Stage Duration (min)",
            "executor_cpu_time_minutes": "CPU Time (min)",
            "total_executor_runtime_minutes": "Executor Runtime (min)",
            "jvm_gc_time_minutes": "GC Time (min)",
            "total_tasks": "Total Tasks",
            "shuffle_read_size_gb": "Shuffle Read (GB)",
            "shuffle_write_size_gb": "Shuffle Write (GB)",
            "input_data_size_gb": "Input Data (GB)",
            "output_data_size_gb": "Output Data (GB)",
            "memory_spilled_gb": "Memory Spilled (GB)",
            "stage_count": "Stage Count",
            "failed_tasks": "Failed Tasks",
        }
        return stage_metric_names.get(metric_key, metric_key.replace("_", " ").title())

    def _format_duration(self, duration_ms: float) -> str:
        """Format duration in human readable format."""
        if duration_ms < 1000:
            return f"{duration_ms:.1f}ms"
        elif duration_ms < 60000:
            return f"{duration_ms / 1000:.1f}s"
        elif duration_ms < 3600000:
            minutes = duration_ms / 60000
            return f"{minutes:.1f}m"
        else:
            hours = duration_ms / 3600000
            return f"{hours:.1f}h"

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
