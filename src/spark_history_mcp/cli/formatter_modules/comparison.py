"""
Comparison result formatting methods for Spark History Server MCP CLI.

Contains detection methods and mixin classes for formatting comparison results.
Due to the complexity and interdependence of comparison formatting methods,
the full implementation remains in the original formatters.py for now.
"""

from typing import Any, Dict, Optional

try:
    from rich.console import Console

    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False


class ComparisonFormatMixin:
    """Mixin class providing comparison result detection and basic structure."""

    def _is_comparison_result(self, data: Dict[str, Any]) -> bool:
        """Detect if data is a comparison result structure."""
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
        agg_keys = {
            "applications",
            "aggregated_stage_metrics",
            "stage_performance_comparison",
            "stage_comparison",
            "efficiency_analysis",
        }
        return len(agg_keys.intersection(data.keys())) >= 2

    def _is_resource_comparison_result(self, data: Dict[str, Any]) -> bool:
        """Detect resource comparison results."""
        resource_keys = {"applications", "resource_comparison"}
        return len(resource_keys.intersection(data.keys())) >= 1

    # Note: For the initial refactor, the actual formatting methods remain in the original
    # formatters.py file due to their complexity and interdependencies. Future iterations
    # can extract individual comparison formatting methods as needed.

    def _format_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format comparison result - delegates to original implementation for now."""
        # Import here to avoid circular import
        from ..formatters import OutputFormatter

        original_formatter = OutputFormatter(
            getattr(self, "format_type", "human"), getattr(self, "quiet", False)
        )
        original_formatter._format_comparison_result(data, title)

    def _format_stage_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format stage comparison result - delegates to original implementation for now."""
        from ..formatters import OutputFormatter

        original_formatter = OutputFormatter(
            getattr(self, "format_type", "human"), getattr(self, "quiet", False)
        )
        original_formatter._format_stage_comparison_result(data, title)

    def _format_timeline_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format timeline comparison result - delegates to original implementation for now."""
        from ..formatters import OutputFormatter

        original_formatter = OutputFormatter(
            getattr(self, "format_type", "human"), getattr(self, "quiet", False)
        )
        original_formatter._format_timeline_comparison_result(data, title)

    def _format_executor_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format executor comparison result - delegates to original implementation for now."""
        from ..formatters import OutputFormatter

        original_formatter = OutputFormatter(
            getattr(self, "format_type", "human"), getattr(self, "quiet", False)
        )
        original_formatter._format_executor_comparison_result(data, title)

    def _format_job_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format job comparison result - delegates to original implementation for now."""
        from ..formatters import OutputFormatter

        original_formatter = OutputFormatter(
            getattr(self, "format_type", "human"), getattr(self, "quiet", False)
        )
        original_formatter._format_job_comparison_result(data, title)

    def _format_aggregated_stage_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format aggregated stage comparison result - delegates to original implementation for now."""
        from ..formatters import OutputFormatter

        original_formatter = OutputFormatter(
            getattr(self, "format_type", "human"), getattr(self, "quiet", False)
        )
        original_formatter._format_aggregated_stage_comparison_result(data, title)

    def _format_resource_comparison_result(
        self, data: Dict[str, Any], title: Optional[str] = None
    ) -> None:
        """Format resource comparison result - delegates to original implementation for now."""
        from ..formatters import OutputFormatter

        original_formatter = OutputFormatter(
            getattr(self, "format_type", "human"), getattr(self, "quiet", False)
        )
        original_formatter._format_resource_comparison_result(data, title)
