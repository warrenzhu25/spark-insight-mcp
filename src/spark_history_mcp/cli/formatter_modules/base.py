"""
Base formatting infrastructure for Spark History Server MCP CLI.

Contains the core OutputFormatter class and basic output methods.
"""

import json
import sys
from typing import Any, Callable, Dict, List, Optional, Type

try:
    from rich.console import Console
    from tabulate import tabulate

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

from .utils import FormatterUtilsMixin

if RICH_AVAILABLE:
    console = Console()


class FormatterRegistry:
    """Registry for data-type specific formatters."""

    def __init__(self):
        # Maps types to formatter functions: Callable[[OutputFormatter, Any, Optional[str]], None]
        self.type_formatters: Dict[Type, Callable] = {}
        # Maps predicates to formatter functions: Callable[[Any], bool] -> Callable
        self.pattern_formatters: List[tuple[Callable[[Any], bool], Callable]] = []

    def register_type(self, data_type: Type, formatter_func: Callable):
        """Register a formatter for a specific data type."""
        self.type_formatters[data_type] = formatter_func

    def register_pattern(
        self, predicate: Callable[[Any], bool], formatter_func: Callable
    ):
        """Register a formatter for data matching a predicate."""
        self.pattern_formatters.append((predicate, formatter_func))

    def get_formatter(self, data: Any) -> Optional[Callable]:
        """Find a formatter for the given data."""
        # Try pattern matches first — they encode more specific criteria
        # and must take priority over generic type matchers (e.g. dict).
        for predicate, formatter_func in self.pattern_formatters:
            if predicate(data):
                return formatter_func

        # Fall back to exact type match
        data_type = type(data)
        if data_type in self.type_formatters:
            return self.type_formatters[data_type]

        return None


# Global registry
registry = FormatterRegistry()


class OutputFormatter(FormatterUtilsMixin):
    """Base output formatter with multiple format options."""

    def __init__(
        self,
        format_type: str = "human",
        quiet: bool = False,
        show_all_metrics: bool = False,
    ):
        self.format_type = format_type
        self.quiet = quiet
        self.show_all_metrics = show_all_metrics
        # Store last app mapping from application list for number references
        self.last_app_mapping: dict[int, str] = {}

    def _write_line(self, text: str = "") -> None:
        """Write a line to stdout for fallback paths."""
        sys.stdout.write(f"{text}\n")

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

        self._write_line(json.dumps(output, indent=2, default=str))

    def _output_table(self, data: Any, title: Optional[str] = None) -> None:
        """Output as table using tabulate."""
        if not RICH_AVAILABLE:
            self._output_simple(data, title)
            return

        # Check registry for custom table formatter
        formatter_func = registry.get_formatter(data)
        if formatter_func:
            # For now, custom formatters are expected to handle human output.
            # We'll refine this if we need custom table formatters too.
            pass

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
                self._write_line(tabulate(table_data, headers=headers, tablefmt="grid"))
            return

        # Single object
        if hasattr(data, "model_dump"):
            obj_data = data.model_dump()
        elif hasattr(data, "__dict__"):
            obj_data = data.__dict__
        elif isinstance(data, dict):
            obj_data = data
        else:
            self._write_line(str(data))
            return

        # Create key-value table
        rows = [[k, v] for k, v in obj_data.items()]
        self._write_line(tabulate(rows, headers=["Property", "Value"], tablefmt="grid"))

    def _output_human(self, data: Any, title: Optional[str] = None) -> None:
        """Output in human-readable format using Rich."""
        if not RICH_AVAILABLE:
            self._output_simple(data, title)
            return

        if title:
            console.print(f"\n[bold blue]{title}[/bold blue]")

        # Check registry for type-specific formatter
        formatter_func = registry.get_formatter(data)
        if formatter_func:
            formatter_func(self, data, title)
            return

        # Fallback to monolithic formatter for un-migrated types
        from ..formatters import OutputFormatter as MonolithicFormatter

        # Create a proxy for the monolithic formatter to handle the request
        # We need to copy state over
        proxy = MonolithicFormatter(self.format_type, self.quiet, self.show_all_metrics)
        proxy.last_app_mapping = self.last_app_mapping
        proxy._output_human(data, None)  # title already printed
        self.last_app_mapping = proxy.last_app_mapping

    def _output_simple(self, data: Any, title: Optional[str] = None) -> None:
        """Simple fallback output when Rich is not available."""
        if title:
            self._write_line()
            self._write_line(title)
            self._write_line("=" * len(title))

        if isinstance(data, list):
            for i, item in enumerate(data, 1):
                self._write_line(f"{i}. {item}")
        elif hasattr(data, "model_dump"):
            obj_data = data.model_dump()
            for k, v in obj_data.items():
                self._write_line(f"{k}: {v}")
        elif isinstance(data, dict):
            for k, v in data.items():
                self._write_line(f"{k}: {v}")
        else:
            self._write_line(str(data))
