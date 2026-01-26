"""
Base formatting infrastructure for Spark History Server MCP CLI.

Contains the core OutputFormatter class and basic output methods.
"""

import json
import sys
from typing import Any, Optional

try:
    from rich.console import Console
    from tabulate import tabulate

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


if RICH_AVAILABLE:
    console = Console()


class OutputFormatter:
    """Base output formatter with multiple format options."""

    def __init__(self, format_type: str = "human", quiet: bool = False):
        self.format_type = format_type
        self.quiet = quiet

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
        # For now, delegate to the original implementation to avoid circular imports
        # Future iterations can implement full modular approach
        from ..formatters import OutputFormatter as OriginalFormatter

        original = OriginalFormatter(self.format_type, self.quiet)
        original._output_human(data, title)

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
