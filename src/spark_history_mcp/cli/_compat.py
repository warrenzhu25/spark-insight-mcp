"""Shared helpers for CLI command modules."""

import sys

CLI_DEPENDENCY_HINT = (
    "CLI dependencies not installed. Install with: uv add click rich tabulate"
)

try:
    import click  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    click = None  # type: ignore[assignment]
    CLI_AVAILABLE = False
else:
    CLI_AVAILABLE = True


def cli_unavailable_stub(command_name: str):
    """Return a stub callable for missing CLI dependencies."""

    def _stub(*args, **kwargs):
        sys.stdout.write(f"{CLI_DEPENDENCY_HINT}\n")
        return None

    _stub.__name__ = command_name
    return _stub
