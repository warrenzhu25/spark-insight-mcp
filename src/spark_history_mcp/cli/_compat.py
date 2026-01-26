"""Shared helpers for CLI command modules."""

import sys
from contextlib import contextmanager

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

__all__ = [
    "CLI_DEPENDENCY_HINT",
    "CLI_AVAILABLE",
    "cli_unavailable_stub",
    "click",
    "create_tool_context",
    "patch_tool_context",
]


class _ToolLifespanContext:
    __slots__ = ("default_client", "clients")

    def __init__(self, client):
        self.default_client = client
        self.clients = {"default": client}


class _ToolRequestContext:
    __slots__ = ("lifespan_context",)

    def __init__(self, client):
        self.lifespan_context = _ToolLifespanContext(client)


class _ToolContext:
    __slots__ = ("request_context",)

    def __init__(self, client):
        self.request_context = _ToolRequestContext(client)


def cli_unavailable_stub(command_name: str):
    """Return a stub callable for missing CLI dependencies."""

    def _stub(*args, **kwargs):
        sys.stdout.write(f"{CLI_DEPENDENCY_HINT}\n")
        return None

    _stub.__name__ = command_name
    return _stub


def create_tool_context(client):
    """Build an MCP tool context wrapper for a Spark client."""

    return _ToolContext(client)


@contextmanager
def patch_tool_context(client, tools_module):
    """Temporarily patch ``tools_module.mcp.get_context`` for CLI tools."""

    original_get_context = getattr(tools_module.mcp, "get_context", None)
    tools_module.mcp.get_context = lambda: create_tool_context(client)
    try:
        yield
    finally:
        if original_get_context:
            tools_module.mcp.get_context = original_get_context
