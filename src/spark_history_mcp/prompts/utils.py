"""Shared utilities for prompt templates."""

from typing import Optional


def server_kwarg(server: Optional[str]) -> str:
    """Return a formatted server keyword argument string for use in tool call examples."""
    return f', server="{server}"' if server else ""
