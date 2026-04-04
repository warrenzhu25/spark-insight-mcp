"""
Application identifier resolution utilities.

Handles mapping number references, app names, and app IDs to a canonical app ID.
"""

import re
from typing import Any, Optional, Sequence
from spark_history_mcp.cli._compat import CLI_AVAILABLE, click

if CLI_AVAILABLE:
    from spark_history_mcp.cli.session import is_number_ref, resolve_number_ref


def is_app_id(identifier: str) -> bool:
    """
    Detect if identifier looks like an app ID vs app name.

    Args:
        identifier: The identifier to check

    Returns:
        True if identifier matches common app ID patterns
    """
    # Common app ID patterns: app-YYYYMMDD-*, application_*, etc.
    app_id_patterns = [
        r"^app-.*$",  # app-*
        r"^spark-.*$",  # spark-*
        r"^application_.*$",  # application_*
        r"^local-.*$",  # local-*
    ]
    return any(
        re.match(pattern, identifier, re.IGNORECASE) for pattern in app_id_patterns
    )


def resolve_app_identifier(identifier: str) -> str:
    """
    Resolve an app identifier to an app ID.

    Handles number references (1, 2, 3...) by looking up the saved mapping.
    Returns the identifier unchanged if it's not a number ref.

    Args:
        identifier: Number ref like "1" or app ID like "app-123"

    Returns:
        The resolved app ID

    Raises:
        click.ClickException: If number ref not found in session
    """
    if not CLI_AVAILABLE:
        return identifier

    if is_number_ref(identifier):
        app_id = resolve_number_ref(int(identifier))
        if app_id:
            click.echo(f"Resolved #{identifier} to: {app_id}")
            return app_id
        raise click.ClickException(
            f"#{identifier} not found. Run 'apps list' first to set up references."
        )
    return identifier


def resolve_app_by_name(
    client, identifier: str, server: Optional[str] = None
) -> str:
    """
    Resolve application name to ID if needed, return ID.

    Args:
        client: Spark REST client
        identifier: App ID or name
        server: Optional server name

    Returns:
        The resolved app ID
    """
    if is_app_id(identifier):
        return identifier  # Already an ID

    # Search by name (contains match) and get latest (limit 1)
    import spark_history_mcp.tools as tools_module
    from spark_history_mcp.cli._compat import patch_tool_context
    from spark_history_mcp.tools import list_applications

    with patch_tool_context(client, tools_module):
        apps = list_applications(
            server=server,
            app_name=identifier,
            search_type="contains",  # Fuzzy match
            limit=1,  # Get only the latest
            compact=False,
        )

        if not apps:
            error_msg = f"No application found matching name: {identifier}"
            if CLI_AVAILABLE:
                raise click.ClickException(error_msg)
            else:
                raise RuntimeError(error_msg)

        return apps[0].id  # Return the latest match


def canonicalize_app_id(
    identifier: str, client: Any, server: Optional[str] = None
) -> str:
    """
    Resolve any application identifier to a canonical app ID.

    Handles:
    - Number references (#1, #2...)
    - Application names (fuzzy match)
    - Application IDs (direct match)

    Args:
        identifier: The identifier to resolve
        client: Spark REST client (for name resolution)
        server: Optional server name

    Returns:
        The canonical app ID
    """
    # 1. Resolve number references first
    resolved_id = resolve_app_identifier(identifier)

    # 2. Resolve by name if it doesn't look like an ID
    return resolve_app_by_name(client, resolved_id, server)
