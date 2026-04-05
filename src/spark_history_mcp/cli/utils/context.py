"""
Shared context utilities for the Spark History MCP CLI.

Handles loading configuration and initializing the Spark REST client.
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional, Tuple

from spark_history_mcp.api.factory import create_spark_client
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.cli._compat import CLI_AVAILABLE, click, patch_tool_context
from spark_history_mcp.config.config import Config


def load_config(config_path: Path) -> Config:
    """Load configuration with error handling."""
    try:
        return Config.from_file(str(config_path))
    except FileNotFoundError as err:
        error_msg = f"Configuration file not found: {config_path}"
        if CLI_AVAILABLE:
            raise click.ClickException(error_msg) from err
        raise RuntimeError(error_msg) from err
    except Exception as err:
        error_msg = f"Error loading configuration: {err}"
        if CLI_AVAILABLE:
            raise click.ClickException(error_msg) from err
        raise RuntimeError(error_msg) from err


def get_spark_client(
    config_path: Path, server: Optional[str] = None
) -> SparkRestClient:
    """Get Spark client from configuration."""
    config = load_config(config_path)

    if server:
        if server not in config.servers:
            error_msg = f"Server '{server}' not found in configuration"
            if CLI_AVAILABLE:
                raise click.ClickException(error_msg)
            else:
                raise RuntimeError(error_msg)
        server_config = config.servers[server]
    else:
        # Find default server
        default_servers = [name for name, cfg in config.servers.items() if cfg.default]
        if not default_servers:
            if len(config.servers) == 1:
                server_config = next(iter(config.servers.values()))
            else:
                error_msg = "No default server configured. Specify --server or set default=true in config."
                if CLI_AVAILABLE:
                    raise click.ClickException(error_msg)
                else:
                    raise RuntimeError(error_msg)
        else:
            server_config = config.servers[default_servers[0]]

    return create_spark_client(server_config)


@contextmanager
def tool_runner(
    ctx,
    client: SparkRestClient,
    server: Optional[str],
    output_format: str,
    app_id: Optional[str] = None,
) -> Generator[Tuple, None, None]:
    """Set up the standard CLI tool execution environment.

    The caller is responsible for creating the client (enabling test patching).

    Yields:
        (formatter, resolved_app_id) where resolved_app_id is None if no app_id given.
    """
    from spark_history_mcp.cli.formatter_modules import OutputFormatter
    from spark_history_mcp.cli.utils.resolution import canonicalize_app_id

    formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))
    resolved_id = canonicalize_app_id(app_id, client, server) if app_id is not None else None

    import spark_history_mcp.tools as tools_module

    with patch_tool_context(client, tools_module):
        yield formatter, resolved_id
