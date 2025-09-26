"""
Application-related CLI commands.

Commands for listing and inspecting Spark applications.
"""

from pathlib import Path
from typing import Optional

try:
    import click

    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import Config

if CLI_AVAILABLE:
    from spark_history_mcp.cli.formatters import OutputFormatter


def load_config(config_path: Path) -> Config:
    """Load configuration with error handling."""
    try:
        return Config.from_file(str(config_path))
    except FileNotFoundError:
        if CLI_AVAILABLE:
            raise click.ClickException(f"Configuration file not found: {config_path}")
        else:
            raise RuntimeError(f"Configuration file not found: {config_path}")
    except Exception as e:
        if CLI_AVAILABLE:
            raise click.ClickException(f"Error loading configuration: {e}")
        else:
            raise RuntimeError(f"Error loading configuration: {e}")


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

    return SparkRestClient(server_config)


if CLI_AVAILABLE:

    @click.group(name="apps")
    def apps():
        """Commands for managing Spark applications."""
        pass

    @apps.command("list")
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--status", multiple=True, help="Filter by status (can be used multiple times)"
    )
    @click.option(
        "--limit", "-n", type=int, help="Maximum number of applications to return"
    )
    @click.option("--name", help="Filter by application name (contains match)")
    @click.option("--name-exact", help="Filter by exact application name")
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def list_apps(
        ctx,
        server: Optional[str],
        status: tuple,
        limit: Optional[int],
        name: Optional[str],
        name_exact: Optional[str],
        format: str,
    ):
        """List Spark applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            # Build parameters
            params = {}
            if status:
                params["status"] = list(status)
            if limit:
                params["limit"] = limit
            if name:
                params["app_name"] = name
                params["search_type"] = "contains"
            elif name_exact:
                params["app_name"] = name_exact
                params["search_type"] = "exact"

            # Use the existing MCP tool function
            from spark_history_mcp.tools.tools import list_applications

            # Create a mock context for the tool
            class MockContext:
                def __init__(self, client):
                    self.request_context = MockRequestContext(client)

            class MockRequestContext:
                def __init__(self, client):
                    self.lifespan_context = MockLifespanContext(client)

            class MockLifespanContext:
                def __init__(self, client):
                    self.default_client = client
                    self.clients = {"default": client}

            # Set up mock context for the tool
            import spark_history_mcp.tools.tools as tools_module

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: MockContext(client)

            try:
                apps = list_applications(server=server, **params)
                formatter.output(apps, "Spark Applications")
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error listing applications: {e}")

    @apps.command("show")
    @click.argument("app_id")
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def show_app(ctx, app_id: str, server: Optional[str], format: str):
        """Show detailed information about a specific application."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            # Use the existing MCP tool function
            from spark_history_mcp.tools.tools import get_application

            # Create mock context (same as above)
            class MockContext:
                def __init__(self, client):
                    self.request_context = MockRequestContext(client)

            class MockRequestContext:
                def __init__(self, client):
                    self.lifespan_context = MockLifespanContext(client)

            class MockLifespanContext:
                def __init__(self, client):
                    self.default_client = client
                    self.clients = {"default": client}

            import spark_history_mcp.tools.tools as tools_module

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: MockContext(client)

            try:
                app = get_application(app_id, server=server)
                formatter.output(app, f"Application {app_id}")
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error getting application {app_id}: {e}")

    @apps.command("jobs")
    @click.argument("app_id")
    @click.option("--server", "-s", help="Server name to use")
    @click.option("--status", multiple=True, help="Filter by job status")
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def list_jobs(ctx, app_id: str, server: Optional[str], status: tuple, format: str):
        """List jobs for a specific application."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            from spark_history_mcp.tools.tools import list_jobs as mcp_list_jobs

            # Create mock context
            class MockContext:
                def __init__(self, client):
                    self.request_context = MockRequestContext(client)

            class MockRequestContext:
                def __init__(self, client):
                    self.lifespan_context = MockLifespanContext(client)

            class MockLifespanContext:
                def __init__(self, client):
                    self.default_client = client
                    self.clients = {"default": client}

            import spark_history_mcp.tools.tools as tools_module

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: MockContext(client)

            try:
                params = {"app_id": app_id, "server": server}
                if status:
                    params["status"] = list(status)

                jobs = mcp_list_jobs(**params)
                formatter.output(jobs, f"Jobs for Application {app_id}")
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error listing jobs for {app_id}: {e}")

    @apps.command("stages")
    @click.argument("app_id")
    @click.option("--server", "-s", help="Server name to use")
    @click.option("--status", multiple=True, help="Filter by stage status")
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def list_stages(
        ctx, app_id: str, server: Optional[str], status: tuple, format: str
    ):
        """List stages for a specific application."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            from spark_history_mcp.tools.tools import list_stages as mcp_list_stages

            # Create mock context
            class MockContext:
                def __init__(self, client):
                    self.request_context = MockRequestContext(client)

            class MockRequestContext:
                def __init__(self, client):
                    self.lifespan_context = MockLifespanContext(client)

            class MockLifespanContext:
                def __init__(self, client):
                    self.default_client = client
                    self.clients = {"default": client}

            import spark_history_mcp.tools.tools as tools_module

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: MockContext(client)

            try:
                params = {"app_id": app_id, "server": server}
                if status:
                    params["status"] = list(status)

                stages = mcp_list_stages(**params)
                formatter.output(stages, f"Stages for Application {app_id}")
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error listing stages for {app_id}: {e}")

else:
    # Fallback when CLI dependencies not available
    def apps():
        print(
            "CLI dependencies not installed. Install with: uv add click rich tabulate"
        )
        return None
