"""
Application-related CLI commands.

Commands for listing and inspecting Spark applications.
"""

from pathlib import Path
from typing import Optional

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.cli._compat import CLI_AVAILABLE, cli_unavailable_stub, click
from spark_history_mcp.config.config import Config

if CLI_AVAILABLE:
    from spark_history_mcp.cli.formatters import OutputFormatter


def load_config(config_path: Path) -> Config:
    """Load configuration with error handling."""
    try:
        return Config.from_file(str(config_path))
    except FileNotFoundError as err:
        if CLI_AVAILABLE:
            raise click.ClickException(
                f"Configuration file not found: {config_path}"
            ) from err
        raise RuntimeError(f"Configuration file not found: {config_path}") from err
    except Exception as err:
        if CLI_AVAILABLE:
            raise click.ClickException(f"Error loading configuration: {err}") from err
        raise RuntimeError(f"Error loading configuration: {err}") from err


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


def is_application_id(identifier: str) -> bool:
    """Check if identifier is an application ID vs application name."""
    return identifier.startswith(("spark-", "app-"))


def create_mock_context(client):
    """Create mock context for MCP tool functions."""

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

    return MockContext(client)


def resolve_app_identifier(
    client, identifier: str, server: Optional[str] = None
) -> str:
    """Resolve application name to ID if needed, return ID."""
    if is_application_id(identifier):
        return identifier  # Already an ID

    # Search by name (contains match) and get latest (limit 1)
    from spark_history_mcp.tools import list_applications

    # Create mock context for tool (same pattern as list command)
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
        apps = list_applications(
            server=server,
            app_name=identifier,
            search_type="contains",  # Fuzzy match
            limit=1,  # Get only the latest
        )

        if not apps:
            if CLI_AVAILABLE:
                raise click.ClickException(
                    f"No application found matching name: {identifier}"
                )
            else:
                raise RuntimeError(f"No application found matching name: {identifier}")

        return apps[0].id  # Return the latest match
    finally:
        if original_get_context:
            tools_module.mcp.get_context = original_get_context


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
        "output_format",
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
        output_format: str,
    ):
        """List Spark applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

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
            from spark_history_mcp.tools import list_applications

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

        except Exception as err:
            raise click.ClickException(f"Error listing applications: {err}") from err

    @apps.command("show")
    @click.argument("app_id")
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--format",
        "-f",
        "output_format",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def show_app(ctx, app_id: str, server: Optional[str], output_format: str):
        """Show detailed information about a specific application."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            # Use the existing MCP tool function
            from spark_history_mcp.tools import get_application

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

        except Exception as err:
            raise click.ClickException(
                f"Error getting application {app_id}: {err}"
            ) from err

    @apps.command("jobs")
    @click.argument("app_id")
    @click.option("--server", "-s", help="Server name to use")
    @click.option("--status", multiple=True, help="Filter by job status")
    @click.option(
        "--format",
        "-f",
        "output_format",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def list_jobs(
        ctx, app_id: str, server: Optional[str], status: tuple, output_format: str
    ):
        """List jobs for a specific application."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            from spark_history_mcp.tools import list_jobs as mcp_list_jobs

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

        except Exception as err:
            raise click.ClickException(
                f"Error listing jobs for {app_id}: {err}"
            ) from err

    @apps.command("stages")
    @click.argument("app_id")
    @click.option("--server", "-s", help="Server name to use")
    @click.option("--status", multiple=True, help="Filter by stage status")
    @click.option(
        "--format",
        "-f",
        "output_format",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def list_stages(
        ctx, app_id: str, server: Optional[str], status: tuple, output_format: str
    ):
        """List stages for a specific application."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            from spark_history_mcp.tools import list_stages as mcp_list_stages

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

        except Exception as err:
            raise click.ClickException(
                f"Error listing stages for {app_id}: {err}"
            ) from err

    @apps.command("summary")
    @click.argument("app_identifier", metavar="APP_ID_OR_NAME")
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--format",
        "-f",
        "output_format",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def summary_app(
        ctx, app_identifier: str, server: Optional[str], output_format: str
    ):
        """Get comprehensive performance summary for a specific application.

        APP_ID_OR_NAME can be either:
        - Application ID (e.g., spark-cc4d115f..., app-20231201-123456)
        - Application name or partial name (e.g., PythonPi, TaxiData)

        When using names, returns summary for the latest matching application.
        """
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        # Field labels for human-readable output
        field_labels = {
            # Remove redundant fields (already in title)
            "application_id": None,  # Skip - in title
            "application_name": None,  # Skip - in title
            "analysis_timestamp": None,  # Skip - not needed in display
            # Time metrics
            "application_duration_minutes": "Duration (Min)",
            "total_executor_runtime_minutes": "Executor Runtime (Min)",
            "executor_cpu_time_minutes": "CPU Time (Min)",
            "jvm_gc_time_minutes": "GC Time (Min)",
            "executor_utilization_percent": "Executor Utilization (%)",
            # Data processing metrics
            "input_data_size_gb": "Input Data (GB)",
            "output_data_size_gb": "Output Data (GB)",
            "shuffle_read_size_gb": "Shuffle Read (GB)",
            "shuffle_write_size_gb": "Shuffle Write (GB)",
            "memory_spilled_gb": "Memory Spilled (GB)",
            "disk_spilled_gb": "Disk Spilled (GB)",
            # Performance metrics
            "shuffle_read_wait_time_minutes": "Shuffle Read Wait (Min)",
            "shuffle_write_time_minutes": "Shuffle Write Time (Min)",
            "failed_tasks": "Failed Tasks",
            # Stage metrics
            "total_stages": "Total Stages",
            "completed_stages": "Completed Stages",
            "failed_stages": "Failed Stages",
        }

        try:
            client = get_spark_client(config_path, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import get_app_summary, list_applications

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                # Resolve name to ID if needed (gets latest match)
                if is_application_id(app_identifier):
                    app_id = app_identifier  # Already an ID
                else:
                    # Search by name (contains match) and get latest (limit 1)
                    apps = list_applications(
                        server=server,
                        app_name=app_identifier,
                        search_type="contains",  # Fuzzy match
                        limit=1,  # Get only the latest
                    )

                    if not apps:
                        raise click.ClickException(
                            f"No application found matching name: {app_identifier}"
                        )

                    app_id = apps[0].id  # Return the latest match

                summary_data = get_app_summary(app_id, server=server)

                # Extract application name for title
                app_name = summary_data.get("application_name", "Unknown Application")
                title = f"Application Summary - {app_name} ({app_id})"

                # Transform for human/table formats, keep original for JSON
                if output_format != "json":
                    display_data = {}
                    for key, value in summary_data.items():
                        readable_label = field_labels.get(key, key)
                        if (
                            readable_label is not None
                        ):  # Skip None values (redundant fields)
                            display_data[readable_label] = value
                    formatter.output(display_data, title)
                else:
                    formatter.output(summary_data, title)
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as err:
            raise click.ClickException(
                f"Error getting summary for application {app_identifier}: {err}"
            ) from err

else:
    apps = cli_unavailable_stub("apps")
