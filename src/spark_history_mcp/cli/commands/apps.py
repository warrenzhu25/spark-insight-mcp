"""
Application-related CLI commands.

Commands for listing and inspecting Spark applications.
"""

from pathlib import Path
from typing import Optional

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.cli._compat import (
    CLI_AVAILABLE,
    cli_unavailable_stub,
    click,
    create_tool_context,
    patch_tool_context,
)
from spark_history_mcp.config.config import Config

if CLI_AVAILABLE:
    from spark_history_mcp.cli.formatters import OutputFormatter
    from spark_history_mcp.cli.session import (
        format_session_hint,
        is_number_ref,
        resolve_number_ref,
        save_app_refs,
    )


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


def resolve_app_id_arg(identifier: str) -> str:
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
    if is_number_ref(identifier):
        app_id = resolve_number_ref(int(identifier))
        if app_id:
            click.echo(f"Resolved #{identifier} to: {app_id}")
            return app_id
        raise click.ClickException(
            f"#{identifier} not found. Run 'apps list' first to set up references."
        )
    return identifier


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
    """Backward-compatible wrapper for tests relying on the old helper."""

    return create_tool_context(client)


def resolve_app_identifier(
    client, identifier: str, server: Optional[str] = None
) -> str:
    """Resolve application name to ID if needed, return ID."""
    if is_application_id(identifier):
        return identifier  # Already an ID

    # Search by name (contains match) and get latest (limit 1)
    import spark_history_mcp.tools.tools as tools_module
    from spark_history_mcp.tools import list_applications

    with patch_tool_context(client, tools_module):
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
    @click.option(
        "--name",
        "-m",
        help="Filter by application name (contains match)",
    )
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

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import list_applications

            with patch_tool_context(client, tools_module):
                apps = list_applications(server=server, **params)
                formatter.output(apps, "Spark Applications")
                # Save app references for number shorthand
                if formatter.last_app_mapping and output_format == "human":
                    save_app_refs(formatter.last_app_mapping, server)
                    click.echo(format_session_hint(len(formatter.last_app_mapping)))

        except Exception as err:
            raise click.ClickException(f"Error listing applications: {err}") from err

    @apps.command("compare")
    @click.argument("app_identifier1")
    @click.argument("app_identifier2", required=False)
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--top-n",
        "-n",
        type=int,
        default=3,
        help="Number of top stage differences to analyze",
    )
    @click.option(
        "--format",
        "-f",
        "output_format",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.option(
        "--interactive",
        "-i",
        is_flag=True,
        help="Show interactive navigation menu after comparison",
    )
    @click.option(
        "--all",
        "-a",
        "show_all",
        is_flag=True,
        help="Show all metrics instead of top 3",
    )
    @click.pass_context
    def compare_apps(
        ctx,
        app_identifier1: str,
        app_identifier2: Optional[str],
        server: Optional[str],
        top_n: int,
        output_format: str,
        interactive: bool,
        show_all: bool,
    ):
        """Alias for `apps compare`."""
        from spark_history_mcp.cli.commands.compare import apps as compare_apps_cmd

        ctx.invoke(
            compare_apps_cmd,
            app_identifier1=app_identifier1,
            app_identifier2=app_identifier2,
            server=server,
            top_n=top_n,
            output_format=output_format,
            interactive=interactive,
            show_all=show_all,
        )

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
        # Resolve number references
        app_id = resolve_app_id_arg(app_id)

        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import get_application

            with patch_tool_context(client, tools_module):
                app = get_application(app_id, server=server)
                formatter.output(app, f"Application {app_id}")
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
        # Resolve number references
        app_id = resolve_app_id_arg(app_id)

        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import list_jobs as mcp_list_jobs

            params = {"app_id": app_id, "server": server}
            if status:
                params["status"] = list(status)

            with patch_tool_context(client, tools_module):
                jobs = mcp_list_jobs(**params)
                formatter.output(jobs, f"Jobs for Application {app_id}")
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
        # Resolve number references
        app_id = resolve_app_id_arg(app_id)

        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import list_stages as mcp_list_stages

            params = {"app_id": app_id, "server": server}
            if status:
                params["status"] = list(status)

            with patch_tool_context(client, tools_module):
                stages = mcp_list_stages(**params)
                formatter.output(stages, f"Stages for Application {app_id}")
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

            with patch_tool_context(client, tools_module):
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
        except Exception as err:
            raise click.ClickException(
                f"Error getting summary for application {app_identifier}: {err}"
            ) from err

else:
    apps = cli_unavailable_stub("apps")
