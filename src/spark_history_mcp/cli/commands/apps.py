"""
Application-related CLI commands.

Commands for listing and inspecting Spark applications.
"""

import os
import sys
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
    from spark_history_mcp.cli.formatter_modules import OutputFormatter
    from spark_history_mcp.cli.session import (
        format_session_hint,
        save_app_refs,
    )
    from spark_history_mcp.cli.utils.context import get_spark_client, load_config
    from spark_history_mcp.cli.utils.resolution import canonicalize_app_id


def create_mock_context(client):
    """Backward-compatible wrapper for tests relying on the old helper."""

    return create_tool_context(client)


def _is_interactive() -> bool:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return True
    return sys.stdin.isatty() and sys.stdout.isatty()


def show_post_list_menu(server, formatter, ctx) -> None:
    """Show follow-up options after application listing."""
    if not _is_interactive():
        click.echo("Interactive menu requires a TTY. Skipping prompt.")
        return

    if not formatter.last_app_mapping:
        click.echo("No applications available for interactive actions.")
        return

    try:
        from rich.console import Console
        from rich.panel import Panel

        console = Console()
    except ImportError:
        console = None

    menu_lines = ["Choose your next action:", ""]
    menu_lines.extend(
        [
            "\\[c] Compare two apps",
            "\\[q] Continue",
        ]
    )

    if console:
        content = "\n".join(menu_lines)
        console.print(Panel(content, title="What's Next?", border_style="green"))
    else:
        click.echo("\n" + "=" * 50)
        click.echo("What's Next?")
        click.echo("=" * 50)
        for line in menu_lines:
            click.echo(line)
        click.echo("=" * 50)

    try:
        try:
            choice = click.getchar().lower()
            click.echo()
        except OSError:
            choice = click.prompt("Enter choice", type=str, default="c").lower()

        if choice in ("\r", "\n", ""):
            choice = "c"

        if choice == "q":
            return
        if choice != "c":
            click.echo(f"Invalid choice: {choice}")
            return

        raw = click.prompt(
            "Enter two app references (e.g., 1 2)", type=str, default="1 2"
        )
        parts = raw.split()
        if len(parts) != 2:
            click.echo("Please enter exactly two app references.")
            return

        app_id1 = resolve_app_identifier(parts[0])
        app_id2 = resolve_app_identifier(parts[1])

        from spark_history_mcp.cli.commands.compare import apps as compare_apps_cmd

        ctx.invoke(
            compare_apps_cmd,
            app_identifier1=app_id1,
            app_identifier2=app_id2,
            server=server,
            top_n=3,
            output_format="human",
            interactive=True,
            show_all=False,
            threshold=0.1,
        )

    except (KeyboardInterrupt, EOFError):
        click.echo("\nExiting interactive mode.")


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
    @click.option(
        "--interactive/--no-interactive",
        "-i",
        default=True,
        help="Show interactive menu after listing",
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
        interactive: bool,
    ):
        """List Spark applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        if limit is not None and limit < 1:
            raise click.BadParameter("limit must be >= 1", param_hint="--limit")

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

            import spark_history_mcp.tools as tools_module
            from spark_history_mcp.tools import list_applications

            with patch_tool_context(client, tools_module):
                apps = list_applications(server=server, **params, compact=False)
                formatter.output(apps, "Spark Applications")
                # Save app references for number shorthand
                if formatter.last_app_mapping and output_format == "human":
                    save_app_refs(formatter.last_app_mapping, server)
                    click.echo(format_session_hint(len(formatter.last_app_mapping)))
                    if interactive:
                        show_post_list_menu(server, formatter, ctx)

        except click.ClickException:
            raise
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
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)
            
            # Resolve app ID (handles #1, name, or ID)
            app_id = canonicalize_app_id(app_id, client, server)

            import spark_history_mcp.tools as tools_module
            from spark_history_mcp.tools import get_application

            with patch_tool_context(client, tools_module):
                app = get_application(app_id, server=server, compact=False)
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
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            # Resolve app ID (handles #1, name, or ID)
            app_id = canonicalize_app_id(app_id, client, server)

            import spark_history_mcp.tools as tools_module
            from spark_history_mcp.tools import list_jobs as mcp_list_jobs

            params = {"app_id": app_id, "server": server}
            if status:
                params["status"] = list(status)

            with patch_tool_context(client, tools_module):
                jobs = mcp_list_jobs(**params, compact=False)
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
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            # Resolve app ID (handles #1, name, or ID)
            app_id = canonicalize_app_id(app_id, client, server)

            import spark_history_mcp.tools as tools_module
            from spark_history_mcp.tools import list_stages as mcp_list_stages

            params = {"app_id": app_id, "server": server}
            if status:
                params["status"] = list(status)

            with patch_tool_context(client, tools_module):
                stages = mcp_list_stages(**params, compact=False)
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

        try:
            client = get_spark_client(config_path, server)
            
            # Resolve app ID (handles #1, name, or ID)
            app_id = canonicalize_app_id(app_identifier, client, server)

            import spark_history_mcp.tools as tools_module
            from spark_history_mcp.tools import get_app_summary

            with patch_tool_context(client, tools_module):
                summary_data = get_app_summary(app_id, server=server)

                # Extract application name for title
                app_name = summary_data.get("application_name", "Unknown Application")
                title = f"Application Summary - {app_name} ({app_id})"

                formatter.output(summary_data, title)
        except Exception as err:
            raise click.ClickException(
                f"Error getting summary for application {app_identifier}: {err}"
            ) from err

else:
    apps = cli_unavailable_stub("apps")
