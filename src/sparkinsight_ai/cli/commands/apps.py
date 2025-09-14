"""
Application-related CLI commands.

Commands for listing and inspecting Spark applications.
"""

from pathlib import Path
from typing import List, Optional

import click

from sparkinsight_ai.api.spark_client import SparkRestClient
from sparkinsight_ai.cli.formatters import OutputFormatter, create_progress
from sparkinsight_ai.config.config import Config


def load_config(config_path: Path) -> Config:
    """Load configuration with error handling."""
    try:
        return Config.from_file(str(config_path))
    except FileNotFoundError:
        raise click.ClickException(f"Configuration file not found: {config_path}")
    except Exception as e:
        raise click.ClickException(f"Error loading configuration: {e}")


def get_spark_client(config_path: Path, server: Optional[str] = None) -> SparkRestClient:
    """Get Spark client from configuration."""
    config = load_config(config_path)

    if server:
        if server not in config.servers:
            raise click.ClickException(f"Server '{server}' not found in configuration")
        server_config = config.servers[server]
    else:
        # Find default server
        default_servers = [name for name, cfg in config.servers.items() if cfg.default]
        if not default_servers:
            if len(config.servers) == 1:
                server_config = next(iter(config.servers.values()))
            else:
                raise click.ClickException("No default server configured. Specify --server or set default=true in config.")
        else:
            server_config = config.servers[default_servers[0]]

    return SparkRestClient(server_config)


@click.group(name="apps")
def apps_group():
    """Commands for managing Spark applications."""
    pass


@apps_group.command("list")
@click.option("--server", "-s", help="Server name to use")
@click.option("--status", multiple=True, help="Filter by status (can be used multiple times)")
@click.option("--limit", "-n", type=int, help="Maximum number of applications to return")
@click.option("--name", help="Filter by application name (contains match)")
@click.option("--name-exact", help="Filter by exact application name")
@click.option("--name-regex", help="Filter by application name using regex")
@click.option("--min-date", help="Minimum start date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)")
@click.option("--max-date", help="Maximum start date")
@click.option("--format", "-f", "format_type",
              type=click.Choice(["human", "json", "table"]),
              default="human", help="Output format")
@click.pass_context
def list_apps(ctx: click.Context, server: Optional[str], status: List[str],
              limit: Optional[int], name: Optional[str], name_exact: Optional[str],
              name_regex: Optional[str], min_date: Optional[str], max_date: Optional[str],
              format_type: str):
    """
    List Spark applications with optional filtering.

    Examples:
        sparkinsight-ai apps list                           # List all applications
        sparkinsight-ai apps list --status COMPLETED       # Only completed applications
        sparkinsight-ai apps list --limit 10               # First 10 applications
        sparkinsight-ai apps list --name "etl"             # Applications with "etl" in name
        sparkinsight-ai apps list --format json            # JSON output
    """
    formatter = OutputFormatter(format_type, ctx.obj.get("quiet", False))

    try:
        client = get_spark_client(ctx.obj["config_path"], server)

        # Determine search type and app name
        app_name = None
        search_type = "contains"

        if name_exact:
            app_name = name_exact
            search_type = "exact"
        elif name_regex:
            app_name = name_regex
            search_type = "regex"
        elif name:
            app_name = name
            search_type = "contains"

        with create_progress() as progress:
            task = progress.add_task("Fetching applications...", total=None)

            applications = client.list_applications(
                status=list(status) if status else None,
                min_date=min_date,
                max_date=max_date,
                limit=limit,
                app_name=app_name,
                search_type=search_type
            )

            progress.remove_task(task)

        if not applications:
            if not ctx.obj.get("quiet", False):
                click.echo("No applications found matching the criteria.")
            return

        formatter.output(applications, f"Found {len(applications)} Spark Applications")

    except Exception as e:
        raise click.ClickException(f"Failed to list applications: {e}")


@apps_group.command("show")
@click.argument("app_id")
@click.option("--server", "-s", help="Server name to use")
@click.option("--format", "-f", "format_type",
              type=click.Choice(["human", "json", "table"]),
              default="human", help="Output format")
@click.pass_context
def show_app(ctx: click.Context, app_id: str, server: Optional[str], format_type: str):
    """
    Show detailed information about a specific Spark application.

    Examples:
        sparkinsight-ai apps show app-20231201-123456
        sparkinsight-ai apps show app-20231201-123456 --format json
    """
    formatter = OutputFormatter(format_type, ctx.obj.get("quiet", False))

    try:
        client = get_spark_client(ctx.obj["config_path"], server)

        with create_progress() as progress:
            task = progress.add_task("Fetching application details...", total=None)
            application = client.get_application(app_id)
            progress.remove_task(task)

        formatter.output(application, f"Application {app_id}")

    except Exception as e:
        raise click.ClickException(f"Failed to get application details: {e}")


@apps_group.command("jobs")
@click.argument("app_id")
@click.option("--server", "-s", help="Server name to use")
@click.option("--status", multiple=True, help="Filter by job status")
@click.option("--format", "-f", "format_type",
              type=click.Choice(["human", "json", "table"]),
              default="human", help="Output format")
@click.pass_context
def list_jobs(ctx: click.Context, app_id: str, server: Optional[str],
              status: List[str], format_type: str):
    """
    List jobs for a specific Spark application.

    Examples:
        sparkinsight-ai apps jobs app-20231201-123456
        sparkinsight-ai apps jobs app-20231201-123456 --status SUCCEEDED
    """
    formatter = OutputFormatter(format_type, ctx.obj.get("quiet", False))

    try:
        client = get_spark_client(ctx.obj["config_path"], server)

        with create_progress() as progress:
            task = progress.add_task("Fetching jobs...", total=None)
            jobs = client.list_jobs(app_id=app_id, status=list(status) if status else None)
            progress.remove_task(task)

        formatter.output(jobs, f"Jobs for Application {app_id}")

    except Exception as e:
        raise click.ClickException(f"Failed to list jobs: {e}")


@apps_group.command("stages")
@click.argument("app_id")
@click.option("--server", "-s", help="Server name to use")
@click.option("--status", multiple=True, help="Filter by stage status")
@click.option("--with-summaries", is_flag=True, help="Include stage summaries")
@click.option("--format", "-f", "format_type",
              type=click.Choice(["human", "json", "table"]),
              default="human", help="Output format")
@click.pass_context
def list_stages(ctx: click.Context, app_id: str, server: Optional[str],
                status: List[str], with_summaries: bool, format_type: str):
    """
    List stages for a specific Spark application.

    Examples:
        sparkinsight-ai apps stages app-20231201-123456
        sparkinsight-ai apps stages app-20231201-123456 --status COMPLETE
        sparkinsight-ai apps stages app-20231201-123456 --with-summaries
    """
    formatter = OutputFormatter(format_type, ctx.obj.get("quiet", False))

    try:
        client = get_spark_client(ctx.obj["config_path"], server)

        with create_progress() as progress:
            task = progress.add_task("Fetching stages...", total=None)
            stages = client.list_stages(
                app_id=app_id,
                status=list(status) if status else None,
                with_summaries=with_summaries
            )
            progress.remove_task(task)

        formatter.output(stages, f"Stages for Application {app_id}")

    except Exception as e:
        raise click.ClickException(f"Failed to list stages: {e}")


@apps_group.command("executors")
@click.argument("app_id")
@click.option("--server", "-s", help="Server name to use")
@click.option("--include-inactive", is_flag=True, help="Include inactive executors")
@click.option("--format", "-f", "format_type",
              type=click.Choice(["human", "json", "table"]),
              default="human", help="Output format")
@click.pass_context
def list_executors(ctx: click.Context, app_id: str, server: Optional[str],
                   include_inactive: bool, format_type: str):
    """
    List executors for a specific Spark application.

    Examples:
        sparkinsight-ai apps executors app-20231201-123456
        sparkinsight-ai apps executors app-20231201-123456 --include-inactive
    """
    formatter = OutputFormatter(format_type, ctx.obj.get("quiet", False))

    try:
        client = get_spark_client(ctx.obj["config_path"], server)

        with create_progress() as progress:
            task = progress.add_task("Fetching executors...", total=None)
            if include_inactive:
                executors = client.list_all_executors(app_id=app_id)
            else:
                executors = client.list_executors(app_id=app_id)
            progress.remove_task(task)

        formatter.output(executors, f"Executors for Application {app_id}")

    except Exception as e:
        raise click.ClickException(f"Failed to list executors: {e}")


if __name__ == "__main__":
    apps_group()