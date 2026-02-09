"""
Cleanup-related CLI commands.

Commands for managing Spark event logs and storage cleanup.
"""

from typing import Optional

from spark_history_mcp.cli._compat import (
    CLI_AVAILABLE,
    cli_unavailable_stub,
    click,
    patch_tool_context,
)

if CLI_AVAILABLE:
    from spark_history_mcp.cli.commands.apps import get_spark_client
    from spark_history_mcp.cli.formatters import OutputFormatter


if CLI_AVAILABLE:

    @click.group(name="cleanup")
    def cleanup():
        """Commands for cleaning up Spark event logs and storage."""
        pass

    @cleanup.command("event-logs")
    @click.argument("gcs_dir")
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--max-duration",
        help="Delete logs for apps shorter than this (e.g. '30s', '5m', '2h', '1d')",
    )
    @click.option(
        "--name-pattern",
        help="Delete logs matching this app name pattern (glob-style, e.g. 'my_etl_*')",
    )
    @click.option(
        "--limit",
        type=int,
        default=1000,
        help="Max applications to fetch from SHS (default: 1000)",
    )
    @click.option(
        "--dry-run/--no-dry-run",
        default=True,
        help="Dry run mode (default: enabled). Use --no-dry-run to actually delete.",
    )
    @click.option(
        "--format",
        "-f",
        "output_format",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def event_logs(
        ctx,
        gcs_dir: str,
        server: Optional[str],
        max_duration: Optional[str],
        name_pattern: Optional[str],
        limit: int,
        dry_run: bool,
        output_format: str,
    ):
        """Delete Spark event logs from GCS for matched applications.

        GCS_DIR is the GCS directory containing event logs (e.g. gs://bucket/spark-events).

        At least one of --max-duration or --name-pattern must be provided.

        Examples:

            # Dry run: see what would be deleted
            spark-mcp --cli cleanup event-logs gs://bucket/spark-events --max-duration 5m

            # Actually delete
            spark-mcp --cli cleanup event-logs gs://bucket/spark-events --max-duration 5m --no-dry-run

            # Filter by name pattern
            spark-mcp --cli cleanup event-logs gs://bucket/spark-events --name-pattern 'test_*'
        """
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import delete_event_logs

            with patch_tool_context(client, tools_module):
                result = delete_event_logs(
                    gcs_dir=gcs_dir,
                    server=server,
                    max_duration=max_duration,
                    name_pattern=name_pattern,
                    limit=limit,
                    dry_run=dry_run,
                )
                title = (
                    "Event Log Cleanup (DRY RUN)" if dry_run else "Event Log Cleanup"
                )
                formatter.output(result, title)
        except Exception as err:
            raise click.ClickException(f"Error cleaning up event logs: {err}") from err

else:
    cleanup = cli_unavailable_stub("cleanup")
