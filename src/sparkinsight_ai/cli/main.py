"""
SparkInsight AI CLI - Enhanced command line interface.

Provides comprehensive command line access to SparkInsight AI features including
direct Spark analysis, server management, and configuration utilities.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.logging import RichHandler

from sparkinsight_ai import __version__
# Import commands - done at the end to avoid circular imports
from sparkinsight_ai.config.config import Config

console = Console()


def setup_logging(debug: bool = False) -> None:
    """Set up logging with Rich handler."""
    level = logging.DEBUG if debug else logging.INFO

    # Remove default handlers and add Rich handler
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, show_time=False, show_path=False)]
    )

    # Set specific loggers
    logging.getLogger("sparkinsight_ai").setLevel(level)

    # Suppress noisy third-party loggers unless in debug mode
    if not debug:
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)


@click.group(invoke_without_command=True)
@click.version_option(version=__version__)
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Configuration file path (default: config.yaml)"
)
@click.option("--debug", is_flag=True, help="Enable debug logging")
@click.option("--quiet", is_flag=True, help="Suppress non-error output")
@click.pass_context
def cli(ctx: click.Context, config: Optional[Path], debug: bool, quiet: bool) -> None:
    """
    SparkInsight AI - AI-Powered Spark Analysis with MCP Integration

    Connect AI agents to Apache Spark History Server for intelligent performance
    monitoring, bottleneck detection, and optimization recommendations.

    Examples:
        sparkinsight-ai apps                    # List Spark applications
        sparkinsight-ai app <app-id>            # Get application details
        sparkinsight-ai analyze <app-id>        # Run comprehensive analysis
        sparkinsight-ai server                  # Start MCP server
        sparkinsight-ai config init             # Create default configuration
    """
    # Set up logging first
    setup_logging(debug and not quiet)

    # Store shared options in context
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config or Path("config.yaml")
    ctx.obj["debug"] = debug
    ctx.obj["quiet"] = quiet

    # If no command specified, show help
    if ctx.invoked_subcommand is None:
        console.print(ctx.get_help())
        return




# Import and add command groups at module level to avoid circular imports
from sparkinsight_ai.cli.commands.apps import apps_group
from sparkinsight_ai.cli.commands.analyze import analyze_group
from sparkinsight_ai.cli.commands.config import config_group
from sparkinsight_ai.cli.commands.server import server_group

cli.add_command(apps_group)
cli.add_command(analyze_group)
cli.add_command(config_group)
cli.add_command(server_group)


if __name__ == "__main__":
    cli()