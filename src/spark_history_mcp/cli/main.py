"""
Spark History Server MCP CLI - Enhanced command line interface.

Provides comprehensive command line access to Spark History Server MCP features including
direct Spark analysis, server management, and configuration utilities.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from spark_history_mcp.cli._compat import CLI_AVAILABLE, CLI_DEPENDENCY_HINT, click

if CLI_AVAILABLE:
    try:
        from rich.console import Console
        from rich.logging import RichHandler
    except ImportError:
        Console = None
        RichHandler = None
        CLI_RUNTIME_AVAILABLE = False
    else:
        CLI_RUNTIME_AVAILABLE = True
else:
    Console = None
    RichHandler = None
    CLI_RUNTIME_AVAILABLE = False

if CLI_RUNTIME_AVAILABLE:
    console = Console()


def setup_logging(debug: bool = False) -> None:
    """Set up logging with Rich handler."""
    if not (CLI_AVAILABLE and CLI_RUNTIME_AVAILABLE):
        return

    level = logging.DEBUG if debug else logging.INFO

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, show_time=False, show_path=False)],
    )

    logging.getLogger("spark_history_mcp").setLevel(level)

    if not debug:
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)


if CLI_RUNTIME_AVAILABLE:

    @click.group(invoke_without_command=True)
    @click.option(
        "--config",
        "-c",
        type=click.Path(exists=True, path_type=Path),
        default=None,
        help="Configuration file path (default: config.yaml)",
    )
    @click.option("--debug", is_flag=True, help="Enable debug logging")
    @click.option("--quiet", is_flag=True, help="Suppress non-error output")
    @click.pass_context
    def cli(
        ctx: click.Context, config: Optional[Path], debug: bool, quiet: bool
    ) -> None:
        """
        Spark History Server MCP - AI-Powered Spark Analysis with MCP Integration.

        Connect AI agents to the Spark History Server for intelligent performance
        monitoring, bottleneck detection, and optimization recommendations.

        Examples:
            spark-mcp --cli apps list               # List Spark applications
            spark-mcp --cli apps show <app-id>      # Get application details
            spark-mcp --cli analyze insights <app-id> # Run comprehensive analysis
            spark-mcp --cli server start            # Start MCP server
            spark-mcp --cli config init             # Create default configuration
        """
        setup_logging(debug and not quiet)

        ctx.ensure_object(dict)
        ctx.obj["config_path"] = config or Path("config.yaml")
        ctx.obj["debug"] = debug
        ctx.obj["quiet"] = quiet

        if ctx.invoked_subcommand is None:
            click.echo(ctx.get_help())

    try:
        from spark_history_mcp.cli.commands.analyze import analyze
        from spark_history_mcp.cli.commands.apps import apps
        from spark_history_mcp.cli.commands.cache import cache_cmd
        from spark_history_mcp.cli.commands.compare import compare
        from spark_history_mcp.cli.commands.config import config_cmd
        from spark_history_mcp.cli.commands.server import server

        cli.add_command(apps)
        cli.add_command(analyze)
        cli.add_command(compare)
        cli.add_command(server)
        cli.add_command(config_cmd, name="config")
        cli.add_command(cache_cmd, name="cache")
    except ImportError:
        pass  # Commands will be unavailable if dependencies missing

else:

    def cli() -> None:
        """Fallback when CLI dependencies are not available."""
        sys.stdout.write(f"{CLI_DEPENDENCY_HINT}\n")
        sys.stdout.write("  # or\n")
        sys.stdout.write("  pip install click rich tabulate\n")
        sys.exit(1)


if __name__ == "__main__":
    cli()
