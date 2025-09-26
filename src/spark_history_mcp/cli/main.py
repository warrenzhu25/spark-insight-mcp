"""
Spark History Server MCP CLI - Enhanced command line interface.

Provides comprehensive command line access to Spark History Server MCP features including
direct Spark analysis, server management, and configuration utilities.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

try:
    import click
    from rich.console import Console
    from rich.logging import RichHandler

    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False
    click = None
    Console = None
    RichHandler = None


if CLI_AVAILABLE:
    console = Console()


def setup_logging(debug: bool = False) -> None:
    """Set up logging with Rich handler."""
    if not CLI_AVAILABLE:
        return

    level = logging.DEBUG if debug else logging.INFO

    # Remove default handlers and add Rich handler
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, show_time=False, show_path=False)],
    )

    # Set specific loggers
    logging.getLogger("spark_history_mcp").setLevel(level)

    # Suppress noisy third-party loggers unless in debug mode
    if not debug:
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)


if CLI_AVAILABLE:

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
        Spark History Server MCP - AI-Powered Spark Analysis with MCP Integration

        Connect AI agents to Apache Spark History Server for intelligent performance
        monitoring, bottleneck detection, and optimization recommendations.

        Examples:
            spark-mcp --cli apps list               # List Spark applications
            spark-mcp --cli apps show <app-id>      # Get application details
            spark-mcp --cli analyze insights <app-id> # Run comprehensive analysis
            spark-mcp --cli server start            # Start MCP server
            spark-mcp --cli config init             # Create default configuration
        """
        # Set up logging first
        setup_logging(debug and not quiet)

        # Store shared options in context
        ctx.ensure_object(dict)
        ctx.obj["config_path"] = config or Path("config.yaml")
        ctx.obj["debug"] = debug
        ctx.obj["quiet"] = quiet

        # If no subcommand, show help
        if ctx.invoked_subcommand is None:
            click.echo(ctx.get_help())

    # Import command groups
    try:
        from spark_history_mcp.cli.commands.analyze import analyze
        from spark_history_mcp.cli.commands.apps import apps
        from spark_history_mcp.cli.commands.compare import compare
        from spark_history_mcp.cli.commands.config import config_cmd
        from spark_history_mcp.cli.commands.server import server

        cli.add_command(apps)
        cli.add_command(analyze)
        cli.add_command(compare)
        cli.add_command(server)
        cli.add_command(config_cmd, name="config")
    except ImportError:
        pass  # Commands will be unavailable if dependencies missing

else:

    def cli():
        """Fallback when CLI dependencies are not available."""
        print("CLI dependencies not installed. Install with:")
        print("  uv add click rich tabulate")
        print("  # or")
        print("  pip install click rich tabulate")
        sys.exit(1)


if __name__ == "__main__":
    cli()
