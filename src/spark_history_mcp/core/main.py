"""Main entry point for Spark History Server MCP."""

import json
import logging
import sys

from spark_history_mcp.config.config import Config
from spark_history_mcp.core import app

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def detect_cli_command():
    """Detect if CLI command is being used."""
    cli_commands = [
        "apps",
        "analyze",
        "server",
        "config",
        "list",
        "show",
        "jobs",
        "stages",
        "summary",
        "insights",
        "bottlenecks",
        "auto-scaling",
        "shuffle-skew",
        "slowest",
        "compare",
        "start",
        "test",
        "status",
        "init",
        "validate",
        "edit",
    ]
    return any(cmd in sys.argv for cmd in cli_commands)


def main():
    """Main entry point with CLI/MCP detection."""

    # CLI mode detection
    if "--cli" in sys.argv or detect_cli_command():
        try:
            from spark_history_mcp.cli.main import cli

            # Remove --cli flag if present
            if "--cli" in sys.argv:
                sys.argv.remove("--cli")

            # Run CLI
            cli()
            return

        except ImportError as e:
            sys.stderr.write("CLI dependencies not installed. Install with:\n")
            sys.stderr.write("  uv add click rich tabulate\n")
            sys.stderr.write("  # or\n")
            sys.stderr.write("  pip install click rich tabulate\n")
            sys.stderr.write(f"\nError: {e}\n")
            sys.exit(1)

    # Default: MCP server mode (PRESERVED)
    try:
        logger.info("Starting Spark History Server MCP...")
        config = Config.from_file("config.yaml")
        if config.mcp.debug:
            logger.setLevel(logging.DEBUG)
        logger.debug(json.dumps(json.loads(config.model_dump_json()), indent=4))
        app.run(config)
    except Exception as e:
        logger.error(f"Failed to start MCP server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
