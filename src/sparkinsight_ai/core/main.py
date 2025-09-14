"""Main entry point for SparkInsight AI."""

import sys
from sparkinsight_ai.cli.main import cli


def main():
    """
    Main entry point that delegates to the new CLI.

    This maintains backward compatibility while providing the new
    enhanced CLI functionality.
    """
    # If no arguments provided, start the server (backward compatibility)
    if len(sys.argv) == 1:
        sys.argv.extend(["server", "start"])

    cli()


if __name__ == "__main__":
    main()
