"""
Cache management CLI commands.
"""

try:
    import click

    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False


if CLI_AVAILABLE:

    @click.group(name="cache")
    def cache_cmd():
        """Commands for managing the disk cache."""

    @cache_cmd.command("clear")
    def cache_clear():
        """Remove all cached Spark History Server API responses."""
        from spark_history_mcp.cache import clear_cache

        count = clear_cache()
        click.echo(f"Cleared {count} cached entries.")

else:

    def cache_cmd():  # type: ignore[misc]
        """Fallback when CLI dependencies are not available."""
        import sys

        sys.stdout.write("CLI dependencies not installed.\n")
        sys.exit(1)
