"""
Server management CLI commands.

Commands for managing the MCP server lifecycle.
"""

from typing import Optional

try:
    import click

    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False

from spark_history_mcp.config.config import Config

if CLI_AVAILABLE:
    pass


if CLI_AVAILABLE:

    @click.group(name="server")
    def server():
        """Commands for managing the MCP server."""
        pass

    @server.command("start")
    @click.option("--port", "-p", type=int, help="Port to run server on")
    @click.option("--debug", is_flag=True, help="Enable debug mode")
    @click.option(
        "--transport",
        type=click.Choice(["stdio", "sse", "streamable-http"]),
        help="Transport protocol",
    )
    @click.pass_context
    def start_server(ctx, port: Optional[int], debug: bool, transport: Optional[str]):
        """Start the MCP server."""
        config_path = ctx.obj["config_path"]

        try:
            # Load config
            config = Config.from_file(str(config_path))

            # Override config with CLI options
            if port:
                config.mcp.port = port
            if debug:
                config.mcp.debug = True
            if transport:
                config.mcp.transports = [transport]

            # Start the server using the original main
            from spark_history_mcp.core import app

            click.echo(f"Starting MCP server on port {config.mcp.port}...")
            if debug:
                click.echo("Debug mode enabled")

            app.run(config)

        except KeyboardInterrupt:
            click.echo("\nServer stopped by user")
        except Exception as e:
            raise click.ClickException(f"Error starting server: {e}")

    @server.command("test")
    @click.option("--timeout", type=int, default=10, help="Test timeout in seconds")
    @click.pass_context
    def test_server(ctx, timeout: int):
        """Test server connectivity and configuration."""
        config_path = ctx.obj["config_path"]

        try:
            # Load config
            config = Config.from_file(str(config_path))

            click.echo("Testing server configuration...")

            # Test config validity
            click.echo(f"✓ Configuration loaded from {config_path}")
            click.echo(f"✓ Found {len(config.servers)} server(s) configured")

            # Test each server connection
            for name, server_config in config.servers.items():
                click.echo(f"Testing connection to '{name}' ({server_config.url})...")

                try:
                    from spark_history_mcp.api.spark_client import SparkRestClient

                    client = SparkRestClient(server_config)

                    # Test basic connectivity with timeout
                    import requests

                    response = requests.get(
                        f"{server_config.url}/api/v1/applications",
                        timeout=timeout,
                        verify=server_config.verify_ssl,
                    )
                    response.raise_for_status()

                    click.echo(f"✓ Connection to '{name}' successful")

                except requests.exceptions.Timeout:
                    click.echo(
                        f"✗ Connection to '{name}' timed out after {timeout}s", err=True
                    )
                except requests.exceptions.ConnectionError:
                    click.echo(f"✗ Could not connect to '{name}'", err=True)
                except Exception as e:
                    click.echo(f"✗ Error connecting to '{name}': {e}", err=True)

            click.echo("\nServer test completed")

        except Exception as e:
            raise click.ClickException(f"Error testing server: {e}")

    @server.command("status")
    @click.pass_context
    def status_server(ctx):
        """Show server status and configuration."""
        config_path = ctx.obj["config_path"]

        try:
            config = Config.from_file(str(config_path))

            click.echo("Server Configuration:")
            click.echo(f"  Config file: {config_path}")
            click.echo(f"  MCP port: {config.mcp.port}")
            click.echo(f"  MCP transports: {', '.join(config.mcp.transports)}")
            click.echo(f"  Debug mode: {config.mcp.debug}")

            click.echo(f"\nConfigured Servers ({len(config.servers)}):")
            for name, server_config in config.servers.items():
                default_marker = " (default)" if server_config.default else ""
                click.echo(f"  {name}{default_marker}: {server_config.url}")

                if server_config.emr_cluster_arn:
                    click.echo(f"    EMR Cluster: {server_config.emr_cluster_arn}")
                if not server_config.verify_ssl:
                    click.echo("    SSL verification: disabled")

        except Exception as e:
            raise click.ClickException(f"Error getting server status: {e}")

else:
    # Fallback when CLI dependencies not available
    def server():
        print(
            "CLI dependencies not installed. Install with: uv add click rich tabulate"
        )
        return None
