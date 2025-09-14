"""
Server management CLI commands.

Commands for starting and managing the MCP server.
"""

import os
from pathlib import Path
from typing import List, Optional

import click
from rich.console import Console

from sparkinsight_ai.config.config import Config

# Import app conditionally to handle missing MCP dependencies gracefully
try:
    from sparkinsight_ai.core import app
    MCP_AVAILABLE = True
except ImportError as e:
    MCP_AVAILABLE = False
    MCP_IMPORT_ERROR = str(e)

console = Console()


def load_config(config_path: Path) -> Config:
    """Load configuration with error handling."""
    try:
        return Config.from_file(str(config_path))
    except FileNotFoundError:
        raise click.ClickException(f"Configuration file not found: {config_path}")
    except Exception as e:
        raise click.ClickException(f"Error loading configuration: {e}")


@click.group(name="server")
def server_group():
    """Commands for managing the MCP server."""
    pass


@server_group.command("start")
@click.option("--host", "-h", help="Server host address (default: from config)")
@click.option("--port", "-p", type=int, help="Server port (default: from config)")
@click.option("--transport", "-t",
              type=click.Choice(["streamable-http", "stdio", "sse"]),
              help="MCP transport protocol (default: from config)")
@click.option("--debug", is_flag=True, help="Enable debug mode")
@click.option("--no-debug", is_flag=True, help="Disable debug mode")
@click.pass_context
def start_server(ctx: click.Context, host: Optional[str], port: Optional[int],
                 transport: Optional[str], debug: bool, no_debug: bool):
    """
    Start the SparkInsight AI MCP server.

    The server provides MCP tools for querying Spark History Server data,
    enabling AI agents to analyze Spark applications.

    Examples:
        sparkinsight-ai server start                    # Use config.yaml settings
        sparkinsight-ai server start --port 9999       # Override port
        sparkinsight-ai server start --debug           # Enable debug mode
        sparkinsight-ai server start --transport stdio # Use STDIO transport
    """
    if not MCP_AVAILABLE:
        raise click.ClickException(
            f"MCP server not available. This requires Python 3.10+ and MCP dependencies.\n"
            f"Error: {MCP_IMPORT_ERROR}\n\n"
            f"To install full dependencies: pip install sparkinsight-ai"
        )

    try:
        config = load_config(ctx.obj["config_path"])

        # Override config with command line options
        if host:
            config.mcp.address = host
        if port:
            config.mcp.port = str(port)
        if transport:
            config.mcp.transports = [transport]
        if debug:
            config.mcp.debug = True
        elif no_debug:
            config.mcp.debug = False

        # Also override with CLI debug setting if provided
        if ctx.obj.get("debug"):
            config.mcp.debug = True

        console.print("[bold green]Starting SparkInsight AI MCP Server...[/bold green]")
        console.print(f"Host: [cyan]{config.mcp.address}[/cyan]")
        console.print(f"Port: [cyan]{config.mcp.port}[/cyan]")
        console.print(f"Transport: [cyan]{config.mcp.transports[0]}[/cyan]")
        console.print(f"Debug: [cyan]{config.mcp.debug}[/cyan]")

        # Print configured servers
        console.print("\n[bold blue]Configured Servers:[/bold blue]")
        for name, server_config in config.servers.items():
            status = " [yellow](default)[/yellow]" if server_config.default else ""
            console.print(f"  • [green]{name}[/green]: {server_config.url}{status}")

        console.print("\n[dim]Press Ctrl+C to stop the server[/dim]")

        # Start the server using the existing app module
        app.run(config)

    except KeyboardInterrupt:
        console.print("\n[yellow]Server stopped by user[/yellow]")
    except Exception as e:
        raise click.ClickException(f"Failed to start server: {e}")


@server_group.command("test")
@click.option("--server", "-s", help="Test specific server (default: all servers)")
@click.option("--timeout", type=int, default=10, help="Connection timeout in seconds")
@click.pass_context
def test_servers(ctx: click.Context, server: Optional[str], timeout: int):
    """
    Test connectivity to configured Spark History Servers.

    Verifies that the servers defined in the configuration are reachable
    and responding correctly.

    Examples:
        sparkinsight-ai server test                    # Test all servers
        sparkinsight-ai server test --server prod     # Test specific server
        sparkinsight-ai server test --timeout 30      # Custom timeout
    """
    try:
        from sparkinsight_ai.api.spark_client import SparkRestClient

        config = load_config(ctx.obj["config_path"])

        servers_to_test = {}
        if server:
            if server not in config.servers:
                raise click.ClickException(f"Server '{server}' not found in configuration")
            servers_to_test[server] = config.servers[server]
        else:
            servers_to_test = config.servers

        if not servers_to_test:
            console.print("[red]No servers configured[/red]")
            return

        console.print("[bold blue]Testing Server Connectivity[/bold blue]\n")

        success_count = 0
        total_count = len(servers_to_test)

        for name, server_config in servers_to_test.items():
            console.print(f"Testing [cyan]{name}[/cyan] ({server_config.url})...", end=" ")

            try:
                # Override timeout temporarily
                server_config.timeout = timeout
                client = SparkRestClient(server_config)

                # Simple test - try to list applications with limit 1
                apps = client.list_applications(limit=1)

                console.print("[green]✓ Success[/green]")
                console.print(f"  Found {len(apps)} applications (showing max 1)")

                if server_config.default:
                    console.print("  [yellow](default server)[/yellow]")

                success_count += 1

            except Exception as e:
                console.print("[red]✗ Failed[/red]")
                console.print(f"  Error: {e}")

            console.print()

        # Summary
        if success_count == total_count:
            console.print(f"[green]All {total_count} servers are working correctly[/green] ✓")
        elif success_count > 0:
            console.print(f"[yellow]{success_count}/{total_count} servers are working[/yellow]")
        else:
            console.print("[red]No servers are reachable[/red] ✗")
            raise click.ClickException("Server connectivity test failed")

    except Exception as e:
        if not isinstance(e, click.ClickException):
            raise click.ClickException(f"Failed to test servers: {e}")
        raise


@server_group.command("status")
@click.pass_context
def server_status(ctx: click.Context):
    """
    Show current server configuration and status.

    Displays the loaded configuration including server settings,
    MCP configuration, and environment variable overrides.

    Examples:
        sparkinsight-ai server status
    """
    try:
        config = load_config(ctx.obj["config_path"])

        console.print("[bold blue]SparkInsight AI Server Configuration[/bold blue]\n")

        # MCP Settings
        console.print("[bold green]MCP Server Settings[/bold green]")
        console.print(f"  Address: [cyan]{config.mcp.address}[/cyan]")
        console.print(f"  Port: [cyan]{config.mcp.port}[/cyan]")
        console.print(f"  Debug: [cyan]{config.mcp.debug}[/cyan]")
        console.print(f"  Transports: [cyan]{', '.join(config.mcp.transports)}[/cyan]")

        # Spark Servers
        console.print(f"\n[bold green]Spark History Servers[/bold green] ({len(config.servers)} configured)")

        if not config.servers:
            console.print("  [red]No servers configured[/red]")
        else:
            for name, server_config in config.servers.items():
                console.print(f"\n  [bold cyan]{name}[/bold cyan]")
                console.print(f"    URL: {server_config.url}")
                console.print(f"    Default: {'Yes' if server_config.default else 'No'}")
                console.print(f"    SSL Verify: {'Yes' if server_config.verify_ssl else 'No'}")
                console.print(f"    Timeout: {server_config.timeout}s")

                if server_config.auth and (server_config.auth.username or server_config.auth.token):
                    if server_config.auth.username:
                        console.print(f"    Auth: Username/Password")
                    if server_config.auth.token:
                        console.print(f"    Auth: Token")

                if server_config.emr_cluster_arn:
                    console.print(f"    EMR Cluster: {server_config.emr_cluster_arn}")

                if server_config.use_proxy:
                    console.print(f"    Proxy: Enabled")

        # Environment overrides
        console.print(f"\n[bold green]Environment Variables[/bold green]")
        env_vars = {
            "SHS_MCP_ADDRESS": "MCP server address",
            "SHS_MCP_PORT": "MCP server port",
            "SHS_MCP_DEBUG": "Debug mode",
            "SHS_MCP_TRANSPORT": "MCP transport"
        }

        overrides_found = False
        for env_var, description in env_vars.items():
            value = os.getenv(env_var)
            if value:
                console.print(f"  {env_var}: [cyan]{value}[/cyan] ({description})")
                overrides_found = True

        if not overrides_found:
            console.print("  [dim]No environment overrides set[/dim]")

        console.print(f"\n[bold green]Configuration File[/bold green]")
        console.print(f"  Path: [cyan]{ctx.obj['config_path']}[/cyan]")
        console.print(f"  Exists: {'Yes' if ctx.obj['config_path'].exists() else 'No'}")

    except Exception as e:
        raise click.ClickException(f"Failed to show server status: {e}")


if __name__ == "__main__":
    server_group()