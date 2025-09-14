"""
Configuration management CLI commands.

Commands for creating, validating, and managing SparkInsight AI configuration.
"""

import json
from pathlib import Path
from typing import Optional

import click
import yaml
from rich.console import Console
from rich.prompt import Prompt, Confirm

from sparkinsight_ai.config.config import Config, ServerConfig, McpConfig, AuthConfig

console = Console()


def load_config(config_path: Path) -> Config:
    """Load configuration with error handling."""
    try:
        return Config.from_file(str(config_path))
    except FileNotFoundError:
        raise click.ClickException(f"Configuration file not found: {config_path}")
    except Exception as e:
        raise click.ClickException(f"Error loading configuration: {e}")


DEFAULT_CONFIG = {
    "servers": {
        "local": {
            "url": "http://localhost:18080",
            "default": True,
            "verify_ssl": True,
            "timeout": 30
        }
    },
    "mcp": {
        "address": "localhost",
        "port": "18888",
        "debug": False,
        "transports": ["streamable-http"]
    }
}


@click.group(name="config")
def config_group():
    """Commands for managing SparkInsight AI configuration."""
    pass


@config_group.command("init")
@click.option("--force", "-f", is_flag=True, help="Overwrite existing configuration")
@click.option("--interactive", "-i", is_flag=True, help="Interactive configuration setup")
@click.pass_context
def init_config(ctx: click.Context, force: bool, interactive: bool):
    """
    Initialize a new SparkInsight AI configuration file.

    Creates a default config.yaml file with basic settings for connecting
    to a local Spark History Server.

    Examples:
        sparkinsight-ai config init                    # Create default config
        sparkinsight-ai config init --interactive     # Interactive setup
        sparkinsight-ai config init --force           # Overwrite existing config
    """
    config_path = ctx.obj["config_path"]

    if config_path.exists() and not force:
        if not Confirm.ask(f"Configuration file {config_path} already exists. Overwrite?"):
            console.print("Configuration initialization cancelled.")
            return

    try:
        if interactive:
            config_data = _interactive_config_setup()
        else:
            config_data = DEFAULT_CONFIG.copy()

        # Write configuration file
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)

        console.print(f"[green]✓ Configuration file created: {config_path}[/green]")
        console.print("\n[bold blue]Next Steps:[/bold blue]")
        console.print("1. Review and edit the configuration file as needed")
        console.print("2. Test server connectivity: [cyan]sparkinsight-ai server test[/cyan]")
        console.print("3. Start the MCP server: [cyan]sparkinsight-ai server start[/cyan]")

        # Validate the created configuration
        try:
            Config.from_file(str(config_path))
            console.print("\n[green]✓ Configuration file is valid[/green]")
        except Exception as e:
            console.print(f"\n[red]⚠ Configuration validation failed: {e}[/red]")

    except Exception as e:
        raise click.ClickException(f"Failed to create configuration: {e}")


def _interactive_config_setup() -> dict:
    """Interactive configuration setup."""
    console.print("[bold blue]SparkInsight AI Configuration Setup[/bold blue]\n")

    config_data = {"servers": {}, "mcp": {}}

    # MCP Configuration
    console.print("[bold green]MCP Server Configuration[/bold green]")
    config_data["mcp"]["address"] = Prompt.ask("MCP server address", default="localhost")
    config_data["mcp"]["port"] = Prompt.ask("MCP server port", default="18888")
    config_data["mcp"]["debug"] = Confirm.ask("Enable debug mode", default=False)

    transport_options = ["streamable-http", "stdio", "sse"]
    transport = Prompt.ask(
        "MCP transport protocol",
        choices=transport_options,
        default="streamable-http"
    )
    config_data["mcp"]["transports"] = [transport]

    # Spark Server Configuration
    console.print("\n[bold green]Spark History Server Configuration[/bold green]")

    server_name = Prompt.ask("Server name", default="local")
    server_url = Prompt.ask("Spark History Server URL", default="http://localhost:18080")

    server_config = {
        "url": server_url,
        "default": True,
        "verify_ssl": Confirm.ask("Verify SSL certificates", default=True),
        "timeout": int(Prompt.ask("Request timeout (seconds)", default="30"))
    }

    # Authentication
    if Confirm.ask("Configure authentication?", default=False):
        auth_type = Prompt.ask(
            "Authentication type",
            choices=["username", "token"],
            default="username"
        )

        server_config["auth"] = {}
        if auth_type == "username":
            server_config["auth"]["username"] = Prompt.ask("Username")
            server_config["auth"]["password"] = Prompt.ask("Password", password=True)
        else:
            server_config["auth"]["token"] = Prompt.ask("Token", password=True)

    # EMR Configuration
    if Confirm.ask("Is this an AWS EMR cluster?", default=False):
        cluster_arn = Prompt.ask("EMR cluster ARN")
        server_config["emr_cluster_arn"] = cluster_arn

    # Proxy Configuration
    if Confirm.ask("Use SOCKS proxy for connections?", default=False):
        server_config["use_proxy"] = True

    config_data["servers"][server_name] = server_config

    # Additional servers
    while Confirm.ask("Add another Spark History Server?", default=False):
        server_name = Prompt.ask("Server name")
        server_url = Prompt.ask("Spark History Server URL")

        server_config = {
            "url": server_url,
            "default": Confirm.ask("Make this the default server?", default=False),
            "verify_ssl": Confirm.ask("Verify SSL certificates", default=True),
            "timeout": int(Prompt.ask("Request timeout (seconds)", default="30"))
        }

        if Confirm.ask("Configure authentication?", default=False):
            auth_type = Prompt.ask(
                "Authentication type",
                choices=["username", "token"],
                default="username"
            )

            server_config["auth"] = {}
            if auth_type == "username":
                server_config["auth"]["username"] = Prompt.ask("Username")
                server_config["auth"]["password"] = Prompt.ask("Password", password=True)
            else:
                server_config["auth"]["token"] = Prompt.ask("Token", password=True)

        if Confirm.ask("Is this an AWS EMR cluster?", default=False):
            cluster_arn = Prompt.ask("EMR cluster ARN")
            server_config["emr_cluster_arn"] = cluster_arn

        if Confirm.ask("Use SOCKS proxy for connections?", default=False):
            server_config["use_proxy"] = True

        config_data["servers"][server_name] = server_config

    return config_data


@config_group.command("show")
@click.option("--format", "-f", "format_type",
              type=click.Choice(["human", "json", "yaml"]),
              default="human", help="Output format")
@click.pass_context
def show_config(ctx: click.Context, format_type: str):
    """
    Display the current configuration.

    Shows the loaded configuration including all servers and MCP settings,
    with environment variable overrides applied.

    Examples:
        sparkinsight-ai config show                    # Human-readable format
        sparkinsight-ai config show --format json     # JSON format
        sparkinsight-ai config show --format yaml     # YAML format
    """
    try:
        config = load_config(ctx.obj["config_path"])

        if format_type == "json":
            config_dict = json.loads(config.model_dump_json())
            print(json.dumps(config_dict, indent=2))
        elif format_type == "yaml":
            config_dict = json.loads(config.model_dump_json())
            print(yaml.dump(config_dict, default_flow_style=False))
        else:  # human
            from sparkinsight_ai.cli.commands.server import server_status
            # Reuse the server status display logic
            server_status.callback(ctx)

    except Exception as e:
        raise click.ClickException(f"Failed to show configuration: {e}")


@config_group.command("validate")
@click.pass_context
def validate_config(ctx: click.Context):
    """
    Validate the current configuration file.

    Checks that the configuration file is valid and all required
    settings are properly configured.

    Examples:
        sparkinsight-ai config validate
    """
    config_path = ctx.obj["config_path"]

    console.print(f"[bold blue]Validating configuration: {config_path}[/bold blue]\n")

    try:
        # Check if file exists
        if not config_path.exists():
            raise click.ClickException(f"Configuration file not found: {config_path}")

        # Load and validate configuration
        config = Config.from_file(str(config_path))

        console.print("[green]✓ Configuration file is valid[/green]")

        # Additional validation checks
        validation_warnings = []

        # Check for default server
        default_servers = [name for name, cfg in config.servers.items() if cfg.default]
        if not default_servers:
            validation_warnings.append("No default server configured")
        elif len(default_servers) > 1:
            validation_warnings.append(f"Multiple default servers configured: {', '.join(default_servers)}")

        # Check server URLs
        for name, server_config in config.servers.items():
            if not server_config.url.startswith(('http://', 'https://')):
                validation_warnings.append(f"Server '{name}' URL should start with http:// or https://")

        # Check MCP port
        try:
            port = int(config.mcp.port)
            if port < 1 or port > 65535:
                validation_warnings.append(f"MCP port {port} is not in valid range (1-65535)")
        except ValueError:
            validation_warnings.append(f"MCP port '{config.mcp.port}' is not a valid integer")

        if validation_warnings:
            console.print("\n[yellow]⚠ Validation warnings:[/yellow]")
            for warning in validation_warnings:
                console.print(f"  • {warning}")
        else:
            console.print("\n[green]✓ No validation warnings found[/green]")

        # Summary
        console.print(f"\n[bold blue]Configuration Summary:[/bold blue]")
        console.print(f"  • Servers configured: {len(config.servers)}")
        console.print(f"  • MCP port: {config.mcp.port}")
        console.print(f"  • Debug mode: {'Enabled' if config.mcp.debug else 'Disabled'}")

    except Exception as e:
        console.print(f"[red]✗ Configuration validation failed[/red]")
        raise click.ClickException(str(e))


@config_group.command("edit")
@click.pass_context
def edit_config(ctx: click.Context):
    """
    Open the configuration file in the default editor.

    Opens config.yaml in your system's default editor for manual editing.

    Examples:
        sparkinsight-ai config edit
    """
    import subprocess
    import os

    config_path = ctx.obj["config_path"]

    if not config_path.exists():
        if not Confirm.ask(f"Configuration file {config_path} does not exist. Create it first?"):
            return
        # Create default config
        init_config.callback(ctx, force=False, interactive=False)

    try:
        # Try to determine the best editor
        editor = os.environ.get('EDITOR')
        if not editor:
            # Common editors in order of preference
            editors = ['code', 'nano', 'vim', 'vi', 'notepad']
            for ed in editors:
                if subprocess.run(['which', ed], capture_output=True).returncode == 0:
                    editor = ed
                    break

        if not editor:
            console.print(f"[yellow]No editor found. Please edit manually: {config_path}[/yellow]")
            return

        console.print(f"Opening {config_path} with {editor}...")
        subprocess.run([editor, str(config_path)])

        # Validate after editing
        if Confirm.ask("Validate configuration after editing?", default=True):
            validate_config.callback(ctx)

    except Exception as e:
        raise click.ClickException(f"Failed to open editor: {e}")


if __name__ == "__main__":
    config_group()