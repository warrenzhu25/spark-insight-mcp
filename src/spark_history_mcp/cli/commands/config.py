"""
Configuration management CLI commands.

Commands for managing Spark History Server MCP configuration.
"""

import os
import shutil
import subprocess  # nosec B404
import sys
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

    @click.group(name="config")
    def config_cmd():
        """Commands for managing configuration."""
        pass

    @config_cmd.command("init")
    @click.option(
        "--interactive", "-i", is_flag=True, help="Interactive configuration setup"
    )
    @click.option("--force", is_flag=True, help="Overwrite existing configuration")
    @click.pass_context
    def init_config(ctx, interactive: bool, force: bool):
        """Initialize a new configuration file."""
        config_path = ctx.obj["config_path"]

        if config_path.exists() and not force:
            if not click.confirm(
                f"Configuration file {config_path} already exists. Overwrite?"
            ):
                click.echo("Configuration initialization cancelled")
                return

        if interactive:
            click.echo("Creating new Spark History Server MCP configuration...")

            # Basic MCP settings
            click.echo("\n--- MCP Server Settings ---")
            port = click.prompt("MCP server port", default=18888, type=int)
            debug = click.confirm("Enable debug mode?", default=False)

            # Server configuration
            click.echo("\n--- Spark History Server Settings ---")
            server_name = click.prompt("Server name", default="local")
            server_url = click.prompt(
                "Spark History Server URL", default="http://localhost:18080"
            )
            is_default = click.confirm("Make this the default server?", default=True)
            verify_ssl = click.confirm("Verify SSL certificates?", default=True)

            # Authentication
            auth_needed = click.confirm(
                "Does this server require authentication?", default=False
            )
            username = password = token = None
            if auth_needed:
                auth_type = click.prompt(
                    "Authentication type",
                    type=click.Choice(["basic", "token"]),
                    default="basic",
                )
                if auth_type == "basic":
                    username = click.prompt("Username")
                    password = click.prompt("Password", hide_input=True)
                else:
                    token = click.prompt("Token", hide_input=True)

            # EMR configuration
            emr_cluster = None
            if click.confirm("Is this an AWS EMR cluster?", default=False):
                emr_cluster = click.prompt("EMR cluster ARN")

            # Build configuration
            config_data = {
                "servers": {
                    server_name: {
                        "url": server_url,
                        "default": is_default,
                        "verify_ssl": verify_ssl,
                    }
                },
                "mcp": {"transports": ["stdio"], "port": port, "debug": debug},
            }

            if auth_needed:
                config_data["servers"][server_name]["auth"] = {}
                if username:
                    config_data["servers"][server_name]["auth"]["username"] = username
                if password:
                    config_data["servers"][server_name]["auth"]["password"] = password
                if token:
                    config_data["servers"][server_name]["auth"]["token"] = token

            if emr_cluster:
                config_data["servers"][server_name]["emr_cluster_arn"] = emr_cluster

        else:
            # Default configuration
            config_data = {
                "servers": {
                    "local": {"default": True, "url": "http://localhost:18080"}
                },
                "mcp": {"transports": ["stdio"], "port": 18888, "debug": False},
            }

        # Write configuration
        try:
            import yaml

            config_path.parent.mkdir(parents=True, exist_ok=True)

            with open(config_path, "w") as f:
                f.write("# Spark History Server MCP Configuration\n")
                f.write("# WARNING: Do not commit your sensitive credentials\n\n")
                yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)

            click.echo(f"\n✓ Configuration created at {config_path}")

            if interactive and auth_needed:
                click.echo("\n⚠️  Security Note:")
                click.echo("Your credentials are stored in the config file.")
                click.echo("Consider using environment variables instead:")
                click.echo("  SHS_SERVERS_LOCAL_AUTH_USERNAME")
                click.echo("  SHS_SERVERS_LOCAL_AUTH_PASSWORD")
                click.echo("  SHS_SERVERS_LOCAL_AUTH_TOKEN")

        except Exception as e:
            raise click.ClickException(f"Error creating configuration: {e}") from e

    @config_cmd.command("show")
    @click.option("--server", "-s", help="Show specific server configuration")
    @click.option(
        "--format",
        "-f",
        "output_format",
        type=click.Choice(["human", "json", "yaml"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def show_config(ctx, server: Optional[str], output_format: str):
        """Show current configuration."""
        config_path = ctx.obj["config_path"]

        try:
            config = Config.from_file(str(config_path))

            if output_format == "json":
                import json

                click.echo(json.dumps(config.model_dump(), indent=2))
            elif output_format == "yaml":
                import yaml

                click.echo(yaml.dump(config.model_dump(), default_flow_style=False))
            else:
                # Human-readable format
                click.echo(f"Configuration from: {config_path}")
                click.echo("\nMCP Server:")
                click.echo(f"  Port: {config.mcp.port}")
                click.echo(f"  Transports: {', '.join(config.mcp.transports)}")
                click.echo(f"  Debug: {config.mcp.debug}")

                if server:
                    if server in config.servers:
                        server_config = config.servers[server]
                        click.echo(f"\nServer '{server}':")
                        click.echo(f"  URL: {server_config.url}")
                        click.echo(f"  Default: {server_config.default}")
                        click.echo(f"  Verify SSL: {server_config.verify_ssl}")
                        if server_config.emr_cluster_arn:
                            click.echo(
                                f"  EMR Cluster: {server_config.emr_cluster_arn}"
                            )
                    else:
                        raise click.ClickException(
                            f"Server '{server}' not found in configuration"
                        )
                else:
                    click.echo(f"\nServers ({len(config.servers)}):")
                    for name, server_config in config.servers.items():
                        default_marker = " (default)" if server_config.default else ""
                        click.echo(f"  {name}{default_marker}: {server_config.url}")

        except Exception as e:
            raise click.ClickException(f"Error reading configuration: {e}") from e

    @config_cmd.command("validate")
    @click.pass_context
    def validate_config(ctx):
        """Validate configuration file."""
        config_path = ctx.obj["config_path"]

        try:
            config = Config.from_file(str(config_path))

            click.echo("✓ Configuration file is valid")

            # Additional validation
            default_servers = [
                name for name, cfg in config.servers.items() if cfg.default
            ]
            if len(default_servers) == 0:
                click.echo("⚠️  Warning: No default server configured")
            elif len(default_servers) > 1:
                click.echo(
                    f"⚠️  Warning: Multiple default servers: {', '.join(default_servers)}"
                )

            # Check for missing URLs
            for name, server_config in config.servers.items():
                if not server_config.url and not server_config.emr_cluster_arn:
                    click.echo(
                        f"⚠️  Warning: Server '{name}' has no URL or EMR cluster configured"
                    )

            click.echo(f"Configuration contains {len(config.servers)} server(s)")

        except Exception as e:
            raise click.ClickException(f"Configuration validation failed: {e}") from e

    @config_cmd.command("edit")
    @click.pass_context
    def edit_config(ctx):
        """Edit configuration file in default editor."""
        config_path = ctx.obj["config_path"]

        if not config_path.exists():
            if click.confirm("Configuration file doesn't exist. Create it first?"):
                ctx.invoke(init_config)
            else:
                return

        try:
            editor = os.environ.get("EDITOR", "nano" if shutil.which("nano") else "vi")
            subprocess.run(  # noqa: S603 # nosec B603
                [editor, str(config_path)], check=True
            )

            # Validate after editing
            try:
                Config.from_file(str(config_path))
                click.echo("✓ Configuration file is valid after editing")
            except Exception as e:
                click.echo(
                    f"⚠️  Warning: Configuration validation failed: {e}", err=True
                )

        except subprocess.CalledProcessError as e:
            raise click.ClickException("Error opening editor") from e
        except FileNotFoundError as e:
            raise click.ClickException(
                f"Editor '{editor}' not found. Set EDITOR environment variable."
            ) from e

else:
    # Fallback when CLI dependencies not available
    def config_cmd():
        sys.stderr.write(
            "CLI dependencies not installed. Install with: uv add click rich tabulate\n"
        )
        return None
