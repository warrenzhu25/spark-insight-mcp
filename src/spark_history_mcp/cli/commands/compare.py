"""
Comparison-related CLI commands.

Commands for comparing multiple Spark applications with stateful context management.
"""

import json
from pathlib import Path
from typing import Optional, Tuple

try:
    import click

    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False


if CLI_AVAILABLE:
    from spark_history_mcp.cli.commands.apps import get_spark_client
    from spark_history_mcp.cli.formatters import OutputFormatter


def get_session_file() -> Path:
    """Get path to session state file."""
    config_dir = Path.home() / ".config" / "spark-history-mcp"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / "compare-session.json"


def save_comparison_context(app_id1: str, app_id2: str, server: Optional[str] = None):
    """Save comparison context to session file."""
    session_file = get_session_file()
    context = {
        "app_id1": app_id1,
        "app_id2": app_id2,
        "server": server,
    }
    with open(session_file, "w") as f:
        json.dump(context, f)


def load_comparison_context() -> Optional[Tuple[str, str, Optional[str]]]:
    """Load comparison context from session file."""
    session_file = get_session_file()
    if not session_file.exists():
        return None

    try:
        with open(session_file, "r") as f:
            context = json.load(f)
        return context["app_id1"], context["app_id2"], context.get("server")
    except (json.JSONDecodeError, KeyError, FileNotFoundError):
        return None


def clear_comparison_context():
    """Clear comparison context."""
    session_file = get_session_file()
    if session_file.exists():
        session_file.unlink()


def get_app_context(
    app_id1: Optional[str] = None,
    app_id2: Optional[str] = None,
    server: Optional[str] = None,
) -> Tuple[str, str, Optional[str]]:
    """Get app context from parameters or session."""
    if app_id1 and app_id2:
        # Save new context
        save_comparison_context(app_id1, app_id2, server)
        return app_id1, app_id2, server

    # Try to load from session
    context = load_comparison_context()
    if context is None:
        raise click.ClickException(
            "No comparison context found. "
            "Run 'compare apps <app1> <app2>' first to set context, "
            "or provide --apps <app1> <app2> to override."
        )

    stored_app1, stored_app2, stored_server = context
    # Use stored server if no server provided
    final_server = server if server is not None else stored_server
    return stored_app1, stored_app2, final_server


def is_app_id(identifier: str) -> bool:
    """Detect if identifier looks like an app ID vs app name."""
    # Common app ID patterns: app-YYYYMMDD-*, application_*, etc.
    import re

    app_id_patterns = [
        r"^app-\d{8}-\w+$",  # app-20231201-123456
        r"^application_\d+_\d+$",  # application_1234567890_001
        r"^app-\w{8,}$",  # app-abcd1234
        r"^\w+-\d{4}\d{2}\d{2}-\w+$",  # any-20231201-something
    ]

    return any(
        re.match(pattern, identifier, re.IGNORECASE) for pattern in app_id_patterns
    )


def resolve_app_name_to_recent_apps(
    app_name: str, client, server: Optional[str] = None, limit: int = 2
) -> Tuple[str, str, list]:
    """
    Resolve app name to the most recent matching applications.

    Returns:
        Tuple of (app_id1, app_id2, app_list) where app_list contains the full app objects
    """
    import spark_history_mcp.tools.tools as tools_module
    from spark_history_mcp.tools.tools import list_applications

    original_get_context = getattr(tools_module.mcp, "get_context", None)

    class MockContext:
        def __init__(self, client):
            self.request_context = MockRequestContext(client)

    class MockRequestContext:
        def __init__(self, client):
            self.lifespan_context = MockLifespanContext(client)

    class MockLifespanContext:
        def __init__(self, client):
            self.default_client = client
            self.clients = {"default": client}

    tools_module.mcp.get_context = lambda: MockContext(client)

    try:
        # Search for applications by name
        apps = list_applications(
            server=server,
            app_name=app_name,
            search_type="contains",
            limit=limit,
        )

        if len(apps) < 2:
            if len(apps) == 0:
                raise click.ClickException(
                    f"No applications found matching '{app_name}'.\n\n"
                    f"Tips:\n"
                    f"  • Try a partial name: 'ETL' instead of 'ETL Pipeline Job'\n"
                    f"  • Check spelling and capitalization\n"
                    f"  • Use exact app IDs if known: compare apps app1 app2\n"
                    f"  • List available apps: apps list --name '{app_name}'"
                )
            else:
                raise click.ClickException(
                    f"Only found 1 application matching '{app_name}'. "
                    f"Need at least 2 applications to compare.\n\n"
                    f"Found: {apps[0].id} - {apps[0].name}\n\n"
                    f"Tips:\n"
                    f"  • Try a broader search term\n"
                    f"  • Use specific app IDs: compare apps {apps[0].id} <other-app-id>"
                )

        if len(apps) > limit:
            # Show available options
            app_list = "\n".join([f"  {app.id} - {app.name}" for app in apps[:10]])
            raise click.ClickException(
                f"Found {len(apps)} applications matching '{app_name}'. "
                f"Please be more specific or use exact app IDs.\n"
                f"Recent matches:\n{app_list}"
            )

        # Return the two most recent (first two in the list)
        return apps[0].id, apps[1].id, apps

    finally:
        if original_get_context:
            tools_module.mcp.get_context = original_get_context


def resolve_app_identifiers(
    identifier1: str, identifier2: Optional[str], client, server: Optional[str] = None
) -> Tuple[str, str, Optional[str]]:
    """
    Resolve app identifiers to app IDs and return user feedback message.

    Returns:
        Tuple of (app_id1, app_id2, feedback_message)
    """
    if identifier2 is None:
        # Single identifier - treat as name and find 2 recent matching apps
        if is_app_id(identifier1):
            raise click.ClickException(
                f"When providing a single argument, it should be an application name, "
                f"not an app ID. Provided: '{identifier1}'\n\n"
                f"Usage:\n"
                f"  compare apps 'App Name'    # Auto-compare 2 recent matching apps\n"
                f"  compare apps app1 app2     # Compare specific app IDs\n\n"
                f'Note: Use quotes around names with spaces: "ETL Pipeline"'
            )

        app_id1, app_id2, apps = resolve_app_name_to_recent_apps(
            identifier1, client, server
        )

        feedback = (
            f"Found 2 recent applications matching '{identifier1}':\n"
            f"  1. {app_id1} - {apps[0].name} ({apps[0].attempts[0].start_time if apps[0].attempts else 'Unknown time'}) ← Latest\n"
            f"  2. {app_id2} - {apps[1].name} ({apps[1].attempts[0].start_time if apps[1].attempts else 'Unknown time'}) ← Previous"
        )

        return app_id1, app_id2, feedback

    else:
        # Two identifiers - resolve each one
        resolved_id1 = identifier1
        resolved_id2 = identifier2
        feedback_parts = []

        # Resolve first identifier if it looks like a name
        if not is_app_id(identifier1):
            try:
                resolved_id1, _, apps1 = resolve_app_name_to_recent_apps(
                    identifier1, client, server, 1
                )
                feedback_parts.append(
                    f"Resolved '{identifier1}' to: {resolved_id1} - {apps1[0].name}"
                )
            except click.ClickException:
                # If name resolution fails, treat as literal ID
                pass

        # Resolve second identifier if it looks like a name
        if not is_app_id(identifier2):
            try:
                resolved_id2, _, apps2 = resolve_app_name_to_recent_apps(
                    identifier2, client, server, 1
                )
                feedback_parts.append(
                    f"Resolved '{identifier2}' to: {resolved_id2} - {apps2[0].name}"
                )
            except click.ClickException:
                # If name resolution fails, treat as literal ID
                pass

        feedback = "\n".join(feedback_parts) if feedback_parts else None
        return resolved_id1, resolved_id2, feedback


def create_mock_context(client):
    """Create mock context for MCP tool functions."""

    class MockContext:
        def __init__(self, client):
            self.request_context = MockRequestContext(client)

    class MockRequestContext:
        def __init__(self, client):
            self.lifespan_context = MockLifespanContext(client)

    class MockLifespanContext:
        def __init__(self, client):
            self.default_client = client
            self.clients = {"default": client}

    return MockContext(client)


def extract_stage_menu_options(comparison_data):
    """Extract stage differences for interactive menu."""
    stage_dive = comparison_data.get("stage_deep_dive", {})
    differences = stage_dive.get("top_stage_differences", [])

    options = []
    for i, diff in enumerate(differences[:3], 1):
        app1_stage = diff.get("app1_stage", {})
        app2_stage = diff.get("app2_stage", {})
        app1_id = app1_stage.get("stage_id")
        app2_id = app2_stage.get("stage_id")
        stage_name = diff.get("stage_name", "Unknown")[:35]  # Truncate for display

        if app1_id is not None and app2_id is not None:
            options.append((i, app1_id, app2_id, stage_name))

    return options


def show_interactive_menu(comparison_data, app_id1, app_id2, server, formatter, ctx):
    """Show interactive navigation menu after comparison."""
    try:
        from rich.console import Console
        from rich.panel import Panel

        console = Console()
    except ImportError:
        # Fallback to simple text menu if Rich not available
        console = None

    # Extract stage options
    stage_options = extract_stage_menu_options(comparison_data)

    if not stage_options:
        click.echo("No stage differences available for interactive navigation.")
        return

    # Build menu content
    menu_lines = ["Press a key for detailed analysis:", ""]

    for i, app1_id, app2_id, stage_name in stage_options:
        menu_lines.append(
            f"[{i}] Analyze Stage #{i} (App1:{app1_id} vs App2:{app2_id})"
        )
        menu_lines.append(f"    {stage_name}")
        menu_lines.append("")

    menu_lines.extend(
        [
            "\\[t] Compare Application Timeline",  # Escape brackets for Rich
            "\\[q] Quit / Continue",
        ]
    )

    # Display menu
    if console:
        content = "\n".join(menu_lines)
        console.print(Panel(content, title="Next Steps", border_style="green"))
    else:
        click.echo("\n" + "=" * 50)
        click.echo("Next Steps")
        click.echo("=" * 50)
        for line in menu_lines:
            click.echo(line)
        click.echo("=" * 50)

    # Get user input
    try:
        try:
            choice = click.getchar().lower()
            click.echo()  # Add newline after input
        except OSError:
            # Fallback to regular input if getchar() fails
            choice = click.prompt("Enter choice", type=str, default="q").lower()

        # Handle user selection
        if choice == "q":
            return
        elif choice == "t":
            # Execute timeline comparison
            execute_timeline_comparison(app_id1, app_id2, server, formatter, ctx)
        elif choice.isdigit():
            choice_num = int(choice)
            # Find matching stage option
            for i, app1_stage_id, app2_stage_id, _ in stage_options:
                if i == choice_num:
                    execute_stage_comparison(
                        app1_stage_id, app2_stage_id, server, formatter, ctx
                    )
                    break
            else:
                click.echo(f"Invalid choice: {choice}")
        else:
            click.echo(f"Invalid choice: {choice}")

    except (KeyboardInterrupt, EOFError):
        click.echo("\nExiting interactive mode.")


def execute_stage_comparison(stage_id1, stage_id2, server, formatter, ctx):
    """Execute stage comparison command."""
    try:
        click.echo(f"Analyzing stages {stage_id1} vs {stage_id2}...")

        # Load comparison context to get app IDs
        context = load_comparison_context()
        if not context:
            click.echo("Error: No comparison context found.")
            return

        app_id1, app_id2, _ = context

        # Import and execute stage comparison
        import spark_history_mcp.tools.tools as tools_module
        from spark_history_mcp.tools.tools import compare_stages

        client = get_spark_client(ctx.obj["config_path"], server)
        original_get_context = getattr(tools_module.mcp, "get_context", None)
        tools_module.mcp.get_context = lambda: create_mock_context(client)

        try:
            comparison_data = compare_stages(
                app_id1=app_id1,
                app_id2=app_id2,
                stage_id1=stage_id1,
                stage_id2=stage_id2,
                server=server,
            )
            formatter.output(
                comparison_data, f"Stage Comparison: {stage_id1} vs {stage_id2}"
            )

            # Show post-stage menu if in human format and comparison context exists
            if formatter.format_type == "human" and load_comparison_context():
                show_post_stage_menu(
                    app_id1, app_id2, stage_id1, stage_id2, server, formatter, ctx
                )
        finally:
            if original_get_context:
                tools_module.mcp.get_context = original_get_context

    except Exception as e:
        click.echo(f"Error executing stage comparison: {e}")


def execute_timeline_comparison(app_id1, app_id2, server, formatter, ctx):
    """Execute timeline comparison command."""
    try:
        click.echo("Analyzing application timeline...")

        # Import and execute timeline comparison
        import spark_history_mcp.tools.tools as tools_module
        from spark_history_mcp.tools.tools import compare_app_executor_timeline

        client = get_spark_client(ctx.obj["config_path"], server)
        original_get_context = getattr(tools_module.mcp, "get_context", None)
        tools_module.mcp.get_context = lambda: create_mock_context(client)

        try:
            comparison_data = compare_app_executor_timeline(
                app_id1=app_id1, app_id2=app_id2, server=server, interval_minutes=1
            )
            formatter.output(
                comparison_data, f"Timeline Comparison: {app_id1} vs {app_id2}"
            )
        finally:
            if original_get_context:
                tools_module.mcp.get_context = original_get_context

    except Exception as e:
        click.echo(f"Error executing timeline comparison: {e}")


def execute_stage_timeline_comparison(stage_id1, stage_id2, server, formatter, ctx):
    """Execute stage-specific timeline comparison."""
    try:
        click.echo(f"Analyzing stage {stage_id1} vs {stage_id2} timeline...")

        # Load comparison context to get app IDs
        context = load_comparison_context()
        if not context:
            click.echo("Error: No comparison context found.")
            return

        app_id1, app_id2, _ = context

        # Import and execute stage timeline comparison
        import spark_history_mcp.tools.tools as tools_module
        from spark_history_mcp.tools.tools import compare_stage_executor_timeline

        client = get_spark_client(ctx.obj["config_path"], server)
        original_get_context = getattr(tools_module.mcp, "get_context", None)
        tools_module.mcp.get_context = lambda: create_mock_context(client)

        try:
            comparison_data = compare_stage_executor_timeline(
                app_id1=app_id1,
                app_id2=app_id2,
                stage_id1=stage_id1,
                stage_id2=stage_id2,
                server=server,
                interval_minutes=1,
            )
            formatter.output(
                comparison_data, f"Stage {stage_id1} vs {stage_id2} Timeline Comparison"
            )
        finally:
            if original_get_context:
                tools_module.mcp.get_context = original_get_context

    except Exception as e:
        click.echo(f"Error executing stage timeline comparison: {e}")


def show_post_stage_menu(
    app_id1, app_id2, stage_id1, stage_id2, server, formatter, ctx
):
    """Show follow-up options after stage comparison completion."""
    try:
        from rich.console import Console
        from rich.panel import Panel

        console = Console()
    except ImportError:
        # Fallback to simple text menu if Rich not available
        console = None

    # Build menu content
    menu_lines = ["Choose your next analysis:", ""]
    menu_lines.extend(
        [
            "\\[t] Compare Application Timeline",
            f"\\[s] Compare Stage {stage_id1} Timeline Patterns",
            "\\[q] Continue",
        ]
    )

    # Display menu
    if console:
        content = "\n".join(menu_lines)
        console.print(Panel(content, title="What's Next?", border_style="green"))
    else:
        click.echo("\n" + "=" * 50)
        click.echo("What's Next?")
        click.echo("=" * 50)
        for line in menu_lines:
            click.echo(line)
        click.echo("=" * 50)

    # Get user input
    try:
        try:
            choice = click.getchar().lower()
            click.echo()  # Add newline after input
        except OSError:
            # Fallback to regular input if getchar() fails
            choice = click.prompt("Enter choice", type=str, default="q").lower()

        # Handle user selection
        if choice == "q":
            return
        elif choice == "t":
            # Execute application timeline comparison
            execute_timeline_comparison(app_id1, app_id2, server, formatter, ctx)
        elif choice == "s":
            # Execute stage timeline comparison
            execute_stage_timeline_comparison(
                stage_id1, stage_id2, server, formatter, ctx
            )
        else:
            click.echo(f"Invalid choice: {choice}")

    except (KeyboardInterrupt, EOFError):
        click.echo("\nExiting interactive mode.")


if CLI_AVAILABLE:

    @click.group(name="compare")
    def compare():
        """Commands for comparing Spark applications."""
        pass

    @compare.command("apps")
    @click.argument("app_identifier1")
    @click.argument("app_identifier2", required=False)
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--top-n",
        "-n",
        type=int,
        default=3,
        help="Number of top stage differences to analyze",
    )
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.option(
        "--interactive",
        "-i",
        is_flag=True,
        help="Show interactive navigation menu after comparison",
    )
    @click.pass_context
    def apps(
        ctx,
        app_identifier1: str,
        app_identifier2: Optional[str],
        server: Optional[str],
        top_n: int,
        format: str,
        interactive: bool,
    ):
        """
        Compare performance between two applications and set comparison context.

        APP_IDENTIFIER1: Application ID or name
        APP_IDENTIFIER2: (Optional) Second application ID or name. If not provided,
                        the first argument is treated as a name and the 2 most recent
                        matching applications are compared.

        Quoting Rules:
            • Single words: quotes optional (ETLPipeline or "ETLPipeline")
            • Names with spaces: quotes required ("ETL Pipeline")
            • Special characters: quotes recommended ("My-App@Production")

        Examples:
            compare apps app-123 app-456                    # Compare by IDs
            compare apps "ETL Pipeline"                     # Auto-compare last 2 matching
            compare apps ETLPipeline                        # Single word, no quotes needed
            compare apps "Daily Job" "Weekly Job"           # Compare by names with spaces
            compare apps MyJob "Production ETL"             # Mixed: single word + quoted
        """
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            # Resolve app identifiers to actual app IDs
            app_id1, app_id2, feedback = resolve_app_identifiers(
                app_identifier1, app_identifier2, client, server
            )

            # Show user feedback about resolution
            if feedback and not ctx.obj.get("quiet", False):
                click.echo(feedback)
                click.echo()

            # Save comparison context
            save_comparison_context(app_id1, app_id2, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools.tools import compare_app_performance

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                comparison_data = compare_app_performance(
                    app_id1=app_id1, app_id2=app_id2, server=server, top_n=top_n
                )
                formatter.output(comparison_data)

                if not ctx.obj.get("quiet", False):
                    click.echo(f"\n✓ Comparison context saved: {app_id1} vs {app_id2}")
                    if interactive and format == "human":
                        # Show interactive menu for further navigation
                        show_interactive_menu(
                            comparison_data, app_id1, app_id2, server, formatter, ctx
                        )
                    else:
                        click.echo(
                            "Use 'compare stages', 'compare timeline', etc. for detailed analysis"
                        )

            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error comparing applications: {e}")

    @compare.command("stages")
    @click.argument("stage_id1", type=int)
    @click.argument("stage_id2", type=int)
    @click.option(
        "--apps",
        nargs=2,
        metavar="APP1 APP2",
        help="Override apps from context (app1 app2)",
    )
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--significance-threshold",
        type=float,
        default=0.2,
        help="Minimum difference threshold to include metric",
    )
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def stages(
        ctx,
        stage_id1: int,
        stage_id2: int,
        apps: Optional[Tuple[str, str]],
        server: Optional[str],
        significance_threshold: float,
        format: str,
    ):
        """Compare specific stages between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools.tools import compare_stages

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                comparison_data = compare_stages(
                    app_id1=app_id1,
                    app_id2=app_id2,
                    stage_id1=stage_id1,
                    stage_id2=stage_id2,
                    server=final_server,
                    significance_threshold=significance_threshold,
                )
                formatter.output(
                    comparison_data,
                    f"Stage Comparison: {app_id1}:stage{stage_id1} vs {app_id2}:stage{stage_id2}",
                )

                # Show post-stage menu if in human format and comparison context exists
                if format == "human" and load_comparison_context():
                    show_post_stage_menu(
                        app_id1,
                        app_id2,
                        stage_id1,
                        stage_id2,
                        final_server,
                        formatter,
                        ctx,
                    )
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error comparing stages: {e}")

    @compare.command("timeline")
    @click.option(
        "--apps",
        nargs=2,
        metavar="APP1 APP2",
        help="Override apps from context (app1 app2)",
    )
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--interval-minutes",
        type=int,
        default=1,
        help="Time interval for analysis in minutes",
    )
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def timeline(
        ctx,
        apps: Optional[Tuple[str, str]],
        server: Optional[str],
        interval_minutes: int,
        format: str,
    ):
        """Compare executor timeline patterns between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools.tools import compare_app_executor_timeline

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                comparison_data = compare_app_executor_timeline(
                    app_id1=app_id1,
                    app_id2=app_id2,
                    server=final_server,
                    interval_minutes=interval_minutes,
                )
                formatter.output(
                    comparison_data,
                    f"Timeline Comparison: {app_id1} vs {app_id2}",
                )
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error comparing timelines: {e}")

    @compare.command("stage-timeline")
    @click.argument("stage_id1", type=int)
    @click.argument("stage_id2", type=int)
    @click.option(
        "--apps",
        nargs=2,
        metavar="APP1 APP2",
        help="Override apps from context (app1 app2)",
    )
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--interval-minutes",
        type=int,
        default=1,
        help="Time interval for analysis in minutes",
    )
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def stage_timeline(
        ctx,
        stage_id1: int,
        stage_id2: int,
        apps: Optional[Tuple[str, str]],
        server: Optional[str],
        interval_minutes: int,
        format: str,
    ):
        """Compare executor timeline for specific stages."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools.tools import compare_stage_executor_timeline

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                comparison_data = compare_stage_executor_timeline(
                    app_id1=app_id1,
                    app_id2=app_id2,
                    stage_id1=stage_id1,
                    stage_id2=stage_id2,
                    server=final_server,
                    interval_minutes=interval_minutes,
                )
                formatter.output(
                    comparison_data,
                    f"Stage Timeline Comparison: {app_id1}:stage{stage_id1} vs {app_id2}:stage{stage_id2}",
                )
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error comparing stage timelines: {e}")

    @compare.command("resources")
    @click.option(
        "--apps",
        nargs=2,
        metavar="APP1 APP2",
        help="Override apps from context (app1 app2)",
    )
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def resources(
        ctx,
        apps: Optional[Tuple[str, str]],
        server: Optional[str],
        format: str,
    ):
        """Compare resource allocation between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools.tools import compare_app_resources

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                comparison_data = compare_app_resources(
                    app_id1=app_id1, app_id2=app_id2, server=final_server
                )
                formatter.output(
                    comparison_data,
                    f"Resource Comparison: {app_id1} vs {app_id2}",
                )
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error comparing resources: {e}")

    @compare.command("executors")
    @click.option(
        "--apps",
        nargs=2,
        metavar="APP1 APP2",
        help="Override apps from context (app1 app2)",
    )
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--significance-threshold",
        type=float,
        default=0.2,
        help="Minimum difference threshold to show metric",
    )
    @click.option(
        "--show-only-significant/--show-all",
        default=True,
        help="Filter out metrics below significance threshold",
    )
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def executors(
        ctx,
        apps: Optional[Tuple[str, str]],
        server: Optional[str],
        significance_threshold: float,
        show_only_significant: bool,
        format: str,
    ):
        """Compare executor performance between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools.tools import compare_app_executors

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                comparison_data = compare_app_executors(
                    app_id1=app_id1,
                    app_id2=app_id2,
                    server=final_server,
                    significance_threshold=significance_threshold,
                    show_only_significant=show_only_significant,
                )
                formatter.output(
                    comparison_data,
                    f"Executor Comparison: {app_id1} vs {app_id2}",
                )
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error comparing executors: {e}")

    @compare.command("jobs")
    @click.option(
        "--apps",
        nargs=2,
        metavar="APP1 APP2",
        help="Override apps from context (app1 app2)",
    )
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def jobs(
        ctx,
        apps: Optional[Tuple[str, str]],
        server: Optional[str],
        format: str,
    ):
        """Compare job performance between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools.tools import compare_app_jobs

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                comparison_data = compare_app_jobs(
                    app_id1=app_id1, app_id2=app_id2, server=final_server
                )
                formatter.output(
                    comparison_data,
                    f"Job Comparison: {app_id1} vs {app_id2}",
                )
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error comparing jobs: {e}")

    @compare.command("stages-aggregated")
    @click.option(
        "--apps",
        nargs=2,
        metavar="APP1 APP2",
        help="Override apps from context (app1 app2)",
    )
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--significance-threshold",
        type=float,
        default=0.2,
        help="Minimum difference threshold to show metric",
    )
    @click.option(
        "--show-only-significant/--show-all",
        default=True,
        help="Filter out metrics below significance threshold",
    )
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def stages_aggregated(
        ctx,
        apps: Optional[Tuple[str, str]],
        server: Optional[str],
        significance_threshold: float,
        show_only_significant: bool,
        format: str,
    ):
        """Compare aggregated stage metrics between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools.tools import compare_app_stages_aggregated

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                comparison_data = compare_app_stages_aggregated(
                    app_id1=app_id1,
                    app_id2=app_id2,
                    server=final_server,
                    significance_threshold=significance_threshold,
                    show_only_significant=show_only_significant,
                )
                formatter.output(
                    comparison_data,
                    f"Stages Aggregated Comparison: {app_id1} vs {app_id2}",
                )
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error comparing aggregated stages: {e}")

    @compare.command("status")
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def status(ctx, format: str):
        """Show current comparison context."""
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        context = load_comparison_context()
        if context is None:
            if format == "json":
                formatter.output({"status": "no_context"})
            else:
                click.echo("No comparison context set.")
                click.echo("Run 'compare apps <app1> <app2>' to set context.")
        else:
            app_id1, app_id2, server = context
            context_data = {
                "app_id1": app_id1,
                "app_id2": app_id2,
                "server": server,
                "status": "active",
            }
            if format == "json":
                formatter.output(context_data)
            else:
                formatter.output(
                    context_data, f"Comparison Context: {app_id1} vs {app_id2}"
                )

    @compare.command("clear")
    @click.pass_context
    def clear(ctx):
        """Clear current comparison context."""
        context = load_comparison_context()
        if context is None:
            click.echo("No comparison context to clear.")
        else:
            app_id1, app_id2, _ = context
            clear_comparison_context()
            if not ctx.obj.get("quiet", False):
                click.echo(f"✓ Cleared comparison context: {app_id1} vs {app_id2}")

    # Add alias for backward compatibility
    @compare.command("performance", hidden=True)
    @click.argument("app_id1")
    @click.argument("app_id2")
    @click.option("--server", "-s", help="Server name to use")
    @click.option("--top-n", "-n", type=int, default=3)
    @click.option(
        "--format", "-f", type=click.Choice(["human", "json", "table"]), default="human"
    )
    @click.pass_context
    def performance(
        ctx, app_id1: str, app_id2: str, server: Optional[str], top_n: int, format: str
    ):
        """Alias for 'compare apps' command."""
        ctx.invoke(
            apps,
            app_id1=app_id1,
            app_id2=app_id2,
            server=server,
            top_n=top_n,
            format=format,
        )

else:
    # Fallback when CLI dependencies not available
    def compare():
        print(
            "CLI dependencies not installed. Install with: uv add click rich tabulate"
        )
        return None
