"""
Comparison-related CLI commands.

Commands for comparing multiple Spark applications with stateful context management.
"""

import json
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

from spark_history_mcp.cli._compat import (
    CLI_AVAILABLE,
    cli_unavailable_stub,
    click,
    create_tool_context,
    patch_tool_context,
)

if CLI_AVAILABLE:
    from spark_history_mcp.cli.commands.apps import get_spark_client
    from spark_history_mcp.cli.formatters import OutputFormatter
    from spark_history_mcp.cli.session import is_number_ref, resolve_number_ref


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
            "Run 'apps compare <app1> <app2>' first to set context, "
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
    from spark_history_mcp.tools import list_applications

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
            compact=False,
        )

        if len(apps) < 2:
            if len(apps) == 0:
                raise click.ClickException(
                    f"No applications found matching '{app_name}'.\n\n"
                    f"Tips:\n"
                    f"  • Try a partial name: 'ETL' instead of 'ETL Pipeline Job'\n"
                    f"  • Check spelling and capitalization\n"
                    f"  • Use exact app IDs if known: apps compare app1 app2\n"
                    f"  • List available apps: apps list --name '{app_name}'"
                )
            else:
                raise click.ClickException(
                    f"Only found 1 application matching '{app_name}'. "
                    f"Need at least 2 applications to compare.\n\n"
                    f"Found: {apps[0].id} - {apps[0].name}\n\n"
                    f"Tips:\n"
                    f"  • Try a broader search term\n"
                    f"  • Use specific app IDs: apps compare {apps[0].id} <other-app-id>"
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


def resolve_single_number_ref(identifier: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve a number reference to an app ID.

    Args:
        identifier: The identifier to check and resolve

    Returns:
        Tuple of (resolved_app_id, feedback_message) if it's a number ref,
        (None, None) if it's not a number ref
    """
    if is_number_ref(identifier):
        app_id = resolve_number_ref(int(identifier))
        if app_id:
            return app_id, f"Resolved #{identifier} to: {app_id}"
        raise click.ClickException(
            f"#{identifier} not found. Run 'apps list' first to set up references."
        )
    return None, None


def resolve_app_identifiers(
    identifier1: str, identifier2: Optional[str], client, server: Optional[str] = None
) -> Tuple[str, str, Optional[str]]:
    """
    Resolve app identifiers to app IDs and return user feedback message.

    Returns:
        Tuple of (app_id1, app_id2, feedback_message)
    """
    # First, try to resolve number refs for both identifiers
    resolved1, feedback1 = resolve_single_number_ref(identifier1)
    if identifier2:
        resolved2, feedback2 = resolve_single_number_ref(identifier2)
    else:
        resolved2, feedback2 = None, None

    # If both are resolved as number refs, return them
    if resolved1 and resolved2:
        feedback_parts = []
        if feedback1:
            feedback_parts.append(feedback1)
        if feedback2:
            feedback_parts.append(feedback2)
        return (
            resolved1,
            resolved2,
            "\n".join(feedback_parts) if feedback_parts else None,
        )

    # If first is a single number ref with no second identifier - error
    if resolved1 and not identifier2:
        raise click.ClickException(
            "When using number references, provide two numbers. "
            "Example: apps compare 1 2"
        )

    if identifier2 is None:
        # Single identifier - treat as name and find 2 recent matching apps
        if is_app_id(identifier1):
            raise click.ClickException(
                f"When providing a single argument, it should be an application name, "
                f"not an app ID. Provided: '{identifier1}'\n\n"
                f"Usage:\n"
                f"  apps compare 'App Name'    # Auto-compare 2 recent matching apps\n"
                f"  apps compare app1 app2     # Compare specific app IDs\n\n"
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
        # Use already-resolved number refs or original identifiers
        resolved_id1 = resolved1 if resolved1 else identifier1
        resolved_id2 = resolved2 if resolved2 else identifier2
        feedback_parts = []

        # Add feedback for number refs that were resolved above
        if feedback1:
            feedback_parts.append(feedback1)
        if feedback2:
            feedback_parts.append(feedback2)

        # Resolve first identifier if it looks like a name (and wasn't a number ref)
        if not resolved1 and not is_app_id(identifier1):
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

        # Resolve second identifier if it looks like a name (and wasn't a number ref)
        if not resolved2 and not is_app_id(identifier2):
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
    """Backward-compatible wrapper for tests relying on the old helper."""

    return create_tool_context(client)


def _is_interactive() -> bool:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return True
    return sys.stdin.isatty() and sys.stdout.isatty()


def _top_metric_differences(metrics: dict, limit: int = 5) -> list[dict[str, object]]:
    """Return the top metric diffs by absolute percent change."""
    diffs: list[dict[str, object]] = []
    for key, value in metrics.items():
        if isinstance(value, tuple) and len(value) == 3:
            left_val, right_val, percent_change = value
            if not isinstance(percent_change, (int, float)):
                continue
            if not isinstance(left_val, (int, float)) or not isinstance(
                right_val, (int, float)
            ):
                continue
            if isinstance(left_val, bool) or isinstance(right_val, bool):
                continue
            diffs.append(
                {
                    "metric": key,
                    "left": left_val,
                    "right": right_val,
                    "percent_change": percent_change,
                }
            )

    non_zero = [d for d in diffs if d["percent_change"] != 0]
    zeros = [d for d in diffs if d["percent_change"] == 0]

    non_zero.sort(key=lambda d: abs(float(d["percent_change"])), reverse=True)
    zeros.sort(key=lambda d: str(d["metric"]))

    ordered = non_zero + zeros
    return ordered[:limit]


def _create_comparison_metrics(
    left_metrics: dict, right_metrics: dict
) -> dict[str, tuple[object, object, float]]:
    comparison: dict[str, tuple[object, object, float]] = {}
    all_keys = set(left_metrics.keys()) | set(right_metrics.keys())
    for key in all_keys:
        left_value = left_metrics.get(key)
        right_value = right_metrics.get(key)
        percentage_change = 0.0
        try:
            if left_value is None or right_value is None:
                percentage_change = 0.0
            elif isinstance(left_value, str) or isinstance(right_value, str):
                percentage_change = 0.0
            else:
                left_num = float(left_value)
                right_num = float(right_value)
                if left_num == 0:
                    percentage_change = 100.0 if right_num != 0 else 0.0
                else:
                    percentage_change = ((right_num - left_num) / left_num) * 100.0
        except (ValueError, TypeError):
            percentage_change = 0.0
        comparison[key] = (left_value, right_value, percentage_change)
    return comparison


def execute_app_comparison(
    app_id1: str,
    app_id2: str,
    server: Optional[str],
    formatter,
    ctx,
    top_n: int = 3,
    significance_threshold: float = 0.1,
) -> None:
    """Execute application comparison and render formatted output."""
    config_path = ctx.obj["config_path"]
    client = get_spark_client(config_path, server)

    import spark_history_mcp.tools.tools as tools_module
    from spark_history_mcp.tools import (
        compare_app_executor_timeline,
        compare_app_performance,
    )

    original_get_context = getattr(tools_module.mcp, "get_context", None)
    tools_module.mcp.get_context = lambda: create_mock_context(client)

    try:
        comparison_data = compare_app_performance(
            app_id1=app_id1,
            app_id2=app_id2,
            server=server,
            top_n=top_n,
            significance_threshold=significance_threshold,
        )

        stage_overview = comparison_data.get("aggregated_overview", {}).get(
            "stage_comparison", {}
        )
        stage_apps = stage_overview.get("applications", {})
        stage_metrics1 = stage_apps.get("app1", {}).get("stage_metrics", {})
        stage_metrics2 = stage_apps.get("app2", {}).get("stage_metrics", {})

        if stage_metrics1 and stage_metrics2:
            stage_metrics_comparison = _create_comparison_metrics(
                stage_metrics1, stage_metrics2
            )
            comparison_data["top_metrics_differences"] = _top_metric_differences(
                stage_metrics_comparison, limit=5
            )
        else:
            comparison_data["top_metrics_differences"] = []

        comparison_data["executor_timeline_comparison"] = compare_app_executor_timeline(
            app_id1=app_id1, app_id2=app_id2, server=server
        )

        formatter.output(comparison_data)
        return comparison_data
    finally:
        if original_get_context:
            tools_module.mcp.get_context = original_get_context


def extract_stage_menu_options(comparison_data):
    """Extract stage differences for interactive menu."""
    # Handle both old and new structure
    stage_dive = comparison_data.get("stage_deep_dive")
    if not stage_dive and "performance_comparison" in comparison_data:
        stage_dive = comparison_data["performance_comparison"].get("stages", {})

    differences = stage_dive.get("top_stage_differences", []) if stage_dive else []

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
    if not _is_interactive():
        click.echo("Interactive menu requires a TTY. Skipping prompt.")
        return
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
            "\\[e] Compare Environment Configurations",
            "\\[s] Compare Application Summaries (includes stages)",
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
            execute_timeline_comparison(app_id1, app_id2, server, formatter, ctx)
        elif choice == "e":
            execute_env_comparison(app_id1, app_id2, server, formatter, ctx)
        elif choice == "s":
            execute_summary_comparison(app_id1, app_id2, server, formatter, ctx)
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
        from spark_history_mcp.tools import compare_stages

        client = get_spark_client(ctx.obj["config_path"], server)
        with patch_tool_context(client, tools_module):
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

            # Show post-stage menu only if interactive explicitly enabled
            if (
                formatter.format_type == "human"
                and load_comparison_context()
                and _is_interactive()
            ):
                show_post_stage_menu(
                    app_id1, app_id2, stage_id1, stage_id2, server, formatter, ctx
                )

    except Exception as e:
        click.echo(f"Error executing stage comparison: {e}")


def execute_timeline_comparison(app_id1, app_id2, server, formatter, ctx):
    """Execute timeline comparison command."""
    try:
        click.echo("Analyzing application timeline...")

        # Import and execute timeline comparison
        import spark_history_mcp.tools.tools as tools_module
        from spark_history_mcp.tools import compare_app_executor_timeline

        client = get_spark_client(ctx.obj["config_path"], server)
        with patch_tool_context(client, tools_module):
            comparison_data = compare_app_executor_timeline(
                app_id1=app_id1, app_id2=app_id2, server=server, interval_minutes=1
            )
            formatter.output(
                comparison_data, f"Timeline Comparison: {app_id1} vs {app_id2}"
            )
            if formatter.format_type == "human" and _is_interactive():
                show_post_timeline_menu(app_id1, app_id2, server, formatter, ctx)

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
        from spark_history_mcp.tools import compare_stage_executor_timeline

        client = get_spark_client(ctx.obj["config_path"], server)
        with patch_tool_context(client, tools_module):
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
            if formatter.format_type == "human" and _is_interactive():
                show_generic_follow_up_menu(app_id1, app_id2, server, formatter, ctx)

    except Exception as e:
        click.echo(f"Error executing stage timeline comparison: {e}")


def execute_env_comparison(app_id1, app_id2, server, formatter, ctx):
    """Execute environment comparison command."""
    try:
        click.echo("Comparing environment configurations...")

        import spark_history_mcp.tools.tools as tools_module
        from spark_history_mcp.tools import compare_app_environments

        client = get_spark_client(ctx.obj["config_path"], server)
        with patch_tool_context(client, tools_module):
            data = compare_app_environments(
                app_id1=app_id1, app_id2=app_id2, server=server
            )
            formatter.output(data, f"Environment Comparison: {app_id1} vs {app_id2}")
            if formatter.format_type == "human" and _is_interactive():
                show_generic_follow_up_menu(app_id1, app_id2, server, formatter, ctx)

    except Exception as e:
        click.echo(f"Error comparing environments: {e}")


def execute_summary_comparison(app_id1, app_id2, server, formatter, ctx):
    """Execute application summary comparison command."""
    try:
        click.echo("Comparing application summaries...")

        import spark_history_mcp.tools.tools as tools_module
        from spark_history_mcp.tools import compare_app_summaries

        client = get_spark_client(ctx.obj["config_path"], server)
        with patch_tool_context(client, tools_module):
            data = compare_app_summaries(
                app_id1=app_id1, app_id2=app_id2, server=server
            )
            formatter.output(data, f"Summary Comparison: {app_id1} vs {app_id2}")
            if formatter.format_type == "human" and _is_interactive():
                show_generic_follow_up_menu(app_id1, app_id2, server, formatter, ctx)

    except Exception as e:
        click.echo(f"Error comparing summaries: {e}")


def show_post_stage_menu(
    app_id1, app_id2, stage_id1, stage_id2, server, formatter, ctx
):
    """Show follow-up options after stage comparison completion."""
    if not _is_interactive():
        click.echo("Interactive menu requires a TTY. Skipping prompt.")
        return
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


def show_generic_follow_up_menu(app_id1, app_id2, server, formatter, ctx):
    """Show generic follow-up options after any comparison command."""
    if not _is_interactive():
        click.echo("Interactive menu requires a TTY. Skipping prompt.")
        return

    try:
        from rich.console import Console
        from rich.panel import Panel

        console = Console()
    except ImportError:
        console = None

    menu_lines = ["Choose your next analysis:", ""]
    menu_lines.extend(
        [
            "\\[a] Compare Applications (full)",
            "\\[s] Compare Summaries (includes stages)",
            "\\[t] Compare Timeline",
            "\\[e] Compare Environment",
            "\\[q] Quit",
        ]
    )

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

    try:
        try:
            choice = click.getchar().lower()
            click.echo()
        except OSError:
            choice = click.prompt("Enter choice", type=str, default="q").lower()

        if choice == "q":
            return
        elif choice == "a":
            execute_app_comparison(app_id1, app_id2, server, formatter, ctx)
        elif choice == "s":
            execute_summary_comparison(app_id1, app_id2, server, formatter, ctx)
        elif choice == "t":
            execute_timeline_comparison(app_id1, app_id2, server, formatter, ctx)
        elif choice == "e":
            execute_env_comparison(app_id1, app_id2, server, formatter, ctx)
        else:
            click.echo(f"Invalid choice: {choice}")
    except (KeyboardInterrupt, EOFError):
        click.echo("\nExiting interactive mode.")


def show_post_timeline_menu(app_id1, app_id2, server, formatter, ctx):
    """Show follow-up options after timeline comparison completion."""
    if not _is_interactive():
        click.echo("Interactive menu requires a TTY. Skipping prompt.")
        return

    try:
        from rich.console import Console
        from rich.panel import Panel

        console = Console()
    except ImportError:
        console = None

    menu_lines = ["Choose your next analysis:", ""]
    menu_lines.extend(
        [
            "\\[a] Compare Applications",
            "\\[q] Continue",
        ]
    )

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

    try:
        try:
            choice = click.getchar().lower()
            click.echo()
        except OSError:
            choice = click.prompt("Enter choice", type=str, default="q").lower()

        if choice == "q":
            return
        if choice == "a":
            execute_app_comparison(app_id1, app_id2, server, formatter, ctx)
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
        "output_format",
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
    @click.option(
        "--all",
        "-a",
        "show_all",
        is_flag=True,
        help="Show all metrics instead of top 3",
    )
    @click.option(
        "--threshold",
        "-t",
        type=float,
        default=0.1,
        help="Significance threshold (0.1 = 10%) - hide metrics below this difference",
    )
    @click.pass_context
    def apps(
        ctx,
        app_identifier1: str,
        app_identifier2: Optional[str],
        server: Optional[str],
        top_n: int,
        output_format: str,
        interactive: bool,
        show_all: bool,
        threshold: float,
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
            apps compare app-123 app-456                    # Compare by IDs
            apps compare "ETL Pipeline"                     # Auto-compare last 2 matching
            apps compare ETLPipeline                        # Single word, no quotes needed
            apps compare "Daily Job" "Weekly Job"           # Compare by names with spaces
            apps compare MyJob "Production ETL"             # Mixed: single word + quoted
        """
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(
            output_format, ctx.obj.get("quiet", False), show_all_metrics=show_all
        )

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

            comparison_data = execute_app_comparison(
                app_id1,
                app_id2,
                server,
                formatter,
                ctx,
                top_n=top_n,
                significance_threshold=threshold,
            )

            if not ctx.obj.get("quiet", False):
                click.echo(f"\n✓ Comparison context saved: {app_id1} vs {app_id2}")
                if interactive and output_format == "human":
                    # Show interactive menu for further navigation
                    show_interactive_menu(
                        comparison_data, app_id1, app_id2, server, formatter, ctx
                    )
                else:
                    if output_format == "human":
                        if not _is_interactive():
                            click.echo(
                                "Interactive menu requires a TTY. Skipping prompt."
                            )
                        else:
                            try:
                                from rich.console import Console
                                from rich.panel import Panel

                                console = Console()
                            except ImportError:
                                console = None

                            menu_lines = ["Choose your next analysis:", ""]
                            menu_lines.extend(
                                [
                                    "\\[1] Compare stages 1 vs 1",
                                    "\\[2] Compare application timeline",
                                    "\\[q] Continue",
                                ]
                            )

                            if console:
                                content = "\n".join(menu_lines)
                                console.print(
                                    Panel(
                                        content,
                                        title="What's Next?",
                                        border_style="green",
                                    )
                                )
                            else:
                                click.echo("\n" + "=" * 50)
                                click.echo("What's Next?")
                                click.echo("=" * 50)
                                for line in menu_lines:
                                    click.echo(line)
                                click.echo("=" * 50)

                            try:
                                try:
                                    choice = click.getchar().lower()
                                    click.echo()
                                except OSError:
                                    choice = (
                                        click.prompt(
                                            "Enter choice",
                                            type=str,
                                            default="q",
                                        )
                                        .lower()
                                        .strip()
                                    )

                                if choice == "q":
                                    pass
                                elif choice == "1":
                                    execute_stage_comparison(
                                        1, 1, server, formatter, ctx
                                    )
                                elif choice == "2":
                                    execute_timeline_comparison(
                                        app_id1, app_id2, server, formatter, ctx
                                    )
                                else:
                                    click.echo(f"Invalid choice: {choice}")
                            except (KeyboardInterrupt, EOFError):
                                click.echo("\nExiting interactive mode.")
                    else:
                        click.echo(
                            "Use 'compare stages' or 'compare timeline' for detailed analysis"
                        )

        except Exception as e:
            raise click.ClickException(f"Error comparing applications: {e}") from e

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
        default=0.1,
        help="Minimum difference threshold to include metric",
    )
    @click.option(
        "--format",
        "-f",
        "output_format",
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
        output_format: str,
    ):
        """Compare specific stages between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import compare_stages

            with patch_tool_context(client, tools_module):
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
                if (
                    output_format == "human"
                    and _is_interactive()
                    and load_comparison_context()
                ):
                    show_post_stage_menu(
                        app_id1,
                        app_id2,
                        stage_id1,
                        stage_id2,
                        final_server,
                        formatter,
                        ctx,
                    )

        except Exception as e:
            raise click.ClickException(f"Error comparing stages: {e}") from e

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
        "output_format",
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
        output_format: str,
    ):
        """Compare executor timeline patterns between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import compare_app_executor_timeline

            with patch_tool_context(client, tools_module):
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
                if output_format == "human" and _is_interactive():
                    show_post_timeline_menu(
                        app_id1, app_id2, final_server, formatter, ctx
                    )

        except Exception as e:
            raise click.ClickException(f"Error comparing timelines: {e}") from e

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
        "output_format",
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
        output_format: str,
    ):
        """Compare executor timeline for specific stages."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import compare_stage_executor_timeline

            with patch_tool_context(client, tools_module):
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
                if output_format == "human" and _is_interactive():
                    show_generic_follow_up_menu(
                        app_id1, app_id2, final_server, formatter, ctx
                    )

        except Exception as e:
            raise click.ClickException(f"Error comparing stage timelines: {e}") from e

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
        "output_format",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def resources(
        ctx,
        apps: Optional[Tuple[str, str]],
        server: Optional[str],
        output_format: str,
    ):
        """Compare resource allocation between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import compare_app_resources

            with patch_tool_context(client, tools_module):
                comparison_data = compare_app_resources(
                    app_id1=app_id1, app_id2=app_id2, server=final_server
                )
                formatter.output(
                    comparison_data,
                    f"Resource Comparison: {app_id1} vs {app_id2}",
                )
                if output_format == "human" and _is_interactive():
                    show_generic_follow_up_menu(
                        app_id1, app_id2, final_server, formatter, ctx
                    )

        except Exception as e:
            raise click.ClickException(f"Error comparing resources: {e}") from e

    @compare.command("env")
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
        "output_format",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def env(
        ctx,
        apps: Optional[Tuple[str, str]],
        server: Optional[str],
        output_format: str,
    ):
        """Compare environment configurations between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import compare_app_environments

            with patch_tool_context(client, tools_module):
                comparison_data = compare_app_environments(
                    app_id1=app_id1, app_id2=app_id2, server=final_server
                )
                formatter.output(
                    comparison_data,
                    f"Environment Comparison: {app_id1} vs {app_id2}",
                )
                if output_format == "human" and _is_interactive():
                    show_generic_follow_up_menu(
                        app_id1, app_id2, final_server, formatter, ctx
                    )

        except Exception as e:
            raise click.ClickException(f"Error comparing environments: {e}") from e

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
        default=0.1,
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
        "output_format",
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
        output_format: str,
    ):
        """Compare executor performance between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import compare_app_executors

            with patch_tool_context(client, tools_module):
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
                if output_format == "human" and _is_interactive():
                    show_generic_follow_up_menu(
                        app_id1, app_id2, final_server, formatter, ctx
                    )

        except Exception as e:
            raise click.ClickException(f"Error comparing executors: {e}") from e

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
        "output_format",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def jobs(
        ctx,
        apps: Optional[Tuple[str, str]],
        server: Optional[str],
        output_format: str,
    ):
        """Compare job performance between applications."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        try:
            # Get app context
            if apps:
                app_id1, app_id2 = apps
                final_server = server
            else:
                app_id1, app_id2, final_server = get_app_context(server=server)

            client = get_spark_client(config_path, final_server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import compare_app_jobs

            with patch_tool_context(client, tools_module):
                comparison_data = compare_app_jobs(
                    app_id1=app_id1, app_id2=app_id2, server=final_server
                )
                formatter.output(
                    comparison_data,
                    f"Job Comparison: {app_id1} vs {app_id2}",
                )
                if output_format == "human" and _is_interactive():
                    show_generic_follow_up_menu(
                        app_id1, app_id2, final_server, formatter, ctx
                    )

        except Exception as e:
            raise click.ClickException(f"Error comparing jobs: {e}") from e

    @compare.command("status")
    @click.option(
        "--format",
        "-f",
        "output_format",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def status(ctx, output_format: str):
        """Show current comparison context."""
        formatter = OutputFormatter(output_format, ctx.obj.get("quiet", False))

        context = load_comparison_context()
        if context is None:
            if output_format == "json":
                formatter.output({"status": "no_context"})
            else:
                click.echo("No comparison context set.")
                click.echo("Run 'apps compare <app1> <app2>' to set context.")
        else:
            app_id1, app_id2, server = context
            context_data = {
                "app_id1": app_id1,
                "app_id2": app_id2,
                "server": server,
                "status": "active",
            }
            if output_format == "json":
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
        "--format",
        "-f",
        "output_format",
        type=click.Choice(["human", "json", "table"]),
        default="human",
    )
    @click.pass_context
    def performance(
        ctx,
        app_id1: str,
        app_id2: str,
        server: Optional[str],
        top_n: int,
        output_format: str,
    ):
        """Alias for 'apps compare' command."""
        ctx.invoke(
            apps,
            app_id1=app_id1,
            app_id2=app_id2,
            server=server,
            top_n=top_n,
            output_format=output_format,
        )

else:
    compare = cli_unavailable_stub("compare")
