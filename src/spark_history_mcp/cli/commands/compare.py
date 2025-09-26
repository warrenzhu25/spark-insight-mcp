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


if CLI_AVAILABLE:

    @click.group(name="compare")
    def compare():
        """Commands for comparing Spark applications."""
        pass

    @compare.command("apps")
    @click.argument("app_id1")
    @click.argument("app_id2")
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
    @click.pass_context
    def apps(
        ctx,
        app_id1: str,
        app_id2: str,
        server: Optional[str],
        top_n: int,
        format: str,
    ):
        """Compare performance between two applications and set comparison context."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        # Save comparison context
        save_comparison_context(app_id1, app_id2, server)

        try:
            client = get_spark_client(config_path, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools.tools import compare_app_performance

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                comparison_data = compare_app_performance(
                    app_id1=app_id1, app_id2=app_id2, server=server, top_n=top_n
                )
                formatter.output(
                    comparison_data,
                    f"Performance Comparison: {app_id1} vs {app_id2}",
                )

                if not ctx.obj.get("quiet", False):
                    click.echo(f"\n✓ Comparison context saved: {app_id1} vs {app_id2}")
                    click.echo(
                        "Use 'compare stages', 'compare timeline', etc. for detailed analysis"
                    )

            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(
                f"Error comparing applications {app_id1} and {app_id2}: {e}"
            )

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
