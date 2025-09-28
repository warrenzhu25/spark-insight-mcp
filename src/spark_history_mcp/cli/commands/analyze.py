"""
Analysis-related CLI commands.

Commands for performing Spark performance analysis and insights.
"""

from typing import Optional

try:
    import click

    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False


if CLI_AVAILABLE:
    from spark_history_mcp.cli.commands.apps import get_spark_client
    from spark_history_mcp.cli.formatters import OutputFormatter


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

    @click.group(name="analyze")
    def analyze():
        """Commands for analyzing Spark applications."""
        pass

    @analyze.command("insights")
    @click.argument("app_id")
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.option(
        "--include-auto-scaling/--no-auto-scaling",
        default=True,
        help="Include auto-scaling analysis",
    )
    @click.option(
        "--include-shuffle-skew/--no-shuffle-skew",
        default=True,
        help="Include shuffle skew analysis",
    )
    @click.option(
        "--include-failed-tasks/--no-failed-tasks",
        default=True,
        help="Include failed task analysis",
    )
    @click.option(
        "--include-executor-utilization/--no-executor-utilization",
        default=True,
        help="Include executor utilization analysis",
    )
    @click.pass_context
    def insights(
        ctx,
        app_id: str,
        server: Optional[str],
        format: str,
        include_auto_scaling: bool,
        include_shuffle_skew: bool,
        include_failed_tasks: bool,
        include_executor_utilization: bool,
    ):
        """Get comprehensive application insights."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import get_application_insights

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                insights_data = get_application_insights(
                    app_id=app_id,
                    server=server,
                    include_auto_scaling=include_auto_scaling,
                    include_shuffle_skew=include_shuffle_skew,
                    include_failed_tasks=include_failed_tasks,
                    include_executor_utilization=include_executor_utilization,
                )
                formatter.output(insights_data, f"Application Insights for {app_id}")
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error analyzing application {app_id}: {e}") from e

    @analyze.command("bottlenecks")
    @click.argument("app_id")
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--top-n", "-n", type=int, default=5, help="Number of top bottlenecks to show"
    )
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def bottlenecks(ctx, app_id: str, server: Optional[str], top_n: int, format: str):
        """Identify performance bottlenecks in the application."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import get_job_bottlenecks

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                bottlenecks_data = get_job_bottlenecks(
                    app_id=app_id, server=server, top_n=top_n
                )
                formatter.output(
                    bottlenecks_data, f"Performance Bottlenecks for {app_id}"
                )
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(f"Error analyzing bottlenecks for {app_id}: {e}") from e

    @analyze.command("auto-scaling")
    @click.argument("app_id")
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--target-duration",
        type=int,
        default=2,
        help="Target stage duration in minutes",
    )
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def auto_scaling(
        ctx, app_id: str, server: Optional[str], target_duration: int, format: str
    ):
        """Analyze auto-scaling recommendations."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import analyze_auto_scaling

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                scaling_data = analyze_auto_scaling(
                    app_id=app_id,
                    server=server,
                    target_stage_duration_minutes=target_duration,
                )
                formatter.output(scaling_data, f"Auto-Scaling Analysis for {app_id}")
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(
                f"Error analyzing auto-scaling for {app_id}: {e}"
            ) from e

    @analyze.command("shuffle-skew")
    @click.argument("app_id")
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--shuffle-threshold",
        type=int,
        default=10,
        help="Minimum total shuffle write in GB to analyze",
    )
    @click.option(
        "--skew-ratio",
        type=float,
        default=2.0,
        help="Minimum ratio of max/median to flag as skewed",
    )
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def shuffle_skew(
        ctx,
        app_id: str,
        server: Optional[str],
        shuffle_threshold: int,
        skew_ratio: float,
        format: str,
    ):
        """Analyze shuffle data skew issues."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import analyze_shuffle_skew

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                skew_data = analyze_shuffle_skew(
                    app_id=app_id,
                    server=server,
                    shuffle_threshold_gb=shuffle_threshold,
                    skew_ratio_threshold=skew_ratio,
                )
                formatter.output(skew_data, f"Shuffle Skew Analysis for {app_id}")
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(
                f"Error analyzing shuffle skew for {app_id}: {e}"
            ) from e

    @analyze.command("slowest")
    @click.argument("app_id")
    @click.option("--server", "-s", help="Server name to use")
    @click.option(
        "--type",
        "analysis_type",
        type=click.Choice(["jobs", "stages", "sql"]),
        default="stages",
        help="Type of analysis",
    )
    @click.option(
        "--top-n", "-n", type=int, default=5, help="Number of slowest items to show"
    )
    @click.option(
        "--format",
        "-f",
        type=click.Choice(["human", "json", "table"]),
        default="human",
        help="Output format",
    )
    @click.pass_context
    def slowest(
        ctx,
        app_id: str,
        server: Optional[str],
        analysis_type: str,
        top_n: int,
        format: str,
    ):
        """Find slowest jobs, stages, or SQL queries."""
        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            if analysis_type == "jobs":
                from spark_history_mcp.tools import (
                    list_slowest_jobs as analysis_func,
                )

                title = f"Slowest Jobs for {app_id}"
            elif analysis_type == "stages":
                from spark_history_mcp.tools import (
                    list_slowest_stages as analysis_func,
                )

                title = f"Slowest Stages for {app_id}"
            elif analysis_type == "sql":
                from spark_history_mcp.tools import (
                    list_slowest_sql_queries as analysis_func,
                )

                title = f"Slowest SQL Queries for {app_id}"

            import spark_history_mcp.tools.tools as tools_module

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                if analysis_type in ["jobs", "stages"]:
                    slowest_data = analysis_func(app_id=app_id, server=server, n=top_n)
                else:  # sql
                    slowest_data = analysis_func(
                        app_id=app_id, server=server, top_n=top_n
                    )
                formatter.output(slowest_data, title)
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(
                f"Error analyzing slowest {analysis_type} for {app_id}: {e}"
            ) from e

    @analyze.command("compare", deprecated=True)
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
    def compare(
        ctx, app_id1: str, app_id2: str, server: Optional[str], top_n: int, format: str
    ):
        """
        Compare performance between two applications.

        ⚠️  DEPRECATED: Use 'compare apps' instead.
        This command will be removed in a future version.
        """
        # Show deprecation warning
        click.echo(
            "⚠️  WARNING: 'analyze compare' is deprecated. Use 'compare apps' instead.",
            err=True,
        )
        click.echo(
            f"   New command: spark-mcp --cli compare apps {app_id1} {app_id2}",
            err=True,
        )
        click.echo()

        config_path = ctx.obj["config_path"]
        formatter = OutputFormatter(format, ctx.obj.get("quiet", False))

        try:
            client = get_spark_client(config_path, server)

            import spark_history_mcp.tools.tools as tools_module
            from spark_history_mcp.tools import compare_app_performance

            original_get_context = getattr(tools_module.mcp, "get_context", None)
            tools_module.mcp.get_context = lambda: create_mock_context(client)

            try:
                comparison_data = compare_app_performance(
                    app_id1=app_id1, app_id2=app_id2, server=server, top_n=top_n
                )
                formatter.output(
                    comparison_data, f"Performance Comparison: {app_id1} vs {app_id2}"
                )
            finally:
                if original_get_context:
                    tools_module.mcp.get_context = original_get_context

        except Exception as e:
            raise click.ClickException(
                f"Error comparing applications {app_id1} and {app_id2}: {e}"
            ) from e

else:
    # Fallback when CLI dependencies not available
    def analyze():
        import sys
        sys.stderr.write(
            "CLI dependencies not installed. Install with: uv add click rich tabulate\n"
        )
        return None
