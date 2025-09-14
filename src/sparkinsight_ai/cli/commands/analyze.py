"""
Analysis CLI commands.

Commands for performing intelligent analysis of Spark applications.
"""

from pathlib import Path
from typing import Optional

import click

from sparkinsight_ai.cli.commands.apps import get_spark_client
from sparkinsight_ai.cli.formatters import OutputFormatter, create_progress

# Import tools conditionally to handle missing MCP dependencies gracefully
try:
    from sparkinsight_ai.tools.tools import (
        analyze_auto_scaling,
        analyze_executor_utilization,
        analyze_failed_tasks,
        analyze_shuffle_skew,
        get_application_insights,
        get_job_bottlenecks,
        list_slowest_jobs,
        list_slowest_stages,
        list_slowest_sql_queries,
    )
    ANALYSIS_AVAILABLE = True
except ImportError as e:
    ANALYSIS_AVAILABLE = False
    ANALYSIS_IMPORT_ERROR = str(e)


def check_analysis_availability():
    """Check if analysis tools are available and raise appropriate error if not."""
    if not ANALYSIS_AVAILABLE:
        raise click.ClickException(
            f"Analysis tools not available. This requires Python 3.10+ and MCP dependencies.\n"
            f"Error: {ANALYSIS_IMPORT_ERROR}\n\n"
            f"To install full dependencies: pip install sparkinsight-ai"
        )


@click.group(name="analyze")
def analyze_group():
    """Commands for analyzing Spark application performance."""
    pass


@analyze_group.command("insights")
@click.argument("app_id")
@click.option("--server", "-s", help="Server name to use")
@click.option("--no-auto-scaling", is_flag=True, help="Skip auto-scaling analysis")
@click.option("--no-shuffle-skew", is_flag=True, help="Skip shuffle skew analysis")
@click.option("--no-failed-tasks", is_flag=True, help="Skip failed tasks analysis")
@click.option("--no-executor-util", is_flag=True, help="Skip executor utilization analysis")
@click.option("--format", "-f", "format_type",
              type=click.Choice(["human", "json", "table"]),
              default="human", help="Output format")
@click.pass_context
def comprehensive_insights(ctx: click.Context, app_id: str, server: Optional[str],
                          no_auto_scaling: bool, no_shuffle_skew: bool,
                          no_failed_tasks: bool, no_executor_util: bool,
                          format_type: str):
    """
    Run comprehensive SparkInsight-style analysis on an application.

    This command performs multiple analyses including auto-scaling recommendations,
    shuffle skew detection, failed task analysis, and executor utilization insights.

    Examples:
        sparkinsight-ai analyze insights app-20231201-123456
        sparkinsight-ai analyze insights app-20231201-123456 --no-shuffle-skew
        sparkinsight-ai analyze insights app-20231201-123456 --format json
    """
    check_analysis_availability()
    formatter = OutputFormatter(format_type, ctx.obj.get("quiet", False))

    try:
        # We need to set up a mock context for the analysis tools
        # since they expect MCP context, but we're calling them directly
        class MockRequest:
            def __init__(self, client):
                self.lifespan_context = type('obj', (object,), {
                    'clients': {'default': client},
                    'default_client': client
                })

        class MockContext:
            def __init__(self, client):
                self.request_context = MockRequest(client)

        client = get_spark_client(ctx.obj["config_path"], server)
        mock_ctx = MockContext(client)

        # Patch the mcp context in the tools module temporarily
        import sparkinsight_ai.tools.tools as tools_module
        original_mcp = tools_module.mcp

        class MockMCP:
            @staticmethod
            def get_context():
                return mock_ctx

        tools_module.mcp = MockMCP()

        try:
            with create_progress() as progress:
                task = progress.add_task("Running comprehensive analysis...", total=None)

                insights = get_application_insights(
                    app_id=app_id,
                    server=None,  # We handle server selection above
                    include_auto_scaling=not no_auto_scaling,
                    include_shuffle_skew=not no_shuffle_skew,
                    include_failed_tasks=not no_failed_tasks,
                    include_executor_utilization=not no_executor_util
                )

                progress.remove_task(task)

            formatter.output(insights, f"Comprehensive Analysis - {app_id}")

        finally:
            # Restore original mcp
            tools_module.mcp = original_mcp

    except Exception as e:
        raise click.ClickException(f"Analysis failed: {e}")


@analyze_group.command("bottlenecks")
@click.argument("app_id")
@click.option("--server", "-s", help="Server name to use")
@click.option("--top-n", "-n", type=int, default=5, help="Number of top bottlenecks to show")
@click.option("--format", "-f", "format_type",
              type=click.Choice(["human", "json", "table"]),
              default="human", help="Output format")
@click.pass_context
def bottlenecks_analysis(ctx: click.Context, app_id: str, server: Optional[str],
                        top_n: int, format_type: str):
    """
    Identify performance bottlenecks in a Spark application.

    Analyzes stages, tasks, and executors to find the most time-consuming
    operations and resource-intensive components.

    Examples:
        sparkinsight-ai analyze bottlenecks app-20231201-123456
        sparkinsight-ai analyze bottlenecks app-20231201-123456 --top-n 10
    """
    formatter = OutputFormatter(format_type, ctx.obj.get("quiet", False))

    try:
        client = get_spark_client(ctx.obj["config_path"], server)

        # Set up mock context for tools
        class MockRequest:
            def __init__(self, client):
                self.lifespan_context = type('obj', (object,), {
                    'clients': {'default': client},
                    'default_client': client
                })

        class MockContext:
            def __init__(self, client):
                self.request_context = MockRequest(client)

        mock_ctx = MockContext(client)

        import sparkinsight_ai.tools.tools as tools_module
        original_mcp = tools_module.mcp

        class MockMCP:
            @staticmethod
            def get_context():
                return mock_ctx

        tools_module.mcp = MockMCP()

        try:
            with create_progress() as progress:
                task = progress.add_task("Analyzing bottlenecks...", total=None)
                bottlenecks = get_job_bottlenecks(app_id=app_id, server=None, top_n=top_n)
                progress.remove_task(task)

            formatter.output(bottlenecks, f"Performance Bottlenecks - {app_id}")

        finally:
            tools_module.mcp = original_mcp

    except Exception as e:
        raise click.ClickException(f"Bottleneck analysis failed: {e}")


@analyze_group.command("auto-scaling")
@click.argument("app_id")
@click.option("--server", "-s", help="Server name to use")
@click.option("--target-duration", type=int, default=2,
              help="Target stage duration in minutes (default: 2)")
@click.option("--format", "-f", "format_type",
              type=click.Choice(["human", "json", "table"]),
              default="human", help="Output format")
@click.pass_context
def auto_scaling_analysis(ctx: click.Context, app_id: str, server: Optional[str],
                         target_duration: int, format_type: str):
    """
    Analyze workload patterns and provide auto-scaling recommendations.

    Provides recommendations for dynamic allocation configuration based on
    application workload patterns.

    Examples:
        sparkinsight-ai analyze auto-scaling app-20231201-123456
        sparkinsight-ai analyze auto-scaling app-20231201-123456 --target-duration 5
    """
    formatter = OutputFormatter(format_type, ctx.obj.get("quiet", False))

    try:
        client = get_spark_client(ctx.obj["config_path"], server)

        # Set up mock context
        class MockRequest:
            def __init__(self, client):
                self.lifespan_context = type('obj', (object,), {
                    'clients': {'default': client},
                    'default_client': client
                })

        class MockContext:
            def __init__(self, client):
                self.request_context = MockRequest(client)

        mock_ctx = MockContext(client)

        import sparkinsight_ai.tools.tools as tools_module
        original_mcp = tools_module.mcp

        class MockMCP:
            @staticmethod
            def get_context():
                return mock_ctx

        tools_module.mcp = MockMCP()

        try:
            with create_progress() as progress:
                task = progress.add_task("Analyzing auto-scaling opportunities...", total=None)
                analysis = analyze_auto_scaling(
                    app_id=app_id,
                    server=None,
                    target_stage_duration_minutes=target_duration
                )
                progress.remove_task(task)

            formatter.output(analysis, f"Auto-scaling Analysis - {app_id}")

        finally:
            tools_module.mcp = original_mcp

    except Exception as e:
        raise click.ClickException(f"Auto-scaling analysis failed: {e}")


@analyze_group.command("shuffle-skew")
@click.argument("app_id")
@click.option("--server", "-s", help="Server name to use")
@click.option("--shuffle-threshold", type=int, default=10,
              help="Minimum shuffle size in GB to analyze (default: 10)")
@click.option("--skew-ratio", type=float, default=2.0,
              help="Minimum skew ratio to flag (default: 2.0)")
@click.option("--format", "-f", "format_type",
              type=click.Choice(["human", "json", "table"]),
              default="human", help="Output format")
@click.pass_context
def shuffle_skew_analysis(ctx: click.Context, app_id: str, server: Optional[str],
                         shuffle_threshold: int, skew_ratio: float, format_type: str):
    """
    Analyze shuffle operations to identify data skew issues.

    Identifies stages with significant shuffle data skew by comparing
    maximum vs median shuffle write bytes across tasks.

    Examples:
        sparkinsight-ai analyze shuffle-skew app-20231201-123456
        sparkinsight-ai analyze shuffle-skew app-20231201-123456 --shuffle-threshold 5
    """
    formatter = OutputFormatter(format_type, ctx.obj.get("quiet", False))

    try:
        client = get_spark_client(ctx.obj["config_path"], server)

        # Set up mock context
        class MockRequest:
            def __init__(self, client):
                self.lifespan_context = type('obj', (object,), {
                    'clients': {'default': client},
                    'default_client': client
                })

        class MockContext:
            def __init__(self, client):
                self.request_context = MockRequest(client)

        mock_ctx = MockContext(client)

        import sparkinsight_ai.tools.tools as tools_module
        original_mcp = tools_module.mcp

        class MockMCP:
            @staticmethod
            def get_context():
                return mock_ctx

        tools_module.mcp = MockMCP()

        try:
            with create_progress() as progress:
                task = progress.add_task("Analyzing shuffle skew...", total=None)
                analysis = analyze_shuffle_skew(
                    app_id=app_id,
                    server=None,
                    shuffle_threshold_gb=shuffle_threshold,
                    skew_ratio_threshold=skew_ratio
                )
                progress.remove_task(task)

            formatter.output(analysis, f"Shuffle Skew Analysis - {app_id}")

        finally:
            tools_module.mcp = original_mcp

    except Exception as e:
        raise click.ClickException(f"Shuffle skew analysis failed: {e}")


@analyze_group.command("slowest")
@click.argument("app_id")
@click.option("--server", "-s", help="Server name to use")
@click.option("--type", "analysis_type",
              type=click.Choice(["jobs", "stages", "sql"]),
              default="stages", help="What to analyze for slowest operations")
@click.option("--top-n", "-n", type=int, default=5, help="Number of slowest items to show")
@click.option("--include-running", is_flag=True, help="Include running operations")
@click.option("--format", "-f", "format_type",
              type=click.Choice(["human", "json", "table"]),
              default="human", help="Output format")
@click.pass_context
def slowest_analysis(ctx: click.Context, app_id: str, server: Optional[str],
                    analysis_type: str, top_n: int, include_running: bool,
                    format_type: str):
    """
    Find the slowest jobs, stages, or SQL queries in an application.

    Examples:
        sparkinsight-ai analyze slowest app-20231201-123456 --type jobs
        sparkinsight-ai analyze slowest app-20231201-123456 --type sql --top-n 3
        sparkinsight-ai analyze slowest app-20231201-123456 --include-running
    """
    formatter = OutputFormatter(format_type, ctx.obj.get("quiet", False))

    try:
        client = get_spark_client(ctx.obj["config_path"], server)

        # Set up mock context
        class MockRequest:
            def __init__(self, client):
                self.lifespan_context = type('obj', (object,), {
                    'clients': {'default': client},
                    'default_client': client
                })

        class MockContext:
            def __init__(self, client):
                self.request_context = MockRequest(client)

        mock_ctx = MockContext(client)

        import sparkinsight_ai.tools.tools as tools_module
        original_mcp = tools_module.mcp

        class MockMCP:
            @staticmethod
            def get_context():
                return mock_ctx

        tools_module.mcp = MockMCP()

        try:
            with create_progress() as progress:
                task = progress.add_task(f"Finding slowest {analysis_type}...", total=None)

                if analysis_type == "jobs":
                    results = list_slowest_jobs(
                        app_id=app_id,
                        server=None,
                        include_running=include_running,
                        n=top_n
                    )
                elif analysis_type == "stages":
                    results = list_slowest_stages(
                        app_id=app_id,
                        server=None,
                        include_running=include_running,
                        n=top_n
                    )
                elif analysis_type == "sql":
                    results = list_slowest_sql_queries(
                        app_id=app_id,
                        server=None,
                        top_n=top_n,
                        include_running=include_running
                    )

                progress.remove_task(task)

            title = f"Slowest {analysis_type.title()} - {app_id}"
            formatter.output(results, title)

        finally:
            tools_module.mcp = original_mcp

    except Exception as e:
        raise click.ClickException(f"Slowest {analysis_type} analysis failed: {e}")


if __name__ == "__main__":
    analyze_group()