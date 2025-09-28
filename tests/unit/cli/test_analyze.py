"""
Tests for analyze CLI commands.

Covers insights, bottlenecks, auto-scaling, shuffle-skew, slowest, and deprecated compare.
"""

from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

try:
    from click.testing import CliRunner
    import click
    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False

pytestmark = pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")

from spark_history_mcp.cli.commands.analyze import analyze
# Ensure submodule is loaded so patch('spark_history_mcp.tools.tools') works
import spark_history_mcp.tools.tools as _tools_loaded  # noqa: F401


class TestAnalyzeInsights:
    @patch('spark_history_mcp.cli.commands.analyze.get_spark_client')
    @patch('spark_history_mcp.tools.tools.get_application_insights')
    def test_insights_basic(self, mock_get_insights, mock_get_client, cli_runner):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_get_insights.return_value = {"application": {"id": "app-123"}, "insights": {}}

        result = cli_runner.invoke(
            analyze,
            [
                'insights', 'app-123',
                '--format', 'json',
                '--no-auto-scaling',
                '--no-shuffle-skew',
                '--no-failed-tasks',
                '--no-executor-utilization',
            ],
            obj={'config_path': Path('/tmp/config.yaml'), 'quiet': False},
        )
        assert result.exit_code == 0
        mock_get_insights.assert_called_once()

    @patch('spark_history_mcp.cli.commands.analyze.get_spark_client')
    def test_insights_client_error(self, mock_get_client, cli_runner):
        mock_get_client.side_effect = Exception("boom")
        result = cli_runner.invoke(
            analyze, ['insights', 'app-123'], obj={'config_path': Path('/tmp/config.yaml')}
        )
        assert result.exit_code != 0
        assert "Error analyzing application" in result.output


class TestAnalyzeBottlenecks:
    @patch('spark_history_mcp.cli.commands.analyze.get_spark_client')
    @patch('spark_history_mcp.tools.tools.get_job_bottlenecks')
    def test_bottlenecks_topn(self, mock_get_bottlenecks, mock_get_client, cli_runner):
        mock_get_client.return_value = MagicMock()
        mock_get_bottlenecks.return_value = {"bottlenecks": []}

        result = cli_runner.invoke(
            analyze, ['bottlenecks', 'app-1', '--top-n', '3', '--format', 'json'],
            obj={'config_path': Path('/tmp/config.yaml')},
        )
        assert result.exit_code == 0
        mock_get_bottlenecks.assert_called_once_with(app_id='app-1', server=None, top_n=3)


class TestAnalyzeAutoScaling:
    @patch('spark_history_mcp.cli.commands.analyze.get_spark_client')
    @patch('spark_history_mcp.tools.tools.analyze_auto_scaling')
    def test_auto_scaling(self, mock_analyze_auto_scaling, mock_get_client, cli_runner):
        mock_get_client.return_value = MagicMock()
        mock_analyze_auto_scaling.return_value = {"recommendations": []}

        result = cli_runner.invoke(
            analyze, ['auto-scaling', 'app-1', '--target-duration', '5', '--format', 'json'],
            obj={'config_path': Path('/tmp/config.yaml')},
        )
        assert result.exit_code == 0
        mock_analyze_auto_scaling.assert_called_once_with(app_id='app-1', server=None, target_stage_duration_minutes=5)


class TestAnalyzeShuffleSkew:
    @patch('spark_history_mcp.cli.commands.analyze.get_spark_client')
    @patch('spark_history_mcp.tools.tools.analyze_shuffle_skew')
    def test_shuffle_skew(self, mock_analyze_skew, mock_get_client, cli_runner):
        mock_get_client.return_value = MagicMock()
        mock_analyze_skew.return_value = {"skew": {}}

        result = cli_runner.invoke(
            analyze,
            ['shuffle-skew', 'app-1', '--shuffle-threshold', '20', '--skew-ratio', '1.5', '--format', 'json'],
            obj={'config_path': Path('/tmp/config.yaml')},
        )
        assert result.exit_code == 0
        mock_analyze_skew.assert_called_once_with(
            app_id='app-1', server=None, shuffle_threshold_gb=20, skew_ratio_threshold=1.5
        )


class TestAnalyzeSlowest:
    @patch('spark_history_mcp.cli.commands.analyze.get_spark_client')
    @patch('spark_history_mcp.tools.tools.list_slowest_jobs')
    def test_slowest_jobs(self, mock_list_jobs, mock_get_client, cli_runner):
        mock_get_client.return_value = MagicMock()
        mock_list_jobs.return_value = []

        result = cli_runner.invoke(
            analyze, ['slowest', 'app-1', '--type', 'jobs', '--top-n', '2', '--format', 'json'],
            obj={'config_path': Path('/tmp/config.yaml')},
        )
        assert result.exit_code == 0
        mock_list_jobs.assert_called_once_with(app_id='app-1', server=None, n=2)

    @patch('spark_history_mcp.cli.commands.analyze.get_spark_client')
    @patch('spark_history_mcp.tools.tools.list_slowest_stages')
    def test_slowest_stages(self, mock_list_stages, mock_get_client, cli_runner):
        mock_get_client.return_value = MagicMock()
        mock_list_stages.return_value = []

        result = cli_runner.invoke(
            analyze, ['slowest', 'app-1', '--type', 'stages', '--top-n', '4', '--format', 'json'],
            obj={'config_path': Path('/tmp/config.yaml')},
        )
        assert result.exit_code == 0
        mock_list_stages.assert_called_once_with(app_id='app-1', server=None, n=4)

    @patch('spark_history_mcp.cli.commands.analyze.get_spark_client')
    @patch('spark_history_mcp.tools.tools.list_slowest_sql_queries')
    def test_slowest_sql(self, mock_list_sql, mock_get_client, cli_runner):
        mock_get_client.return_value = MagicMock()
        mock_list_sql.return_value = []

        result = cli_runner.invoke(
            analyze, ['slowest', 'app-1', '--type', 'sql', '--top-n', '5', '--format', 'json'],
            obj={'config_path': Path('/tmp/config.yaml')},
        )
        assert result.exit_code == 0
        mock_list_sql.assert_called_once_with(app_id='app-1', server=None, top_n=5)


class TestAnalyzeCompareDeprecated:
    @patch('spark_history_mcp.cli.commands.analyze.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_performance')
    def test_compare_deprecated(self, mock_compare, mock_get_client, cli_runner):
        mock_get_client.return_value = MagicMock()
        mock_compare.return_value = {"applications": {}}

        result = cli_runner.invoke(
            analyze, ['compare', 'app-1', 'app-2', '--format', 'json'],
            obj={'config_path': Path('/tmp/config.yaml')},
        )
        # Should still succeed and print deprecation warning
        assert result.exit_code == 0
        assert "deprecated" in result.output.lower()
        mock_compare.assert_called_once_with(app_id1='app-1', app_id2='app-2', server=None, top_n=3)
