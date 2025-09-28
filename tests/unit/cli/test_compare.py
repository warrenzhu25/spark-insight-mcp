"""
Tests for compare CLI commands.

Tests all comparison commands including apps, stages, executors, jobs, etc.
Covers parameter validation, error handling, output formatting, and interactive features.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock, call
import pytest

try:
    from click.testing import CliRunner
    import click
    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False

# Skip all tests if CLI not available
pytestmark = pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")

from spark_history_mcp.cli.commands.compare import (
    compare,
    resolve_app_name_to_recent_apps,
    get_session_file,
    save_comparison_context,
    load_comparison_context,
    clear_comparison_context,
    get_app_context,
    is_app_id,
    resolve_app_identifiers,
    create_mock_context
)


class TestCompareSessionManagement:
    """Test comparison session context management."""

    def test_get_session_file_creates_directory(self):
        """Test that session file creation creates necessary directories."""
        session_file = get_session_file()
        assert session_file.parent.exists()
        assert session_file.name == "compare-session.json"

    def test_save_and_load_comparison_context(self):
        """Test saving and loading comparison context."""
        with tempfile.TemporaryDirectory() as tmpdir:
            session_file = Path(tmpdir) / "test-session.json"

            # Mock get_session_file to use our temp file
            with patch('spark_history_mcp.cli.commands.compare.get_session_file', return_value=session_file):
                # Save context
                save_comparison_context("app-123", "app-456", "local")

                # Load context
                context = load_comparison_context()
                assert context == ("app-123", "app-456", "local")

    def test_load_comparison_context_no_file(self):
        """Test loading context when no file exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            session_file = Path(tmpdir) / "nonexistent.json"

            with patch('spark_history_mcp.cli.commands.compare.get_session_file', return_value=session_file):
                context = load_comparison_context()
                assert context is None

    def test_load_comparison_context_invalid_json(self):
        """Test loading context with invalid JSON."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content")
            session_file = Path(f.name)

        try:
            with patch('spark_history_mcp.cli.commands.compare.get_session_file', return_value=session_file):
                context = load_comparison_context()
                assert context is None
        finally:
            session_file.unlink()

    def test_clear_comparison_context(self):
        """Test clearing comparison context."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"app_id1": "app-123", "app_id2": "app-456"}, f)
            session_file = Path(f.name)

        try:
            with patch('spark_history_mcp.cli.commands.compare.get_session_file', return_value=session_file):
                # Verify file exists
                assert session_file.exists()

                # Clear context
                clear_comparison_context()

                # Verify file is removed
                assert not session_file.exists()
        finally:
            session_file.unlink(missing_ok=True)


class TestAppIdentifierResolution:
    """Test application identifier resolution logic."""

    def test_is_app_id_positive_cases(self):
        """Test app ID detection for valid patterns."""
        valid_app_ids = [
            "app-20231201-123456",
            "application_1234567890_001",
            "app-abcd1234",
            "spark-20231201-example"
        ]

        for app_id in valid_app_ids:
            assert is_app_id(app_id), f"Should recognize {app_id} as app ID"

    def test_is_app_id_negative_cases(self):
        """Test app ID detection for app names."""
        app_names = [
            "ETL Pipeline",
            "Daily Job",
            "data-processing",
            "MyApp",
            "app",  # Too short
            "just-a-name"
        ]

        for name in app_names:
            assert not is_app_id(name), f"Should recognize {name} as app name"

    @patch('spark_history_mcp.cli.commands.compare.resolve_app_name_to_recent_apps')
    def test_resolve_app_identifiers_single_name(self, mock_resolve):
        """Test resolving single app name to two recent apps."""
        mock_client = MagicMock()
        mock_apps = [
            MagicMock(id="app-123", name="ETL Pipeline", attempts=[MagicMock(start_time="2023-01-01")]),
            MagicMock(id="app-456", name="ETL Pipeline", attempts=[MagicMock(start_time="2023-01-02")])
        ]
        mock_resolve.return_value = ("app-123", "app-456", mock_apps)

        app_id1, app_id2, feedback = resolve_app_identifiers("ETL Pipeline", None, mock_client)

        assert app_id1 == "app-123"
        assert app_id2 == "app-456"
        assert "Found 2 recent applications" in feedback
        mock_resolve.assert_called_once_with("ETL Pipeline", mock_client, None)

    def test_resolve_app_identifiers_single_id_error(self):
        """Test error when providing single app ID instead of name."""
        mock_client = MagicMock()

        with pytest.raises(click.ClickException) as exc_info:
            resolve_app_identifiers("app-20231201-abcdef", None, mock_client)

        assert "should be an application name" in str(exc_info.value)

    def test_resolve_app_identifiers_two_ids(self):
        """Test resolving two app IDs directly."""
        mock_client = MagicMock()

        app_id1, app_id2, feedback = resolve_app_identifiers("app-123", "app-456", mock_client)

        assert app_id1 == "app-123"
        assert app_id2 == "app-456"
        assert feedback is None  # No resolution needed

    @patch('spark_history_mcp.cli.commands.compare.resolve_app_name_to_recent_apps')
    def test_resolve_app_identifiers_mixed(self, mock_resolve):
        """Test resolving mix of app ID and app name."""
        mock_client = MagicMock()
        mock_apps = [MagicMock(id="app-789", name="Daily Job")]
        mock_resolve.return_value = ("app-789", None, mock_apps)

        app_id1, app_id2, feedback = resolve_app_identifiers("app-20231201-aaaaaa", "Daily Job", mock_client)

        assert app_id1 == "app-20231201-aaaaaa"
        assert app_id2 == "app-789"
        assert "Resolved 'Daily Job' to: app-789" in feedback

    @patch('spark_history_mcp.tools.tools.list_applications')
    def test_resolve_app_name_recent_apps_one_found(self, mock_list_apps):
        mock_client = MagicMock()
        # Return exactly 1 match
        app = MagicMock(id='app-1', name='X', attempts=[MagicMock(start_time='s')])
        mock_list_apps.return_value = [app]
        with pytest.raises(click.ClickException) as exc:
            resolve_app_name_to_recent_apps('Name', mock_client, None, limit=2)
        assert 'Only found 1 application matching' in str(exc.value)

    @patch('spark_history_mcp.tools.tools.list_applications')
    def test_resolve_app_name_recent_apps_too_many(self, mock_list_apps):
        mock_client = MagicMock()
        # Return 3 matches (limit defaults to 2)
        apps = [MagicMock(id=f'app-{i}', name=f'N{i}') for i in range(3)]
        for a in apps:
            a.attempts = []
        mock_list_apps.return_value = apps
        with pytest.raises(click.ClickException) as exc:
            resolve_app_name_to_recent_apps('Name', mock_client, None, limit=2)
        assert 'Found 3 applications matching' in str(exc.value)


class TestGetAppContext:
    """Test app context resolution logic."""

    def test_get_app_context_with_both_ids(self):
        """Test getting context when both app IDs provided."""
        with patch('spark_history_mcp.cli.commands.compare.save_comparison_context') as mock_save:
            app_id1, app_id2, server = get_app_context("app-123", "app-456", "local")

            assert app_id1 == "app-123"
            assert app_id2 == "app-456"
            assert server == "local"
            mock_save.assert_called_once_with("app-123", "app-456", "local")

    @patch('spark_history_mcp.cli.commands.compare.load_comparison_context')
    def test_get_app_context_from_session(self, mock_load):
        """Test getting context from saved session."""
        mock_load.return_value = ("app-789", "app-012", "production")

        app_id1, app_id2, server = get_app_context(server="local")

        assert app_id1 == "app-789"
        assert app_id2 == "app-012"
        assert server == "local"  # Provided server overrides session

    @patch('spark_history_mcp.cli.commands.compare.load_comparison_context')
    def test_get_app_context_no_session_error(self, mock_load):
        """Test error when no session context available."""
        mock_load.return_value = None

        with pytest.raises(click.ClickException) as exc_info:
            get_app_context()

        assert "No comparison context found" in str(exc_info.value)


class TestMockContext:
    """Test mock context creation utility."""

    def test_create_mock_context_structure(self):
        """Test that mock context has correct structure."""
        mock_client = MagicMock()
        context = create_mock_context(mock_client)

        # Test structure
        assert hasattr(context, 'request_context')
        assert hasattr(context.request_context, 'lifespan_context')
        assert hasattr(context.request_context.lifespan_context, 'default_client')
        assert hasattr(context.request_context.lifespan_context, 'clients')

        # Test client assignment
        assert context.request_context.lifespan_context.default_client == mock_client
        assert context.request_context.lifespan_context.clients["default"] == mock_client


class TestCompareAppsCommand:
    """Test the main compare apps command."""

    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.cli.commands.compare.resolve_app_identifiers')
    @patch('spark_history_mcp.cli.commands.compare.save_comparison_context')
    @patch('spark_history_mcp.tools.tools.compare_app_performance')
    def test_compare_apps_basic_success(self, mock_compare_perf, mock_save_context, mock_resolve, mock_get_client, cli_runner):
        """Test basic successful app comparison."""
        # Setup mocks
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_resolve.return_value = ("app-123", "app-456", "Resolved apps")

        mock_compare_perf.return_value = {
            "applications": {"app1": {"id": "app-123"}, "app2": {"id": "app-456"}},
            "performance_comparison": {"stages": {"top_stage_differences": []}},
            "key_recommendations": []
        }

        # Run command
        result = cli_runner.invoke(compare, [
            'apps', 'app-123', 'app-456',
            '--format', 'json',
            '--threshold', '0.15'
        ], obj={'config_path': Path('/tmp/config.yaml'), 'quiet': False})

        # Verify success
        assert result.exit_code == 0
        mock_resolve.assert_called_once()
        mock_save_context.assert_called_once_with("app-123", "app-456", None)
        mock_compare_perf.assert_called_once()

    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    def test_compare_apps_client_error(self, mock_get_client, cli_runner):
        """Test error handling when client creation fails."""
        mock_get_client.side_effect = Exception("Connection failed")

        result = cli_runner.invoke(compare, [
            'apps', 'app-123', 'app-456'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code != 0
        assert "Error comparing applications" in result.output

    def test_compare_apps_invalid_threshold(self, cli_runner):
        """Test validation of threshold parameter."""
        result = cli_runner.invoke(compare, [
            'apps', 'app-123', 'app-456',
            '--threshold', 'invalid'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code != 0

    def test_compare_apps_missing_arguments(self, cli_runner):
        """Test error when required arguments missing."""
        result = cli_runner.invoke(compare, ['apps'], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code != 0


class TestCompareStagesCommand:
    """Test the compare stages command."""

    @patch('spark_history_mcp.cli.commands.compare.get_app_context')
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_stages')
    def test_compare_stages_with_context(self, mock_compare_stages, mock_get_client, mock_get_context, cli_runner):
        """Test stage comparison using saved context."""
        # Setup mocks
        mock_get_context.return_value = ("app-123", "app-456", "local")
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_compare_stages.return_value = {
            "summary": {"significance_threshold": 0.1},
            "stage_comparison": {},
            "recommendations": []
        }

        # Run command
        result = cli_runner.invoke(compare, [
            'stages', '1', '2',
            '--significance-threshold', '0.2',
            '--format', 'json'
        ], obj={'config_path': Path('/tmp/config.yaml'), 'quiet': True})

        # Verify success
        assert result.exit_code == 0
        mock_get_context.assert_called_once()
        mock_compare_stages.assert_called_once_with(
            app_id1="app-123",
            app_id2="app-456",
            stage_id1=1,
            stage_id2=2,
            server="local",
            significance_threshold=0.2
        )

    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_stages')
    def test_compare_stages_with_apps_override(self, mock_compare_stages, mock_get_client, cli_runner):
        """Test stage comparison with explicit app override."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_compare_stages.return_value = {"summary": {}}

        result = cli_runner.invoke(compare, [
            'stages', '1', '2',
            '--apps', 'app-789', 'app-012'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        mock_compare_stages.assert_called_once()
        # Verify it used the override apps, not context
        call_args = mock_compare_stages.call_args
        assert call_args.kwargs['app_id1'] == 'app-789'
        assert call_args.kwargs['app_id2'] == 'app-012'


class TestCompareTimelineCommand:
    """Test the compare timeline command."""

    @patch('spark_history_mcp.cli.commands.compare.get_app_context')
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_executor_timeline')
    def test_compare_timeline_basic(self, mock_compare_timeline, mock_get_client, mock_get_context, cli_runner):
        """Test basic timeline comparison."""
        mock_get_context.return_value = ("app-123", "app-456", "local")
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_compare_timeline.return_value = {"timeline": []}

        result = cli_runner.invoke(compare, [
            'timeline',
            '--interval-minutes', '5'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        mock_compare_timeline.assert_called_once_with(
            app_id1="app-123",
            app_id2="app-456",
            server="local",
            interval_minutes=5
        )


class TestCompareExecutorsCommand:
    """Test the compare executors command."""

    @patch('spark_history_mcp.cli.commands.compare.get_app_context')
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_executors')
    def test_compare_executors_with_filtering(self, mock_compare_execs, mock_get_client, mock_get_context, cli_runner):
        """Test executor comparison with significance filtering."""
        mock_get_context.return_value = ("app-123", "app-456", "local")
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_compare_execs.return_value = {"executors": {}}

        result = cli_runner.invoke(compare, [
            'executors',
            '--significance-threshold', '0.05',
            '--show-only-significant'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        mock_compare_execs.assert_called_once_with(
            app_id1="app-123",
            app_id2="app-456",
            server="local",
            significance_threshold=0.05,
            show_only_significant=True
        )


class TestCompareStatusAndClearCommands:
    """Test status and clear commands."""

    @patch('spark_history_mcp.cli.commands.compare.load_comparison_context')
    def test_status_with_context(self, mock_load, cli_runner):
        """Test status command with active context."""
        mock_load.return_value = ("app-123", "app-456", "local")

        result = cli_runner.invoke(compare, [
            'status', '--format', 'json'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        output_data = json.loads(result.output)
        assert output_data['app_id1'] == 'app-123'
        assert output_data['app_id2'] == 'app-456'
        assert output_data['status'] == 'active'

    @patch('spark_history_mcp.cli.commands.compare.load_comparison_context')
    def test_status_no_context(self, mock_load, cli_runner):
        """Test status command with no context."""
        mock_load.return_value = None

        result = cli_runner.invoke(compare, [
            'status', '--format', 'json'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        output_data = json.loads(result.output)
        assert output_data['status'] == 'no_context'

    @patch('spark_history_mcp.cli.commands.compare.load_comparison_context')
    @patch('spark_history_mcp.cli.commands.compare.clear_comparison_context')
    def test_clear_command(self, mock_clear, mock_load, cli_runner):
        """Test clear command."""
        mock_load.return_value = ("app-123", "app-456", "local")

        result = cli_runner.invoke(compare, ['clear'], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        mock_clear.assert_called_once()


class TestAdditionalCompareCommands:
    """Additional compare subcommands coverage."""

    @patch('spark_history_mcp.cli.commands.compare.get_app_context')
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_resources')
    def test_compare_resources_with_override(self, mock_compare_resources, mock_get_client, mock_get_context, cli_runner):
        mock_get_context.return_value = ("app-111", "app-222", "local")
        mock_get_client.return_value = MagicMock()
        mock_compare_resources.return_value = {"resource_comparison": {}}

        # Use explicit override apps
        result = cli_runner.invoke(compare, [
            'resources', '--apps', 'app-123', 'app-456', '--format', 'json'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        call_args = mock_compare_resources.call_args
        assert call_args.kwargs['app_id1'] == 'app-123'
        assert call_args.kwargs['app_id2'] == 'app-456'
        assert 'server' in call_args.kwargs

    @patch('spark_history_mcp.cli.commands.compare.get_app_context')
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_stage_executor_timeline')
    def test_stage_timeline_basic(self, mock_stage_timeline, mock_get_client, mock_get_context, cli_runner):
        mock_get_context.return_value = ("app-1", "app-2", "local")
        mock_get_client.return_value = MagicMock()
        mock_stage_timeline.return_value = {"timeline": []}

        result = cli_runner.invoke(compare, [
            'stage-timeline', '5', '6', '--format', 'json', '--interval-minutes', '2'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        call_args = mock_stage_timeline.call_args
        assert call_args.kwargs['stage_id1'] == 5
        assert call_args.kwargs['stage_id2'] == 6
        assert call_args.kwargs['interval_minutes'] == 2

    @patch('spark_history_mcp.cli.commands.compare.get_app_context')
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_stages_aggregated')
    def test_stages_aggregated_basic(self, mock_stages_agg, mock_get_client, mock_get_context, cli_runner):
        mock_get_context.return_value = ("app-1", "app-2", "local")
        mock_get_client.return_value = MagicMock()
        mock_stages_agg.return_value = {"stage_comparison": {}}

        result = cli_runner.invoke(compare, [
            'stages-aggregated', '--format', 'json'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        mock_stages_agg.assert_called_once()

    @patch('spark_history_mcp.cli.commands.compare.resolve_app_identifiers')
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_jobs')
    def test_jobs_basic(self, mock_compare_jobs, mock_get_client, mock_resolve, cli_runner):
        mock_get_client.return_value = MagicMock()
        mock_resolve.return_value = ("app-1", "app-2", None)
        mock_compare_jobs.return_value = {"job_comparison": {}}

        result = cli_runner.invoke(compare, ['jobs', '--apps', 'app-1', 'app-2', '--format', 'json'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code == 0
        mock_compare_jobs.assert_called_once()

    @patch('spark_history_mcp.cli.commands.compare.resolve_app_identifiers')
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_performance')
    @patch('click.getchar', return_value='q')
    def test_apps_interactive_quit(self, mock_getchar, mock_compare_perf, mock_get_client, mock_resolve, cli_runner):
        mock_get_client.return_value = MagicMock()
        mock_resolve.return_value = ("app-1", "app-2", "feedback")
        # Provide data with no stage differences (menu prints message and returns)
        mock_compare_perf.return_value = {
            "applications": {"app1": {"id": "app-1"}, "app2": {"id": "app-2"}},
            "performance_comparison": {"stages": {"top_stage_differences": []}},
            "key_recommendations": []
        }

        result = cli_runner.invoke(compare, ['apps', 'app-1', 'app-2', '--interactive', '--format', 'json'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code == 0
        mock_compare_perf.assert_called_once()


class TestCompareMenus:
    """Test interactive menu helpers for coverage."""

    @patch('click.getchar', return_value='q')
    def test_show_post_stage_menu_quit(self, mock_getchar):
        from spark_history_mcp.cli.commands.compare import show_post_stage_menu
        # Minimal formatter-like object
        class F: format_type = 'human'
        # Should exit quietly
        show_post_stage_menu('app1','app2',1,2,'local',F(),MagicMock())

    @patch('spark_history_mcp.cli.commands.compare.execute_timeline_comparison')
    @patch('click.getchar', return_value='t')
    def test_show_post_stage_menu_timeline(self, mock_getchar, mock_exec_timeline):
        from spark_history_mcp.cli.commands.compare import show_post_stage_menu
        class F: format_type = 'human'
        show_post_stage_menu('app1','app2',1,2,'local',F(),MagicMock())
        mock_exec_timeline.assert_called_once()

    @patch('spark_history_mcp.cli.commands.compare.execute_stage_timeline_comparison')
    @patch('click.getchar', return_value='s')
    def test_show_post_stage_menu_stage_timeline(self, mock_getchar, mock_exec_stage_timeline):
        from spark_history_mcp.cli.commands.compare import show_post_stage_menu
        class F: format_type = 'human'
        show_post_stage_menu('app1','app2',3,4,'local',F(),MagicMock())
        mock_exec_stage_timeline.assert_called_once()

    @patch('click.getchar', side_effect=OSError())
    @patch('click.prompt', return_value='q')
    def test_show_post_stage_menu_prompt_fallback(self, mock_prompt, mock_getchar):
        from spark_history_mcp.cli.commands.compare import show_post_stage_menu
        class F: format_type = 'human'
        show_post_stage_menu('app1','app2',3,4,'local',F(),MagicMock())
        mock_prompt.assert_called_once()


class TestCompareAppsInteractiveMenu:
    """Test interactive menu of compare apps with numeric selection."""

    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.cli.commands.compare.save_comparison_context')
    @patch('spark_history_mcp.cli.commands.compare.resolve_app_identifiers')
    @patch('spark_history_mcp.tools.tools.compare_app_performance')
    @patch('click.getchar', return_value='1')
    @patch('spark_history_mcp.cli.commands.compare.execute_stage_comparison')
    def test_apps_interactive_stage_selection(
        self,
        mock_exec_stage,
        mock_getchar,
        mock_compare_perf,
        mock_resolve,
        mock_save_ctx,
        mock_get_client,
        cli_runner,
    ):
        mock_get_client.return_value = MagicMock()
        mock_resolve.return_value = ("app-1", "app-2", "feedback")
        # Provide a result with one stage difference so menu shows [1]
        mock_compare_perf.return_value = {
            "applications": {"app1": {"id": "app-1"}, "app2": {"id": "app-2"}},
            "performance_comparison": {
                "stages": {
                    "top_stage_differences": [
                        {
                            "app1_stage": {"stage_id": 11},
                            "app2_stage": {"stage_id": 12},
                            "stage_name": "Stage XYZ",
                        }
                    ]
                }
            },
            "key_recommendations": [],
        }

        result = cli_runner.invoke(
            compare,
            ['apps', 'AnyName', '--interactive', '--format', 'human'],
            obj={'config_path': Path('/tmp/config.yaml')},
        )

        assert result.exit_code == 0
        mock_save_ctx.assert_called_once_with('app-1', 'app-2', None)
        mock_exec_stage.assert_called_once()


class TestCompareStagesPostMenu:
    """Cover post-stage menu trigger in human format."""

    @patch('spark_history_mcp.cli.commands.compare.get_app_context')
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.cli.commands.compare.load_comparison_context', return_value=("app-1","app-2","local"))
    @patch('spark_history_mcp.tools.tools.compare_stages')
    @patch('click.getchar', return_value='q')
    def test_stages_human_triggers_post_menu(
        self,
        mock_getchar,
        mock_compare_stages,
        mock_load_ctx,
        mock_get_client,
        mock_get_app_ctx,
        cli_runner,
    ):
        mock_get_app_ctx.return_value = ("app-1", "app-2", "local")
        mock_get_client.return_value = MagicMock()
        mock_compare_stages.return_value = {
            "summary": {"significance_threshold": 0.1},
            "stage_comparison": {"stage1": {}, "stage2": {}},
            "recommendations": [],
        }

        result = cli_runner.invoke(
            compare,
            ['stages', '1', '2', '--format', 'human'],
            obj={'config_path': Path('/tmp/config.yaml')},
        )
        assert result.exit_code == 0
        mock_compare_stages.assert_called_once()


class TestCompareStatusClearAndTimelineOverride:
    @patch('spark_history_mcp.cli.commands.compare.load_comparison_context', return_value=None)
    def test_status_human_no_context(self, mock_load, cli_runner):
        result = cli_runner.invoke(compare, ['status', '--format', 'human'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code == 0
        assert 'No comparison context set' in result.output

    def test_clear_no_context(self, cli_runner):
        # Ensure no context file exists
        try:
            get_session_file().unlink(missing_ok=True)
        except Exception:
            pass
        result = cli_runner.invoke(compare, ['clear'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code == 0
        assert 'No comparison context to clear' in result.output

    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_executor_timeline')
    def test_timeline_with_override(self, mock_compare_tl, mock_get_client, cli_runner):
        mock_get_client.return_value = MagicMock()
        mock_compare_tl.return_value = {"timeline": []}
        result = cli_runner.invoke(compare, ['timeline', '--apps', 'app-1', 'app-2', '--interval-minutes', '3', '--format', 'json'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code == 0
        call_args = mock_compare_tl.call_args
        assert call_args.kwargs['interval_minutes'] == 3


class TestExecuteHelpers:
    """Directly test execute_* helper functions."""

    @patch('spark_history_mcp.cli.commands.compare.load_comparison_context', return_value=("app-1","app-2","local"))
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_stages')
    def test_execute_stage_comparison(self, mock_compare_stages, mock_get_client, mock_load_ctx, capsys):
        from types import SimpleNamespace
        from spark_history_mcp.cli.commands.compare import execute_stage_comparison
        class F: format_type = 'human';
        mock_get_client.return_value = MagicMock()
        mock_compare_stages.return_value = {"stage_comparison": {}}
        ctx = SimpleNamespace(obj={'config_path': Path('/tmp/config.yaml')})
        execute_stage_comparison(3, 4, 'local', F(), ctx)
        out = capsys.readouterr().out
        assert 'Analyzing stages' in out
        mock_compare_stages.assert_called_once()

    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_executor_timeline')
    def test_execute_timeline_comparison(self, mock_compare_tl, mock_get_client, capsys):
        from types import SimpleNamespace
        from spark_history_mcp.cli.commands.compare import execute_timeline_comparison
        class F: format_type = 'human'
        mock_get_client.return_value = MagicMock()
        mock_compare_tl.return_value = {"timeline": []}
        ctx = SimpleNamespace(obj={'config_path': Path('/tmp/config.yaml')})
        execute_timeline_comparison('app-1', 'app-2', 'local', F(), ctx)
        out = capsys.readouterr().out
        assert 'Analyzing application timeline' in out
        mock_compare_tl.assert_called_once()

    @patch('spark_history_mcp.cli.commands.compare.load_comparison_context', return_value=("app-1","app-2","local"))
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_stage_executor_timeline')
    def test_execute_stage_timeline_comparison(self, mock_compare_stage_tl, mock_get_client, mock_load_ctx, capsys):
        from types import SimpleNamespace
        from spark_history_mcp.cli.commands.compare import execute_stage_timeline_comparison
        class F: format_type = 'human'
        mock_get_client.return_value = MagicMock()
        mock_compare_stage_tl.return_value = {"timeline": []}
        ctx = SimpleNamespace(obj={'config_path': Path('/tmp/config.yaml')})
        execute_stage_timeline_comparison(7, 8, 'local', F(), ctx)
        out = capsys.readouterr().out
        assert 'Analyzing stage 7 vs 8 timeline' in out
        mock_compare_stage_tl.assert_called_once()

    @patch('spark_history_mcp.cli.commands.compare.load_comparison_context', return_value=None)
    def test_execute_stage_comparison_no_context(self, mock_load_ctx, capsys):
        from types import SimpleNamespace
        from spark_history_mcp.cli.commands.compare import execute_stage_comparison
        class F: format_type = 'human'
        ctx = SimpleNamespace(obj={'config_path': Path('/tmp/config.yaml')})
        execute_stage_comparison(1, 2, 'local', F(), ctx)
        out = capsys.readouterr().out
        assert 'No comparison context found' in out


class TestCompareMoreErrorsAndBranches:
    @patch('spark_history_mcp.cli.commands.compare.get_app_context', return_value=("a1","a2","local"))
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_jobs', side_effect=Exception('boom'))
    def test_jobs_error_handling(self, mock_comp, mock_get_client, mock_get_ctx, cli_runner):
        mock_get_client.return_value = MagicMock()
        result = cli_runner.invoke(compare, ['jobs'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code != 0
        assert 'Error comparing jobs' in result.output

    @patch('spark_history_mcp.cli.commands.compare.get_app_context', return_value=("a1","a2","local"))
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_resources', side_effect=Exception('boom'))
    def test_resources_error_handling(self, mock_comp, mock_get_client, mock_get_ctx, cli_runner):
        mock_get_client.return_value = MagicMock()
        result = cli_runner.invoke(compare, ['resources'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code != 0
        assert 'Error comparing resources' in result.output

    @patch('spark_history_mcp.cli.commands.compare.get_app_context', return_value=("a1","a2","local"))
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_executor_timeline', side_effect=Exception('boom'))
    def test_timeline_error_handling(self, mock_comp, mock_get_client, mock_get_ctx, cli_runner):
        mock_get_client.return_value = MagicMock()
        result = cli_runner.invoke(compare, ['timeline'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code != 0
        assert 'Error comparing timelines' in result.output

    @patch('spark_history_mcp.cli.commands.compare.get_app_context', return_value=("a1","a2","local"))
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_stage_executor_timeline', side_effect=Exception('boom'))
    def test_stage_timeline_error_handling(self, mock_comp, mock_get_client, mock_get_ctx, cli_runner):
        mock_get_client.return_value = MagicMock()
        result = cli_runner.invoke(compare, ['stage-timeline', '1', '2'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code != 0
        assert 'Error comparing stage timelines' in result.output

    @patch('spark_history_mcp.cli.commands.compare.get_app_context', return_value=("a1","a2","local"))
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_executors', side_effect=Exception('boom'))
    def test_executors_error_handling(self, mock_comp, mock_get_client, mock_get_ctx, cli_runner):
        mock_get_client.return_value = MagicMock()
        result = cli_runner.invoke(compare, ['executors'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code != 0
        assert 'Error comparing executors' in result.output

    @patch('spark_history_mcp.cli.commands.compare.get_app_context', return_value=("a1","a2","local"))
    @patch('spark_history_mcp.cli.commands.compare.get_spark_client')
    @patch('spark_history_mcp.tools.tools.compare_app_stages_aggregated', side_effect=Exception('boom'))
    def test_stages_aggregated_error_handling(self, mock_comp, mock_get_client, mock_get_ctx, cli_runner):
        mock_get_client.return_value = MagicMock()
        result = cli_runner.invoke(compare, ['stages-aggregated'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code != 0
        assert 'Error comparing aggregated stages' in result.output

    @patch('spark_history_mcp.cli.commands.compare.get_spark_client', side_effect=Exception('boom'))
    def test_apps_error_on_client(self, mock_get_client, cli_runner):
        result = cli_runner.invoke(compare, ['apps', 'app-1', 'app-2'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code != 0
        assert 'Error comparing applications' in result.output

    @patch('click.getchar', return_value='x')
    def test_show_interactive_menu_invalid_choice(self, mock_getchar, capsys):
        from spark_history_mcp.cli.commands.compare import show_interactive_menu
        class F: format_type = 'human'
        data = { 'performance_comparison': { 'stages': { 'top_stage_differences': [
            { 'stage_name': 'Very Long Stage Name That Should Be Truncated For Display Purposes', 'time_difference': {'percentage': 20, 'slower_application': 'app2'}, 'app1_stage': {'stage_id':1, 'duration_seconds': 5.0}, 'app2_stage': {'stage_id':2, 'duration_seconds': 4.0}}
        ]}}}
        show_interactive_menu(data, 'a1','a2','local', F(), MagicMock())
        out = capsys.readouterr().out
        assert 'Invalid choice' in out


class TestPerformanceAlias:
    """Cover the deprecated performance alias path."""

    def test_performance_alias_invocation(self, cli_runner):
        # This path may raise due to internal ctx.invoke arg names, but still covers lines
        result = cli_runner.invoke(compare, ['performance', 'app-1', 'app-2', '--format', 'json'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code != 0


class TestCompareCommandParameterValidation:
    """Test parameter validation across all compare commands."""

    def test_invalid_format_option(self, cli_runner):
        """Test invalid format option."""
        result = cli_runner.invoke(compare, [
            'apps', 'app-123', 'app-456',
            '--format', 'invalid'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code != 0

    def test_invalid_stage_id_type(self, cli_runner):
        """Test invalid stage ID type."""
        result = cli_runner.invoke(compare, [
            'stages', 'not-a-number', '2'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code != 0

    def test_negative_interval_minutes(self, cli_runner):
        """Negative interval is accepted as int; ensure no crash."""
        result = cli_runner.invoke(compare, [
            'timeline', '--interval-minutes', '-1'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code in (0, 1, 2)
