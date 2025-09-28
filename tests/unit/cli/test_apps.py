"""
Tests for apps CLI commands.

Tests application listing, inspection, and resolution functionality.
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

# Skip all tests if CLI not available
pytestmark = pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")

from spark_history_mcp.cli.commands.apps import (
    apps,
    load_config,
    get_spark_client,
    is_application_id,
    resolve_app_identifier,
    create_mock_context
)
from spark_history_mcp.config.config import Config


class TestConfigurationManagement:
    """Test configuration loading and client creation."""

    def test_load_config_success(self, mock_config_file):
        """Test successful config loading."""
        config = load_config(mock_config_file)
        assert isinstance(config, Config)
        assert "local" in config.servers
        assert config.servers["local"].default is True

    def test_load_config_file_not_found(self):
        """Test default config is returned when file missing."""
        config = load_config(Path("/nonexistent/config.yaml"))
        assert isinstance(config, Config)
        # Should include a default server
        assert "local" in config.servers

    @patch('spark_history_mcp.cli.commands.apps.Config.from_file')
    def test_load_config_parse_error(self, mock_from_file):
        """Test error when config file is invalid."""
        mock_from_file.side_effect = ValueError("Invalid YAML")

        with pytest.raises(click.ClickException) as exc_info:
            load_config(Path("/tmp/config.yaml"))

        assert "Error loading configuration" in str(exc_info.value)

    def test_get_spark_client_with_server(self, mock_config_file):
        """Test client creation with specific server."""
        with patch('spark_history_mcp.cli.commands.apps.SparkRestClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            client = get_spark_client(mock_config_file, "local")

            assert client == mock_client
            mock_client_class.assert_called_once()

    def test_get_spark_client_invalid_server(self, mock_config_file):
        """Test error when specified server doesn't exist."""
        with pytest.raises(click.ClickException) as exc_info:
            get_spark_client(mock_config_file, "nonexistent")

        assert "Server 'nonexistent' not found" in str(exc_info.value)

    def test_get_spark_client_default_server(self, mock_config_file):
        """Test client creation with default server."""
        with patch('spark_history_mcp.cli.commands.apps.SparkRestClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            client = get_spark_client(mock_config_file)

            assert client == mock_client


class TestApplicationIdentification:
    """Test application ID vs name identification."""

    def test_is_application_id_positive_cases(self):
        """Test positive cases for app ID detection."""
        app_ids = [
            "spark-123456789",
            "app-20231201-abcdef",
            "spark-application-1"
        ]

        for app_id in app_ids:
            assert is_application_id(app_id), f"Should recognize {app_id} as app ID"

    def test_is_application_id_negative_cases(self):
        """Test negative cases for app ID detection."""
        app_names = [
            "ETL Pipeline",
            "data-processing-job",
            "MyApplication",
            "daily-batch"
        ]

        for name in app_names:
            assert not is_application_id(name), f"Should recognize {name} as app name"

    def test_resolve_app_identifier_with_id(self):
        """Test resolving identifier that's already an app ID."""
        mock_client = MagicMock()

        result = resolve_app_identifier(mock_client, "app-123456")

        assert result == "app-123456"

    @patch('spark_history_mcp.tools.tools.list_applications')
    def test_resolve_app_identifier_with_name(self, mock_list_applications):
        """Test resolving app name to ID."""
        mock_client = MagicMock()
        mock_app = MagicMock(); mock_app.id = "app-resolved-123"
        mock_list_applications.return_value = [mock_app]

        result = resolve_app_identifier(mock_client, "ETL Pipeline")

        assert result == "app-resolved-123"
        mock_list_applications.assert_called_once_with(
            server=None,
            app_name="ETL Pipeline",
            search_type="contains",
            limit=1
        )

    @patch('spark_history_mcp.tools.tools.list_applications')
    def test_resolve_app_identifier_name_not_found(self, mock_list_applications):
        """Test error when app name can't be resolved."""
        mock_client = MagicMock()
        mock_list_applications.return_value = []

        with pytest.raises(click.ClickException) as exc_info:
            resolve_app_identifier(mock_client, "NonexistentApp")

        assert "No application found matching name: NonexistentApp" in str(exc_info.value)


class TestMockContextCreation:
    """Test mock context creation for tools integration."""

    def test_create_mock_context_structure(self):
        """Test mock context has correct structure."""
        mock_client = MagicMock()
        context = create_mock_context(mock_client)

        assert hasattr(context, 'request_context')
        assert hasattr(context.request_context, 'lifespan_context')
        assert context.request_context.lifespan_context.default_client == mock_client
        assert context.request_context.lifespan_context.clients["default"] == mock_client


class TestAppsListCommand:
    """Test the apps list command."""

    @patch('spark_history_mcp.cli.commands.apps.get_spark_client')
    @patch('spark_history_mcp.tools.tools.list_applications')
    def test_apps_list_basic(self, mock_list_applications, mock_get_client, cli_runner):
        """Test basic app listing."""
        # Setup mocks
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_apps = [
            MagicMock(id="app-123", name="App 1"),
            MagicMock(id="app-456", name="App 2")
        ]
        mock_list_applications.return_value = mock_apps

        # Run command
        result = cli_runner.invoke(apps, [
            'list',
            '--format', 'json',
            '--limit', '10'
        ], obj={'config_path': Path('/tmp/config.yaml'), 'quiet': False})

        # Verify success
        assert result.exit_code == 0
        mock_list_applications.assert_called_once()

    @patch('spark_history_mcp.cli.commands.apps.get_spark_client')
    @patch('spark_history_mcp.tools.tools.list_applications')
    def test_apps_list_with_status_filter(self, mock_list_applications, mock_get_client, cli_runner):
        """Test app listing with status filter."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_list_applications.return_value = []

        result = cli_runner.invoke(apps, [
            'list',
            '--status', 'COMPLETED',
            '--status', 'RUNNING'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        mock_list_applications.assert_called_once()
        call_args = mock_list_applications.call_args
        assert 'COMPLETED' in call_args.kwargs['status']
        assert 'RUNNING' in call_args.kwargs['status']

    @patch('spark_history_mcp.cli.commands.apps.get_spark_client')
    @patch('spark_history_mcp.tools.tools.list_applications')
    def test_apps_list_with_name_filter(self, mock_list_applications, mock_get_client, cli_runner):
        """Test app listing with name filter."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_list_applications.return_value = []

        result = cli_runner.invoke(apps, [
            'list',
            '--name', 'ETL'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        call_args = mock_list_applications.call_args
        assert call_args.kwargs['app_name'] == 'ETL'

    @patch('spark_history_mcp.cli.commands.apps.get_spark_client')
    def test_apps_list_client_error(self, mock_get_client, cli_runner):
        """Test error handling when client creation fails."""
        mock_get_client.side_effect = Exception("Connection failed")

        result = cli_runner.invoke(apps, ['list'], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code != 0
        assert "Error listing applications" in result.output


class TestAppsShowCommand:
    """Test the apps show command."""

    @patch('spark_history_mcp.cli.commands.apps.get_spark_client')
    @patch('spark_history_mcp.tools.tools.get_application')
    def test_apps_show_success(self, mock_get_application, mock_get_client, cli_runner):
        """Test successful app show command."""
        # Setup mocks
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_app = MagicMock(id="app-123", name="Test App")
        mock_get_application.return_value = mock_app

        # Run command
        result = cli_runner.invoke(apps, [
            'show', 'app-123',
            '--format', 'json'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        # Verify success
        assert result.exit_code == 0
        call_args = mock_get_application.call_args
        assert call_args.args[0] == "app-123"
        assert call_args.kwargs.get('server') is None

    @patch('spark_history_mcp.cli.commands.apps.get_spark_client')
    @patch('spark_history_mcp.tools.tools.get_application')
    def test_apps_show_error(self, mock_get_application, mock_get_client, cli_runner):
        """Test error when app identifier can't be resolved."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_get_application.side_effect = click.ClickException("App not found")

        result = cli_runner.invoke(apps, ['show', 'NonexistentApp'], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code != 0


class TestAppsSummaryCommand:
    """Test the apps summary command."""

    @patch('spark_history_mcp.cli.commands.apps.get_spark_client')
    @patch('spark_history_mcp.tools.tools.get_app_summary')
    def test_apps_summary_success(self, mock_get_app_summary, mock_get_client, cli_runner):
        """Test successful app summary command."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_summary = {"performance": {"cpu_time": 180000}}
        mock_get_app_summary.return_value = mock_summary

        result = cli_runner.invoke(apps, [
            'summary', 'app-123'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        call_args = mock_get_app_summary.call_args
        assert call_args.args[0] == "app-123"
        assert call_args.kwargs.get('server') is None


class TestAppsJobsCommand:
    """Test the apps jobs command."""

    @patch('spark_history_mcp.cli.commands.apps.get_spark_client')
    @patch('spark_history_mcp.tools.tools.list_jobs')
    def test_apps_jobs_basic(self, mock_list_jobs, mock_get_client, cli_runner):
        """Test basic jobs listing."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_jobs = [MagicMock(job_id=1, name="Job 1")]
        mock_list_jobs.return_value = mock_jobs

        result = cli_runner.invoke(apps, [
            'jobs', 'app-123',
            '--status', 'SUCCEEDED'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        call_args = mock_list_jobs.call_args
        assert call_args.kwargs.get('app_id') == 'app-123'
        assert call_args.kwargs.get('server') is None
        assert call_args.kwargs.get('status') == ['SUCCEEDED']

    # Note: slowest jobs is covered under analyze CLI tests


class TestAppsStagesCommand:
    """Test the apps stages command."""

    @patch('spark_history_mcp.cli.commands.apps.get_spark_client')
    @patch('spark_history_mcp.tools.tools.list_stages')
    def test_apps_stages_basic(self, mock_list_stages, mock_get_client, cli_runner):
        """Test basic stages listing."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_stages = [MagicMock(stage_id=1, name="Stage 1")]
        mock_list_stages.return_value = mock_stages

        result = cli_runner.invoke(apps, ['stages', 'app-123'], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code == 0
        call_args = mock_list_stages.call_args
        assert call_args.kwargs.get('app_id') == 'app-123'
        assert call_args.kwargs.get('server') is None


class TestAppsParameterValidation:
    """Test parameter validation for apps commands."""

    def test_invalid_format_option(self, cli_runner):
        """Test invalid format option."""
        result = cli_runner.invoke(apps, [
            'list', '--format', 'invalid'
        ], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code != 0

    def test_negative_limit(self, cli_runner):
        """Negative limit is accepted (int type), ensure no crash."""
        result = cli_runner.invoke(apps, ['list', '--limit', '-1'], obj={'config_path': Path('/tmp/config.yaml')})
        assert result.exit_code in (0, 2)

    def test_missing_app_identifier(self, cli_runner):
        """Test missing app identifier for show command."""
        result = cli_runner.invoke(apps, ['show'], obj={'config_path': Path('/tmp/config.yaml')})

        assert result.exit_code != 0
