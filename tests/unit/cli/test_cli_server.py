"""
Tests for spark_history_mcp.cli.commands.server module.
"""

import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

try:
    from click.testing import CliRunner
    import click
    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False

if CLI_AVAILABLE:
    from spark_history_mcp.cli.commands.server import server, start_server, test_server, status_server


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestServerCommand:
    """Test the server command group."""

    def test_server_group(self):
        """Test that server command group is properly defined."""
        assert server.name == "server"
        assert server.help == "Commands for managing the MCP server."

        # Check that all subcommands are registered
        command_names = [cmd.name for cmd in server.commands.values()]
        assert "start" in command_names
        assert "test" in command_names
        assert "status" in command_names


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestStartServer:
    """Test the start server command."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def sample_config_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"
            config_data = {
                "servers": {
                    "local": {
                        "url": "http://localhost:18080",
                        "default": True,
                        "verify_ssl": True
                    }
                },
                "mcp": {
                    "transports": ["stdio"],
                    "port": 18888,
                    "debug": False
                }
            }

            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            yield config_path

    @patch('spark_history_mcp.core.app.run')
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_start_server_default(self, mock_config, mock_app_run, runner, sample_config_path):
        """Test starting server with default configuration."""
        from spark_history_mcp.config.config import Config, McpConfig

        # Create mock config
        mock_mcp_config = McpConfig(transports=["stdio"], port=18888, debug=False)
        mock_config_obj = MagicMock()
        mock_config_obj.mcp = mock_mcp_config
        mock_config.return_value = mock_config_obj

        # Mock app.run to avoid actually starting server
        mock_app_run.return_value = None

        result = runner.invoke(
            start_server,
            [],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0
        assert "Starting MCP server on port 18888..." in result.output
        mock_config.assert_called_once_with(str(sample_config_path))
        mock_app_run.assert_called_once_with(mock_config_obj)

    @patch('spark_history_mcp.core.app.run')
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_start_server_with_port_override(self, mock_config, mock_app_run, runner, sample_config_path):
        """Test starting server with custom port."""
        from spark_history_mcp.config.config import Config, McpConfig

        # Create mock config
        mock_mcp_config = McpConfig(transports=["stdio"], port=18888, debug=False)
        mock_config_obj = MagicMock()
        mock_config_obj.mcp = mock_mcp_config
        mock_config.return_value = mock_config_obj

        mock_app_run.return_value = None

        result = runner.invoke(
            start_server,
            ["--port", "19000"],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0
        assert "Starting MCP server on port 19000..." in result.output
        # Verify port was overridden
        assert mock_config_obj.mcp.port == 19000
        mock_app_run.assert_called_once_with(mock_config_obj)

    @patch('spark_history_mcp.core.app.run')
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_start_server_with_debug(self, mock_config, mock_app_run, runner, sample_config_path):
        """Test starting server with debug mode enabled."""
        from spark_history_mcp.config.config import Config, McpConfig

        # Create mock config
        mock_mcp_config = McpConfig(transports=["stdio"], port=18888, debug=False)
        mock_config_obj = MagicMock()
        mock_config_obj.mcp = mock_mcp_config
        mock_config.return_value = mock_config_obj

        mock_app_run.return_value = None

        result = runner.invoke(
            start_server,
            ["--debug"],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0
        assert "Starting MCP server on port 18888..." in result.output
        assert "Debug mode enabled" in result.output
        # Verify debug was enabled
        assert mock_config_obj.mcp.debug is True
        mock_app_run.assert_called_once_with(mock_config_obj)

    @patch('spark_history_mcp.core.app.run')
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_start_server_with_transport(self, mock_config, mock_app_run, runner, sample_config_path):
        """Test starting server with custom transport."""
        from spark_history_mcp.config.config import Config, McpConfig

        # Create mock config
        mock_mcp_config = McpConfig(transports=["stdio"], port=18888, debug=False)
        mock_config_obj = MagicMock()
        mock_config_obj.mcp = mock_mcp_config
        mock_config.return_value = mock_config_obj

        mock_app_run.return_value = None

        result = runner.invoke(
            start_server,
            ["--transport", "streamable-http"],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0
        # Verify transport was overridden
        assert mock_config_obj.mcp.transports == ["streamable-http"]
        mock_app_run.assert_called_once_with(mock_config_obj)

    @patch('spark_history_mcp.core.app.run')
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_start_server_all_options(self, mock_config, mock_app_run, runner, sample_config_path):
        """Test starting server with all options combined."""
        from spark_history_mcp.config.config import Config, McpConfig

        # Create mock config
        mock_mcp_config = McpConfig(transports=["stdio"], port=18888, debug=False)
        mock_config_obj = MagicMock()
        mock_config_obj.mcp = mock_mcp_config
        mock_config.return_value = mock_config_obj

        mock_app_run.return_value = None

        result = runner.invoke(
            start_server,
            ["--port", "19999", "--debug", "--transport", "sse"],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0
        assert "Starting MCP server on port 19999..." in result.output
        assert "Debug mode enabled" in result.output

        # Verify all overrides
        assert mock_config_obj.mcp.port == 19999
        assert mock_config_obj.mcp.debug is True
        assert mock_config_obj.mcp.transports == ["sse"]

    @patch('spark_history_mcp.core.app.run', side_effect=KeyboardInterrupt())
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_start_server_keyboard_interrupt(self, mock_config, mock_app_run, runner, sample_config_path):
        """Test handling of keyboard interrupt during server start."""
        from spark_history_mcp.config.config import Config, McpConfig

        mock_mcp_config = McpConfig(transports=["stdio"], port=18888, debug=False)
        mock_config_obj = MagicMock()
        mock_config_obj.mcp = mock_mcp_config
        mock_config.return_value = mock_config_obj

        result = runner.invoke(
            start_server,
            [],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0
        assert "Server stopped by user" in result.output

    @patch('spark_history_mcp.config.config.Config.from_file', side_effect=Exception("Config error"))
    def test_start_server_config_error(self, mock_config, runner, sample_config_path):
        """Test handling of configuration loading errors."""
        result = runner.invoke(
            start_server,
            [],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code != 0
        assert "Error starting server: Config error" in result.output

    @patch('spark_history_mcp.core.app.run', side_effect=Exception("Server error"))
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_start_server_runtime_error(self, mock_config, mock_app_run, runner, sample_config_path):
        """Test handling of runtime errors during server start."""
        from spark_history_mcp.config.config import Config, McpConfig

        mock_mcp_config = McpConfig(transports=["stdio"], port=18888, debug=False)
        mock_config_obj = MagicMock()
        mock_config_obj.mcp = mock_mcp_config
        mock_config.return_value = mock_config_obj

        result = runner.invoke(
            start_server,
            [],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code != 0
        assert "Error starting server: Server error" in result.output


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestTestServer:
    """Test the test server command."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def sample_config_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"
            config_data = {
                "servers": {
                    "local": {
                        "url": "http://localhost:18080",
                        "default": True,
                        "verify_ssl": True
                    },
                    "production": {
                        "url": "https://prod.spark.com:18080",
                        "default": False,
                        "verify_ssl": False
                    }
                },
                "mcp": {
                    "transports": ["stdio"],
                    "port": 18888,
                    "debug": False
                }
            }

            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            yield config_path

    @patch('requests.get')
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_test_server_success(self, mock_config, mock_requests, runner, sample_config_path):
        """Test successful server connectivity test."""
        from spark_history_mcp.config.config import Config, ServerConfig

        # Create mock config
        mock_server_config1 = ServerConfig(url="http://localhost:18080", default=True, verify_ssl=True)
        mock_server_config2 = ServerConfig(url="https://prod.spark.com:18080", default=False, verify_ssl=False)
        mock_config_obj = MagicMock()
        mock_config_obj.servers = {
            "local": mock_server_config1,
            "production": mock_server_config2
        }
        mock_config.return_value = mock_config_obj

        # Mock successful HTTP responses
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_requests.return_value = mock_response

        result = runner.invoke(
            test_server,
            [],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0
        assert "Testing server configuration..." in result.output
        assert f"Configuration loaded from {sample_config_path}" in result.output
        assert "Found 2 server(s) configured" in result.output
        assert "Testing connection to 'local'" in result.output
        assert "Testing connection to 'production'" in result.output
        assert "Connection to 'local' successful" in result.output
        assert "Connection to 'production' successful" in result.output
        assert "Server test completed" in result.output

        # Verify HTTP requests were made with correct parameters
        assert mock_requests.call_count == 2
        calls = mock_requests.call_args_list

        # First call (local server)
        assert calls[0][0][0] == "http://localhost:18080/api/v1/applications"
        assert calls[0][1]["timeout"] == 10
        assert calls[0][1]["verify"] is True

        # Second call (production server)
        assert calls[1][0][0] == "https://prod.spark.com:18080/api/v1/applications"
        assert calls[1][1]["verify"] is False

    @patch('requests.get')
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_test_server_with_custom_timeout(self, mock_config, mock_requests, runner, sample_config_path):
        """Test server connectivity test with custom timeout."""
        from spark_history_mcp.config.config import Config, ServerConfig

        mock_server_config = ServerConfig(url="http://localhost:18080", default=True, verify_ssl=True)
        mock_config_obj = MagicMock()
        mock_config_obj.servers = {"local": mock_server_config}
        mock_config.return_value = mock_config_obj

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_requests.return_value = mock_response

        result = runner.invoke(
            test_server,
            ["--timeout", "30"],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0
        assert "Connection to 'local' successful" in result.output

        # Verify timeout was used
        mock_requests.assert_called_once()
        call_args = mock_requests.call_args
        assert call_args[1]["timeout"] == 30

    @patch('requests.get')
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_test_server_timeout_error(self, mock_config, mock_requests, runner, sample_config_path):
        """Test handling of timeout errors during server test."""
        import requests
        from spark_history_mcp.config.config import Config, ServerConfig

        mock_server_config = ServerConfig(url="http://localhost:18080", default=True, verify_ssl=True)
        mock_config_obj = MagicMock()
        mock_config_obj.servers = {"local": mock_server_config}
        mock_config.return_value = mock_config_obj

        # Mock timeout error
        mock_requests.side_effect = requests.exceptions.Timeout("Request timed out")

        result = runner.invoke(
            test_server,
            ["--timeout", "5"],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0  # Command succeeds but reports connection failure
        assert "Connection to 'local' timed out after 5s" in result.output

    @patch('requests.get')
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_test_server_connection_error(self, mock_config, mock_requests, runner, sample_config_path):
        """Test handling of connection errors during server test."""
        import requests
        from spark_history_mcp.config.config import Config, ServerConfig

        mock_server_config = ServerConfig(url="http://localhost:18080", default=True, verify_ssl=True)
        mock_config_obj = MagicMock()
        mock_config_obj.servers = {"unreachable": mock_server_config}
        mock_config.return_value = mock_config_obj

        # Mock connection error
        mock_requests.side_effect = requests.exceptions.ConnectionError("Connection refused")

        result = runner.invoke(
            test_server,
            [],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0  # Command succeeds but reports connection failure
        assert "Could not connect to 'unreachable'" in result.output

    @patch('requests.get')
    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_test_server_http_error(self, mock_config, mock_requests, runner, sample_config_path):
        """Test handling of HTTP errors during server test."""
        import requests
        from spark_history_mcp.config.config import Config, ServerConfig

        mock_server_config = ServerConfig(url="http://localhost:18080", default=True, verify_ssl=True)
        mock_config_obj = MagicMock()
        mock_config_obj.servers = {"local": mock_server_config}
        mock_config.return_value = mock_config_obj

        # Mock HTTP error
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_requests.return_value = mock_response

        result = runner.invoke(
            test_server,
            [],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0  # Command succeeds but reports connection failure
        assert "Error connecting to 'local': 404 Not Found" in result.output

    @patch('spark_history_mcp.config.config.Config.from_file', side_effect=Exception("Config error"))
    def test_test_server_config_error(self, mock_config, runner, sample_config_path):
        """Test handling of configuration loading errors."""
        result = runner.invoke(
            test_server,
            [],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code != 0
        assert "Error testing server: Config error" in result.output


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestStatusServer:
    """Test the status server command."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def sample_config_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"
            config_data = {
                "servers": {
                    "local": {
                        "url": "http://localhost:18080",
                        "default": True,
                        "verify_ssl": True
                    },
                    "production": {
                        "url": "https://prod.spark.com:18080",
                        "default": False,
                        "verify_ssl": False,
                        "emr_cluster_arn": "arn:aws:elasticmapreduce:us-west-2:123:cluster/j-ABC"
                    }
                },
                "mcp": {
                    "transports": ["stdio", "streamable-http"],
                    "port": 19000,
                    "debug": True
                }
            }

            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            yield config_path

    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_status_server_basic(self, mock_config, runner, sample_config_path):
        """Test basic server status display."""
        from spark_history_mcp.config.config import Config, ServerConfig, McpConfig

        # Create mock config
        mock_server_config1 = ServerConfig(url="http://localhost:18080", default=True, verify_ssl=True)
        mock_server_config2 = ServerConfig(
            url="https://prod.spark.com:18080",
            default=False,
            verify_ssl=False,
            emr_cluster_arn="arn:aws:elasticmapreduce:us-west-2:123:cluster/j-ABC"
        )
        mock_mcp_config = McpConfig(transports=["stdio", "streamable-http"], port=19000, debug=True)

        mock_config_obj = MagicMock()
        mock_config_obj.servers = {
            "local": mock_server_config1,
            "production": mock_server_config2
        }
        mock_config_obj.mcp = mock_mcp_config
        mock_config.return_value = mock_config_obj

        result = runner.invoke(
            status_server,
            [],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0
        assert "Server Configuration:" in result.output
        assert f"Config file: {sample_config_path}" in result.output
        assert "MCP port: 19000" in result.output
        assert "MCP transports: stdio, streamable-http" in result.output
        assert "Debug mode: True" in result.output
        assert "Configured Servers (2):" in result.output
        assert "local (default): http://localhost:18080" in result.output
        assert "production: https://prod.spark.com:18080" in result.output
        assert "EMR Cluster: arn:aws:elasticmapreduce:us-west-2:123:cluster/j-ABC" in result.output
        assert "SSL verification: disabled" in result.output

    @patch('spark_history_mcp.config.config.Config.from_file')
    def test_status_server_simple_config(self, mock_config, runner, sample_config_path):
        """Test status display with simple configuration."""
        from spark_history_mcp.config.config import Config, ServerConfig, McpConfig

        # Create simple mock config
        mock_server_config = ServerConfig(url="http://localhost:18080", default=True, verify_ssl=True)
        mock_mcp_config = McpConfig(transports=["stdio"], port=18888, debug=False)

        mock_config_obj = MagicMock()
        mock_config_obj.servers = {"local": mock_server_config}
        mock_config_obj.mcp = mock_mcp_config
        mock_config.return_value = mock_config_obj

        result = runner.invoke(
            status_server,
            [],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0
        assert "MCP port: 18888" in result.output
        assert "MCP transports: stdio" in result.output
        assert "Debug mode: False" in result.output
        assert "Configured Servers (1):" in result.output
        assert "local (default): http://localhost:18080" in result.output

        # These should not appear in simple config
        assert "EMR Cluster:" not in result.output
        assert "SSL verification: disabled" not in result.output

    @patch('spark_history_mcp.config.config.Config.from_file', side_effect=Exception("Config error"))
    def test_status_server_config_error(self, mock_config, runner, sample_config_path):
        """Test handling of configuration loading errors."""
        result = runner.invoke(
            status_server,
            [],
            obj={"config_path": sample_config_path}
        )

        assert result.exit_code != 0
        assert "Error getting server status: Config error" in result.output