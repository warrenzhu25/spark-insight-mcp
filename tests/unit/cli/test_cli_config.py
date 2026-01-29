"""
Tests for spark_history_mcp.cli.commands.config module.
"""

import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

try:
    from click.testing import CliRunner

    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False

if CLI_AVAILABLE:
    from spark_history_mcp.cli.commands.config import (
        config_cmd,
        edit_config,
        init_config,
        show_config,
        validate_config,
    )


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestConfigCommand:
    """Test the config command group."""

    def test_config_cmd_group(self):
        """Test that config command group is properly defined."""
        assert config_cmd.name == "config"
        assert config_cmd.help == "Commands for managing configuration."

        # Check that all subcommands are registered
        command_names = [cmd.name for cmd in config_cmd.commands.values()]
        assert "init" in command_names
        assert "show" in command_names
        assert "validate" in command_names
        assert "edit" in command_names


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestInitConfig:
    """Test the init config command."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def temp_config_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir) / "config.yaml"

    def test_init_config_default_non_interactive(self, runner, temp_config_path):
        """Test default configuration creation in non-interactive mode."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                init_config, [], obj={"config_path": temp_config_path}
            )

            assert result.exit_code == 0
            assert f"Configuration created at {temp_config_path}" in result.output
            assert temp_config_path.exists()

            # Verify configuration content
            with open(temp_config_path) as f:
                config_data = yaml.safe_load(f)

            assert "servers" in config_data
            assert "local" in config_data["servers"]
            assert config_data["servers"]["local"]["default"] is True
            assert config_data["servers"]["local"]["url"] == "http://localhost:18080"
            assert "mcp" in config_data
            assert config_data["mcp"]["port"] == 18888
            assert config_data["mcp"]["debug"] is False

    def test_init_config_interactive_basic_auth(self, runner, temp_config_path):
        """Test interactive configuration with basic authentication."""
        with runner.isolated_filesystem():
            # Mock user inputs for interactive mode
            inputs = [
                "19000",  # port
                "y",  # debug mode
                "production",  # server name
                "https://spark.example.com:18080",  # server URL
                "y",  # make default
                "y",  # verify SSL
                "y",  # auth needed
                "basic",  # auth type
                "testuser",  # username
                "testpass",  # password
                "n",  # not EMR
            ]

            result = runner.invoke(
                init_config,
                ["--interactive"],
                input="\n".join(inputs),
                obj={"config_path": temp_config_path},
            )

            assert result.exit_code == 0
            assert temp_config_path.exists()

            # Verify configuration content
            with open(temp_config_path) as f:
                config_data = yaml.safe_load(f)

            assert (
                config_data["servers"]["production"]["url"]
                == "https://spark.example.com:18080"
            )
            assert (
                config_data["servers"]["production"]["auth"]["username"] == "testuser"
            )
            assert (
                config_data["servers"]["production"]["auth"]["password"] == "testpass"  # noqa: S105
            )
            assert config_data["mcp"]["port"] == 19000
            assert config_data["mcp"]["debug"] is True

    def test_init_config_interactive_token_auth(self, runner, temp_config_path):
        """Test interactive configuration with token authentication."""
        with runner.isolated_filesystem():
            inputs = [
                "18888",  # port
                "n",  # no debug
                "staging",  # server name
                "http://staging.spark.com:18080",  # server URL
                "n",  # not default
                "n",  # no SSL verification
                "y",  # auth needed
                "token",  # auth type
                "abc123token",  # token
                "n",  # not EMR
            ]

            result = runner.invoke(
                init_config,
                ["--interactive"],
                input="\n".join(inputs),
                obj={"config_path": temp_config_path},
            )

            assert result.exit_code == 0

            with open(temp_config_path) as f:
                config_data = yaml.safe_load(f)

            assert config_data["servers"]["staging"]["auth"]["token"] == "abc123token"  # noqa: S105
            assert config_data["servers"]["staging"]["default"] is False
            assert config_data["servers"]["staging"]["verify_ssl"] is False

    def test_init_config_interactive_emr_cluster(self, runner, temp_config_path):
        """Test interactive configuration with EMR cluster."""
        with runner.isolated_filesystem():
            inputs = [
                "18888",  # port
                "n",  # no debug
                "emr",  # server name
                "http://emr.amazonaws.com:18080",  # server URL
                "y",  # make default
                "y",  # verify SSL
                "n",  # no auth needed
                "y",  # is EMR
                "arn:aws:elasticmapreduce:us-west-2:123456789:cluster/j-EXAMPLE",  # EMR ARN
            ]

            result = runner.invoke(
                init_config,
                ["--interactive"],
                input="\n".join(inputs),
                obj={"config_path": temp_config_path},
            )

            assert result.exit_code == 0

            with open(temp_config_path) as f:
                config_data = yaml.safe_load(f)

            assert "emr_cluster_arn" in config_data["servers"]["emr"]
            assert (
                config_data["servers"]["emr"]["emr_cluster_arn"]
                == "arn:aws:elasticmapreduce:us-west-2:123456789:cluster/j-EXAMPLE"
            )

    def test_init_config_force_overwrite(self, runner, temp_config_path):
        """Test force overwrite of existing configuration."""
        # Create existing config
        temp_config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(temp_config_path, "w") as f:
            f.write("existing: config\n")

        with runner.isolated_filesystem():
            result = runner.invoke(
                init_config, ["--force"], obj={"config_path": temp_config_path}
            )

            assert result.exit_code == 0
            assert "Configuration created" in result.output

            # Verify old config was overwritten
            with open(temp_config_path) as f:
                config_data = yaml.safe_load(f)
            assert "existing" not in config_data
            assert "servers" in config_data

    def test_init_config_confirm_overwrite(self, runner, temp_config_path):
        """Test confirmation prompt for overwriting existing config."""
        # Create existing config
        temp_config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(temp_config_path, "w") as f:
            f.write("existing: config\n")

        with runner.isolated_filesystem():
            # User confirms overwrite
            result = runner.invoke(
                init_config, [], input="y\n", obj={"config_path": temp_config_path}
            )

            assert result.exit_code == 0
            assert "Configuration created" in result.output

    def test_init_config_cancel_overwrite(self, runner, temp_config_path):
        """Test canceling overwrite of existing config."""
        # Create existing config
        temp_config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(temp_config_path, "w") as f:
            f.write("existing: config\n")

        with runner.isolated_filesystem():
            # User cancels overwrite
            result = runner.invoke(
                init_config, [], input="n\n", obj={"config_path": temp_config_path}
            )

            assert result.exit_code == 0
            assert "Configuration initialization cancelled" in result.output

            # Verify original config unchanged
            with open(temp_config_path) as f:
                content = f.read()
            assert "existing: config" in content

    @patch("builtins.open", side_effect=PermissionError("Permission denied"))
    def test_init_config_permission_error(self, mock_file, runner, temp_config_path):
        """Test handling of permission errors during config creation."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                init_config, [], obj={"config_path": temp_config_path}
            )

            assert result.exit_code != 0
            assert "Error creating configuration" in result.output
            assert "Permission denied" in result.output

    def test_init_config_create_parent_directories(self, runner):
        """Test that parent directories are created if they don't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "deep" / "nested" / "config.yaml"

            with runner.isolated_filesystem():
                result = runner.invoke(
                    init_config, [], obj={"config_path": config_path}
                )

                assert result.exit_code == 0
                assert config_path.exists()
                assert config_path.parent.exists()


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestShowConfig:
    """Test the show config command."""

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
                        "verify_ssl": True,
                    },
                    "production": {
                        "url": "https://prod.spark.com:18080",
                        "default": False,
                        "verify_ssl": True,
                        "emr_cluster_arn": "arn:aws:elasticmapreduce:us-west-2:123:cluster/j-ABC",
                    },
                },
                "mcp": {"transports": ["stdio"], "port": 18888, "debug": False},
            }

            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            yield config_path

    def test_show_config_human_format(self, runner, sample_config_path):
        """Test showing configuration in human-readable format."""
        result = runner.invoke(show_config, [], obj={"config_path": sample_config_path})

        assert result.exit_code == 0
        assert f"Configuration from: {sample_config_path}" in result.output
        assert "MCP Server:" in result.output
        assert "Port: 18888" in result.output
        assert "Transports: stdio" in result.output
        assert "Debug: False" in result.output
        assert "Servers (2):" in result.output
        assert "local (default): http://localhost:18080" in result.output
        assert "production: https://prod.spark.com:18080" in result.output

    def test_show_config_json_format(self, runner, sample_config_path):
        """Test showing configuration in JSON format."""
        result = runner.invoke(
            show_config, ["--format", "json"], obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0

        # Verify it's valid JSON
        config_data = json.loads(result.output)
        assert "servers" in config_data
        assert "mcp" in config_data
        assert config_data["mcp"]["port"] == 18888

    def test_show_config_yaml_format(self, runner, sample_config_path):
        """Test showing configuration in YAML format."""
        result = runner.invoke(
            show_config, ["--format", "yaml"], obj={"config_path": sample_config_path}
        )

        assert result.exit_code == 0

        # Verify it's valid YAML
        config_data = yaml.safe_load(result.output)
        assert "servers" in config_data
        assert "mcp" in config_data

    def test_show_specific_server(self, runner, sample_config_path):
        """Test showing specific server configuration."""
        result = runner.invoke(
            show_config,
            ["--server", "production"],
            obj={"config_path": sample_config_path},
        )

        assert result.exit_code == 0
        assert "Server 'production':" in result.output
        assert "URL: https://prod.spark.com:18080" in result.output
        assert "Default: False" in result.output
        assert "Verify SSL: True" in result.output
        assert (
            "EMR Cluster: arn:aws:elasticmapreduce:us-west-2:123:cluster/j-ABC"
            in result.output
        )

    def test_show_nonexistent_server(self, runner, sample_config_path):
        """Test showing configuration for non-existent server."""
        result = runner.invoke(
            show_config,
            ["--server", "nonexistent"],
            obj={"config_path": sample_config_path},
        )

        assert result.exit_code != 0
        assert "Server 'nonexistent' not found in configuration" in result.output

    def test_show_config_file_not_found(self, runner):
        """Test showing configuration when file doesn't exist (returns default config)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_path = Path(temp_dir) / "missing.yaml"

            result = runner.invoke(show_config, [], obj={"config_path": missing_path})

            assert result.exit_code == 0
            assert "Configuration from:" in result.output
            assert "MCP Server:" in result.output

    def test_show_config_invalid_yaml(self, runner):
        """Test showing configuration with invalid YAML."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "invalid.yaml"
            with open(config_path, "w") as f:
                f.write("invalid: yaml: content: [\n")

            result = runner.invoke(show_config, [], obj={"config_path": config_path})

            assert result.exit_code != 0
            assert "Error reading configuration" in result.output


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestValidateConfig:
    """Test the validate config command."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_validate_valid_config(self, runner):
        """Test validation of a valid configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "valid.yaml"
            config_data = {
                "servers": {
                    "local": {"url": "http://localhost:18080", "default": True}
                },
                "mcp": {"transports": ["stdio"], "port": 18888},
            }

            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            result = runner.invoke(
                validate_config, [], obj={"config_path": config_path}
            )

            assert result.exit_code == 0
            assert "Configuration file is valid" in result.output
            assert "Configuration contains 1 server(s)" in result.output

    def test_validate_no_default_server(self, runner):
        """Test validation warning for no default server."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "no_default.yaml"
            config_data = {
                "servers": {
                    "server1": {"url": "http://server1:18080", "default": False},
                    "server2": {"url": "http://server2:18080", "default": False},
                },
                "mcp": {"transports": ["stdio"], "port": 18888},
            }

            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            result = runner.invoke(
                validate_config, [], obj={"config_path": config_path}
            )

            assert result.exit_code == 0
            assert "Configuration file is valid" in result.output
            assert "Warning: No default server configured" in result.output

    def test_validate_multiple_default_servers(self, runner):
        """Test validation warning for multiple default servers."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "multi_default.yaml"
            config_data = {
                "servers": {
                    "server1": {"url": "http://server1:18080", "default": True},
                    "server2": {"url": "http://server2:18080", "default": True},
                },
                "mcp": {"transports": ["stdio"], "port": 18888},
            }

            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            result = runner.invoke(
                validate_config, [], obj={"config_path": config_path}
            )

            assert result.exit_code == 0
            assert "Configuration file is valid" in result.output
            assert (
                "Warning: Multiple default servers: server1, server2" in result.output
            )

    def test_validate_server_no_url_or_emr(self, runner):
        """Test validation warning for server with no URL or EMR cluster."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "no_url.yaml"
            config_data = {
                "servers": {
                    "incomplete": {
                        "default": True
                        # Missing URL and EMR cluster
                    }
                },
                "mcp": {"transports": ["stdio"], "port": 18888},
            }

            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            result = runner.invoke(
                validate_config, [], obj={"config_path": config_path}
            )

            assert result.exit_code == 0
            assert "Configuration file is valid" in result.output
            assert (
                "Warning: Server 'incomplete' has no URL or EMR cluster configured"
                in result.output
            )

    def test_validate_invalid_config(self, runner):
        """Test validation of invalid configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "invalid.yaml"
            with open(config_path, "w") as f:
                f.write("invalid: yaml: structure: [\n")

            result = runner.invoke(
                validate_config, [], obj={"config_path": config_path}
            )

            assert result.exit_code != 0
            assert "Configuration validation failed" in result.output


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestEditConfig:
    """Test the edit config command."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @patch("spark_history_mcp.cli.commands.config.subprocess.run")
    @patch("spark_history_mcp.cli.commands.config.os.environ.get")
    @patch("spark_history_mcp.cli.commands.config.shutil.which")
    def test_edit_existing_config(
        self, mock_which, mock_env_get, mock_subprocess, runner
    ):
        """Test editing existing configuration file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"

            # Create valid config
            config_data = {
                "servers": {
                    "local": {"url": "http://localhost:18080", "default": True}
                },
                "mcp": {"transports": ["stdio"], "port": 18888},
            }
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            # Mock environment and subprocess
            def env_get_side_effect(key, default=None):
                if key == "EDITOR":
                    return "nano"
                return default

            mock_env_get.side_effect = env_get_side_effect
            mock_which.return_value = "/usr/bin/nano"
            mock_subprocess.return_value = None

            result = runner.invoke(edit_config, [], obj={"config_path": config_path})

            assert result.exit_code == 0
            assert "Configuration file is valid after editing" in result.output
            mock_subprocess.assert_called_once_with(
                ["nano", str(config_path)], check=True
            )

    @patch("spark_history_mcp.cli.commands.config.subprocess.run")
    @patch("spark_history_mcp.cli.commands.config.os.environ.get")
    @patch("spark_history_mcp.cli.commands.config.shutil.which")
    def test_edit_config_custom_editor(
        self, mock_which, mock_env_get, mock_subprocess, runner
    ):
        """Test editing with custom editor."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"

            # Create valid config
            config_data = {
                "servers": {
                    "local": {"url": "http://localhost:18080", "default": True}
                },
                "mcp": {"transports": ["stdio"], "port": 18888},
            }
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            # Mock custom editor
            def env_get_side_effect(key, default=None):
                if key == "EDITOR":
                    return "vim"
                return default

            mock_env_get.side_effect = env_get_side_effect
            mock_which.return_value = "/usr/bin/vim"
            mock_subprocess.return_value = None

            result = runner.invoke(edit_config, [], obj={"config_path": config_path})

            assert result.exit_code == 0
            mock_subprocess.assert_called_once_with(
                ["vim", str(config_path)], check=True
            )

    def test_edit_nonexistent_config_create(self, runner):
        """Test editing non-existent config with user confirming creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "missing.yaml"

            with patch("subprocess.run") as mock_subprocess:
                mock_subprocess.return_value = None

                result = runner.invoke(
                    edit_config,
                    [],
                    input="y\n",  # Confirm creation
                    obj={"config_path": config_path},
                )

                # Should create config and then attempt to edit
                assert result.exit_code == 0
                assert config_path.exists()

    def test_edit_nonexistent_config_cancel(self, runner):
        """Test editing non-existent config with user canceling creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "missing.yaml"

            result = runner.invoke(
                edit_config,
                [],
                input="n\n",  # Cancel creation
                obj={"config_path": config_path},
            )

            assert result.exit_code == 0
            assert not config_path.exists()

    @patch(
        "spark_history_mcp.cli.commands.config.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "nano"),
    )
    @patch("spark_history_mcp.cli.commands.config.os.environ.get")
    @patch("spark_history_mcp.cli.commands.config.shutil.which")
    def test_edit_config_editor_error(
        self, mock_which, mock_env_get, mock_subprocess, runner
    ):
        """Test handling of editor errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"

            # Create valid config
            with open(config_path, "w") as f:
                yaml.dump(
                    {
                        "servers": {
                            "local": {"url": "http://localhost:18080", "default": True}
                        }
                    },
                    f,
                )

            def env_get_side_effect(key, default=None):
                if key == "EDITOR":
                    return "nano"
                return default

            mock_env_get.side_effect = env_get_side_effect
            mock_which.return_value = "/usr/bin/nano"

            result = runner.invoke(edit_config, [], obj={"config_path": config_path})

            assert result.exit_code != 0
            assert "Error opening editor" in result.output

    @patch("spark_history_mcp.cli.commands.config.subprocess.run")
    @patch("spark_history_mcp.cli.commands.config.os.environ.get")
    @patch("spark_history_mcp.cli.commands.config.shutil.which", return_value=None)
    def test_edit_config_editor_not_found(
        self, mock_which, mock_env_get, mock_subprocess, runner
    ):
        """Test handling when editor is not found."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"

            # Create valid config
            with open(config_path, "w") as f:
                yaml.dump(
                    {
                        "servers": {
                            "local": {"url": "http://localhost:18080", "default": True}
                        }
                    },
                    f,
                )

            def env_get_side_effect(key, default=None):
                if key == "EDITOR":
                    return "nonexistent_editor"
                return default

            mock_env_get.side_effect = env_get_side_effect
            mock_subprocess.side_effect = FileNotFoundError()

            result = runner.invoke(edit_config, [], obj={"config_path": config_path})

            assert result.exit_code != 0
            assert "Editor 'nonexistent_editor' not found" in result.output
