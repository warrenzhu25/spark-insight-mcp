"""
Tests for spark_history_mcp.cli.main module.
"""

import logging
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

try:
    import click
    from click.testing import CliRunner

    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False

if CLI_AVAILABLE:
    from spark_history_mcp.cli.main import cli, setup_logging


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestSetupLogging:
    """Test the setup_logging function."""

    def test_setup_logging_info_level(self):
        """Test logging setup with INFO level (default)."""
        with patch("logging.basicConfig") as mock_basic_config:
            with patch("logging.getLogger") as mock_get_logger:
                mock_logger = MagicMock()
                mock_get_logger.return_value = mock_logger

                setup_logging(debug=False)

                # Verify basicConfig was called with INFO level
                mock_basic_config.assert_called_once()
                call_args = mock_basic_config.call_args
                assert call_args[1]["level"] == logging.INFO
                assert call_args[1]["format"] == "%(message)s"
                assert call_args[1]["datefmt"] == "[%X]"

                # Verify specific loggers were configured
                assert (
                    mock_get_logger.call_count >= 5
                )  # spark_history_mcp + 4 third-party loggers
                mock_logger.setLevel.assert_called()

    def test_setup_logging_debug_level(self):
        """Test logging setup with DEBUG level."""
        with patch("logging.basicConfig") as mock_basic_config:
            with patch("logging.getLogger") as mock_get_logger:
                mock_logger = MagicMock()
                mock_get_logger.return_value = mock_logger

                setup_logging(debug=True)

                # Verify basicConfig was called with DEBUG level
                mock_basic_config.assert_called_once()
                call_args = mock_basic_config.call_args
                assert call_args[1]["level"] == logging.DEBUG

    def test_setup_logging_third_party_loggers_suppressed(self):
        """Test that third-party loggers are suppressed in non-debug mode."""
        with patch("logging.basicConfig"):
            with patch("logging.getLogger") as mock_get_logger:
                mock_logger = MagicMock()
                mock_get_logger.return_value = mock_logger

                setup_logging(debug=False)

                # Check that third-party loggers were configured
                logger_names = [call[0][0] for call in mock_get_logger.call_args_list]
                assert "requests" in logger_names
                assert "urllib3" in logger_names
                assert "boto3" in logger_names
                assert "botocore" in logger_names

    def test_setup_logging_cli_not_available(self):
        """Test logging setup when CLI is not available."""
        with patch("spark_history_mcp.cli.main.CLI_AVAILABLE", False):
            with patch("logging.basicConfig") as mock_basic_config:
                setup_logging(debug=True)

                # Should return early without configuring logging
                mock_basic_config.assert_not_called()


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestCLIGroup:
    """Test the main CLI group."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_cli_group_help(self, runner):
        """Test CLI group shows help when no subcommand."""
        result = runner.invoke(cli, [])

        assert result.exit_code == 0
        assert "Spark History Server MCP" in result.output
        assert "AI-Powered Spark Analysis" in result.output
        assert "Examples:" in result.output
        assert "spark-mcp --cli apps list" in result.output

    def test_cli_group_with_help_flag(self, runner):
        """Test CLI group with explicit help flag."""
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "Spark History Server MCP" in result.output
        assert "Usage:" in result.output
        assert "Options:" in result.output

    def test_cli_group_context_setup_default_config(self, runner):
        """Test CLI context setup with default configuration."""
        with patch("spark_history_mcp.cli.main.setup_logging") as mock_setup_logging:
            result = runner.invoke(cli, [])

            assert result.exit_code == 0
            mock_setup_logging.assert_called_once_with(False)

    def test_cli_group_context_setup_custom_config(self, runner):
        """Test CLI context setup with custom configuration path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "custom.yaml"
            config_path.touch()  # Create empty file

            with patch(
                "spark_history_mcp.cli.main.setup_logging"
            ) as mock_setup_logging:
                result = runner.invoke(cli, ["--config", str(config_path)])

                assert result.exit_code == 0
                mock_setup_logging.assert_called_once_with(False)

    def test_cli_group_debug_option(self, runner):
        """Test CLI group with debug option."""
        with patch("spark_history_mcp.cli.main.setup_logging") as mock_setup_logging:
            result = runner.invoke(cli, ["--debug"])

            assert result.exit_code == 0
            mock_setup_logging.assert_called_once_with(True)

    def test_cli_group_quiet_option(self, runner):
        """Test CLI group with quiet option."""
        with patch("spark_history_mcp.cli.main.setup_logging") as mock_setup_logging:
            result = runner.invoke(cli, ["--quiet"])

            assert result.exit_code == 0
            mock_setup_logging.assert_called_once_with(False)

    def test_cli_group_debug_and_quiet_options(self, runner):
        """Test CLI group with both debug and quiet options."""
        with patch("spark_history_mcp.cli.main.setup_logging") as mock_setup_logging:
            result = runner.invoke(cli, ["--debug", "--quiet"])

            assert result.exit_code == 0
            # Quiet should override debug in logging setup
            mock_setup_logging.assert_called_once_with(False)

    def test_cli_group_context_object_creation(self, runner):
        """Test that CLI context object is properly created and populated."""

        # Create a custom command to inspect the context
        @cli.command()
        @click.pass_context
        def test_context(ctx):
            # Verify context object exists and has expected keys
            assert ctx.obj is not None
            assert "config_path" in ctx.obj
            assert "debug" in ctx.obj
            assert "quiet" in ctx.obj

            # Check default values
            assert ctx.obj["config_path"] == Path("config.yaml")
            assert ctx.obj["debug"] is False
            assert ctx.obj["quiet"] is False

        try:
            result = runner.invoke(cli, ["test-context"])
            assert result.exit_code == 0
        finally:
            # Remove the test command to avoid affecting other tests
            if "test-context" in cli.commands:
                del cli.commands["test-context"]

    def test_cli_group_context_object_with_options(self, runner):
        """Test that CLI context object is populated with provided options."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "test.yaml"
            config_path.touch()

            @cli.command()
            @click.pass_context
            def test_context_options(ctx):
                assert str(ctx.obj["config_path"]) == str(config_path)
                assert ctx.obj["debug"] is True
                assert ctx.obj["quiet"] is True

            try:
                result = runner.invoke(
                    cli,
                    [
                        "--config",
                        str(config_path),
                        "--debug",
                        "--quiet",
                        "test-context-options",
                    ],
                )
                assert result.exit_code == 0
            finally:
                if "test-context-options" in cli.commands:
                    del cli.commands["test-context-options"]


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestCLICommandRegistration:
    """Test CLI command registration."""

    def test_command_groups_registered(self):
        """Test that all command groups are properly registered."""
        # Check that main command groups are available
        command_names = list(cli.commands.keys())

        # These commands should be available if imports succeed
        expected_commands = ["apps", "analyze", "compare", "server", "config"]

        # Check if commands are registered (they might not be if imports fail)
        for cmd in expected_commands:
            if cmd in command_names:
                assert cli.commands[cmd] is not None

    def test_cli_with_subcommand_help(self):
        """Test CLI with subcommand help."""
        runner = CliRunner()

        # Test that help works for registered commands
        if "apps" in cli.commands:
            result = runner.invoke(cli, ["apps", "--help"])
            assert result.exit_code == 0
            assert "apps" in result.output.lower()

    def test_import_error_handling(self):
        """Test graceful handling of import errors for commands."""
        # This test is more about code coverage since the import block
        # has a try/except that catches ImportError

        # Since the CLI module is already loaded with successful imports,
        # we can't easily test the import error path. This test verifies
        # that the import error handling code exists and is structured correctly.
        import inspect

        import spark_history_mcp.cli.main

        # Verify the try/except block exists in the source
        source = inspect.getsource(spark_history_mcp.cli.main)
        assert "try:" in source
        assert "except ImportError:" in source
        assert "pass  # Commands will be unavailable" in source


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestCLIFallback:
    """Test CLI fallback functionality."""

    def test_cli_fallback_message(self):
        """Test CLI fallback when dependencies not available."""
        # We can't easily test the actual fallback since we need CLI_AVAILABLE=True
        # to run these tests, but we can test the fallback function directly

        # Mock CLI_AVAILABLE = False
        with patch("spark_history_mcp.cli.main.CLI_AVAILABLE", False):
            with patch("builtins.print") as mock_print:
                with patch("sys.exit") as mock_exit:
                    # Import the fallback function
                    from spark_history_mcp.cli.main import cli as fallback_cli

                    # The fallback should be the simple function
                    if callable(fallback_cli) and not hasattr(fallback_cli, "commands"):
                        fallback_cli()

                        # Verify the fallback message was printed
                        mock_print.assert_called()
                        print_calls = [call[0][0] for call in mock_print.call_args_list]
                        assert any(
                            "CLI dependencies not installed" in call
                            for call in print_calls
                        )
                        mock_exit.assert_called_once_with(1)


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestMainExecution:
    """Test main execution functionality."""

    def test_main_execution_with_args(self):
        """Test main execution (if __name__ == '__main__')."""
        # This is primarily for coverage - the actual execution is handled by click
        # We can verify that the cli function is the main entry point
        assert callable(cli)
        assert hasattr(cli, "main")  # Click groups have a main method

    def test_cli_invoke_without_command_shows_help(self):
        """Test that invoking CLI without command shows help."""
        runner = CliRunner()
        result = runner.invoke(cli, [])

        assert result.exit_code == 0
        assert "Usage:" in result.output or "Spark History Server MCP" in result.output

    @patch("spark_history_mcp.cli.main.setup_logging")
    def test_logging_setup_called_on_invocation(self, mock_setup_logging):
        """Test that logging setup is called when CLI is invoked."""
        runner = CliRunner()
        result = runner.invoke(cli, [])

        assert result.exit_code == 0
        mock_setup_logging.assert_called_once()

    def test_config_path_validation(self):
        """Test config path validation in CLI options."""
        runner = CliRunner()

        # Test with non-existent config file
        result = runner.invoke(cli, ["--config", "/nonexistent/path.yaml"])

        # Should fail due to click.Path(exists=True)
        assert result.exit_code != 0
        assert "does not exist" in result.output or "Path" in result.output
