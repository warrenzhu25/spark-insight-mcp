"""
Tests for spark_history_mcp.cli.commands.repl module.
"""

from unittest.mock import MagicMock, patch

import pytest

try:
    import click
    from click.testing import CliRunner

    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False

if CLI_AVAILABLE:
    from spark_history_mcp.cli.commands.repl import ALIASES, _build_completions
    from spark_history_mcp.cli.main import cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session(inputs):
    """Return a mock PromptSession whose prompt() yields *inputs* then EOFError."""
    mock_session = MagicMock()
    mock_session.prompt.side_effect = list(inputs) + [EOFError()]
    return mock_session


def _invoke_repl(runner, inputs, extra_args=None, obj=None):
    """Invoke the REPL with mocked PromptSession and given user inputs."""
    extra_args = extra_args or []
    with patch("prompt_toolkit.history.FileHistory"), patch(
        "prompt_toolkit.PromptSession"
    ) as mock_prompt_session:
        mock_prompt_session.return_value = _make_session(inputs)
        return runner.invoke(cli, ["repl"] + extra_args, obj=obj or {})


# ---------------------------------------------------------------------------
# Static / unit tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestAliasesAndCompletions:
    def test_aliases_keys_are_single_words(self):
        for alias in ALIASES:
            assert " " not in alias, f"Alias '{alias}' must be a single word"

    def test_aliases_values_are_valid_commands(self):
        """Each alias must expand to a known top-level group + subcommand."""
        known_groups = {"apps", "analyze", "compare", "server", "config", "cache", "cleanup"}
        for alias, expansion in ALIASES.items():
            group = expansion.split()[0]
            assert group in known_groups, (
                f"Alias '{alias}' expands to unknown group '{group}'"
            )

    def test_expected_aliases_present(self):
        expected = {
            "list", "show", "summary", "jobs", "stages",
            "insights", "bottlenecks", "slowest", "skew", "scaling", "compare",
        }
        assert expected.issubset(set(ALIASES)), (
            f"Missing aliases: {expected - set(ALIASES)}"
        )

    def test_build_completions_on_group(self):
        @click.group()
        def grp():
            pass

        @grp.command("sub-a")
        def sub_a():
            pass

        @grp.command("sub-b")
        def sub_b():
            pass

        result = _build_completions(grp)
        assert result == {"sub-a": None, "sub-b": None}

    def test_build_completions_nested(self):
        @click.group()
        def outer():
            pass

        @outer.group("inner")
        def inner():
            pass

        @inner.command("leaf")
        def leaf():
            pass

        result = _build_completions(outer)
        assert result == {"inner": {"leaf": None}}

    def test_build_completions_on_non_group_returns_none(self):
        @click.command()
        def cmd():
            pass

        assert _build_completions(cmd) is None

    def test_build_completions_on_real_cli(self):
        result = _build_completions(cli)
        assert result is not None
        assert "apps" in result
        assert "analyze" in result
        assert "compare" in result
        assert "repl" in result


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestReplRegistration:
    def test_repl_in_cli_commands(self):
        assert "repl" in cli.commands

    def test_repl_help_text(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["repl", "--help"])
        assert result.exit_code == 0
        assert "REPL" in result.output or "interactive" in result.output.lower()


# ---------------------------------------------------------------------------
# REPL exit behaviour
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestReplExit:
    def test_exit_command(self):
        runner = CliRunner()
        result = _invoke_repl(runner, ["exit"])
        assert result.exit_code == 0
        assert "Bye!" in result.output

    def test_quit_command(self):
        runner = CliRunner()
        result = _invoke_repl(runner, ["quit"])
        assert result.exit_code == 0
        assert "Bye!" in result.output

    def test_q_command(self):
        runner = CliRunner()
        result = _invoke_repl(runner, ["q"])
        assert result.exit_code == 0
        assert "Bye!" in result.output

    def test_ctrl_d_eoferror(self):
        """EOFError (Ctrl+D) should exit cleanly."""
        runner = CliRunner()
        with patch("prompt_toolkit.history.FileHistory"), patch(
            "prompt_toolkit.PromptSession"
        ) as mock_prompt_session:
            mock_session = MagicMock()
            mock_session.prompt.side_effect = EOFError()
            mock_prompt_session.return_value = mock_session
            result = runner.invoke(cli, ["repl"], obj={})
        assert result.exit_code == 0
        assert "Bye!" in result.output

    def test_ctrl_c_continues_loop(self):
        """KeyboardInterrupt should not exit; next command is processed."""
        runner = CliRunner()
        with patch("prompt_toolkit.history.FileHistory"), patch(
            "prompt_toolkit.PromptSession"
        ) as mock_prompt_session:
            mock_session = MagicMock()
            # First call raises Ctrl+C, second call exits
            mock_session.prompt.side_effect = [KeyboardInterrupt(), "exit"]
            mock_prompt_session.return_value = mock_session
            result = runner.invoke(cli, ["repl"], obj={})
        assert result.exit_code == 0
        assert "Bye!" in result.output


# ---------------------------------------------------------------------------
# Built-in REPL commands
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestReplBuiltins:
    def test_help_command(self):
        runner = CliRunner()
        result = _invoke_repl(runner, ["help"])
        assert "Short aliases" in result.output
        assert "list" in result.output

    def test_question_mark_help(self):
        runner = CliRunner()
        result = _invoke_repl(runner, ["?"])
        assert "Short aliases" in result.output

    def test_h_help(self):
        runner = CliRunner()
        result = _invoke_repl(runner, ["h"])
        assert "Short aliases" in result.output

    def test_empty_line_ignored(self):
        runner = CliRunner()
        # empty line then exit — should not error
        result = _invoke_repl(runner, ["", "exit"])
        assert result.exit_code == 0

    def test_comment_line_ignored(self):
        runner = CliRunner()
        result = _invoke_repl(runner, ["# this is a comment", "exit"])
        assert result.exit_code == 0

    def test_welcome_message_shown(self):
        runner = CliRunner()
        result = _invoke_repl(runner, ["exit"])
        assert "Spark REPL" in result.output

    def test_prompt_string(self):
        runner = CliRunner()
        with patch("prompt_toolkit.history.FileHistory"), patch(
            "prompt_toolkit.PromptSession"
        ) as mock_prompt_session:
            mock_session = MagicMock()
            mock_session.prompt.side_effect = ["exit"]
            mock_prompt_session.return_value = mock_session
            runner.invoke(cli, ["repl"], obj={})
        # Verify prompt() was called with "spark> "
        mock_session.prompt.assert_called_with("spark> ")


# ---------------------------------------------------------------------------
# Alias expansion
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestAliasExpansion:
    """Verify that short aliases are expanded to full commands before dispatch."""

    def _run_alias(self, alias_input, runner):
        """Run a single alias command, capture args passed to cli.main."""
        captured = {}

        def fake_main(args, standalone_mode, obj):
            captured["args"] = args

        with patch("prompt_toolkit.history.FileHistory"), patch(
            "prompt_toolkit.PromptSession"
        ) as mock_prompt_session, patch(
            "spark_history_mcp.cli.main.cli"
        ) as mock_root:
            mock_root.main.side_effect = fake_main
            mock_session = MagicMock()
            mock_session.prompt.side_effect = [alias_input, EOFError()]
            mock_prompt_session.return_value = mock_session
            runner.invoke(cli, ["repl"], obj={})

        return captured.get("args", [])

    def test_list_expands(self):
        runner = CliRunner()
        args = self._run_alias("list", runner)
        assert args == ["apps", "list"]

    def test_show_expands_with_id(self):
        runner = CliRunner()
        args = self._run_alias("show app-123", runner)
        assert args == ["apps", "show", "app-123"]

    def test_summary_expands_with_id(self):
        runner = CliRunner()
        args = self._run_alias("summary app-abc", runner)
        assert args == ["apps", "summary", "app-abc"]

    def test_jobs_expands(self):
        runner = CliRunner()
        args = self._run_alias("jobs app-1", runner)
        assert args == ["apps", "jobs", "app-1"]

    def test_stages_expands(self):
        runner = CliRunner()
        args = self._run_alias("stages app-1", runner)
        assert args == ["apps", "stages", "app-1"]

    def test_insights_expands(self):
        runner = CliRunner()
        args = self._run_alias("insights app-1", runner)
        assert args == ["analyze", "insights", "app-1"]

    def test_bottlenecks_expands(self):
        runner = CliRunner()
        args = self._run_alias("bottlenecks app-1", runner)
        assert args == ["analyze", "bottlenecks", "app-1"]

    def test_slowest_expands(self):
        runner = CliRunner()
        args = self._run_alias("slowest app-1", runner)
        assert args == ["analyze", "slowest", "app-1"]

    def test_skew_expands(self):
        runner = CliRunner()
        args = self._run_alias("skew app-1", runner)
        assert args == ["analyze", "shuffle-skew", "app-1"]

    def test_scaling_expands(self):
        runner = CliRunner()
        args = self._run_alias("scaling app-1", runner)
        assert args == ["analyze", "auto-scaling", "app-1"]

    def test_compare_expands(self):
        runner = CliRunner()
        args = self._run_alias("compare app-1 app-2", runner)
        assert args == ["compare", "apps", "app-1", "app-2"]

    def test_full_command_not_expanded(self):
        """Full-path commands should be passed through unchanged."""
        runner = CliRunner()
        args = self._run_alias("apps list --limit 5", runner)
        assert args == ["apps", "list", "--limit", "5"]

    def test_unknown_command_no_expansion(self):
        """Unknown commands should be passed as-is (Click will report the error)."""
        runner = CliRunner()
        args = self._run_alias("unknowncmd foo", runner)
        assert args == ["unknowncmd", "foo"]


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not CLI_AVAILABLE, reason="CLI dependencies not available")
class TestReplErrorHandling:
    def test_usage_error_shown(self):
        """Click UsageError from a bad command is displayed, not crash."""
        runner = CliRunner()
        with patch("prompt_toolkit.history.FileHistory"), patch(
            "prompt_toolkit.PromptSession"
        ) as mock_prompt_session, patch("spark_history_mcp.cli.main.cli") as mock_root:
            mock_root.main.side_effect = click.exceptions.UsageError("bad args")
            mock_session = MagicMock()
            mock_session.prompt.side_effect = ["apps list", "exit"]
            mock_prompt_session.return_value = mock_session
            result = runner.invoke(cli, ["repl"], obj={})
        assert result.exit_code == 0
        assert "Usage error" in result.output

    def test_generic_exception_shown(self):
        """Unexpected exceptions are caught and displayed, REPL stays alive."""
        runner = CliRunner()
        with patch("prompt_toolkit.history.FileHistory"), patch(
            "prompt_toolkit.PromptSession"
        ) as mock_prompt_session, patch("spark_history_mcp.cli.main.cli") as mock_root:
            mock_root.main.side_effect = [RuntimeError("something went wrong"), None]
            mock_session = MagicMock()
            mock_session.prompt.side_effect = ["apps list", "exit"]
            mock_prompt_session.return_value = mock_session
            result = runner.invoke(cli, ["repl"], obj={})
        assert result.exit_code == 0
        assert "Error" in result.output

    def test_system_exit_swallowed(self):
        """SystemExit (e.g. from --help) should not crash the REPL."""
        runner = CliRunner()
        with patch("prompt_toolkit.history.FileHistory"), patch(
            "prompt_toolkit.PromptSession"
        ) as mock_prompt_session, patch("spark_history_mcp.cli.main.cli") as mock_root:
            mock_root.main.side_effect = [SystemExit(0), None]
            mock_session = MagicMock()
            mock_session.prompt.side_effect = ["apps --help", "exit"]
            mock_prompt_session.return_value = mock_session
            result = runner.invoke(cli, ["repl"], obj={})
        assert result.exit_code == 0

    def test_parse_error_on_bad_quotes(self):
        """Malformed shell quoting should show a parse error, not crash."""
        runner = CliRunner()
        result = _invoke_repl(runner, ["show 'unterminated", "exit"])
        assert result.exit_code == 0
        assert "Parse error" in result.output

    def test_missing_prompt_toolkit_shows_error(self):
        """If prompt_toolkit is not importable, a clear error message is shown."""
        runner = CliRunner()
        with patch.dict("sys.modules", {"prompt_toolkit": None}):
            result = runner.invoke(cli, ["repl"], obj={})
        # Should fail gracefully with a helpful message
        assert result.exit_code != 0 or "prompt_toolkit" in result.output
