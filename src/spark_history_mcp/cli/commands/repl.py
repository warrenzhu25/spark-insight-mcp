"""
Interactive REPL mode for the Spark History MCP CLI.

Provides a persistent interactive shell with tab completion and command history.
Short aliases expand to their full command equivalents automatically.
"""

import shlex
from pathlib import Path
from typing import Optional

from spark_history_mcp.cli._compat import CLI_AVAILABLE, cli_unavailable_stub, click

# Short aliases → full command strings (first word of alias is expanded)
ALIASES: dict[str, str] = {
    "list": "apps list",
    "show": "apps show",
    "summary": "apps summary",
    "jobs": "apps jobs",
    "stages": "apps stages",
    "insights": "analyze insights",
    "bottlenecks": "analyze bottlenecks",
    "slowest": "analyze slowest",
    "skew": "analyze shuffle-skew",
    "scaling": "analyze auto-scaling",
    "compare": "compare apps",
}

_HELP_TEXT = """\
Spark REPL — interactive Spark History analysis shell

Short aliases:
  list                  → apps list
  show <id>             → apps show <id>
  summary <id>          → apps summary <id>
  jobs <id>             → apps jobs <id>
  stages <id>           → apps stages <id>
  insights <id>         → analyze insights <id>
  bottlenecks <id>      → analyze bottlenecks <id>
  slowest <id>          → analyze slowest <id>
  skew <id>             → analyze shuffle-skew <id>
  scaling <id>          → analyze auto-scaling <id>
  compare <id1> <id2>   → compare apps <id1> <id2>

Full commands also work (e.g. "apps list --limit 5").
Use Tab for autocomplete, Up/Down for history.
Type 'exit', 'quit', or press Ctrl+D to leave.
"""


if CLI_AVAILABLE:

    def _build_completions(cmd: "click.BaseCommand") -> Optional[dict]:
        """Recursively build a nested dict for NestedCompleter from a Click command tree."""
        if not isinstance(cmd, click.Group):
            return None
        result: dict = {}
        for name, sub in cmd.commands.items():  # type: ignore[attr-defined]
            result[name] = _build_completions(sub)
        return result

    @click.command("repl")
    @click.pass_context
    def repl(ctx: click.Context) -> None:
        """Start an interactive REPL with tab completion and command history."""
        try:
            from prompt_toolkit import PromptSession
            from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
            from prompt_toolkit.completion import NestedCompleter
            from prompt_toolkit.history import FileHistory
        except ImportError:
            click.echo(
                "prompt_toolkit is required for REPL mode. "
                "Install it with: uv add prompt_toolkit",
                err=True,
            )
            raise SystemExit(1)

        # Lazy import to avoid circular import at module load time
        from spark_history_mcp.cli.main import cli as root_cli

        # Build completion dict from the live Click command tree
        completions: dict = _build_completions(root_cli) or {}
        # Add short aliases at the top level
        for alias, expansion in ALIASES.items():
            first_word = expansion.split()[0]
            completions[alias] = completions.get(first_word)

        completer = NestedCompleter.from_nested_dict(completions)
        history_file = Path.home() / ".spark_mcp_history"

        session: PromptSession = PromptSession(
            history=FileHistory(str(history_file)),
            completer=completer,
            auto_suggest=AutoSuggestFromHistory(),
            complete_while_typing=True,
        )

        click.echo("Spark REPL — type 'help' for commands, 'exit' to quit, Tab to autocomplete\n")

        # Preserve the parent context object so commands inherit config/debug/quiet
        obj = dict(ctx.obj) if ctx.obj else {}

        while True:
            try:
                line = session.prompt("spark> ").strip()
            except EOFError:  # Ctrl+D
                click.echo("Bye!")
                break
            except KeyboardInterrupt:  # Ctrl+C — cancel current line
                continue

            if not line or line.startswith("#"):
                continue

            if line in ("exit", "quit", "q"):
                click.echo("Bye!")
                break

            if line in ("help", "?", "h"):
                click.echo(_HELP_TEXT)
                continue

            # Expand short alias
            try:
                parts = shlex.split(line)
            except ValueError as exc:
                click.echo(f"Parse error: {exc}", err=True)
                continue

            if parts and parts[0] in ALIASES:
                expanded = shlex.split(ALIASES[parts[0]])
                parts = expanded + parts[1:]

            # Invoke through Click, reusing ctx.obj so config/debug/quiet carry over
            try:
                root_cli.main(args=parts, standalone_mode=False, obj=obj)
            except SystemExit:
                # Click calls sys.exit(0) after --help; swallow silently
                pass
            except click.exceptions.UsageError as exc:
                click.echo(f"Usage error: {exc}", err=True)
            except Exception as exc:
                click.echo(f"Error: {exc}", err=True)

else:
    repl = cli_unavailable_stub("repl")  # type: ignore[assignment]
