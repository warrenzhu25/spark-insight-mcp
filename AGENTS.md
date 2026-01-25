# AGENTS

Quick guide for automated agents working in this repo.

## Repo Overview
- Python 3.12 project for Spark History Server MCP + CLI.
- Entry point: `uv run spark-mcp --cli ...`

## Setup
- Install deps: `uv sync --dev`

## Common Commands
- Run CLI: `uv run spark-mcp --cli --help`
- Lint: `uv run ruff check .`
- Format: `uv run ruff format .`
- Unit tests: `.venv/bin/python -m pytest -q tests/unit`

## CLI Compare Flow
- `compare apps` saves context for later comparisons.
- Example: `uv run spark-mcp --cli compare apps <app1> <app2>`

## Notes
- Some pre-commit hooks run on commit (ruff, bandit, markdownlint).
- If running in non-interactive shells, interactive menus are skipped.

## Code Style
- Python formatting via ruff.
- Keep output formatting in `src/spark_history_mcp/cli/formatters.py`.
- Keep CLI commands in `src/spark_history_mcp/cli/commands/`.
