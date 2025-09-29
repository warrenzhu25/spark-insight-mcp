# Repository Guidelines

## Project Structure & Module Organization
The MCP server lives in `src/spark_history_mcp` with feature folders (`api/`, `core/`, `tools/`, `prompts/`), mirrored by agent adapters in `src/sparkinsight_ai`. Treat the package `config/` directories as templates and keep real secrets in env vars. Tests live in `tests/unit/`, shared fixtures in `tests/fixtures/` and `tests/emr/`, with the Spark-backed scenario in `tests/e2e.py`. Deployment manifests sit in `deploy/`, and reference docs in `CLI-README.md` plus `screenshots/`.

## Build, Test, and Development Commands
Use Taskfile-driven workflows: run `task dev-setup` once to install uv deps and pre-commit. The validation loop is `task lint`, `task format`, `task type-check`, and `task test`; `task validate` batches lint and tests. Execute `task test-e2e` for Spark + MCP coverage and `task dev-all` / `task stop-all` when you need those services running for manual QA.

## Coding Style & Naming Conventions
Target Python 3.12, four-space indentation, Ruff-enforced 88 character lines. Keep snake_case for functions/variables, PascalCase for classes, and organize schemas under `models/`, prompts under `prompts/`, CLI entry points under `cli/`. Run `task lint-fix` and `task format` before pushing to normalize style and imports.

## Testing Guidelines
Pytest drives all suites with coverage via `--cov=. --cov-report=term-missing`. Name files `test_<subject>.py`, classes `Test<Subject>`, and pull shared setup from `tests/fixtures/`. The e2e suite in `tests/e2e.py` expects the Spark history server and MCP endpoints; prefer `task test-e2e` to orchestrate dependencies.

## Commit & Pull Request Guidelines
Use the conventional commits already in history (`feat:`, `fix:`, `test:`) with summaries under 72 characters. Reference issues or Taskfile targets in the body, note config changes such as `config.yaml`, and document verification commands (`task lint`, `task test`). Run `task ci` before requesting review and include `task security` output when security-sensitive surfaces change.

## Security & Configuration Tips
Keep credentials in environment variables; checked-in configs are safe defaults only. Refresh EMR fixtures under `tests/emr/` as needed and clean generated logs like `htmlcov/` before pushing. Review `SECURITY.md` for reporting and hardening expectations.
