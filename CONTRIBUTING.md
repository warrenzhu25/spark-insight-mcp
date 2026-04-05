# 🤝 Contributing to Spark History Server MCP

Thank you for your interest in contributing! This guide covers everything you need to understand the project, set up your environment, and get code merged — without having to spelunk through the source yourself.

## 🚀 Quick Start for Contributors

### 📋 Prerequisites

- 🐍 Python 3.12+
- ⚡ [uv](https://docs.astral.sh/uv/getting-started/installation/) package manager
- 🔧 [go-task](https://taskfile.dev/installation/) (strongly recommended — all dev commands use it)
- 🔥 Docker (for local testing with Spark History Server)
- 📦 Node.js (for MCP Inspector testing)

### 🛠️ Development Setup

1. **🍴 Fork and clone the repository**

```bash
git clone https://github.com/YOUR_USERNAME/mcp-apache-spark-history-server.git
cd mcp-apache-spark-history-server
```

2. **📦 Complete environment setup (installs deps + pre-commit hooks)**

```bash
task dev-setup
```

Without `go-task`:

```bash
uv sync --group dev --frozen
uv run pre-commit install
```

3. **🚀 Start all development services**

```bash
task dev-all          # Spark History Server + MCP server + MCP Inspector
```

Individual services:

```bash
task start-spark-bg   # Spark History Server only (background)
task start-mcp-bg     # MCP server only (background)
task start-inspector-bg  # MCP Inspector only → http://localhost:6274
```

4. **🧹 Stop all background services**

```bash
task stop-all
```

## 🏗️ Project Architecture

Understanding the module layout saves you from searching for where to make a change.

```
src/spark_history_mcp/
├── core/            — FastMCP server entry point (main.py) and app/client wiring (app.py)
├── api/             — Spark History Server REST clients
│   ├── base_client.py          — shared HTTP session, auth, retry logic
│   ├── spark_client.py         — Spark REST API endpoints
│   ├── emr_persistent_ui_client.py  — AWS EMR presigned URL + session management
│   ├── spark_html_client.py    — HTML scraping fallback
│   └── factory.py              — client factory (selects client by server config)
├── tools/           — All 50+ MCP tool implementations
│   ├── tools.py / application.py / analysis.py / jobs_stages.py
│   ├── executors.py / comparisons.py / timelines.py
│   ├── common.py               — shared helpers (get_client_or_default, etc.)
│   ├── metrics.py / recommendations.py / matching.py
│   └── comparison_modules/     — comparison sub-system (see deep dive below)
│       ├── core.py             — compare_app_performance (orchestrator)
│       ├── stages.py           — compare_stages, find_top_stage_differences
│       ├── executors.py        — compare_app_executors, executor timelines
│       ├── environment.py      — compare_app_environments, compare_app_resources, compare_app_jobs, compare_app_stages_aggregated
│       ├── utils.py            — delta calculation, significance filtering, aggregation helpers
│       └── constants.py        — SIGNIFICANCE_THRESHOLD, SIMILARITY_THRESHOLD, recommendation thresholds
├── cli/             — Click-based CLI mirroring all MCP tools
│   ├── main.py / session.py / formatters.py
│   └── commands/               — apps, analyze, compare, config, server, cache, cleanup
│       └── formatter_modules/  — human/json/table output renderers
├── prompts/         — 16 intelligent prompt templates for structured analysis
│   ├── performance.py / optimization.py / troubleshooting.py / reporting.py
├── models/          — Pydantic data models
│   ├── spark_types.py          — Spark API response types
│   └── mcp_types.py            — MCP tool input/output types
├── config/          — YAML config loading with Pydantic Settings + env-var override
├── utils/           — Shared sorting utilities
└── cache.py         — Response caching layer
```

### Where to add things

| What you're adding | Where to put it |
|---|---|
| New MCP tool | `tools/` — pick the matching category file (`analysis.py`, `jobs_stages.py`, etc.) or `tools.py` |
| New comparison tool | `tools/comparison_modules/` — add to the relevant file, orchestrate in `core.py` |
| New CLI command | `cli/commands/` — add to existing group file or create a new one |
| New prompt template | `prompts/` — `performance.py`, `optimization.py`, `troubleshooting.py`, or `reporting.py` |
| New Spark API call | `api/spark_client.py` (or `emr_persistent_ui_client.py` for EMR) |
| New data model | `models/spark_types.py` or `models/mcp_types.py` |

## 🔧 Configuration

### `config.yaml` — server and MCP settings

```yaml
servers:
  local:
    default: true
    url: "http://localhost:18080"

  # Add additional servers here:
  production:
    url: "https://spark-history.company.com:18080"
    verify_ssl: true
    auth:
      username: ${PROD_USERNAME}
      password: ${PROD_PASSWORD}

mcp:
  transports:
    - stdio          # or streamable-http (only one allowed)
  port: "18888"
  debug: false
  address: localhost
```

Tools accept an optional `server` parameter to target a non-default server:

```python
get_application(app_id="spark-abc123", server="production")
```

### Environment Variables

All config values can be overridden via environment variables — copy `.env.example` to `.env` for local overrides.

| Variable | Description |
|---|---|
| `SHS_MCP_PORT` | MCP server port (default: `18888`) |
| `SHS_MCP_DEBUG` | Enable debug logging (default: `false`) |
| `SHS_MCP_ADDRESS` | Bind address (default: `localhost`) |
| `SHS_MCP_TRANSPORT` | Transport mode: `stdio` or `streamable-http` |
| `SHS_SERVERS_<NAME>_URL` | Server URL (e.g. `SHS_SERVERS_PRODUCTION_URL`) |
| `SHS_SERVERS_<NAME>_AUTH_USERNAME` | Basic auth username |
| `SHS_SERVERS_<NAME>_AUTH_PASSWORD` | Basic auth password |
| `SHS_SERVERS_<NAME>_AUTH_TOKEN` | Bearer token |
| `SHS_SERVERS_<NAME>_VERIFY_SSL` | SSL verification (`true`/`false`) |
| `SHS_SERVERS_<NAME>_EMR_CLUSTER_ARN` | AWS EMR cluster ARN |

## 🧪 Testing Your Changes

### Test data available

Three real Spark event logs are included in the repository root:

| Event Log | Scenario |
|---|---|
| `spark-bcec39f6201b42b9925124595baad260` | Successful ETL job |
| `spark-110be3a8424d4a2789cb88134418217b` | Data processing job |
| `spark-cc4d115f011443d787f03a71a476a745` | Multi-stage analytics job |

The `start_local_spark_history.sh` script starts Spark History Server with these files already loaded.

### Run tests

```bash
task test            # Unit tests with coverage
task test-verbose    # Verbose output
task test-e2e        # End-to-end tests (requires running services)
```

Without `go-task`:

```bash
uv run pytest                          # Unit tests
uv run pytest --cov=. --cov-report=html  # With coverage report
uv run pytest tests/unit/test_tools.py -v  # Specific file
```

### Test breakdown

| Layer | Location | Requires services | Command |
|---|---|---|---|
| Unit tests | `tests/unit/` | No (mocked) | `task test` |
| Integration / E2E | `tests/` | Yes | `task test-e2e` |

### Interactive testing with MCP Inspector

```bash
task dev-all
# Opens MCP Inspector at http://localhost:6274
# Call any tool interactively and inspect the exact JSON response
```

Manual start:

```bash
npx @modelcontextprotocol/inspector uv run -m spark_history_mcp.core.main
```

## 🔍 Code Quality Checks

### Pre-commit hooks (run automatically on every commit)

| Hook | What it checks |
|---|---|
| `trailing-whitespace`, `end-of-file-fixer`, `mixed-line-ending` | File hygiene (LF line endings) |
| `check-yaml`, `check-json`, `check-toml` | Config file syntax |
| `check-merge-conflict` | No unresolved merge markers |
| `check-added-large-files` | Files under 1 MB |
| `ruff-check --fix` + `ruff-format` | Python linting and formatting |
| `bandit` | Security scanning (excludes `tests/`) |
| `check-config-security` | Detects hardcoded credentials in YAML |
| `markdownlint` | Documentation style |

> Note: MyPy type checking is currently disabled (tracked as a TODO in `.pre-commit-config.yaml`).

### Run checks manually

```bash
task validate      # lint + test (what CI requires)
task ci            # lint + test + security scan (full local CI)
task lint          # Ruff lint only
task lint-fix      # Ruff lint with auto-fix
task format        # Ruff format
```

Without `go-task`:

```bash
uv run ruff check --fix
uv run ruff format
uv run bandit -r . -f json -o bandit-report.json
```

### Code style

- **Formatter/linter**: Ruff (`pyproject.toml` → `[tool.ruff]`)
- **Line length**: 88 characters
- **Target**: Python 3.12+
- **Import sorting**: Automatic (Ruff rule `I`)
- **Type hints**: Encouraged but not yet enforced by MyPy

## ⚙️ CI/CD Pipeline

Three GitHub Actions jobs run on every push and pull request (`.github/workflows/ci.yml`):

1. **Code Quality** — runs all pre-commit hooks against the full repo
2. **Test** — Ruff lint + `pytest` with coverage on Python 3.12
3. **Integration** — E2E tests against a real Spark History Server (depends on Test passing)

Releases are versioned automatically from git tags via `hatch-vcs` (configured in `pyproject.toml`).

## 📝 Contribution Guidelines

### 🎯 Areas for Contribution

#### 🔧 High Priority

- **New MCP Tools**: Additional Spark analysis tools
- **Performance Improvements**: Optimize API calls and data processing
- **Error Handling**: Better error messages and recovery
- **Documentation**: Examples, tutorials, and guides

#### 📊 Medium Priority

- **Testing**: More comprehensive test coverage
- **Monitoring**: Metrics and observability features
- **Configuration**: More flexible configuration options
- **CI/CD**: GitHub Actions improvements

#### 💡 Ideas Welcome

- **AI Agent Examples**: New integration patterns
- **Deployment**: Additional deployment methods
- **Analytics**: Advanced Spark job analysis tools

### 🔀 Pull Request Process

1. **🌿 Create a feature branch**

```bash
git checkout -b feature/your-new-feature
git checkout -b fix/bug-description
git checkout -b docs/improve-readme
```

2. **💻 Make your changes** following existing code style and patterns

3. **✅ Test thoroughly**

```bash
task validate       # Required: must pass before opening a PR
task test-e2e       # Recommended: run if you changed tool logic
# Also test with MCP Inspector for any new or modified tools
```

4. **📤 Submit pull request** — use descriptive commit messages, reference related issues, update `CHANGELOG.md` if applicable

## 🧪 Adding New MCP Tools

Tools are split into category files under `tools/`. Pick the file that matches your tool's domain:

| File | Domain |
|---|---|
| `tools/application.py` | Application metadata and overview |
| `tools/jobs_stages.py` | Job and stage analysis |
| `tools/executors.py` | Executor and resource utilization |
| `tools/analysis.py` | SparkInsight intelligence tools |
| `tools/comparisons.py` | Cross-application comparison (re-exports from `comparison_modules/`) |
| `tools/timelines.py` | Timeline and executor timeline tools |

Follow this pattern when implementing a tool:

```python
@mcp.tool()
def your_new_tool(
    app_id: str,
    server: Optional[str] = None,
    # other parameters
) -> Dict[str, Any]:
    """
    Brief description of what this tool does.

    Args:
        app_id: The Spark application ID
        server: Optional server name (uses default server if omitted)

    Returns:
        Description of return value
    """
    client = get_client_or_default(mcp.get_context(), server)
    return client.your_method(app_id)
```

`get_client_or_default` lives in `tools/common.py` and handles server selection and client instantiation.

**Add a unit test:**

```python
@patch("spark_history_mcp.tools.your_module.get_client_or_default")
def test_your_new_tool(self, mock_get_client):
    mock_client = MagicMock()
    mock_client.your_method.return_value = {"result": "data"}
    mock_get_client.return_value = mock_client

    result = your_new_tool("spark-app-123")

    self.assertEqual(result, {"result": "data"})
    mock_client.your_method.assert_called_once_with("spark-app-123")
```

**Also test interactively** via MCP Inspector — unit tests alone don't catch serialization or schema issues.

## 🖥️ Adding CLI Commands

Every MCP tool should also be accessible via the CLI. CLI commands live in `cli/commands/` grouped by domain (same grouping as MCP tools). The CLI uses Click and a shared session from `cli/session.py`.

```python
import click
from cli.session import get_client

@click.command("your-tool")
@click.argument("app_id")
@click.option("--server", default=None)
@click.option("--format", "fmt", type=click.Choice(["human", "json", "table"]), default="human")
def your_tool_cmd(app_id: str, server: str | None, fmt: str) -> None:
    """Brief description shown in --help."""
    client = get_client(server)
    result = client.your_method(app_id)
    # render using the formatter matching `fmt`
```

All three output formats (`human`, `json`, `table`) must be supported. Formatter utilities live in `cli/formatter_modules/`.

## 🔬 Comparison Logic Deep Dive

The comparison subsystem is the most complex part of the codebase. This section explains how it works so you can extend it without reading all the source.

### How `compare_app_performance` works

The top-level tool (`tools/comparison_modules/core.py`) orchestrates four sub-comparisons:

```
compare_app_performance(app_id1, app_id2, top_n=3)
├── fetch app summaries          → aggregated_overview.application_summary
├── compare_app_jobs()           → aggregated_overview.job_performance
├── compare_app_executors()      → aggregated_overview.executor_performance
├── find_top_stage_differences() → stage_deep_dive
├── compare_app_environments()   → environment_comparison
└── generate_recommendations()   → key_recommendations (priority >= medium, top 5)
```

Full return shape:

```python
{
    "schema_version": 1,
    "applications": {
        "app1": {"id": str, "name": str},
        "app2": {"id": str, "name": str},
    },
    "aggregated_overview": {
        "application_summary": {...},
        "job_performance": {...},
        "executor_performance": {...},
    },
    "stage_deep_dive": {
        "applications": {...},
        "top_stage_differences": [...],   # see find_top_stage_differences below
        "analysis_parameters": {
            "requested_top_n": int,
            "similarity_threshold": float,
            "available_stages_app1": int,
            "available_stages_app2": int,
            "matched_stages": int,
        },
        "stage_summary": {
            "matched_stages": int,
            "total_time_difference_seconds": float,
            "average_time_difference_seconds": float,
            "max_time_difference_seconds": float,
        },
    },
    "environment_comparison": {...},
    "recommendations": [...],
    "key_recommendations": [...]   # top 5, priority >= medium
}
```

### Delta calculation formulas

Every comparison metric is returned in two forms — a human-readable `_percent_change` string and a machine-readable `_ratio` float. Both are computed from these helpers in `tools/comparison_modules/utils.py`:

```python
def _calculate_percentage_change(val1: float, val2: float) -> str:
    """Returns e.g. '+15.3%', 'N/A', '+∞'"""
    if val1 == 0:
        return "N/A" if val2 == 0 else "+∞"
    change = ((val2 - val1) / val1) * 100
    return f"{change:+.1f}%"

def calculate_safe_ratio(val1: float, val2: float) -> float:
    """Zero-safe ratio: val2 / val1"""
    if val1 == 0:
        return float("inf") if val2 > 0 else 1.0
    return val2 / val1
```

### Significance filtering

To suppress noise, metrics are filtered before being returned. The `significance_threshold` parameter (default defined in `tools/comparison_modules/constants.py`) controls what's kept:

```python
# _ratio fields: keep if deviation from 1.0 exceeds threshold
if metric.endswith("_ratio") and abs(value - 1.0) >= threshold:
    include()

# _percent_change fields: keep if absolute % change exceeds threshold * 100
if metric.endswith("_percent_change") and abs(value) >= threshold * 100:
    include()
```

Every response includes a `filtering_summary` or `filtering_info` key reporting:

```python
{
    "total_metrics": int,
    "significant_metrics": int,
    "significance_threshold": float,
    "filtering_applied": bool,
}
```

### `compare_stages` — specific stage pair comparison

`tools/comparison_modules/stages.py` — compares two stages by explicit ID and returns only the metrics exceeding the threshold:

```python
{
    "stage_comparison": {
        "stage1": {"app_id": str, "stage_id": int, "name": str, "status": str},
        "stage2": {"app_id": str, "stage_id": int, "name": str, "status": str},
    },
    "significant_differences": {
        "stage_metrics": {
            "<metric_name>": {
                "stage1": float,
                "stage2": float,
                "change": str,        # e.g. "+42.7%"
                "significance": float,
            }
        },
        "task_metrics": {
            "<metric_name>_median": {
                "stage1": float, "stage2": float,
                "change": str, "significance": float,
            }
        },
    },
    "summary": {
        "significance_threshold": float,
        "total_differences_found": int,
    },
}
```

Stage duration formula:

```python
duration = (stage.completion_time - stage.submission_time).total_seconds()
# Returns 0.0 if either timestamp is missing
```

### `find_top_stage_differences` — automatic stage matching

Stages are matched across apps by name similarity (score 0.0–1.0). Only pairs above `similarity_threshold` are considered. Each entry in `top_stage_differences`:

```python
{
    "stage_name": str,
    "similarity_score": float,      # 0.0–1.0
    "app1_stage": {
        "stage_id": int, "name": str, "status": str, "duration_seconds": float
    },
    "app2_stage": {
        "stage_id": int, "name": str, "status": str, "duration_seconds": float
    },
    "time_difference": {
        "absolute_seconds": float,
        "percentage": float,
        "slower_application": "app1" | "app2",
    },
    "stage_metrics_comparison": {...},
    "executor_analysis": {...},
}
```

Time difference percentage:

```python
time_diff_percent = (abs(d2 - d1) / max(d1, d2)) * 100
```

### `compare_app_stages_aggregated` — rolled-up stage totals

`tools/comparison_modules/environment.py` — aggregates all stages per app then compares the totals:

```python
{
    "aggregated_stage_metrics": {
        "app1": {
            "total_stages": int,
            "total_input_bytes": int,
            "total_output_bytes": int,
            "total_shuffle_read_bytes": int,
            "total_shuffle_write_bytes": int,
            "total_memory_spilled_bytes": int,
            "total_disk_spilled_bytes": int,
            "total_executor_run_time_ms": int,
            "total_executor_cpu_time_ns": int,
            "total_gc_time_ms": int,
            "avg_stage_duration_ms": float,
            "total_tasks": int,
            "total_failed_tasks": int,
        },
        "app2": { ...same keys... },
    },
    "stage_performance_comparison": {
        # Every metric above has a _ratio (float) and _percent_change (str) twin
        "total_stages_ratio": float,
        "total_stages_percent_change": str,
        "total_shuffle_read_bytes_ratio": float,
        "total_shuffle_read_bytes_percent_change": str,
        # ...
    },
    "filtering_info": {
        "significance_threshold": float,
        "show_only_significant": bool,
    },
    "recommendations": [...]
}
```

### Recommendation priority levels

All compare functions return `recommendations` lists with a `priority` field:

| Priority | Value | Meaning |
|---|---|---|
| `"critical"` | 5 | Must fix |
| `"high"` | 4 | Strongly recommended |
| `"medium"` | 3 | Worth addressing |
| `"low"` | 2 | Minor suggestion |
| `"info"` | 1 | Informational |

`compare_app_performance` surfaces only `priority >= medium` in `key_recommendations` (top 5 after deduplication and sorting).

Recommendation trigger thresholds (from `tools/comparison_modules/constants.py`):

```python
RECOMMENDATION_CORE_RATIO_THRESHOLD   = 1.5   # flag when core count differs by >50%
RECOMMENDATION_MEMORY_RATIO_THRESHOLD = 1.5   # flag when memory differs by >50%
RECOMMENDATION_SPILL_BYTES_THRESHOLD  = 100 * 1024 * 1024  # flag when spill > 100 MB
```

## 🐛 Reporting Issues

### 🔍 Bug Reports

Include:

- **Environment**: Python version, OS, uv version
- **Steps to reproduce**: Clear step-by-step instructions
- **Expected vs actual behavior**: What should happen vs what happens
- **Logs**: Relevant error messages or logs
- **Sample data**: Spark application IDs that reproduce the issue (if possible)

### 💡 Feature Requests

Include:

- **Use case**: Why is this feature needed?
- **Proposed solution**: How should it work?
- **Alternatives**: Other approaches considered
- **Examples**: Sample usage or screenshots

## 📖 Documentation

### 📝 Types of Documentation

- **README.md**: Main project overview and quick start
- **CONTRIBUTING_TECHNICAL.md**: Deep dive into architecture and internal logic
- **TESTING.md**: Comprehensive testing guide
- **docs/mcp-tools.md**: Full MCP tool reference
- **examples/**: AI agent integration examples
- **Code comments**: Inline documentation for complex logic

### 🎨 Documentation Style

- Use emojis consistently for visual appeal
- Include code examples for all features
- Provide screenshots for UI elements
- Keep language clear and beginner-friendly

## 🌟 Recognition

Contributors are recognized in:

- **GitHub Contributors** section
- **Release notes** for significant contributions
- **Project documentation** for major features

## 📞 Getting Help

- **💬 Discussions**: [GitHub Discussions](https://github.com/DeepDiagnostix-AI/mcp-apache-spark-history-server/discussions)
- **🐛 Issues**: [GitHub Issues](https://github.com/DeepDiagnostix-AI/mcp-apache-spark-history-server/issues)
- **📚 Documentation**: Check existing docs first

## 📜 Code of Conduct

- **🤝 Be respectful**: Treat everyone with kindness and professionalism
- **🎯 Stay on topic**: Keep discussions relevant to the project
- **🧠 Be constructive**: Provide helpful feedback and suggestions
- **🌍 Be inclusive**: Welcome contributors of all backgrounds and skill levels

---

**🎉 Thank you for contributing to Spark History Server MCP!**

Your contributions help make Apache Spark monitoring more intelligent and accessible to the community.
