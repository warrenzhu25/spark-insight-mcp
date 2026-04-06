# Spark History Server MCP CLI

🖥️ **Command-line interface for direct Spark performance analysis**

The CLI mode provides direct command-line access to all 50+ MCP tools without requiring an MCP client. Perfect for system administrators, DevOps engineers, and data engineers who need quick Spark performance insights.

## 🚀 Quick Start

### Installation

```bash
# Clone and install
git clone https://github.com/DeepDiagnostix-AI/mcp-apache-spark-history-server.git
cd mcp-apache-spark-history-server
uv sync

# Test CLI immediately
uv run spark-mcp --cli --help
```

### Global Installation (Optional)

```bash
# Install globally with pipx
pipx install .

# Or create an alias for convenience
echo 'alias spark-mcp="uv run spark-mcp"' >> ~/.bashrc
source ~/.bashrc
```

## 🎯 Basic Usage

All CLI commands follow this pattern:
```bash
uv run spark-mcp --cli <command> <subcommand> [options]
```

### Core Commands

| Command | Description |
|---------|-------------|
| `repl` | **Interactive shell** with tab completion and command history |
| `apps` | Application management (list, show, details) |
| `analyze` | Single-app performance analysis and insights |
| `compare` | Multi-app comparisons with stateful context |
| `server` | MCP server management |
| `config` | Configuration management |

## 🎮 Interactive REPL Mode (`repl`)

The REPL (Read-Eval-Print Loop) drops you into a persistent interactive shell — no need to retype `spark-mcp --cli` for every command. Tab completion and command history are built in.

### Starting the REPL

```bash
uv run spark-mcp --cli repl
```

```
Spark REPL — type 'help' for commands, 'exit' to quit, Tab to autocomplete

spark>
```

### Short Aliases

Inside the REPL, the most common commands have single-word shortcuts. You no longer need the group prefix:

| Short alias | Full command |
|-------------|-------------|
| `list` | `apps list` |
| `show <id>` | `apps show <id>` |
| `summary <id>` | `apps summary <id>` |
| `jobs <id>` | `apps jobs <id>` |
| `stages <id>` | `apps stages <id>` |
| `insights <id>` | `analyze insights <id>` |
| `bottlenecks <id>` | `analyze bottlenecks <id>` |
| `slowest <id>` | `analyze slowest <id>` |
| `skew <id>` | `analyze shuffle-skew <id>` |
| `scaling <id>` | `analyze auto-scaling <id>` |
| `compare <id1> <id2>` | `compare apps <id1> <id2>` |

Full commands also work unchanged (e.g. `apps list --limit 5`).

### Tab Completion

Press **Tab** at any point to autocomplete commands, subcommands, and options:

```
spark> an<Tab>       → analyze
spark> analyze <Tab> → insights  bottlenecks  auto-scaling  shuffle-skew  slowest
spark> li<Tab>       → list
```

### Command History

- **Up/Down arrows** to cycle through previous commands
- History persists across sessions in `~/.spark_mcp_history`

### Typical REPL Session

```bash
spark> list                          # apps list
spark> show 1                        # apps show <first result>
spark> insights 1                    # analyze insights 1
spark> compare 1 2                   # compare apps 1 2
spark> apps list --limit 5           # full commands work too
spark> help                          # show alias reference
spark> exit                          # or Ctrl+D
```

### Built-in REPL Commands

| Command | Action |
|---------|--------|
| `help` / `?` / `h` | Show alias reference and tips |
| `exit` / `quit` / `q` | Exit the REPL |
| `Ctrl+D` | Exit the REPL |
| `Ctrl+C` | Cancel current line (stays in REPL) |
| `# comment` | Line is ignored |

---

## 📊 Applications (`apps`)

### List Applications
```bash
# List recent applications
uv run spark-mcp --cli apps list

# Limit to 10 applications
uv run spark-mcp --cli apps list --limit 10

# Filter by status
uv run spark-mcp --cli apps list --status COMPLETED --status RUNNING

# Filter by date range
uv run spark-mcp --cli apps list --min-date 2024-01-01 --max-date 2024-01-31

# Search by name (supports regex)
uv run spark-mcp --cli apps list --app-name "ETL.*job" --search-type regex

# Use different server
uv run spark-mcp --cli apps list --server production
```

### Show Application Details
```bash
# Show application overview
uv run spark-mcp --cli apps show app-20240315-123456

# Show with specific format
uv run spark-mcp --cli apps show app-20240315-123456 --format json
uv run spark-mcp --cli apps show app-20240315-123456 --format table
```

### Application Jobs and Stages
```bash
# List jobs for an application
uv run spark-mcp --cli apps jobs app-20240315-123456

# List stages for an application
uv run spark-mcp --cli apps stages app-20240315-123456

# Filter stages by status
uv run spark-mcp --cli apps stages app-20240315-123456 --status COMPLETE
```

### Executor Information
```bash
# List executors
uv run spark-mcp --cli apps executors app-20240315-123456

# Include inactive executors
uv run spark-mcp --cli apps executors app-20240315-123456 --include-inactive

# Get executor summary
uv run spark-mcp --cli apps executor-summary app-20240315-123456
```

### Application Performance Summary
```bash
# Get high-level performance summary
uv run spark-mcp --cli apps summary app-20240315-123456
```

## 🔢 Numbered App References

After running `apps list`, you can use **numbers** instead of full app IDs in subsequent commands. This makes workflows much faster and easier.

### How It Works

```bash
# Step 1: List apps - note the # column showing row numbers
uv run spark-mcp --cli apps list --limit 5

#                    Spark Applications
# # | Application ID          | Name    | Status    | Duration
# 1 | app-20231201-123456     | ETL-1   | Completed | 30s
# 2 | app-20231201-234567     | ETL-2   | Completed | 45s
# 3 | app-20231201-345678     | ETL-3   | Completed | 60s
#
# Tip: Use numbers 1-3 to reference these apps. Example: apps compare 1 2

# Step 2: Use numbers instead of app IDs
uv run spark-mcp --cli apps show 1
# Resolved #1 to: app-20231201-123456
# [App details...]

uv run spark-mcp --cli apps compare 1 2
# Resolved #1 to: app-20231201-123456
# Resolved #2 to: app-20231201-234567
# [Comparison results...]

uv run spark-mcp --cli analyze insights 1
# Resolved #1 to: app-20231201-123456
# [Insights...]
```

### Supported Commands

Number references work in all commands that accept an app ID:

| Command | Example |
|---------|---------|
| `apps show` | `apps show 1` |
| `apps jobs` | `apps jobs 1` |
| `apps stages` | `apps stages 1` |
| `apps compare` | `apps compare 1 2` |
| `analyze insights` | `analyze insights 1` |
| `analyze bottlenecks` | `analyze bottlenecks 1` |
| `analyze auto-scaling` | `analyze auto-scaling 1` |
| `analyze shuffle-skew` | `analyze shuffle-skew 1` |
| `analyze slowest` | `analyze slowest 1` |

### Session Timeout

- References are saved for **1 hour** after running `apps list`
- After 1 hour, run `apps list` again to refresh the references
- References are stored in `~/.config/spark-history-mcp/app-refs-session.json`

### Valid Number Formats

| Input | Valid? | Notes |
|-------|--------|-------|
| `1` | ✅ | Valid number reference |
| `10` | ✅ | Multi-digit numbers work |
| `0` | ❌ | Zero is not valid |
| `01` | ❌ | Leading zeros are not valid |
| `-1` | ❌ | Negative numbers are not valid |
| `1a` | ❌ | Not a pure number |
| `app-1` | ❌ | Treated as app ID, not a number |

## 🔍 Analysis (`analyze`)

### Comprehensive Insights
```bash
# Get complete SparkInsight analysis
uv run spark-mcp --cli analyze insights app-20240315-123456

# Customize analysis components
uv run spark-mcp --cli analyze insights app-20240315-123456 \
  --no-auto-scaling \
  --no-shuffle-skew
```

### Performance Bottlenecks
```bash
# Identify bottlenecks
uv run spark-mcp --cli analyze bottlenecks app-20240315-123456

# Show top 10 bottlenecks
uv run spark-mcp --cli analyze bottlenecks app-20240315-123456 --top-n 10
```

### Auto-scaling Analysis
```bash
# Get auto-scaling recommendations
uv run spark-mcp --cli analyze auto-scaling app-20240315-123456

# Set target duration (minutes)
uv run spark-mcp --cli analyze auto-scaling app-20240315-123456 --target-duration 5
```

### Shuffle Skew Detection
```bash
# Analyze shuffle data skew
uv run spark-mcp --cli analyze shuffle-skew app-20240315-123456

# Customize thresholds
uv run spark-mcp --cli analyze shuffle-skew app-20240315-123456 \
  --shuffle-threshold 5 \
  --skew-ratio 3.0
```

### Find Slowest Components
```bash
# Find slowest stages
uv run spark-mcp --cli analyze slowest app-20240315-123456 --type stages

# Find slowest jobs
uv run spark-mcp --cli analyze slowest app-20240315-123456 --type jobs

# Find slowest SQL queries
uv run spark-mcp --cli analyze slowest app-20240315-123456 --type sql

# Customize number of results
uv run spark-mcp --cli analyze slowest app-20240315-123456 --type stages --top-n 10
```

### Application Comparison (MOVED TO COMPARE COMMAND)
```bash
# ⚠️ DEPRECATED - Use 'compare' command instead
uv run spark-mcp --cli analyze compare app1 app2

# ✅ NEW: Use compare command with stateful context
uv run spark-mcp --cli apps compare app1 app2
```

## 🔄 Comparisons (`compare`)

The `compare` command group provides **stateful multi-app comparisons** with session context management. Set your comparison context once, then drill down into granular analysis without repeating app IDs.

## 📚 Command Reference (All CLI Commands)

Top-level groups:

| Group | Description |
|-------|-------------|
| `repl` | Interactive shell with tab completion and persistent history |
| `apps` | List, inspect, and compare Spark applications |
| `analyze` | Single-app performance analysis and AI-powered insights |
| `compare` | Stateful multi-app comparisons with session context |
| `server` | MCP server lifecycle management |
| `config` | Configuration file management |
| `cache` | Disk cache management |
| `cleanup` | Event log and storage cleanup |

---

### `apps` subcommands

| Subcommand | Description | Key options |
|------------|-------------|-------------|
| `apps list` | Fetch and display all Spark applications from the History Server. Assigns numbered references (`#1`, `#2`, …) valid for 1 hour so you can use them in subsequent commands instead of full app IDs. | `--limit N`, `--status COMPLETED\|RUNNING\|FAILED`, `--name <contains>`, `--name-exact <exact>`, `--no-interactive` |
| `apps show <id>` | Show full metadata for one application: attempt history, start/end times, duration, Spark user, and status. Accepts app IDs or `#N` number references. | `--server`, `--format` |
| `apps jobs <id>` | List all Spark jobs within an application with their status, task counts, and duration. Useful for identifying which jobs contributed most to total runtime. | `--status`, `--server`, `--format` |
| `apps stages <id>` | List all stages within an application including task metrics, shuffle read/write bytes, and executor time. Filter by stage status to focus on failed or active stages. | `--status`, `--server`, `--format` |
| `apps summary <id>` | High-level performance summary: total duration, task counts, input/output bytes, shuffle size, spill, executor utilization, and GC time ratio. Accepts app ID or partial name. | `--server`, `--format` |
| `apps compare <id1> [id2]` | Shortcut alias for `compare apps` — runs a full performance comparison and sets the comparison context for subsequent `compare` subcommands. | `--top-n N`, `--all`, `--threshold`, `--interactive`, `--server`, `--format` |

---

### `analyze` subcommands

| Subcommand | Description | Key options |
|------------|-------------|-------------|
| `analyze insights <id>` | All-in-one AI-powered analysis combining bottleneck detection, auto-scaling recommendations, shuffle skew detection, failed task analysis, and executor utilization — all in one call. Individual components can be toggled off. | `--no-auto-scaling`, `--no-shuffle-skew`, `--no-failed-tasks`, `--no-executor-utilization`, `--server`, `--format` |
| `analyze bottlenecks <id>` | Rank the top N performance bottlenecks across all jobs by computing weighted scores from task duration, failure rate, shuffle overhead, and spill. Returns actionable recommendations per bottleneck. | `--top-n N` (default 5), `--server`, `--format` |
| `analyze auto-scaling <id>` | Recommend the optimal executor count for each stage by comparing actual resource usage against a target stage duration. Highlights over-provisioned and under-provisioned stages. | `--target-duration N` (minutes, default 2), `--server`, `--format` |
| `analyze shuffle-skew <id>` | Detect data skew by comparing the maximum shuffle write per task against the median. Stages where the max/median ratio exceeds the threshold and total shuffle write exceeds the GB threshold are flagged with skew severity and remediation hints. | `--shuffle-threshold N` (GB, default 10), `--skew-ratio R` (default 2.0), `--server`, `--format` |
| `analyze slowest <id>` | Find the N slowest jobs, stages, or SQL queries by elapsed time. Use `--type stages` (default) to find stage bottlenecks, `--type jobs` for job-level, or `--type sql` for SQL execution plans. | `--type jobs\|stages\|sql`, `--top-n N` (default 5), `--server`, `--format` |
| `analyze compare <id1> <id2>` | ⚠️ **Deprecated** — use `apps compare` or `compare apps` instead. Runs a basic performance comparison without saving session context. Will be removed in a future version. | `--top-n`, `--server`, `--format` |

---

### `compare` subcommands

All `compare` subcommands use a **saved session context** — run `apps compare <id1> <id2>` once to set the two apps, then call any compare subcommand without repeating the IDs. Override with `--apps <id1> <id2>` on any individual command.

| Subcommand | Description | Key options |
|------------|-------------|-------------|
| `compare apps <id1> [id2]` | Full performance comparison: stage differences, executor overview, resource summary, and top metric deltas sorted by % change. Saves context for all subsequent `compare` calls. Accepts IDs, names, or `#N` number references; providing a single name auto-selects the 2 most recent matching apps. | `--top-n N`, `--threshold F` (significance, default 0.1 = 10%), `--all` (show all metrics), `--interactive`, `--server`, `--format` |
| `compare stages <s1> <s2>` | Deep-dive comparison of two specific stage IDs (one from each app in context). Shows per-task metrics, shuffle read/write, memory spill, GC time, and CPU efficiency side-by-side. Uses saved app context; override with `--apps`. | `--apps APP1 APP2`, `--significance-threshold F`, `--server`, `--format` |
| `compare timeline` | Compare executor utilization over time by dividing each app's run into fixed intervals and counting active executors per interval. Highlights periods of under-utilization or executor churn. | `--apps APP1 APP2`, `--interval-minutes N` (default 1), `--server`, `--format` |
| `compare stage-timeline <s1> <s2>` | Same as `compare timeline` but scoped to two specific stages. Useful for diagnosing why a stage ran longer in one app. | `--apps APP1 APP2`, `--interval-minutes N` (default 1), `--server`, `--format` |
| `compare resources` | Compare resource allocation: executor count, total cores, driver/executor memory settings, and max concurrent tasks. Surfaces configuration differences that explain runtime discrepancies. | `--apps APP1 APP2`, `--server`, `--format` |
| `compare env` | Compare Spark configuration properties between two apps. Auto-generated or transient values are filtered out so only meaningful config differences are shown (e.g. `spark.executor.memory`, `spark.sql.shuffle.partitions`). | `--apps APP1 APP2`, `--server`, `--format` |
| `compare summaries` | Compare aggregated stage-level metrics summaries: total executor run time, input/output bytes, shuffle totals, and task counts. More granular than `compare apps`; less detail than `compare stages`. | `--apps APP1 APP2`, `--server`, `--format` |
| `compare executors` | Compare per-executor performance: completed tasks, total duration, GC time, and shuffle metrics for each executor in both apps. Significant-only filtering removes noise from small differences. | `--apps APP1 APP2`, `--significance-threshold F`, `--show-all` (disable filter), `--server`, `--format` |
| `compare jobs` | Compare job-level metrics between the two apps in context: duration, task counts, failure rates, and stage breakdown per job. | `--apps APP1 APP2`, `--server`, `--format` |
| `compare status` | Display the currently saved comparison context — which two app IDs are set and which server they are on. | `--format` |
| `compare clear` | Delete the saved comparison context. Next `compare` subcommand will require `--apps` or will prompt to run `apps compare` first. | — |

---

### `server` subcommands

| Subcommand | Description | Key options |
|------------|-------------|-------------|
| `server start` | Start the MCP server using settings from `config.yaml`. CLI flags override the config values for that run only. | `--port N`, `--debug`, `--transport stdio\|sse\|streamable-http` |
| `server test` | Test HTTP connectivity to every server defined in `config.yaml`. Reports success or failure (with error type) per server. Useful for validating config before using the CLI or MCP mode. | `--timeout N` (seconds, default 10) |
| `server status` | Print MCP settings (port, transports, debug flag) and a list of every configured Spark History Server with its URL, default flag, and EMR cluster ARN if applicable. | — |

---

### `config` subcommands

| Subcommand | Description | Key options |
|------------|-------------|-------------|
| `config init` | Create a new `config.yaml` with default values. `--interactive` walks through each field (server URL, auth, SSL, EMR cluster ARN, MCP port). Warns if credentials are stored in plaintext and suggests environment variable alternatives. | `--interactive`, `--force` (overwrite existing) |
| `config show` | Print the current configuration. Use `--server <name>` to show only one server's settings. Supports `human`, `json`, and `yaml` output formats. | `--server`, `--format human\|json\|yaml` |
| `config validate` | Parse `config.yaml` and report structural errors, warnings (no default server, multiple defaults, missing URLs), and total server count. Exits non-zero on invalid config. | — |
| `config edit` | Open `config.yaml` in `$EDITOR` (falls back to `nano` then `vi`). Automatically re-validates the file after the editor closes and warns if the result is invalid. | — |

---

### `cache` subcommands

| Subcommand | Description | Key options |
|------------|-------------|-------------|
| `cache clear` | Remove all cached Spark History Server API responses from disk. The cache speeds up repeated reads of the same application data; clear it when the History Server data has changed. | — |

---

### `cleanup` subcommands

| Subcommand | Description | Key options |
|------------|-------------|-------------|
| `cleanup event-logs <gcs_dir>` | Delete Spark event log files from a GCS directory. Filters by maximum age and optional name pattern. `--dry-run` lists files that would be deleted without removing them. | `--max-duration DURATION`, `--name-pattern REGEX`, `--limit N`, `--dry-run`, `--server`, `--format` |

## 🧹 Cleanup (`cleanup`)

### Delete Event Logs
```bash
# Delete event logs from GCS older than 30 days
uv run spark-mcp --cli cleanup event-logs --gcs-dir gs://my-bucket/event-logs --max-duration 30d

# Dry run (list what would be deleted)
uv run spark-mcp --cli cleanup event-logs --gcs-dir gs://my-bucket/event-logs --max-duration 7d --dry-run

# Filter by application name pattern
uv run spark-mcp --cli cleanup event-logs --gcs-dir gs://my-bucket/event-logs --name-pattern "Test.*"
```

`cleanup` subcommands:
- `cleanup event-logs`

### Sample Output

Example: `apps list --limit 3`

```
Spark Applications (limit: 3)
------------------------------------------------------------
[1] app-20250131-010203  Daily ETL Job        FINISHED  8.4m
[2] app-20250131-004455  Daily ETL Job        FINISHED  7.9m
[3] app-20250130-231122  Weekly Backfill     FAILED    42.1m
```

### 📝 Quoting Application Names

**When quotes are REQUIRED:**
- Names with spaces: `"ETL Pipeline"`, `"Daily Job"`
- Special characters: `"My-App@Production"`, `"Job(v2)"`
- Names starting with hyphens: `"-debug-job"`

**When quotes are OPTIONAL:**
- Single words: `ETLPipeline` or `"ETLPipeline"` (both work)
- Underscores/hyphens: `ETL_Pipeline` or `"ETL_Pipeline"`

**Common Mistakes:**
```bash
# ❌ Wrong - shell splits into multiple arguments
uv run spark-mcp --cli apps compare ETL Pipeline

# ✅ Correct - quotes keep it as one argument
uv run spark-mcp --cli apps compare "ETL Pipeline"

# ✅ Also correct - single word, no spaces
uv run spark-mcp --cli apps compare ETLPipeline
```

### 🎯 Stateful Workflow

```bash
# Step 1: List apps to see available applications and get number references
uv run spark-mcp --cli apps list --limit 5
# Shows numbered list: 1, 2, 3...

# Step 2: Set comparison context using numbers or app IDs
uv run spark-mcp --cli apps compare 1 2                               # Use numbers from list
uv run spark-mcp --cli apps compare "ETL Pipeline"                    # Auto-compare last 2
uv run spark-mcp --cli apps compare app-20231201-123 app-20231202-456 # Specific IDs

# Step 3: Use saved context for detailed analysis
uv run spark-mcp --cli compare stages 1 1        # Compare stage 1 in both apps
uv run spark-mcp --cli compare timeline          # Timeline comparison
uv run spark-mcp --cli compare resources         # Resource comparison
uv run spark-mcp --cli compare executors         # Executor performance

# Step 4: Override context when needed
uv run spark-mcp --cli compare jobs --apps "Daily ETL" "Weekly ETL"
```

### 🏗️ Application-Level Comparisons

```bash
# Basic app performance comparison (sets context)
uv run spark-mcp --cli apps compare "ETL Pipeline"            # Auto-compare last 2
uv run spark-mcp --cli apps compare app1 app2                 # Compare specific IDs

# Focus on top differences
uv run spark-mcp --cli apps compare "Data Processing" --top-n 5

# Show all available executor metrics in the overview
uv run spark-mcp --cli apps compare "Data Processing" --all

# Resource allocation comparison (uses saved context)
uv run spark-mcp --cli compare resources

# Executor performance comparison (uses saved context)
uv run spark-mcp --cli compare executors

# Job-level performance comparison (uses saved context)
uv run spark-mcp --cli compare jobs

# Environment config comparison (auto-generated values are filtered)
uv run spark-mcp --cli compare env

# Application summaries (includes aggregated stage metrics)
uv run spark-mcp --cli compare summaries --apps app-123 app-456
```

### ⚡ Stage-Level Comparisons

```bash
# Compare specific stages (uses saved app context)
uv run spark-mcp --cli compare stages stage1 stage2

# Override app context for specific comparison
uv run spark-mcp --cli compare stages 1 2 --apps app3 app4

# Adjust significance threshold
uv run spark-mcp --cli compare stages 1 1 --significance-threshold 0.1
```

### ⏰ Timeline Comparisons

```bash
# Application-level executor timeline comparison
uv run spark-mcp --cli compare timeline

# Stage-level executor timeline comparison
uv run spark-mcp --cli compare stage-timeline stage1 stage2

# Custom time intervals
uv run spark-mcp --cli compare timeline --interval-minutes 5
uv run spark-mcp --cli compare stage-timeline 1 1 --interval-minutes 2
```

### 📊 Context Management

```bash
# Show current comparison context
uv run spark-mcp --cli compare status

# Clear comparison context
uv run spark-mcp --cli compare clear

# Check status in JSON format
uv run spark-mcp --cli compare status --format json
```

### 🔧 Advanced Usage

```bash
# Override context for specific command
uv run spark-mcp --cli compare timeline --apps different-app1 different-app2

# Filter by significance in executor comparison
uv run spark-mcp --cli compare executors --significance-threshold 0.3 --show-all

# Combine with different servers
uv run spark-mcp --cli apps compare prod-app staging-app --server production
uv run spark-mcp --cli compare stages 1 1 --server staging
```

## ⚙️ Configuration (`config`)

### Initialize Configuration
```bash
# Create default configuration
uv run spark-mcp --cli config init

# Interactive setup
uv run spark-mcp --cli config init --interactive

# Force overwrite existing config
uv run spark-mcp --cli config init --force
```

### View Configuration
```bash
# Show full configuration
uv run spark-mcp --cli config show

# Show specific server
uv run spark-mcp --cli config show --server local

# Different output formats
uv run spark-mcp --cli config show --format json
uv run spark-mcp --cli config show --format yaml
```

### Validate Configuration
```bash
# Validate config file
uv run spark-mcp --cli config validate
```

### Edit Configuration
```bash
# Open in default editor
uv run spark-mcp --cli config edit
```

## 🖥️ Server Management (`server`)

### Start MCP Server
```bash
# Start with default settings
uv run spark-mcp --cli server start

# Customize port and enable debug
uv run spark-mcp --cli server start --port 18889 --debug

# Specify transport
uv run spark-mcp --cli server start --transport stdio
```

### Test Server Connectivity
```bash
# Test all configured servers
uv run spark-mcp --cli server test

# Custom timeout
uv run spark-mcp --cli server test --timeout 30
```

### Show Server Status
```bash
# Show server configuration and status
uv run spark-mcp --cli server status
```

## 🎨 Output Formats

### Format Options
- **human** (default): Rich formatted output with colors and tables
- **json**: Machine-readable JSON output
- **table**: Simple tabular format

### Examples
```bash
# Human-readable (default)
uv run spark-mcp --cli apps list

# JSON output for scripting
uv run spark-mcp --cli apps list --format json | jq '.[] | .name'

# Table format
uv run spark-mcp --cli apps list --format table
```

## 🔧 Advanced Usage

### Multiple Servers
```bash
# List servers from different environments
uv run spark-mcp --cli apps list --server production
uv run spark-mcp --cli apps list --server staging
uv run spark-mcp --cli apps list --server local

# Compare across servers
uv run spark-mcp --cli analyze compare \
  prod-app-123 staging-app-456 \
  --server production
```

### Scripting and Automation
```bash
#!/bin/bash
# Daily performance report script

echo "=== Daily Spark Performance Report ==="
echo "Generated: $(date)"
echo

# Get recent applications
echo "Recent Applications:"
uv run spark-mcp --cli apps list \
  --limit 10 \
  --format table

# Analyze the most recent app
LATEST_APP=$(uv run spark-mcp --cli apps list --limit 1 --format json | jq -r '.[0].id')

echo "Analysis for $LATEST_APP:"
uv run spark-mcp --cli analyze insights "$LATEST_APP" \
  --format json > "report-$LATEST_APP.json"

echo "Report saved to report-$LATEST_APP.json"
```

### Filtering and Search
```bash
# Complex application filtering
uv run spark-mcp --cli apps list \
  --status COMPLETED \
  --min-date 2024-01-01 \
  --app-name "ETL" \
  --search-type contains \
  --limit 50

# Performance analysis with filtering
uv run spark-mcp --cli analyze insights app-123 \
  --include-auto-scaling \
  --include-shuffle-skew \
  --no-failed-tasks \
  --format json
```

## 🌐 Multi-Server Configuration

### Configuration Example
```yaml
# config.yaml
servers:
  production:
    default: true
    url: "http://prod-spark-history:18080"
    auth:
      username: "prod-user"
      password: "prod-pass"
    verify_ssl: true

  staging:
    url: "http://staging-spark-history:18080"
    verify_ssl: false

  local:
    url: "http://localhost:18080"

mcp:
  transports: ["stdio"]
  port: 18888
  debug: false
```

### Server-Specific Commands
```bash
# Use specific server for analysis
uv run spark-mcp --cli analyze insights app-123 --server production

# Compare across servers (not recommended - different data)
uv run spark-mcp --cli apps list --server production
uv run spark-mcp --cli apps list --server staging
```

## 🚨 Troubleshooting

### Common Issues

#### Command Not Found
```bash
# If spark-mcp command not found after global install
echo 'alias spark-mcp="uv run spark-mcp"' >> ~/.bashrc
source ~/.bashrc

# Or use full path
uv run spark-mcp --cli --help
```

#### Configuration Issues
```bash
# Validate configuration
uv run spark-mcp --cli config validate

# Test server connectivity
uv run spark-mcp --cli server test

# Check configuration
uv run spark-mcp --cli config show
```

#### Connection Problems
```bash
# Test specific server
uv run spark-mcp --cli server test --timeout 60

# Check server status
uv run spark-mcp --cli server status

# Verify URL in config
uv run spark-mcp --cli config show --server local
```

#### Permission Errors
```bash
# Use uv run instead of global install
uv run spark-mcp --cli apps list

# Check file permissions on config
ls -la ~/.config/spark-history-mcp/config.yaml
```

### Debug Mode
```bash
# Enable debug output
export SHS_MCP_DEBUG=true
uv run spark-mcp --cli apps list

# Or use server debug mode
uv run spark-mcp --cli server start --debug
```

### Environment Variables
```bash
# Override configuration with environment variables
export SHS_SERVERS_LOCAL_URL="http://localhost:18080"
export SHS_SERVERS_LOCAL_AUTH_USERNAME="admin"
export SHS_SERVERS_LOCAL_AUTH_PASSWORD="password"

uv run spark-mcp --cli apps list --server local
```

## 🔄 Migration from MCP Mode

If you're currently using MCP mode and want to try CLI:

```bash
# Your existing MCP setup
python -m spark_history_mcp.core.main

# Now try CLI with same configuration
uv run spark-mcp --cli apps list

# Both modes use the same config.yaml file
uv run spark-mcp --cli config show
```

## 📈 Performance Tips

### Efficient Usage
```bash
# Use limits for faster responses
uv run spark-mcp --cli apps list --limit 20

# Use JSON format for scripting
uv run spark-mcp --cli apps list --format json | jq '.[].name'

# Filter at the source
uv run spark-mcp --cli apps list --status COMPLETED --min-date 2024-01-01
```

### Batch Operations
```bash
# Analyze multiple applications
for app in app1 app2 app3; do
  echo "Analyzing $app..."
  uv run spark-mcp --cli analyze insights "$app" --format json > "analysis-$app.json"
done

# Compare multiple application pairs
uv run spark-mcp --cli analyze compare app1 app2 > comparison1.txt
uv run spark-mcp --cli analyze compare app2 app3 > comparison2.txt
```

## 🔗 Integration Examples

### CI/CD Pipeline
```yaml
# .github/workflows/spark-analysis.yml
name: Spark Performance Analysis

on:
  schedule:
    - cron: '0 9 * * *'  # Daily at 9 AM

jobs:
  analyze:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        run: pip install uv

      - name: Setup project
        run: uv sync

      - name: Run analysis
        run: |
          uv run spark-mcp --cli apps list \
            --limit 5 \
            --format json > apps.json

          # Analyze latest app
          LATEST_APP=$(jq -r '.[0].id' apps.json)
          uv run spark-mcp --cli analyze insights "$LATEST_APP" \
            --format json > analysis.json

      - name: Upload results
        uses: actions/upload-artifact@v4
        with:
          name: spark-analysis
          path: "*.json"
```

### Monitoring Script
```bash
#!/bin/bash
# monitor-spark.sh - Continuous monitoring

while true; do
  echo "=== $(date) ==="

  # Check for failed applications
  FAILED_COUNT=$(uv run spark-mcp --cli apps list \
    --status FAILED \
    --format json | jq length)

  if [ "$FAILED_COUNT" -gt 0 ]; then
    echo "⚠️  Found $FAILED_COUNT failed applications"
    uv run spark-mcp --cli apps list --status FAILED
  else
    echo "✅ No failed applications"
  fi

  sleep 300  # Check every 5 minutes
done
```

## 🔍 CLI vs MCP Mode Comparison

| Feature | CLI Mode | MCP Mode |
|---------|----------|----------|
| **Use Case** | Direct human interaction | AI agent integration |
| **Output** | Rich formatted tables/JSON | Structured data for LLMs |
| **Installation** | `uv sync` | MCP client + server setup |
| **Authentication** | Same config.yaml | Same config.yaml |
| **Tools Available** | All 50+ tools | All 50+ tools |
| **Performance** | Single command execution | Persistent server |
| **Scripting** | Shell scripts + JSON | AI agent workflows |
| **Learning Curve** | Familiar CLI patterns | MCP protocol knowledge |

## 🎓 Learning Path

### Beginner
1. Start with `apps list` to see available applications (note the numbered list!)
2. Use `apps show 1` to examine the first application (using number reference)
3. Try `analyze insights 1` for comprehensive analysis

### Intermediate
4. Use `repl` to enter the interactive shell — Tab completion makes exploration fast
5. Use short aliases inside REPL: `list`, `show 1`, `insights 1`, `compare 1 2`
6. Use `analyze bottlenecks 1` to identify issues
7. Explore `analyze slowest 1` for component analysis

### Advanced
8. Script with JSON output and jq processing
9. Set up monitoring with cron jobs
10. Integrate with CI/CD pipelines
11. Build custom analysis workflows

## 🆘 Getting Help

### Built-in Help
```bash
# General help
uv run spark-mcp --cli --help

# Start interactive REPL (type 'help' inside for alias reference)
uv run spark-mcp --cli repl

# Command-specific help
uv run spark-mcp --cli apps --help
uv run spark-mcp --cli analyze --help
uv run spark-mcp --cli config --help
uv run spark-mcp --cli server --help

# Subcommand help
uv run spark-mcp --cli apps list --help
uv run spark-mcp --cli analyze insights --help
```

### Documentation
- **Main README**: [README.md](README.md) - Overview and MCP integration
- **Testing Guide**: [TESTING.md](TESTING.md) - Setup and validation
- **Examples**: [examples/](examples/) - Integration examples

### Community
- **Issues**: [GitHub Issues](https://github.com/DeepDiagnostix-AI/mcp-apache-spark-history-server/issues)
- **Discussions**: [GitHub Discussions](https://github.com/DeepDiagnostix-AI/mcp-apache-spark-history-server/discussions)

---

🎯 **Ready to analyze your Spark applications?** Start with:
```bash
# Option A: one-off commands
uv run spark-mcp --cli apps list --limit 5   # List apps (numbered)
uv run spark-mcp --cli apps show 1           # Show first app
uv run spark-mcp --cli analyze insights 1    # Analyze first app
uv run spark-mcp --cli apps compare 1 2      # Compare first two apps

# Option B: interactive REPL (recommended for exploration)
uv run spark-mcp --cli repl
# spark> list
# spark> show 1
# spark> insights 1
# spark> compare 1 2
```
