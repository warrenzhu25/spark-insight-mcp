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
| `apps` | Application management (list, show, details) |
| `analyze` | Performance analysis and insights |
| `server` | MCP server management |
| `config` | Configuration management |

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

### Application Comparison
```bash
# Compare two applications
uv run spark-mcp --cli analyze compare app1 app2

# Focus on top differences
uv run spark-mcp --cli analyze compare app1 app2 --top-n 5

# Use specific server
uv run spark-mcp --cli analyze compare app1 app2 --server production
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
1. Start with `apps list` to see available applications
2. Use `apps show` to examine specific applications
3. Try `analyze insights` for comprehensive analysis

### Intermediate
4. Learn `analyze compare` for performance comparisons
5. Use `analyze bottlenecks` to identify issues
6. Explore `analyze slowest` for component analysis

### Advanced
7. Script with JSON output and jq processing
8. Set up monitoring with cron jobs
9. Integrate with CI/CD pipelines
10. Build custom analysis workflows

## 🆘 Getting Help

### Built-in Help
```bash
# General help
uv run spark-mcp --cli --help

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
uv run spark-mcp --cli apps list --limit 5
```
