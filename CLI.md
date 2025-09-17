# 🖥️ Command Line Interface

SparkInsight AI includes a comprehensive CLI for direct Spark analysis without needing MCP clients! Use the enhanced command line interface for interactive analysis, server management, and configuration.

## 🚀 CLI Quick Start

```bash
# Initialize configuration
sparkinsight-ai config init --interactive

# List Spark applications
sparkinsight-ai apps list

# Get detailed application info
sparkinsight-ai apps show app-20231201-123456

# Run comprehensive analysis
sparkinsight-ai analyze insights app-20231201-123456

# Find performance bottlenecks
sparkinsight-ai analyze bottlenecks app-20231201-123456

# Start MCP server (same as before)
sparkinsight-ai server start
```

## 📊 CLI Command Overview

| Command Category | Commands | Description |
|------------------|----------|-------------|
| **📱 Applications** | `apps list`, `apps show`, `apps jobs`, `apps stages`, `apps executors` | List and inspect Spark applications, jobs, stages, and executors |
| **🔍 Analysis** | `analyze insights`, `analyze bottlenecks`, `analyze auto-scaling`, `analyze shuffle-skew`, `analyze slowest` | Intelligent performance analysis and optimization recommendations |
| **⚙️ Server** | `server start`, `server test`, `server status` | Manage MCP server and test connectivity |
| **🔧 Config** | `config init`, `config show`, `config validate`, `config edit` | Create and manage configuration files |

## 🎯 CLI Usage Examples

### Basic Application Analysis
```bash
# List recent applications
sparkinsight-ai apps list --limit 10

# Filter by status
sparkinsight-ai apps list --status COMPLETED --status FAILED

# Search by name
sparkinsight-ai apps list --name "etl" --format table

# Get application details with JSON output
sparkinsight-ai apps show app-20231201-123456 --format json
```

### Performance Analysis
```bash
# Comprehensive SparkInsight analysis
sparkinsight-ai analyze insights app-20231201-123456

# Focus on specific analysis types
sparkinsight-ai analyze auto-scaling app-20231201-123456 --target-duration 5
sparkinsight-ai analyze shuffle-skew app-20231201-123456 --shuffle-threshold 5
sparkinsight-ai analyze slowest app-20231201-123456 --type sql --top-n 3

# Identify bottlenecks
sparkinsight-ai analyze bottlenecks app-20231201-123456 --top-n 10
```

### Configuration Management
```bash
# Interactive configuration setup
sparkinsight-ai config init --interactive

# Test server connectivity
sparkinsight-ai server test

# Show current configuration
sparkinsight-ai config show --format yaml

# Validate configuration
sparkinsight-ai config validate
```

### Multi-Server Usage
```bash
# Use specific server
sparkinsight-ai apps list --server production

# Test specific server
sparkinsight-ai server test --server staging

# Run analysis on different servers
sparkinsight-ai analyze insights app-123 --server dev
```

## 📄 Output Formats

All CLI commands support multiple output formats:

- **`--format human`** (default): Rich, colorized output with tables and panels
- **`--format json`**: Machine-readable JSON for scripting
- **`--format table`**: Simple tabular output

## 📊 Sample Data
The repository includes real Spark event logs for testing:
- `spark-bcec39f6201b42b9925124595baad260` - ✅ Successful ETL job
- `spark-110be3a8424d4a2789cb88134418217b` - 🔄 Data processing job
- `spark-cc4d115f011443d787f03a71a476a745` - 📈 Multi-stage analytics job

See **[TESTING.md](TESTING.md)** for using them.

## ⚙️ Server Configuration
Edit `config.yaml` for your Spark History Server:
```yaml
servers:
  local:
    default: true
    url: "http://your-spark-history-server:18080"
    auth:  # optional
      username: "user"
      password: "pass"
mcp:
  transports:
    - streamable-http # streamable-http or stdio.
  port: "18888"
  debug: true
```