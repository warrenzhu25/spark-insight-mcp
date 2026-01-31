# Gemini CLI Integration

Connect Gemini CLI to SparkInsight AI for intelligent Spark job analysis through natural language queries.

## Prerequisites

1. **Install Gemini CLI**: Follow the [official installation guide](https://gemini-cli.xyz/docs/en/installation)
2. **Running Spark History Server**: Either use our test setup or have your own server accessible

### Source Code Development

**Use case**: You're developing or need custom modifications

**Configuration**: Add to your `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
     "mcp-apache-spark-history-server": {
        "command": "uv",
        "args": [
           "run",
           "-m",
           "spark_history_mcp.core.main",
           "--frozen"
        ],
        "cwd": "/path/to/spark-insight-mcp",
        "env": {
           "SHS_MCP_TRANSPORT": "stdio"
        }
     }
  }
}
```

## Example Usage

### Basic Queries
```bash
# List available Spark applications
gemini ask "List all Spark applications in the history server"

# Analyze a specific application
gemini ask "Analyze the performance of Spark application spark-110be3a8424d4a2789cb88134418217b"

# Compare applications
gemini ask "Compare the performance between applications spark-cc4d115f011443d787f03a71a476a745 and spark-110be3a8424d4a2789cb88134418217b"
```

### Advanced Analysis
```bash
# Identify bottlenecks
gemini ask "What are the main performance bottlenecks in application spark-bcec39f6201b42b9925124595baad260?"

# Check for data skew
gemini ask "Analyze data skew in shuffle operations for the latest Spark application"

# Get optimization recommendations
gemini ask "Provide auto-scaling recommendations for application spark-110be3a8424d4a2789cb88134418217b"
```
