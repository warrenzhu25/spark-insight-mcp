# SparkInsight Integration Example

This example demonstrates how to use the SparkInsight-inspired analysis tools integrated into the MCP Spark History Server.

## 🧠 SparkInsight Features

The MCP server now includes intelligent analysis tools based on SparkInsight:

### 1. Auto-scaling Analysis
```bash
# Get auto-scaling recommendations for an application
analyze_auto_scaling(app_id="application_123456789_0001")
```

**What it does:**
- Analyzes workload patterns during application execution
- Recommends optimal initial and maximum executor counts
- Targets 2-minute stage completion times by default
- Provides configuration diff between current and recommended settings

### 2. Shuffle Skew Detection
```bash
# Detect data skew in shuffle operations
analyze_shuffle_skew(app_id="application_123456789_0001", shuffle_threshold_gb=5)
```

**What it does:**
- Identifies stages with uneven data distribution in shuffle operations
- Calculates skew ratios (max task shuffle write / median task shuffle write)
- Flags stages with ratios > 2.0 as problematic
- Provides actionable recommendations for data partitioning optimization

### 3. Failed Task Analysis
```bash
# Investigate task failures and problematic executors
analyze_failed_tasks(app_id="application_123456789_0001")
```

**What it does:**
- Analyzes patterns in task failures across stages and executors
- Identifies problematic hosts or executors with high failure rates
- Provides recommendations for infrastructure improvements
- Suggests retry policy adjustments

### 4. Executor Utilization Analysis
```bash
# Track executor usage patterns over time
analyze_executor_utilization(app_id="application_123456789_0001")
```

**What it does:**
- Creates timeline of executor allocation throughout application lifecycle
- Calculates utilization efficiency metrics
- Identifies over/under-provisioning periods
- Suggests dynamic allocation optimizations

### 5. Comprehensive Application Insights
```bash
# Run all SparkInsight analyses at once
get_application_insights(app_id="application_123456789_0001")
```

**What it does:**
- Runs all SparkInsight analysis tools in parallel
- Aggregates recommendations by priority (critical, high, medium, low)
- Provides overall application health assessment
- Gives complete performance optimization overview

## 🚀 Example Usage with AI Agents

### Claude Desktop Integration
```json
{
  "mcpServers": {
    "spark-history": {
      "command": "uvx",
      "args": ["mcp-apache-spark-history-server"],
      "env": {
        "SHS_MCP_CONFIG": "config.yaml"
      }
    }
  }
}
```

### Example Queries
Ask your AI assistant:

- *"Analyze application_123456789_0001 with SparkInsight-style analysis"*
- *"What auto-scaling settings should I use for this app?"*
- *"Are there any data skew issues I should fix?"*
- *"Why are my tasks failing and how can I fix it?"*
- *"Show me executor utilization patterns and optimization opportunities"*

## 🔧 Configuration Options

### Auto-scaling Analysis
- `target_stage_duration_minutes`: Target completion time (default: 2 minutes)

### Shuffle Skew Analysis
- `shuffle_threshold_gb`: Minimum shuffle data to analyze (default: 10 GB)
- `skew_ratio_threshold`: Minimum ratio to flag as skewed (default: 2.0)

### Failed Task Analysis
- `failure_threshold`: Minimum failures to include (default: 1)

### Executor Utilization
- `interval_minutes`: Time interval for analysis (default: 1 minute)

## 📊 Sample Output

### Auto-scaling Recommendations
```json
{
  "application_id": "application_123456789_0001",
  "analysis_type": "Auto-scaling Configuration",
  "recommendations": {
    "initial_executors": {
      "current": "2",
      "recommended": "8",
      "description": "Based on stages running in first 2 minutes"
    },
    "max_executors": {
      "current": "20",
      "recommended": "32",
      "description": "Based on peak concurrent stage demand"
    }
  }
}
```

### Shuffle Skew Detection
```json
{
  "application_id": "application_123456789_0001",
  "analysis_type": "Shuffle Skew Analysis",
  "skewed_stages": [
    {
      "stage_id": 15,
      "name": "join at DataProcessor.scala:45",
      "skew_ratio": 8.5,
      "max_shuffle_write_mb": 1024.5,
      "median_shuffle_write_mb": 120.3,
      "recommendations": ["Consider data repartitioning", "Use salting technique"]
    }
  ]
}
```

## 💡 Best Practices

1. **Run comprehensive analysis first**: Use `get_application_insights()` for overview
2. **Focus on critical issues**: Address recommendations marked as "critical" priority first
3. **Iterative optimization**: Re-run analysis after applying recommendations
4. **Monitor trends**: Compare metrics across multiple application runs
5. **Combine with existing tools**: Use alongside existing MCP tools for complete analysis

## 🔗 Related Tools

Combine SparkInsight analysis with existing MCP tools:
- `get_job_bottlenecks()` - Performance bottleneck identification
- `compare_job_performance()` - Cross-application comparison
- `get_resource_usage_timeline()` - Resource allocation timeline
- `list_slowest_stages()` - Stage-level performance analysis
