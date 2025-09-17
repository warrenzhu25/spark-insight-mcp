# 🎯 Example Use Cases

This document provides detailed examples of how to use SparkInsight AI for common Spark monitoring and optimization scenarios.

## 🔍 Performance Investigation

### Scenario: ETL Job Running Slower
```
🤖 AI Query: "Why is my ETL job running slower than usual?"

📊 MCP Actions:
✅ Analyze application metrics
✅ Compare with historical performance
✅ Identify bottleneck stages
✅ Generate optimization recommendations
```

**Example Commands:**
```bash
# CLI approach
sparkinsight-ai analyze insights app-20231201-123456
sparkinsight-ai analyze bottlenecks app-20231201-123456 --top-n 5

# MCP tools used by AI:
# - get_application_insights
# - get_job_bottlenecks
# - compare_job_performance
# - list_slowest_stages
```

## 🚨 Failure Analysis

### Scenario: Job Failure Investigation
```
🤖 AI Query: "What caused job 42 to fail?"

🔍 MCP Actions:
✅ Examine failed tasks and error messages
✅ Review executor logs and resource usage
✅ Identify root cause and suggest fixes
```

**Example Commands:**
```bash
# CLI approach
sparkinsight-ai analyze insights app-20231201-123456
sparkinsight-ai apps jobs app-20231201-123456

# MCP tools used by AI:
# - analyze_failed_tasks
# - get_executor_summary
# - list_jobs
# - get_stage_task_summary
```

## 📈 Comparative Analysis

### Scenario: Performance Regression Detection
```
🤖 AI Query: "Compare today's batch job with yesterday's run"

📊 MCP Actions:
✅ Compare execution times and resource usage
✅ Identify performance deltas
✅ Highlight configuration differences
```

**Example Commands:**
```bash
# CLI approach
sparkinsight-ai analyze insights app-today-123
sparkinsight-ai analyze insights app-yesterday-456

# MCP tools used by AI:
# - compare_job_performance
# - compare_job_environments
# - get_application_insights
```

## 🚀 Auto-scaling Optimization

### Scenario: Resource Optimization
```
🤖 AI Query: "How can I optimize my Spark cluster auto-scaling?"

🔧 MCP Actions:
✅ Analyze executor utilization patterns
✅ Identify over/under-provisioning
✅ Generate scaling recommendations
```

**Example Commands:**
```bash
# CLI approach
sparkinsight-ai analyze auto-scaling app-20231201-123456 --target-duration 5
sparkinsight-ai analyze insights app-20231201-123456

# MCP tools used by AI:
# - analyze_auto_scaling
# - analyze_executor_utilization
# - get_resource_usage_timeline
```

## 📊 Data Skew Detection

### Scenario: Shuffle Performance Issues
```
🤖 AI Query: "My shuffle operations are slow. Is there data skew?"

📊 MCP Actions:
✅ Analyze shuffle skew patterns
✅ Identify problematic partitions
✅ Suggest optimization strategies
```

**Example Commands:**
```bash
# CLI approach
sparkinsight-ai analyze shuffle-skew app-20231201-123456 --shuffle-threshold 5
sparkinsight-ai analyze slowest app-20231201-123456 --type stages

# MCP tools used by AI:
# - analyze_shuffle_skew
# - get_stage_task_summary
# - list_slowest_stages
```

## 🔎 SQL Query Optimization

### Scenario: Slow SQL Performance
```
🤖 AI Query: "Which SQL queries are the slowest in my application?"

🔍 MCP Actions:
✅ Identify slowest SQL queries
✅ Compare execution plans
✅ Suggest query optimizations
```

**Example Commands:**
```bash
# CLI approach
sparkinsight-ai analyze slowest app-20231201-123456 --type sql --top-n 5

# MCP tools used by AI:
# - list_slowest_sql_queries
# - compare_sql_execution_plans
# - get_application_insights
```

## 📝 SparkInsight Integration

The MCP server includes intelligent analysis capabilities inspired by SparkInsight! See the **[SparkInsight Integration Guide](examples/sparkinsight/README.md)** for:

- 🚀 **Auto-scaling optimization** recommendations
- 📊 **Data skew detection** and mitigation strategies
- 🚨 **Failure analysis** with root cause identification
- 📈 **Executor utilization** optimization insights
- 🧠 **Comprehensive analysis** combining all insights

## 🔧 Advanced Use Cases

### Multi-Application Analysis
```bash
# Compare multiple applications
sparkinsight-ai apps list --status COMPLETED --limit 5
# Then analyze each individually or use MCP tools for batch analysis
```

### Historical Trend Analysis
```bash
# Analyze trends over time
sparkinsight-ai apps list --name "daily-etl" --limit 10
# Use compare_job_performance tool for historical comparison
```

### Environment Comparison
```bash
# Compare between environments
sparkinsight-ai apps list --server production
sparkinsight-ai apps list --server staging
# Use compare_job_environments for configuration diff
```