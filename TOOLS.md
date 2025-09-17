# 🛠️ Available MCP Tools

> **Note**: These tools are subject to change as we scale and improve the performance of the MCP server.

The MCP server provides **22 specialized tools** organized by analysis patterns. LLMs can intelligently select and combine these tools based on user queries:

## 📊 Application Information
*Basic application metadata and overview*
| 🔧 Tool | 📝 Description |
|---------|----------------|
| `get_application` | 📊 Get detailed information about a specific Spark application including status, resource usage, duration, and attempt details |
| `list_applications` | 📋 Get a list of all Spark applications with optional filtering by status, dates, limits, and name patterns (exact, contains, regex) |

## 🔗 Job Analysis
*Job-level performance analysis and identification*
| 🔧 Tool | 📝 Description |
|---------|----------------|
| `list_jobs` | 🔗 Get a list of all jobs for a Spark application with optional status filtering |
| `list_slowest_jobs` | ⏱️ Get the N slowest jobs for a Spark application (excludes running jobs by default) |

## ⚡ Stage Analysis
*Stage-level performance deep dive and task metrics*
| 🔧 Tool | 📝 Description |
|---------|----------------|
| `list_stages` | ⚡ Get a list of all stages for a Spark application with optional status filtering and summaries |
| `list_slowest_stages` | 🐌 Get the N slowest stages for a Spark application (excludes running stages by default) |
| `get_stage` | 🎯 Get information about a specific stage with optional attempt ID and summary metrics |
| `get_stage_task_summary` | 📊 Get statistical distributions of task metrics for a specific stage (execution times, memory usage, I/O metrics) |

## 🖥️ Executor & Resource Analysis
*Resource utilization, executor performance, and allocation tracking*
| 🔧 Tool | 📝 Description |
|---------|----------------|
| `list_executors` | 🖥️ Get executor information with optional inactive executor inclusion |
| `get_executor` | 🔍 Get information about a specific executor including resource allocation, task statistics, and performance metrics |
| `get_executor_summary` | 📈 Aggregates metrics across all executors (memory usage, disk usage, task counts, performance metrics) |
| `get_resource_usage_timeline` | 📅 Get chronological view of resource allocation and usage patterns including executor additions/removals |

## ⚙️ Configuration & Environment
*Spark configuration, environment variables, and runtime settings*
| 🔧 Tool | 📝 Description |
|---------|----------------|
| `get_environment` | ⚙️ Get comprehensive Spark runtime configuration including JVM info, Spark properties, system properties, and classpath |

## 🔎 SQL & Query Analysis
*SQL performance analysis and execution plan comparison*
| 🔧 Tool | 📝 Description |
|---------|----------------|
| `list_slowest_sql_queries` | 🐌 Get the top N slowest SQL queries for an application with detailed execution metrics |
| `compare_sql_execution_plans` | 🔍 Compare SQL execution plans between two Spark jobs, analyzing logical/physical plans and execution metrics |

## 🚨 Performance & Bottleneck Analysis
*Intelligent bottleneck identification and performance recommendations*
| 🔧 Tool | 📝 Description |
|---------|----------------|
| `get_job_bottlenecks` | 🚨 Identify performance bottlenecks by analyzing stages, tasks, and executors with actionable recommendations |

## 🔄 Comparative Analysis
*Cross-application comparison for regression detection and optimization*
| 🔧 Tool | 📝 Description |
|---------|----------------|
| `compare_job_environments` | ⚙️ Compare Spark environment configurations between two jobs to identify differences in properties and settings |
| `compare_job_performance` | 📈 Compare performance metrics between two Spark jobs including execution times, resource usage, and task distribution |

## 🧠 SparkInsight Intelligence
*AI-powered analysis tools inspired by SparkInsight for intelligent performance optimization*
| 🔧 Tool | 📝 Description |
|---------|----------------|
| `analyze_auto_scaling` | 🚀 Analyze workload patterns and provide intelligent auto-scaling recommendations for dynamic allocation |
| `analyze_shuffle_skew` | 📊 Detect and analyze data skew in shuffle operations with actionable optimization suggestions |
| `analyze_failed_tasks` | 🚨 Investigate task failures to identify patterns, problematic executors, and root causes |
| `analyze_executor_utilization` | 📈 Track executor utilization over time to identify over/under-provisioning and optimization opportunities |
| `get_application_insights` | 🧠 **Comprehensive SparkInsight analysis** - Runs all analyzers to provide complete performance overview and recommendations |

## 🤖 How LLMs Use These Tools

**Query Pattern Examples:**
- *"Why is my job slow?"* → `get_job_bottlenecks` + `list_slowest_stages` + `get_executor_summary`
- *"Compare today vs yesterday"* → `compare_job_performance` + `compare_job_environments`
- *"What's wrong with stage 5?"* → `get_stage` + `get_stage_task_summary`
- *"Show me resource usage over time"* → `get_resource_usage_timeline` + `get_executor_summary`
- *"Find my slowest SQL queries"* → `list_slowest_sql_queries` + `compare_sql_execution_plans`
- *"Analyze my app performance with insights"* → `get_application_insights` (comprehensive SparkInsight analysis)
- *"Help me optimize auto-scaling"* → `analyze_auto_scaling` + `analyze_executor_utilization`
- *"Why are my tasks failing?"* → `analyze_failed_tasks` + `get_executor_summary`
- *"Check for data skew issues"* → `analyze_shuffle_skew` + `get_stage_task_summary`