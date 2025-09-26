"""
Performance Analysis Prompts for Spark Applications

These prompts guide AI agents in performing structured performance analysis
of Spark applications, identifying bottlenecks, and suggesting optimizations.
"""

from typing import Optional

from spark_history_mcp.core.app import mcp


@mcp.prompt()
def analyze_slow_application(
    app_id: str, baseline_duration_minutes: int = 60, server: Optional[str] = None
) -> str:
    """Generate a structured analysis prompt for slow-running Spark applications.

    Args:
        app_id: The Spark application ID to analyze
        baseline_duration_minutes: Expected duration to compare against
        server: Optional server name to use for analysis
    """
    server_param = f', server="{server}"' if server else ""

    return f"""Please perform a comprehensive performance analysis of Spark application {app_id}.

**Analysis Framework:**

1. **Application Overview**
   - Get basic application information and runtime statistics
   - Compare actual runtime against expected {baseline_duration_minutes} minutes baseline
   - Identify if this is significantly slower than expected

2. **Performance Bottleneck Investigation**
   - Identify the slowest stages and jobs
   - Examine task distribution and execution patterns
   - Check for resource utilization inefficiencies

3. **Deep Dive Analysis**
   - Analyze data skew and shuffle operations
   - Examine executor utilization patterns over time
   - Review failed tasks and error patterns

4. **Configuration Assessment**
   - Compare Spark configuration with best practices
   - Identify potential configuration optimizations

**Recommended MCP Tool Sequence:**

```
# Basic Application Analysis
get_application("{app_id}"{server_param})

# Comprehensive Performance Analysis
get_application_insights("{app_id}"{server_param})

# Specific Bottleneck Investigation
get_job_bottlenecks("{app_id}"{server_param})
list_slowest_stages("{app_id}"{server_param})

# Resource and Data Analysis
analyze_executor_utilization("{app_id}"{server_param})
analyze_shuffle_skew("{app_id}"{server_param})

# Configuration Review
get_environment("{app_id}"{server_param})
```

**Expected Output:**
- Performance summary with key metrics
- Top 3 performance bottlenecks identified
- Specific, actionable optimization recommendations
- Resource utilization efficiency assessment
- Configuration tuning suggestions

Focus on providing **concrete, measurable recommendations** that can directly improve application performance."""


@mcp.prompt()
def investigate_stage_bottlenecks(
    app_id: str, stage_id: Optional[int] = None, server: Optional[str] = None
) -> str:
    """Generate a detailed prompt for investigating stage-level performance bottlenecks.

    Args:
        app_id: The Spark application ID
        stage_id: Optional specific stage ID to focus on
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""
    stage_focus = f" focusing on stage {stage_id}" if stage_id else ""

    return f"""Perform a detailed stage-level performance investigation for Spark application {app_id}{stage_focus}.

**Stage Analysis Framework:**

1. **Stage Performance Overview**
   - Identify the slowest stages in the application
   - Analyze stage execution patterns and dependencies
   - Compare stage durations and task distributions

2. **Task-Level Deep Dive**
   - Examine task execution times and resource usage
   - Identify task stragglers and performance outliers
   - Analyze task failure patterns and retry behavior

3. **Resource Utilization Analysis**
   - Review executor allocation during stage execution
   - Check for resource contention and underutilization
   - Examine memory usage and spill patterns

4. **Data Processing Patterns**
   - Analyze shuffle operations and data movement
   - Check for data skew affecting task performance
   - Review input/output patterns and partition sizing

**MCP Tool Investigation Sequence:**

```
# Stage Performance Analysis
list_slowest_stages("{app_id}"{server_param})
{"get_stage(" + f'"{app_id}", {stage_id}' + f"{server_param})" if stage_id else "# Focus on the slowest stages identified above"}

# Task-Level Metrics
{"get_stage_task_summary(" + f'"{app_id}", {stage_id}' + f"{server_param})" if stage_id else "# Get task summaries for problematic stages"}

# Resource and Data Analysis
get_resource_usage_timeline("{app_id}"{server_param})
analyze_shuffle_skew("{app_id}"{server_param})

# Executor Performance
list_executors("{app_id}"{server_param})
get_executor_summary("{app_id}"{server_param})
```

**Analysis Focus Areas:**
- Task execution time distributions (look for high variance)
- Memory spill indicators (high disk usage)
- Shuffle read/write patterns and data skew
- Executor utilization during stage execution
- Failed task patterns and root causes

**Expected Deliverables:**
- Stage performance ranking with duration analysis
- Task-level bottleneck identification
- Resource utilization efficiency report
- Specific recommendations for stage optimization
- Data partitioning and skew mitigation strategies"""


@mcp.prompt()
def diagnose_resource_issues(
    app_id: str, focus_area: str = "all", server: Optional[str] = None
) -> str:
    """Generate a prompt for diagnosing resource utilization issues in Spark applications.

    Args:
        app_id: The Spark application ID
        focus_area: Resource area to focus on ('memory', 'cpu', 'disk', 'network', 'all')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    focus_guidance = {
        "memory": "Pay special attention to memory usage patterns, GC pressure, and spill metrics",
        "cpu": "Focus on executor utilization, task parallelism, and compute efficiency",
        "disk": "Examine disk usage, spill operations, and I/O patterns",
        "network": "Analyze shuffle operations, data transfer patterns, and network utilization",
        "all": "Provide comprehensive analysis across all resource dimensions",
    }.get(focus_area, "Provide comprehensive analysis across all resource dimensions")

    return f"""Diagnose resource utilization issues for Spark application {app_id}.

**Resource Analysis Focus:** {focus_guidance}

**Diagnostic Framework:**

1. **Resource Allocation Assessment**
   - Review requested vs. actual resource allocation
   - Analyze executor and core distribution
   - Check dynamic allocation effectiveness

2. **Utilization Patterns Analysis**
   - Track resource usage over application lifetime
   - Identify periods of over/under-utilization
   - Examine resource contention indicators

3. **Efficiency Metrics Evaluation**
   - Calculate resource utilization efficiency ratios
   - Identify waste and optimization opportunities
   - Compare against resource allocation best practices

4. **Performance Impact Assessment**
   - Correlate resource issues with performance bottlenecks
   - Identify resource constraints affecting throughput
   - Assess impact of resource issues on overall job performance

**MCP Tool Diagnostic Sequence:**

```
# Application and Executor Overview
get_application("{app_id}"{server_param})
list_executors("{app_id}"{server_param}, include_inactive=true)
get_executor_summary("{app_id}"{server_param})

# Resource Timeline and Utilization
get_resource_usage_timeline("{app_id}"{server_param})
analyze_executor_utilization("{app_id}"{server_param})

# Performance and Configuration Context
get_job_bottlenecks("{app_id}"{server_param})
get_environment("{app_id}"{server_param})

# Auto-scaling Analysis
analyze_auto_scaling("{app_id}"{server_param})
```

**Key Diagnostic Areas:**

**Memory Issues:**
- GC pressure and memory spill patterns
- Heap utilization and off-heap usage
- Memory-related task failures

**CPU/Compute Issues:**
- Executor utilization rates and idle time
- Task parallelism and core efficiency
- Compute-bound vs. I/O-bound identification

**Disk Issues:**
- Spill-to-disk operations and frequency
- Local disk usage and I/O patterns
- Disk-related performance bottlenecks

**Network Issues:**
- Shuffle data transfer volumes and patterns
- Network-related task delays
- Data locality and network utilization

**Expected Output:**
- Resource utilization efficiency scores
- Specific resource bottlenecks identified
- Resource allocation optimization recommendations
- Configuration changes to improve resource usage
- Cost optimization opportunities based on resource analysis"""


@mcp.prompt()
def compare_job_performance(
    app_id1: str,
    app_id2: str,
    comparison_focus: str = "comprehensive",
    server: Optional[str] = None,
) -> str:
    """Generate a structured prompt for comparing performance between two Spark applications.

    Args:
        app_id1: First Spark application ID (baseline)
        app_id2: Second Spark application ID (comparison target)
        comparison_focus: Focus area ('performance', 'resources', 'configuration', 'comprehensive')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    focus_guidance = {
        "performance": "Focus primarily on execution times, throughput, and performance metrics",
        "resources": "Emphasize resource utilization, allocation, and efficiency comparisons",
        "configuration": "Concentrate on configuration differences and their performance impact",
        "comprehensive": "Provide detailed comparison across all dimensions",
    }.get(comparison_focus, "Provide detailed comparison across all dimensions")

    return f"""Compare performance between Spark applications {app_id1} (baseline) and {app_id2} (target).

**Comparison Focus:** {focus_guidance}

**Performance Comparison Framework:**

1. **Executive Summary Comparison**
   - Overall runtime and performance delta
   - Resource utilization efficiency changes
   - Key performance indicators comparison

2. **Detailed Performance Analysis**
   - Job and stage execution time comparisons
   - Throughput and processing rate analysis
   - Task performance distribution changes

3. **Resource Utilization Comparison**
   - Executor allocation and usage patterns
   - Memory, CPU, and I/O utilization deltas
   - Resource efficiency improvements/regressions

4. **Configuration Impact Analysis**
   - Configuration differences between applications
   - Impact of configuration changes on performance
   - Optimization opportunities identified

**MCP Tool Comparison Sequence:**

```
# Direct Performance Comparison
compare_job_performance("{app_id1}", "{app_id2}"{server_param})

# Configuration Comparison
compare_job_environments("{app_id1}", "{app_id2}"{server_param})

# Individual Application Analysis for Context
get_application("{app_id1}"{server_param})
get_application("{app_id2}"{server_param})

# Detailed Performance Insights
get_application_insights("{app_id1}"{server_param})
get_application_insights("{app_id2}"{server_param})

# Resource Utilization Analysis
get_executor_summary("{app_id1}"{server_param})
get_executor_summary("{app_id2}"{server_param})

# Bottleneck Comparison
get_job_bottlenecks("{app_id1}"{server_param})
get_job_bottlenecks("{app_id2}"{server_param})
```

**Comparison Dimensions:**

**Performance Metrics:**
- Total execution time and stage duration changes
- Task completion rates and failure patterns
- Throughput and processing efficiency

**Resource Efficiency:**
- Executor utilization improvement/degradation
- Memory usage patterns and GC pressure changes
- I/O and network utilization differences

**Configuration Impact:**
- Spark property changes and their effects
- Resource allocation differences
- Dynamic allocation and scaling behavior

**Data Processing:**
- Shuffle operation efficiency changes
- Data skew patterns and improvements
- SQL query performance variations

**Expected Deliverables:**
- Performance delta summary with key metrics
- Root cause analysis for performance changes
- Configuration change impact assessment
- Recommendations for optimizing the target application
- Trend analysis and performance trajectory insights

**Output Format:**
Present findings in a structured comparison table showing:
- Metric | App {app_id1} | App {app_id2} | Delta | Impact Analysis
- Focus on the most significant changes and their business impact"""
