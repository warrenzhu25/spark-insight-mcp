"""
Troubleshooting and Debugging Prompts for Spark Applications

These prompts guide AI agents in systematic investigation of Spark application
issues, failures, and configuration problems.
"""

from typing import Optional

from spark_history_mcp.core.app import mcp


@mcp.prompt()
def investigate_failures(
    app_id: str, failure_type: str = "all", server: Optional[str] = None
) -> str:
    """Generate a systematic prompt for investigating application failures and errors.

    Args:
        app_id: The Spark application ID to investigate
        failure_type: Type of failure to focus on ('task', 'job', 'stage', 'executor', 'all')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    focus_area = {
        "task": "Focus specifically on task-level failures, retry patterns, and task error analysis",
        "job": "Concentrate on job-level failures and their cascading effects",
        "stage": "Examine stage failures and their impact on application progress",
        "executor": "Investigate executor failures, node issues, and infrastructure problems",
        "all": "Provide comprehensive failure analysis across all levels",
    }.get(failure_type, "Provide comprehensive failure analysis across all levels")

    return f"""Investigate failures and errors in Spark application {app_id}.

**Investigation Focus:** {focus_area}

**Failure Investigation Framework:**

1. **Failure Pattern Analysis**
   - Identify failure frequency and distribution patterns
   - Categorize failure types and severity levels
   - Determine failure timing and correlation with application phases

2. **Root Cause Investigation**
   - Examine error messages and stack traces
   - Investigate infrastructure and resource-related failures
   - Analyze configuration issues leading to failures

3. **Impact Assessment**
   - Evaluate performance impact of failures and retries
   - Assess resource waste due to failed tasks
   - Determine business impact of application reliability issues

4. **Remediation Strategy Development**
   - Identify immediate fixes for critical issues
   - Develop preventive measures for recurring failures
   - Recommend configuration and infrastructure improvements

**MCP Tool Investigation Sequence:**

```
# Application Overview and Status
get_application("{app_id}"{server_param})

# Comprehensive Failure Analysis
analyze_failed_tasks("{app_id}"{server_param})

# Executor and Infrastructure Analysis
list_executors("{app_id}"{server_param}, include_inactive=true)
get_executor_summary("{app_id}"{server_param})

# Performance Context
get_job_bottlenecks("{app_id}"{server_param})
list_slowest_stages("{app_id}"{server_param})

# Configuration Review
get_environment("{app_id}"{server_param})

# Comprehensive Analysis
get_application_insights("{app_id}"{server_param})
```

**Investigation Focus Areas:**

**Task Failures:**
- Task retry patterns and success rates
- Error message analysis and categorization
- Resource-related task failures (OOM, disk space)
- Data corruption or processing errors

**Job/Stage Failures:**
- Job failure cascading effects
- Stage dependency failures
- Critical path impact analysis
- Recovery and retry effectiveness

**Executor Failures:**
- Node/host-specific failure patterns
- Container or process-related issues
- Network connectivity problems
- Resource exhaustion on specific nodes

**Configuration Issues:**
- Timeout and retry configuration problems
- Resource allocation misconfigurations
- Serialization and compatibility issues
- Driver-executor communication problems

**Expected Investigation Results:**
- Failure pattern summary with statistics
- Root cause analysis for major failure categories
- Infrastructure reliability assessment
- Configuration recommendations for failure reduction
- Monitoring and alerting suggestions for proactive issue detection

**Actionable Recommendations:**
Focus on providing specific fixes such as:
- Configuration parameter adjustments
- Infrastructure improvements needed
- Code changes to handle edge cases
- Monitoring setup for early failure detection"""


@mcp.prompt()
def examine_memory_issues(
    app_id: str, memory_focus: str = "comprehensive", server: Optional[str] = None
) -> str:
    """Generate a detailed prompt for investigating memory-related problems in Spark applications.

    Args:
        app_id: The Spark application ID
        memory_focus: Memory area to focus on ('heap', 'offheap', 'spill', 'gc', 'comprehensive')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    focus_guidance = {
        "heap": "Focus on heap memory usage patterns, allocation, and OutOfMemory errors",
        "offheap": "Examine off-heap memory usage and configuration effectiveness",
        "spill": "Investigate memory spill patterns and disk spill operations",
        "gc": "Analyze garbage collection patterns, pressure, and impact on performance",
        "comprehensive": "Provide thorough analysis across all memory-related aspects",
    }.get(memory_focus, "Provide thorough analysis across all memory-related aspects")

    return f"""Investigate memory-related issues in Spark application {app_id}.

**Memory Analysis Focus:** {focus_guidance}

**Memory Investigation Framework:**

1. **Memory Usage Pattern Analysis**
   - Analyze memory allocation and utilization patterns
   - Identify memory pressure points during application execution
   - Examine memory usage distribution across executors

2. **Memory Configuration Assessment**
   - Review current memory configuration settings
   - Compare configuration against workload requirements
   - Identify misconfigurations leading to memory issues

3. **Garbage Collection Analysis**
   - Evaluate GC frequency, duration, and impact
   - Identify GC pressure indicators and performance impact
   - Assess GC algorithm effectiveness for workload

4. **Spill Operations Investigation**
   - Analyze memory-to-disk spill patterns
   - Evaluate impact of spill operations on performance
   - Identify opportunities to reduce spill frequency

**MCP Tool Memory Analysis Sequence:**

```
# Basic Application and Executor Info
get_application("{app_id}"{server_param})
list_executors("{app_id}"{server_param}, include_inactive=true)
get_executor_summary("{app_id}"{server_param})

# Performance and Bottleneck Context
get_job_bottlenecks("{app_id}"{server_param})
list_slowest_stages("{app_id}"{server_param})

# Stage-Level Memory Analysis
# (Focus on stages with high memory usage or spill)
get_stage_task_summary("{app_id}", stage_id, attempt_id=0{server_param})

# Configuration Review
get_environment("{app_id}"{server_param})

# Comprehensive Insights
get_application_insights("{app_id}"{server_param})
```

**Memory Analysis Dimensions:**

**Heap Memory Issues:**
- OutOfMemoryError patterns and causes
- Heap utilization efficiency
- Large object allocation patterns
- Memory fragmentation indicators

**Off-Heap Memory:**
- Off-heap storage utilization
- Serialization efficiency
- Cache eviction patterns
- Memory leak indicators

**Garbage Collection:**
- GC time percentage of total runtime
- GC algorithm effectiveness
- Memory pressure indicators
- GC tuning recommendations

**Memory Spill Operations:**
- Frequency and volume of memory spills
- Disk I/O impact from spill operations
- Partition size optimization opportunities
- Memory vs. computation trade-offs

**Expected Analysis Results:**
- Memory usage efficiency assessment
- GC pressure analysis with performance impact
- Memory spill pattern identification
- Configuration optimization recommendations
- Memory allocation strategy improvements

**Actionable Memory Recommendations:**
- Specific memory configuration adjustments (executor memory, fraction allocations)
- GC algorithm and tuning suggestions
- Data structure and serialization optimizations
- Caching strategy improvements
- Partition size recommendations to reduce memory pressure"""


@mcp.prompt()
def diagnose_shuffle_problems(
    app_id: str, shuffle_aspect: str = "comprehensive", server: Optional[str] = None
) -> str:
    """Generate a focused prompt for diagnosing shuffle operation issues and data movement problems.

    Args:
        app_id: The Spark application ID
        shuffle_aspect: Shuffle aspect to focus on ('skew', 'performance', 'size', 'comprehensive')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    focus_guidance = {
        "skew": "Focus specifically on data skew detection and mitigation strategies",
        "performance": "Emphasize shuffle performance impact and optimization opportunities",
        "size": "Concentrate on shuffle data volumes and size optimization",
        "comprehensive": "Provide complete analysis of all shuffle-related aspects",
    }.get(shuffle_aspect, "Provide complete analysis of all shuffle-related aspects")

    return f"""Diagnose shuffle operation problems in Spark application {app_id}.

**Shuffle Analysis Focus:** {focus_guidance}

**Shuffle Problem Diagnosis Framework:**

1. **Shuffle Operation Overview**
   - Identify stages with significant shuffle operations
   - Analyze shuffle data volumes and patterns
   - Map shuffle operations to application logic

2. **Data Skew Investigation**
   - Detect uneven data distribution across partitions
   - Identify hot keys and partition skew patterns
   - Assess impact of skew on overall performance

3. **Shuffle Performance Analysis**
   - Evaluate shuffle read/write times and throughput
   - Analyze network utilization during shuffle phases
   - Identify shuffle-related bottlenecks and delays

4. **Optimization Strategy Development**
   - Recommend partitioning strategies to reduce skew
   - Suggest configuration optimizations for shuffle operations
   - Identify code-level optimizations to minimize shuffle

**MCP Tool Shuffle Analysis Sequence:**

```
# Application and Stage Overview
get_application("{app_id}"{server_param})
list_stages("{app_id}"{server_param})

# Focused Shuffle Skew Analysis
analyze_shuffle_skew("{app_id}"{server_param})

# Stage Performance Deep Dive
list_slowest_stages("{app_id}"{server_param})
# For stages with significant shuffle, get detailed task summaries
get_stage_task_summary("{app_id}", stage_id{server_param})

# Executor Performance Context
get_executor_summary("{app_id}"{server_param})

# Configuration Review
get_environment("{app_id}"{server_param})

# Comprehensive Analysis
get_application_insights("{app_id}"{server_param})
```

**Shuffle Problem Categories:**

**Data Skew Issues:**
- Extreme partition size variations
- Hot key identification and impact
- Task execution time skew patterns
- Memory pressure from large partitions

**Performance Problems:**
- High shuffle read/write latencies
- Network bandwidth utilization issues
- Disk I/O bottlenecks during shuffle
- CPU utilization during serialization/deserialization

**Volume and Size Issues:**
- Excessive shuffle data volumes
- Inefficient data serialization
- Unnecessary data movement patterns
- Cartesian products and broadcast optimization opportunities

**Configuration Problems:**
- Suboptimal shuffle partitioning configuration
- Inadequate buffer sizes and timeouts
- Inefficient compression settings
- Poor locality and placement strategies

**Expected Diagnostic Results:**
- Shuffle operation efficiency assessment
- Data skew severity and impact analysis
- Performance bottleneck identification in shuffle phases
- Network and I/O utilization analysis during shuffles
- Configuration optimization recommendations

**Actionable Shuffle Optimizations:**
- Partitioning strategy recommendations (salting, bucketing, etc.)
- Configuration parameter tuning for shuffle performance
- Code-level optimizations to reduce shuffle necessity
- Data preprocessing suggestions to improve distribution
- Alternative algorithm recommendations to avoid problematic shuffles"""


@mcp.prompt()
def identify_configuration_issues(
    app_id: str, config_category: str = "all", server: Optional[str] = None
) -> str:
    """Generate a systematic prompt for identifying Spark configuration problems and optimization opportunities.

    Args:
        app_id: The Spark application ID
        config_category: Configuration category to focus on ('resources', 'performance', 'reliability', 'all')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    focus_guidance = {
        "resources": "Focus on resource allocation configuration issues (memory, cores, executors)",
        "performance": "Emphasize performance-related configuration optimizations",
        "reliability": "Concentrate on fault tolerance and reliability configuration settings",
        "all": "Provide comprehensive configuration analysis across all categories",
    }.get(
        config_category,
        "Provide comprehensive configuration analysis across all categories",
    )

    return f"""Identify configuration issues and optimization opportunities for Spark application {app_id}.

**Configuration Analysis Focus:** {focus_guidance}

**Configuration Assessment Framework:**

1. **Current Configuration Inventory**
   - Document current Spark configuration settings
   - Identify non-default configurations and their purposes
   - Map configuration to observed application behavior

2. **Performance Impact Analysis**
   - Correlate configuration settings with performance metrics
   - Identify configuration bottlenecks and suboptimal settings
   - Assess resource utilization efficiency

3. **Best Practices Comparison**
   - Compare current configuration against established best practices
   - Identify missing optimizations for the specific workload type
   - Highlight potentially harmful or deprecated settings

4. **Optimization Recommendation Development**
   - Prioritize configuration changes by potential impact
   - Provide specific parameter values and justifications
   - Include implementation and testing recommendations

**MCP Tool Configuration Analysis Sequence:**

```
# Configuration and Environment Analysis
get_environment("{app_id}"{server_param})
get_application("{app_id}"{server_param})

# Performance Context for Configuration Assessment
get_application_insights("{app_id}"{server_param})
get_job_bottlenecks("{app_id}"{server_param})

# Resource and Scaling Analysis
analyze_auto_scaling("{app_id}"{server_param})

# Specific Issue Analysis
analyze_shuffle_skew("{app_id}"{server_param})
analyze_failed_tasks("{app_id}"{server_param})

# Executor and Resource Context
get_executor_summary("{app_id}"{server_param})
```

**Configuration Assessment Categories:**

**Resource Allocation:**
- Executor memory and core allocation effectiveness
- Dynamic allocation configuration appropriateness
- Driver resources and scaling considerations
- Storage and serialization configuration optimization

**Performance Optimization:**
- Shuffle and I/O configuration efficiency
- SQL and Catalyst optimizer settings
- Caching and persistence strategy configuration
- Parallelism and task sizing optimizations

**Reliability and Fault Tolerance:**
- Retry and timeout configuration adequacy
- Checkpointing and recovery settings
- Speculative execution configuration
- Failure handling and blacklisting parameters

**Advanced Optimizations:**
- JVM and garbage collection tuning
- Network and locality optimization settings
- Security and authentication configuration efficiency
- Monitoring and metrics collection setup

**Expected Configuration Analysis:**
- Configuration health score and assessment
- Top configuration issues impacting performance
- Resource allocation efficiency evaluation
- Comparison with workload-specific best practices
- Priority-ordered optimization recommendations

**Actionable Configuration Recommendations:**

**High Impact Changes:**
- Specific parameter values with performance justification
- Resource allocation optimizations based on observed usage
- Performance tuning parameters for identified bottlenecks

**Implementation Guidance:**
- Step-by-step configuration change process
- Testing and validation approaches
- Rollback strategies for configuration experiments
- Monitoring recommendations to track improvement

**Risk Assessment:**
- Potential negative impacts of recommended changes
- Configuration dependencies and interaction effects
- Environment-specific considerations and constraints"""
