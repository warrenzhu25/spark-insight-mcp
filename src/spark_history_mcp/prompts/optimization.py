"""
Optimization Prompts for Spark Applications

These prompts guide AI agents in developing optimization strategies for Spark
applications, including resource allocation, auto-scaling, and performance tuning.
"""

from typing import Optional

from spark_history_mcp.core.app import mcp


@mcp.prompt()
def suggest_autoscaling_config(
    app_id: str,
    target_duration_minutes: int = 120,
    cost_optimization: bool = True,
    server: Optional[str] = None,
) -> str:
    """Generate optimization recommendations for Spark auto-scaling configuration.

    Args:
        app_id: The Spark application ID to analyze for auto-scaling
        target_duration_minutes: Target application duration for optimization
        cost_optimization: Whether to include cost optimization considerations
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""
    cost_note = " with cost optimization considerations" if cost_optimization else ""

    return f"""Generate auto-scaling configuration recommendations for Spark application {app_id}.

**Optimization Objectives:**
- Target application duration: {target_duration_minutes} minutes
- Optimize for performance{cost_note}
- Ensure resource allocation efficiency

**Auto-scaling Analysis Framework:**

1. **Current Resource Usage Assessment**
   - Analyze existing executor allocation patterns
   - Evaluate resource utilization efficiency over time
   - Identify over-provisioning and under-provisioning periods

2. **Workload Pattern Analysis**
   - Examine application execution phases and resource demands
   - Identify predictable scaling patterns
   - Assess dynamic allocation effectiveness

3. **Performance Impact Modeling**
   - Calculate optimal executor counts for target duration
   - Model impact of different scaling configurations
   - Assess scaling responsiveness requirements

4. **Cost-Performance Trade-off Analysis**
   - Evaluate resource cost vs. performance benefits
   - Identify sweet spots for cost-effective scaling
   - Consider SLA requirements and business priorities

**MCP Tool Auto-scaling Analysis Sequence:**

```
# Core Auto-scaling Analysis
analyze_auto_scaling("{app_id}"{server_param}, target_stage_duration_minutes={target_duration_minutes // 60})

# Executor Utilization Context
analyze_executor_utilization("{app_id}"{server_param})
get_executor_summary("{app_id}"{server_param})

# Resource Timeline Analysis
get_resource_usage_timeline("{app_id}"{server_param})

# Configuration Review
get_environment("{app_id}"{server_param})
get_application("{app_id}"{server_param})

# Performance Context
get_job_bottlenecks("{app_id}"{server_param})
get_application_insights("{app_id}"{server_param})
```

**Auto-scaling Configuration Dimensions:**

**Initial Executor Configuration:**
- Optimal initial executor count based on workload ramp-up
- Memory and core allocation per executor
- Initial resource allocation strategy

**Dynamic Scaling Parameters:**
- Minimum and maximum executor limits
- Scaling up/down thresholds and triggers
- Scaling responsiveness and aggressiveness settings

**Performance Optimization:**
- Target utilization levels for efficient scaling
- Stage-based scaling considerations
- Locality and placement optimizations

**Cost Management:**
- Spot instance integration strategies
- Resource preemption handling
- Cost-aware scaling policies

**Expected Auto-scaling Recommendations:**

**Configuration Parameters:**
```
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.initialExecutors=[calculated value]
spark.dynamicAllocation.minExecutors=[calculated value]
spark.dynamicAllocation.maxExecutors=[calculated value]
spark.dynamicAllocation.targetExecutorsPerStage=[calculated value]
spark.dynamicAllocation.executorAllocationRatio=[calculated value]
```

**Scaling Strategy:**
- Aggressive vs. conservative scaling approach
- Application phase-aware scaling policies
- Resource contention and cluster utilization considerations

**Monitoring and Validation:**
- Key metrics to monitor for scaling effectiveness
- Performance benchmarks for validation
- Alert thresholds for scaling issues

**Implementation Roadmap:**
- Phased rollout strategy for configuration changes
- A/B testing recommendations for scaling parameters
- Rollback plans for optimization experiments

{"**Cost Optimization Considerations:**" if cost_optimization else ""}
{"- Spot instance utilization strategies" if cost_optimization else ""}
{"- Reserved capacity planning recommendations" if cost_optimization else ""}
{"- Cost per performance unit analysis" if cost_optimization else ""}
{"- Budget constraint integration with scaling policies" if cost_optimization else ""}"""


@mcp.prompt()
def optimize_resource_allocation(
    app_id: str,
    optimization_goal: str = "performance",
    resource_constraints: Optional[str] = None,
    server: Optional[str] = None,
) -> str:
    """Generate comprehensive resource allocation optimization recommendations.

    Args:
        app_id: The Spark application ID to optimize
        optimization_goal: Primary goal ('performance', 'cost', 'balanced')
        resource_constraints: Any specific resource constraints to consider
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    goal_guidance = {
        "performance": "Optimize for maximum performance with efficient resource utilization",
        "cost": "Optimize for minimum cost while maintaining acceptable performance",
        "balanced": "Balance performance and cost for optimal total value",
    }.get(optimization_goal, "Balance performance and cost for optimal total value")

    constraints_note = (
        f"\n**Resource Constraints:** {resource_constraints}"
        if resource_constraints
        else ""
    )

    return f"""Optimize resource allocation for Spark application {app_id}.

**Optimization Goal:** {goal_guidance}{constraints_note}

**Resource Optimization Framework:**

1. **Current Resource Assessment**
   - Analyze actual resource utilization patterns
   - Identify resource waste and underutilization
   - Evaluate allocation efficiency across application phases

2. **Workload Characterization**
   - Classify application as CPU-bound, memory-bound, or I/O-bound
   - Identify resource bottlenecks and scaling limitations
   - Assess parallelization effectiveness

3. **Resource Allocation Modeling**
   - Calculate optimal resource ratios for workload type
   - Model impact of different allocation strategies
   - Consider infrastructure and cluster constraints

4. **Performance-Cost Trade-off Analysis**
   - Evaluate cost per performance unit for different configurations
   - Identify diminishing returns in resource scaling
   - Consider SLA and business requirements

**MCP Tool Resource Analysis Sequence:**

```
# Application and Resource Overview
get_application("{app_id}"{server_param})
get_executor_summary("{app_id}"{server_param})

# Resource Utilization Analysis
analyze_executor_utilization("{app_id}"{server_param})
get_resource_usage_timeline("{app_id}"{server_param})

# Performance Context
get_job_bottlenecks("{app_id}"{server_param})
get_application_insights("{app_id}"{server_param})

# Configuration and Environment
get_environment("{app_id}"{server_param})

# Specific Resource Analysis
analyze_auto_scaling("{app_id}"{server_param})
analyze_shuffle_skew("{app_id}"{server_param})
```

**Resource Allocation Optimization Dimensions:**

**Executor Configuration:**
- Optimal executor memory allocation
- CPU core count per executor
- Executor JVM heap and off-heap sizing
- Storage memory and execution memory fractions

**Cluster Resource Distribution:**
- Number of executors vs. cores per executor trade-offs
- Driver resource allocation optimization
- Dynamic vs. static resource allocation strategies

**Memory Management:**
- Heap size optimization for workload patterns
- Off-heap storage utilization
- Cache and storage memory allocation
- Spill and compression configuration

**CPU and Parallelism:**
- Task parallelism optimization
- CPU utilization efficiency
- Thread pool and concurrency tuning

**Expected Resource Optimization Results:**

**Optimized Configuration:**
```
# Executor Resource Allocation
spark.executor.memory=[optimized value]
spark.executor.cores=[optimized value]
spark.executor.memoryFraction=[optimized value]
spark.executor.storageFraction=[optimized value]

# Driver Resource Allocation
spark.driver.memory=[optimized value]
spark.driver.cores=[optimized value]

# Dynamic Allocation
spark.dynamicAllocation.*=[optimized parameters]
```

**Performance Predictions:**
- Expected performance improvement percentage
- Resource utilization efficiency gains
- Cost impact analysis for optimization changes

**Implementation Strategy:**
- Phased optimization approach
- Testing and validation methodology
- Rollback procedures for failed optimizations

**Monitoring and Validation:**
- Key metrics to track optimization success
- Performance regression detection
- Resource utilization monitoring setup

**Workload-Specific Recommendations:**

**CPU-Intensive Workloads:**
- Emphasize core count and CPU efficiency
- Minimize memory overhead
- Optimize task granularity

**Memory-Intensive Workloads:**
- Focus on memory allocation and GC tuning
- Optimize caching and storage strategies
- Balance memory vs. computation trade-offs

**I/O-Intensive Workloads:**
- Optimize disk and network utilization
- Configure compression and serialization
- Balance I/O parallelism with resource allocation

**Risk Assessment:**
- Potential negative impacts of resource changes
- Cluster capacity and contention considerations
- Application dependency and compatibility issues"""


@mcp.prompt()
def improve_query_performance(
    app_id: str,
    focus_area: str = "comprehensive",
    optimization_priority: str = "execution_time",
    server: Optional[str] = None,
) -> str:
    """Generate SQL query and data processing performance optimization recommendations.

    Args:
        app_id: The Spark application ID with SQL workloads
        focus_area: Area to focus on ('sql', 'dataframe', 'rdd', 'comprehensive')
        optimization_priority: Priority ('execution_time', 'resource_efficiency', 'throughput')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    focus_guidance = {
        "sql": "Focus specifically on SQL query optimization and Catalyst optimizer effectiveness",
        "dataframe": "Emphasize DataFrame API optimizations and structured data processing",
        "rdd": "Concentrate on RDD transformations and low-level optimization opportunities",
        "comprehensive": "Provide optimization recommendations across all data processing APIs",
    }.get(
        focus_area,
        "Provide optimization recommendations across all data processing APIs",
    )

    priority_guidance = {
        "execution_time": "Prioritize reducing total query execution time",
        "resource_efficiency": "Optimize for efficient resource utilization and cost reduction",
        "throughput": "Maximize data processing throughput and parallelism",
    }.get(optimization_priority, "Balance execution time, resources, and throughput")

    return f"""Optimize query and data processing performance for Spark application {app_id}.

**Optimization Focus:** {focus_guidance}
**Optimization Priority:** {priority_guidance}

**Query Performance Optimization Framework:**

1. **Query Execution Plan Analysis**
   - Analyze SQL execution plans and operator efficiency
   - Identify expensive operations and optimization opportunities
   - Evaluate Catalyst optimizer effectiveness

2. **Data Processing Pattern Assessment**
   - Examine data access patterns and I/O efficiency
   - Assess join strategies and broadcast optimization opportunities
   - Identify partition pruning and predicate pushdown effectiveness

3. **Resource Utilization During Queries**
   - Analyze resource usage patterns during query execution
   - Identify resource bottlenecks specific to query processing
   - Evaluate parallelization effectiveness for queries

4. **Data Layout and Storage Optimization**
   - Assess data format efficiency and compression effectiveness
   - Evaluate partitioning strategies for query patterns
   - Identify caching opportunities for frequently accessed data

**MCP Tool Query Analysis Sequence:**

```
# SQL Query Analysis
list_slowest_sql_queries("{app_id}"{server_param}, top_n=5)

# Application and Performance Context
get_application("{app_id}"{server_param})
get_job_bottlenecks("{app_id}"{server_param})

# Stage and Task Performance
list_slowest_stages("{app_id}"{server_param})
get_stage_task_summary("{app_id}", stage_id{server_param})

# Data Processing Issues
analyze_shuffle_skew("{app_id}"{server_param})

# Resource and Configuration Context
get_executor_summary("{app_id}"{server_param})
get_environment("{app_id}"{server_param})

# Comprehensive Analysis
get_application_insights("{app_id}"{server_param})
```

**Query Optimization Categories:**

**SQL Query Optimization:**
- Join order and strategy optimization
- Predicate pushdown and filter optimization
- Subquery optimization and common table expressions
- Window function and aggregation optimization

**Data Processing Optimization:**
- Partition elimination and pruning strategies
- Broadcast join threshold tuning
- Bucketing and pre-partitioning for joins
- Cache strategy optimization for reused datasets

**Resource and Parallelism Optimization:**
- Task granularity optimization for queries
- Parallelism tuning for query operations
- Memory allocation for query processing
- Spill reduction for large aggregations and joins

**Data Layout Optimization:**
- Column pruning and projection optimization
- Data format selection (Parquet, Delta, etc.)
- Compression strategy for query performance
- Partitioning schema optimization for query patterns

**Expected Query Optimization Results:**

**Execution Plan Improvements:**
- Identification of inefficient operators and alternatives
- Join strategy recommendations (broadcast vs. sort-merge)
- Predicate pushdown optimization opportunities
- Catalyst optimizer configuration tuning

**Data Processing Enhancements:**
- Partitioning strategy recommendations
- Caching strategy for improved query reuse
- Data preprocessing recommendations
- Index and bucketing strategies

**Configuration Optimizations:**
```
# SQL and Query Optimization
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.serializer.objectStreamReset=[optimized value]

# Join and Broadcast Optimization
spark.sql.autoBroadcastJoinThreshold=[optimized value]
spark.sql.broadcastTimeout=[optimized value]

# Memory and Spill Optimization
spark.sql.shuffle.partitions=[optimized value]
spark.sql.adaptive.advisoryPartitionSizeInBytes=[optimized value]
```

**Performance Predictions:**
- Expected query execution time improvements
- Resource utilization efficiency gains
- Throughput improvement estimates
- Data processing cost reduction potential

**Implementation Recommendations:**

**High-Impact Changes:**
- Most critical query optimization opportunities
- Configuration changes with immediate impact
- Data layout improvements for long-term benefits

**Testing and Validation:**
- Query performance regression testing approach
- Benchmark dataset and query suite recommendations
- A/B testing strategies for optimization validation

**Monitoring and Metrics:**
- Query performance monitoring setup
- Key metrics for tracking optimization success
- Alert thresholds for performance regression detection"""


@mcp.prompt()
def reduce_data_skew(
    app_id: str,
    skew_type: str = "comprehensive",
    mitigation_strategy: str = "adaptive",
    server: Optional[str] = None,
) -> str:
    """Generate comprehensive recommendations for reducing data skew in Spark applications.

    Args:
        app_id: The Spark application ID with data skew issues
        skew_type: Type of skew to focus on ('shuffle', 'join', 'aggregation', 'comprehensive')
        mitigation_strategy: Strategy approach ('preprocessing', 'runtime', 'adaptive')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    skew_focus = {
        "shuffle": "Focus on shuffle operation skew and partition imbalance",
        "join": "Emphasize join skew and key distribution problems",
        "aggregation": "Concentrate on aggregation skew and grouping imbalances",
        "comprehensive": "Address all types of data skew comprehensively",
    }.get(skew_type, "Address all types of data skew comprehensively")

    strategy_guidance = {
        "preprocessing": "Focus on data preprocessing and upstream solutions",
        "runtime": "Emphasize runtime configuration and Spark-level solutions",
        "adaptive": "Combine preprocessing and runtime solutions adaptively",
    }.get(mitigation_strategy, "Combine preprocessing and runtime solutions adaptively")

    return f"""Develop comprehensive data skew reduction strategies for Spark application {app_id}.

**Skew Analysis Focus:** {skew_focus}
**Mitigation Strategy:** {strategy_guidance}

**Data Skew Reduction Framework:**

1. **Skew Detection and Characterization**
   - Identify and quantify data skew patterns
   - Categorize skew types and severity levels
   - Map skew to specific operations and stages

2. **Root Cause Analysis**
   - Analyze data distribution and key patterns
   - Identify business logic contributing to skew
   - Assess upstream data generation patterns

3. **Mitigation Strategy Development**
   - Develop targeted solutions for each skew type
   - Balance preprocessing vs. runtime solutions
   - Consider performance vs. complexity trade-offs

4. **Implementation and Validation Planning**
   - Prioritize mitigation strategies by impact
   - Plan phased implementation approach
   - Design validation and monitoring frameworks

**MCP Tool Skew Analysis Sequence:**

```
# Primary Skew Analysis
analyze_shuffle_skew("{app_id}"{server_param})

# Stage and Task Performance Context
list_slowest_stages("{app_id}"{server_param})
get_stage_task_summary("{app_id}", stage_id{server_param})

# Application Performance Context
get_job_bottlenecks("{app_id}"{server_param})
get_application_insights("{app_id}"{server_param})

# Executor and Resource Analysis
get_executor_summary("{app_id}"{server_param})
analyze_executor_utilization("{app_id}"{server_param})

# Configuration and Environment
get_environment("{app_id}"{server_param})
```

**Data Skew Mitigation Strategies:**

**Preprocessing Solutions:**
- Data sampling and key distribution analysis
- Upstream data partitioning and bucketing
- Data preprocessing to balance key distributions
- Pre-aggregation and dimensionality reduction

**Runtime Configuration Solutions:**
- Adaptive query execution (AQE) optimization
- Dynamic partition coalescing and splitting
- Speculative execution configuration
- Resource allocation adjustments for skewed stages

**Algorithmic Solutions:**
- Salting techniques for hot keys
- Two-phase aggregation strategies
- Alternative join strategies (broadcast, bucketed joins)
- Custom partitioning functions

**Advanced Mitigation Techniques:**
- Key-based bucketing for join optimization
- Range partitioning for ordered data
- Bloom filters for selective processing
- Custom data distribution strategies

**Expected Skew Reduction Results:**

**Skew Analysis Report:**
- Quantified skew metrics and severity assessment
- Identification of most problematic stages and operations
- Root cause analysis for major skew contributors
- Performance impact assessment of current skew

**Mitigation Strategy Roadmap:**

**Phase 1 - Quick Wins:**
- Runtime configuration optimizations
- AQE and dynamic optimization enablement
- Immediate speculative execution tuning

**Phase 2 - Algorithmic Solutions:**
- Salting implementation for identified hot keys
- Join strategy optimization for skewed joins
- Custom partitioning for problematic operations

**Phase 3 - Data Architecture Solutions:**
- Upstream data pipeline modifications
- Data preprocessing and bucketing strategies
- Long-term data layout optimization

**Implementation Specifications:**

**Configuration Changes:**
```
# Adaptive Query Execution
spark.sql.adaptive.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=[optimized]

# Speculative Execution
spark.speculation=true
spark.speculation.multiplier=[optimized]
spark.speculation.quantile=[optimized]

# Partition Management
spark.sql.shuffle.partitions=[optimized for skew]
spark.sql.adaptive.coalescePartitions.enabled=true
```

**Custom Solutions:**
- Salting function implementations
- Custom partitioner code examples
- Data preprocessing pipeline modifications
- Monitoring queries for skew detection

**Performance Validation:**
- Expected performance improvement metrics
- Task execution time variance reduction
- Resource utilization improvement
- Data processing throughput gains

**Monitoring and Alerting:**
- Skew detection metrics and thresholds
- Performance regression monitoring
- Data distribution monitoring setup
- Automated skew mitigation triggers"""
