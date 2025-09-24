"""
Reporting and Summary Prompts for Spark Applications

These prompts guide AI agents in generating comprehensive reports, executive
summaries, and trend analysis for Spark applications and infrastructure.
"""

from typing import Optional
from spark_history_mcp.core.app import mcp


@mcp.prompt()
def generate_performance_report(
    app_id: str,
    report_type: str = "comprehensive",
    audience: str = "technical",
    server: Optional[str] = None
) -> str:
    """Generate a comprehensive performance report for a Spark application.

    Args:
        app_id: The Spark application ID to report on
        report_type: Type of report ('executive', 'technical', 'comprehensive')
        audience: Target audience ('executive', 'technical', 'mixed')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    report_guidance = {
        "executive": "Focus on high-level metrics, business impact, and strategic recommendations",
        "technical": "Provide detailed technical analysis with specific metrics and configurations",
        "comprehensive": "Include both executive summary and detailed technical analysis"
    }.get(report_type, "Include both executive summary and detailed technical analysis")

    audience_guidance = {
        "executive": "Use business-friendly language, focus on cost and efficiency impacts",
        "technical": "Include technical details, configuration specifics, and implementation guidance",
        "mixed": "Balance technical depth with business context and clear explanations"
    }.get(audience, "Balance technical depth with business context and clear explanations")

    return f"""Generate a comprehensive performance report for Spark application {app_id}.

**Report Specifications:**
- Report Type: {report_guidance}
- Target Audience: {audience_guidance}

**Performance Report Structure:**

1. **Executive Summary**
   - Application overview and business context
   - Key performance indicators and health status
   - Critical issues and immediate action items
   - Cost and efficiency impact assessment

2. **Performance Analysis**
   - Runtime performance metrics and trends
   - Resource utilization efficiency analysis
   - Bottleneck identification and impact assessment
   - Comparative analysis against benchmarks or previous runs

3. **Technical Deep Dive**
   - Stage and job performance breakdown
   - Resource allocation and utilization patterns
   - Configuration analysis and optimization opportunities
   - Data processing efficiency and optimization recommendations

4. **Recommendations and Action Plan**
   - Prioritized improvement recommendations
   - Implementation roadmap and effort estimates
   - Risk assessment and mitigation strategies
   - Monitoring and success metrics

**MCP Tool Report Generation Sequence:**

```
# Comprehensive Application Analysis
get_application_insights("{app_id}"{server_param})

# Basic Application Information
get_application("{app_id}"{server_param})

# Performance Bottleneck Analysis
get_job_bottlenecks("{app_id}"{server_param})
list_slowest_stages("{app_id}"{server_param})
list_slowest_jobs("{app_id}"{server_param})

# Resource and Efficiency Analysis
get_executor_summary("{app_id}"{server_param})
analyze_executor_utilization("{app_id}"{server_param})
get_resource_usage_timeline("{app_id}"{server_param})

# Specific Issue Analysis
analyze_shuffle_skew("{app_id}"{server_param})
analyze_failed_tasks("{app_id}"{server_param})
analyze_auto_scaling("{app_id}"{server_param})

# Configuration and Environment Context
get_environment("{app_id}"{server_param})

# SQL Performance (if applicable)
list_slowest_sql_queries("{app_id}"{server_param}, top_n=3)
```

**Report Content Guidelines:**

**Executive Summary Section:**
- Application health score (Green/Yellow/Red)
- Key performance metrics: runtime, resource efficiency, cost per hour
- Top 3 critical issues requiring immediate attention
- Business impact assessment of performance issues
- ROI potential of recommended optimizations

**Performance Analysis Section:**
- Runtime comparison against SLA or historical baselines
- Resource utilization heatmaps and efficiency scores
- Bottleneck analysis with stage/job breakdown
- Failure rate and reliability assessment
- Data processing throughput and efficiency metrics

**Technical Deep Dive Section:**
- Detailed stage performance analysis with task distributions
- Memory usage patterns, GC pressure, and spill analysis
- Shuffle operation efficiency and data skew assessment
- Executor allocation patterns and utilization effectiveness
- Configuration review against best practices

**Recommendations Section:**
- Immediate fixes for critical performance issues
- Medium-term optimization strategies
- Long-term architectural improvements
- Implementation priority and effort estimation
- Expected performance improvement quantification

**Report Formatting Requirements:**

**For Executive Audience:**
- Use charts, graphs, and visual indicators
- Quantify business impact (cost savings, time reduction)
- Provide clear success metrics and KPIs
- Include implementation timeline and resource requirements

**For Technical Audience:**
- Include specific configuration parameters and values
- Provide code examples and implementation details
- Reference specific stages, jobs, and technical metrics
- Include monitoring and validation approaches

**For Mixed Audience:**
- Start with executive summary, follow with technical details
- Use progressive disclosure of technical complexity
- Include both business and technical success metrics
- Provide glossary for technical terms

**Report Quality Standards:**
- Include confidence levels for recommendations
- Provide data sources and analysis methodology
- Include assumptions and limitations of analysis
- Offer alternative approaches for complex optimizations
- Include contact information for follow-up technical discussions"""


@mcp.prompt()
def create_executive_summary(
    app_id: str,
    focus_metric: str = "cost_efficiency",
    time_context: str = "current",
    server: Optional[str] = None
) -> str:
    """Generate a high-level executive summary for Spark application performance.

    Args:
        app_id: The Spark application ID to summarize
        focus_metric: Primary metric to emphasize ('performance', 'cost_efficiency', 'reliability')
        time_context: Time context for analysis ('current', 'trend', 'comparative')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    metric_focus = {
        "performance": "Emphasize execution time, throughput, and performance optimization opportunities",
        "cost_efficiency": "Focus on cost per unit processed, resource utilization, and cost optimization",
        "reliability": "Highlight failure rates, stability, and reliability improvement opportunities"
    }.get(focus_metric, "Balance performance, cost, and reliability metrics")

    time_guidance = {
        "current": "Focus on current state analysis and immediate actionable insights",
        "trend": "Analyze performance trends over time and trajectory assessment",
        "comparative": "Compare against benchmarks, SLAs, or previous versions"
    }.get(time_context, "Focus on current state analysis and immediate actionable insights")

    return f"""Generate an executive summary for Spark application {app_id} performance.

**Executive Summary Specifications:**
- Primary Focus: {metric_focus}
- Time Context: {time_guidance}
- Audience: C-level executives, engineering managers, product owners

**Executive Summary Framework:**

1. **Business Impact Assessment**
   - Application business context and criticality
   - Current performance against business objectives
   - Financial impact of performance issues (if any)
   - User experience and SLA compliance status

2. **Key Performance Indicators**
   - Overall application health score
   - Critical metrics dashboard (runtime, cost, reliability)
   - Performance against established benchmarks
   - Trend indicators and trajectory assessment

3. **Strategic Recommendations**
   - Top 3 high-impact optimization opportunities
   - Investment requirements and expected ROI
   - Timeline for implementation and results
   - Risk assessment for recommended changes

4. **Resource and Budget Implications**
   - Current resource costs and utilization efficiency
   - Optimization potential and cost savings opportunities
   - Infrastructure scaling requirements
   - Team effort and timeline for improvements

**MCP Tool Executive Analysis Sequence:**

```
# High-Level Comprehensive Analysis
get_application_insights("{app_id}"{server_param})

# Application Performance Overview
get_application("{app_id}"{server_param})
get_job_bottlenecks("{app_id}"{server_param})

# Resource and Cost Analysis
get_executor_summary("{app_id}"{server_param})
analyze_executor_utilization("{app_id}"{server_param})

# Reliability and Failure Analysis
analyze_failed_tasks("{app_id}"{server_param})

# Optimization Opportunities
analyze_auto_scaling("{app_id}"{server_param})
analyze_shuffle_skew("{app_id}"{server_param})
```

**Executive Summary Content Structure:**

**Opening Statement (30 seconds read):**
- Application health status: ✅ Healthy / ⚠️ Needs Attention / 🚨 Critical Issues
- One-sentence performance assessment
- Primary recommendation with expected impact

**Performance Dashboard:**
```
Runtime Performance:    [XX.X hrs] vs [target] ([+/-XX%])
Resource Efficiency:    [XX%] utilization ([cost impact])
Reliability Score:      [XX%] success rate ([failure impact])
Optimization Potential: [XX%] improvement possible ([$ savings])
```

**Key Findings (2 minute read):**
- Most significant performance opportunity with quantified impact
- Critical issue requiring immediate attention (if any)
- Resource optimization opportunity with cost implications
- Reliability or stability concern (if applicable)

**Strategic Recommendations:**

**Immediate Actions (0-30 days):**
- Highest impact, lowest effort optimizations
- Expected improvement: [X% performance / $X savings]
- Resources required: [X hours engineering time]

**Medium-term Strategy (1-3 months):**
- Significant optimization projects with measurable ROI
- Infrastructure improvements or architectural changes
- Expected improvement: [X% performance / $X savings]

**Long-term Vision (3+ months):**
- Transformational improvements and strategic investments
- Platform evolution and capability enhancement
- Expected improvement: [X% performance / $X savings]

**Business Impact Summary:**

**Performance Impact:**
- Current vs. target performance metrics
- User experience implications
- Competitive advantage or disadvantage assessment

**Financial Impact:**
- Current compute costs and optimization potential
- Cost per transaction/query/job processed
- ROI timeline for recommended investments

**Risk Assessment:**
- Risks of maintaining status quo
- Implementation risks for recommended changes
- Mitigation strategies for critical issues

**Success Metrics and Monitoring:**
- KPIs to track improvement success
- Monitoring dashboard recommendations
- Review cadence and governance approach

**Executive Decision Points:**
- Budget approval requirements for recommendations
- Resource allocation decisions needed
- Timeline approval for implementation phases
- Risk tolerance assessment for optimization strategies"""


@mcp.prompt()
def summarize_trends(
    app_ids: list[str],
    trend_period: str = "weekly",
    trend_focus: str = "performance",
    server: Optional[str] = None
) -> str:
    """Generate trend analysis and pattern recognition across multiple Spark applications.

    Args:
        app_ids: List of Spark application IDs to analyze for trends
        trend_period: Time period for trend analysis ('daily', 'weekly', 'monthly')
        trend_focus: Primary trend focus ('performance', 'resources', 'reliability', 'all')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""
    app_list = ', '.join([f'"{app_id}"' for app_id in app_ids])

    trend_guidance = {
        "performance": "Focus on execution time trends, throughput patterns, and performance degradation/improvement",
        "resources": "Analyze resource utilization trends, cost patterns, and allocation efficiency evolution",
        "reliability": "Track failure rate trends, stability patterns, and reliability improvement/degradation",
        "all": "Comprehensive trend analysis across performance, resources, and reliability dimensions"
    }.get(trend_focus, "Comprehensive trend analysis across performance, resources, and reliability dimensions")

    return f"""Analyze trends and patterns across multiple Spark applications over {trend_period} periods.

**Application Set:** {app_list}
**Trend Analysis Focus:** {trend_guidance}

**Trend Analysis Framework:**

1. **Pattern Recognition**
   - Identify recurring performance patterns across applications
   - Detect seasonal or cyclical variations in resource usage
   - Recognize degradation or improvement trends over time

2. **Comparative Analysis**
   - Compare applications within the same time period
   - Identify best and worst performing applications
   - Analyze variation in performance characteristics

3. **Root Cause Correlation**
   - Correlate trends with configuration changes
   - Link performance patterns to workload characteristics
   - Identify infrastructure or environmental factors

4. **Predictive Insights**
   - Project future performance based on current trends
   - Identify emerging issues before they become critical
   - Recommend proactive optimization strategies

**MCP Tool Trend Analysis Sequence:**

```
# Individual Application Analysis
{chr(10).join([f'get_application_insights("{app_id}"{server_param})' for app_id in app_ids])}

# Performance Comparison Between Applications
{chr(10).join([f'compare_job_performance("{app_ids[i]}", "{app_ids[j]}"{server_param})' for i in range(len(app_ids)) for j in range(i+1, min(i+3, len(app_ids)))])}

# Configuration Comparison for Trend Correlation
{chr(10).join([f'compare_job_environments("{app_ids[i]}", "{app_ids[j]}"{server_param})' for i in range(len(app_ids)) for j in range(i+1, min(i+3, len(app_ids)))])}

# Detailed Analysis for Outliers
{chr(10).join([f'get_job_bottlenecks("{app_id}"{server_param})' for app_id in app_ids[:3]])}
```

**Trend Analysis Dimensions:**

**Performance Trends:**
- Execution time evolution and velocity changes
- Throughput patterns and processing efficiency trends
- Stage and job performance degradation/improvement
- Query performance evolution (for SQL workloads)

**Resource Utilization Trends:**
- Executor allocation and utilization pattern changes
- Memory usage efficiency trends
- CPU and I/O utilization evolution
- Cost per unit processed trends

**Reliability and Stability Trends:**
- Failure rate evolution and pattern changes
- Error type distribution and trend analysis
- Recovery time and resilience improvement/degradation
- Infrastructure stability trends

**Configuration and Optimization Trends:**
- Configuration change impact over time
- Optimization implementation effectiveness
- Auto-scaling behavior evolution
- Best practice adoption trends

**Expected Trend Analysis Results:**

**Performance Trend Summary:**
```
Time Period: {trend_period.capitalize()} analysis
Application Count: {len(app_ids)} applications
Trend Direction: [Improving/Stable/Degrading]
Key Pattern: [Most significant pattern identified]
```

**Quantified Trend Metrics:**
- Average performance change: [+/-XX%] over period
- Resource efficiency trend: [+/-XX%] utilization change
- Reliability trend: [+/-XX%] in success rate
- Cost efficiency trend: [+/-XX%] cost per unit change

**Pattern Classification:**
- Seasonal patterns: [Description of recurring patterns]
- Growth patterns: [Scaling and growth trend analysis]
- Degradation patterns: [Performance decline identification]
- Optimization patterns: [Improvement trend analysis]

**Trend-Based Recommendations:**

**Immediate Actions:**
- Address degrading trend patterns
- Replicate successful optimization patterns
- Investigate anomalous performance variations

**Strategic Planning:**
- Resource capacity planning based on growth trends
- Optimization investment prioritization
- Infrastructure scaling recommendations

**Monitoring and Alerting:**
- Trend-based alerting thresholds
- Early warning indicators for degradation
- Success metric tracking for improvements

**Comparative Analysis Insights:**

**Best Performers:**
- Applications with consistently improving trends
- Configuration patterns associated with good performance
- Optimization strategies showing sustained results

**Underperformers:**
- Applications with degrading performance trends
- Common factors contributing to performance decline
- Intervention strategies for performance recovery

**Trend Correlation Analysis:**
- Configuration changes correlated with performance trends
- Workload changes impacting resource utilization
- Infrastructure changes affecting application reliability

**Predictive Insights:**
- Projected performance trajectory for next {trend_period}
- Resource requirement forecasting
- Potential issues emerging from current trends
- Optimization opportunity timeline and impact projections"""


@mcp.prompt()
def benchmark_comparison(
    app_id: str,
    benchmark_type: str = "internal",
    comparison_dimension: str = "comprehensive",
    server: Optional[str] = None
) -> str:
    """Generate benchmark comparison analysis for Spark applications.

    Args:
        app_id: The Spark application ID to benchmark
        benchmark_type: Type of benchmark ('internal', 'industry', 'historical', 'target')
        comparison_dimension: Dimension to focus on ('performance', 'cost', 'efficiency', 'comprehensive')
        server: Optional server name to use
    """
    server_param = f', server="{server}"' if server else ""

    benchmark_guidance = {
        "internal": "Compare against other applications in your environment and historical performance",
        "industry": "Compare against industry standards and best practices for similar workloads",
        "historical": "Compare against historical performance of the same application over time",
        "target": "Compare against established performance targets and SLA requirements"
    }.get(benchmark_type, "Compare against multiple benchmark types for comprehensive assessment")

    dimension_focus = {
        "performance": "Focus on execution time, throughput, and processing efficiency benchmarks",
        "cost": "Emphasize cost per unit processed and resource cost efficiency comparisons",
        "efficiency": "Concentrate on resource utilization efficiency and waste reduction metrics",
        "comprehensive": "Provide benchmarking across all performance, cost, and efficiency dimensions"
    }.get(comparison_dimension, "Provide benchmarking across all performance, cost, and efficiency dimensions")

    return f"""Generate comprehensive benchmark comparison analysis for Spark application {app_id}.

**Benchmark Specifications:**
- Benchmark Type: {benchmark_guidance}
- Comparison Focus: {dimension_focus}

**Benchmark Comparison Framework:**

1. **Baseline Establishment**
   - Define current application performance baseline
   - Establish benchmark comparison criteria and metrics
   - Identify appropriate benchmark targets for comparison

2. **Multi-Dimensional Comparison**
   - Performance benchmarking against established standards
   - Cost efficiency comparison with similar workloads
   - Resource utilization efficiency benchmarking

3. **Gap Analysis**
   - Quantify performance gaps against benchmarks
   - Identify areas of competitive advantage or disadvantage
   - Assess optimization potential to reach benchmark targets

4. **Improvement Roadmap**
   - Prioritize improvements based on benchmark gaps
   - Develop action plan to achieve benchmark performance
   - Establish monitoring framework for benchmark tracking

**MCP Tool Benchmark Analysis Sequence:**

```
# Comprehensive Application Analysis
get_application_insights("{app_id}"{server_param})

# Performance Baseline Establishment
get_application("{app_id}"{server_param})
get_job_bottlenecks("{app_id}"{server_param})

# Resource Efficiency Analysis
get_executor_summary("{app_id}"{server_param})
analyze_executor_utilization("{app_id}"{server_param})

# Optimization Potential Assessment
analyze_auto_scaling("{app_id}"{server_param})
analyze_shuffle_skew("{app_id}"{server_param})

# Configuration and Best Practice Review
get_environment("{app_id}"{server_param})
```

**Benchmark Comparison Categories:**

**Performance Benchmarks:**
- Execution time per unit of data processed
- Throughput (records/second, GB/hour)
- Query response time and latency percentiles
- Stage completion time efficiency

**Resource Efficiency Benchmarks:**
- CPU utilization efficiency percentage
- Memory utilization and waste metrics
- I/O efficiency and throughput utilization
- Network utilization during shuffle operations

**Cost Efficiency Benchmarks:**
- Cost per GB processed or per query executed
- Resource cost per hour vs. throughput delivered
- Infrastructure efficiency (cost per core-hour utilized)
- Total cost of ownership per workload

**Reliability and Quality Benchmarks:**
- Success rate and failure tolerance
- Recovery time and resilience metrics
- Data quality and processing accuracy
- SLA compliance and availability metrics

**Expected Benchmark Analysis Results:**

**Performance Scorecard:**
```
Overall Performance Grade: [A/B/C/D/F]
Execution Time: [Current] vs [Benchmark] ([Gap %])
Throughput: [Current] vs [Benchmark] ([Gap %])
Resource Efficiency: [Current] vs [Benchmark] ([Gap %])
Cost Efficiency: [Current] vs [Benchmark] ([Gap %])
```

**Benchmark Positioning:**
- Performance Percentile: [Application ranks in Xth percentile]
- Cost Efficiency Ranking: [Above/Below average by X%]
- Best-in-Class Gap: [X% improvement needed to reach top 10%]
- Industry Standard Comparison: [Meets/Exceeds/Below standards]

**Detailed Gap Analysis:**

**Performance Gaps:**
- Critical performance bottlenecks vs. benchmark expectations
- Optimization opportunities with quantified improvement potential
- Configuration misalignments with benchmark standards

**Cost Efficiency Gaps:**
- Resource overallocation or underutilization patterns
- Cost optimization opportunities vs. benchmark efficiency
- Infrastructure rightsizing recommendations

**Best Practice Alignment:**
- Configuration alignment with industry best practices
- Architecture pattern comparison with benchmark standards
- Operational maturity assessment against benchmarks

**Improvement Action Plan:**

**Quick Wins (0-30 days):**
- Configuration optimizations to close immediate gaps
- Resource allocation adjustments for efficiency gains
- Expected improvement: [X% closer to benchmark]

**Strategic Improvements (1-6 months):**
- Application architecture optimizations
- Workflow and pipeline improvements
- Expected improvement: [X% closer to benchmark]

**Transformational Changes (6+ months):**
- Platform modernization for benchmark performance
- Advanced optimization and automation implementation
- Expected improvement: [X% closer to benchmark]

**Benchmark Monitoring Strategy:**
- KPIs to track progress toward benchmark targets
- Regular benchmark reassessment schedule
- Competitive analysis and benchmark update process
- Success criteria and milestone definition"""