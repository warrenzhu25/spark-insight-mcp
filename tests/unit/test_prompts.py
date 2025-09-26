import unittest

from spark_history_mcp.prompts.optimization import (
    improve_query_performance,
    optimize_resource_allocation,
    reduce_data_skew,
    suggest_autoscaling_config,
)
from spark_history_mcp.prompts.performance import (
    analyze_slow_application,
    compare_job_performance,
    diagnose_resource_issues,
    investigate_stage_bottlenecks,
)
from spark_history_mcp.prompts.reporting import (
    benchmark_comparison,
    create_executive_summary,
    generate_performance_report,
    summarize_trends,
)
from spark_history_mcp.prompts.troubleshooting import (
    diagnose_shuffle_problems,
    examine_memory_issues,
    identify_configuration_issues,
    investigate_failures,
)


class TestPerformancePrompts(unittest.TestCase):
    """Test performance analysis prompts"""

    def test_analyze_slow_application_basic(self):
        """Test basic slow application analysis prompt"""
        result = analyze_slow_application("app-123")

        self.assertIsInstance(result, str)
        self.assertIn("app-123", result)
        self.assertIn("performance analysis", result.lower())
        self.assertIn("get_application", result)
        self.assertIn("get_application_insights", result)
        self.assertIn("60 minutes", result)  # default baseline

    def test_analyze_slow_application_with_params(self):
        """Test slow application analysis prompt with custom parameters"""
        result = analyze_slow_application(
            "app-456", baseline_duration_minutes=120, server="production"
        )

        self.assertIn("app-456", result)
        self.assertIn("120 minutes", result)
        self.assertIn('server="production"', result)

    def test_investigate_stage_bottlenecks_basic(self):
        """Test basic stage bottleneck investigation prompt"""
        result = investigate_stage_bottlenecks("app-789")

        self.assertIsInstance(result, str)
        self.assertIn("app-789", result)
        self.assertIn("stage-level performance", result.lower())
        self.assertIn("list_slowest_stages", result)

    def test_investigate_stage_bottlenecks_with_stage_id(self):
        """Test stage bottleneck investigation with specific stage"""
        result = investigate_stage_bottlenecks("app-789", stage_id=5)

        self.assertIn("focusing on stage 5", result)
        self.assertIn("get_stage(", result)
        self.assertIn("stage_id=5", result)

    def test_diagnose_resource_issues_focus_areas(self):
        """Test resource diagnosis with different focus areas"""
        focus_areas = ["memory", "cpu", "disk", "network", "all"]

        for focus in focus_areas:
            result = diagnose_resource_issues("app-123", focus_area=focus)
            self.assertIn("app-123", result)
            self.assertIn("resource utilization", result.lower())

    def test_compare_job_performance_basic(self):
        """Test job performance comparison prompt"""
        result = compare_job_performance("app-123", "app-456")

        self.assertIn("app-123", result)
        self.assertIn("app-456", result)
        self.assertIn("baseline", result.lower())
        self.assertIn("compare_job_performance", result)


class TestTroubleshootingPrompts(unittest.TestCase):
    """Test troubleshooting and debugging prompts"""

    def test_investigate_failures_basic(self):
        """Test basic failure investigation prompt"""
        result = investigate_failures("app-123")

        self.assertIsInstance(result, str)
        self.assertIn("app-123", result)
        self.assertIn("failure", result.lower())
        self.assertIn("analyze_failed_tasks", result)

    def test_investigate_failures_with_type(self):
        """Test failure investigation with specific failure type"""
        failure_types = ["task", "job", "stage", "executor", "all"]

        for failure_type in failure_types:
            result = investigate_failures("app-123", failure_type=failure_type)
            self.assertIn("app-123", result)

    def test_examine_memory_issues_focus_areas(self):
        """Test memory issue examination with different focus areas"""
        memory_focuses = ["heap", "offheap", "spill", "gc", "comprehensive"]

        for focus in memory_focuses:
            result = examine_memory_issues("app-123", memory_focus=focus)
            self.assertIn("app-123", result)
            self.assertIn("memory", result.lower())

    def test_diagnose_shuffle_problems_aspects(self):
        """Test shuffle problem diagnosis with different aspects"""
        shuffle_aspects = ["skew", "performance", "size", "comprehensive"]

        for aspect in shuffle_aspects:
            result = diagnose_shuffle_problems("app-123", shuffle_aspect=aspect)
            self.assertIn("app-123", result)
            self.assertIn("shuffle", result.lower())
            self.assertIn("analyze_shuffle_skew", result)

    def test_identify_configuration_issues_categories(self):
        """Test configuration issue identification with different categories"""
        config_categories = ["resources", "performance", "reliability", "all"]

        for category in config_categories:
            result = identify_configuration_issues("app-123", config_category=category)
            self.assertIn("app-123", result)
            self.assertIn("configuration", result.lower())
            self.assertIn("get_environment", result)


class TestOptimizationPrompts(unittest.TestCase):
    """Test optimization prompts"""

    def test_suggest_autoscaling_config_basic(self):
        """Test basic auto-scaling configuration suggestions"""
        result = suggest_autoscaling_config("app-123")

        self.assertIsInstance(result, str)
        self.assertIn("app-123", result)
        self.assertIn("auto-scaling", result.lower())
        self.assertIn("analyze_auto_scaling", result)
        self.assertIn("120 minutes", result)  # default target duration

    def test_suggest_autoscaling_config_with_params(self):
        """Test auto-scaling suggestions with custom parameters"""
        result = suggest_autoscaling_config(
            "app-123", target_duration_minutes=90, cost_optimization=False
        )

        self.assertIn("90 minutes", result)
        self.assertNotIn("cost optimization considerations", result)

    def test_optimize_resource_allocation_goals(self):
        """Test resource allocation optimization with different goals"""
        optimization_goals = ["performance", "cost", "balanced"]

        for goal in optimization_goals:
            result = optimize_resource_allocation("app-123", optimization_goal=goal)
            self.assertIn("app-123", result)
            self.assertIn("resource", result.lower())

    def test_improve_query_performance_focus_areas(self):
        """Test query performance improvement with different focus areas"""
        focus_areas = ["sql", "dataframe", "rdd", "comprehensive"]
        priorities = ["execution_time", "resource_efficiency", "throughput"]

        for focus in focus_areas:
            for priority in priorities:
                result = improve_query_performance(
                    "app-123", focus_area=focus, optimization_priority=priority
                )
                self.assertIn("app-123", result)
                self.assertIn("query", result.lower())

    def test_reduce_data_skew_types_and_strategies(self):
        """Test data skew reduction with different types and strategies"""
        skew_types = ["shuffle", "join", "aggregation", "comprehensive"]
        strategies = ["preprocessing", "runtime", "adaptive"]

        for skew_type in skew_types:
            for strategy in strategies:
                result = reduce_data_skew(
                    "app-123", skew_type=skew_type, mitigation_strategy=strategy
                )
                self.assertIn("app-123", result)
                self.assertIn("skew", result.lower())


class TestReportingPrompts(unittest.TestCase):
    """Test reporting and summary prompts"""

    def test_generate_performance_report_types(self):
        """Test performance report generation with different types and audiences"""
        report_types = ["executive", "technical", "comprehensive"]
        audiences = ["executive", "technical", "mixed"]

        for report_type in report_types:
            for audience in audiences:
                result = generate_performance_report(
                    "app-123", report_type=report_type, audience=audience
                )
                self.assertIn("app-123", result)
                self.assertIn("report", result.lower())
                self.assertIn("get_application_insights", result)

    def test_create_executive_summary_focus_metrics(self):
        """Test executive summary creation with different focus metrics"""
        focus_metrics = ["performance", "cost_efficiency", "reliability"]
        time_contexts = ["current", "trend", "comparative"]

        for metric in focus_metrics:
            for context in time_contexts:
                result = create_executive_summary(
                    "app-123", focus_metric=metric, time_context=context
                )
                self.assertIn("app-123", result)
                self.assertIn("executive", result.lower())

    def test_summarize_trends_multiple_apps(self):
        """Test trend summarization with multiple applications"""
        app_ids = ["app-123", "app-456", "app-789"]
        trend_periods = ["daily", "weekly", "monthly"]
        trend_focuses = ["performance", "resources", "reliability", "all"]

        for period in trend_periods:
            for focus in trend_focuses:
                result = summarize_trends(
                    app_ids, trend_period=period, trend_focus=focus
                )
                for app_id in app_ids:
                    self.assertIn(app_id, result)
                self.assertIn("trend", result.lower())

    def test_benchmark_comparison_types(self):
        """Test benchmark comparison with different types and dimensions"""
        benchmark_types = ["internal", "industry", "historical", "target"]
        dimensions = ["performance", "cost", "efficiency", "comprehensive"]

        for bench_type in benchmark_types:
            for dimension in dimensions:
                result = benchmark_comparison(
                    "app-123", benchmark_type=bench_type, comparison_dimension=dimension
                )
                self.assertIn("app-123", result)
                self.assertIn("benchmark", result.lower())


class TestPromptContentQuality(unittest.TestCase):
    """Test prompt content quality and completeness"""

    def test_prompts_contain_mcp_tool_calls(self):
        """Test that prompts contain appropriate MCP tool call examples"""
        prompts_to_test = [
            ("analyze_slow_application", analyze_slow_application("app-123")),
            ("investigate_failures", investigate_failures("app-123")),
            ("suggest_autoscaling_config", suggest_autoscaling_config("app-123")),
            ("generate_performance_report", generate_performance_report("app-123")),
        ]

        for prompt_name, prompt_content in prompts_to_test:
            with self.subTest(prompt=prompt_name):
                # Check for MCP tool call examples
                self.assertTrue(
                    any(
                        tool in prompt_content
                        for tool in [
                            "get_application",
                            "get_application_insights",
                            "analyze_",
                            "list_",
                            "compare_",
                        ]
                    ),
                    f"{prompt_name} should contain MCP tool call examples",
                )

    def test_prompts_have_structured_frameworks(self):
        """Test that prompts provide structured analysis frameworks"""
        prompts_to_test = [
            analyze_slow_application("app-123"),
            investigate_failures("app-123"),
            optimize_resource_allocation("app-123"),
            generate_performance_report("app-123"),
        ]

        for prompt_content in prompts_to_test:
            # Check for structured framework indicators
            framework_indicators = [
                "framework",
                "analysis",
                "investigation",
                "sequence",
                "expected",
                "recommendations",
            ]
            self.assertTrue(
                any(
                    indicator.lower() in prompt_content.lower()
                    for indicator in framework_indicators
                ),
                "Prompt should contain structured framework elements",
            )

    def test_prompts_include_actionable_guidance(self):
        """Test that prompts include actionable guidance and expected outcomes"""
        prompts_to_test = [
            analyze_slow_application("app-123"),
            examine_memory_issues("app-123"),
            reduce_data_skew("app-123"),
            create_executive_summary("app-123"),
        ]

        for prompt_content in prompts_to_test:
            # Check for actionable guidance indicators
            action_indicators = [
                "recommendation",
                "expected",
                "deliverable",
                "action",
                "specific",
                "implementation",
                "suggestion",
            ]
            self.assertTrue(
                any(
                    indicator.lower() in prompt_content.lower()
                    for indicator in action_indicators
                ),
                "Prompt should contain actionable guidance",
            )

    def test_prompt_parameter_integration(self):
        """Test that prompt parameters are properly integrated into content"""
        # Test with different parameters
        result1 = analyze_slow_application("test-app-1", baseline_duration_minutes=30)
        result2 = analyze_slow_application("test-app-2", baseline_duration_minutes=180)

        self.assertIn("test-app-1", result1)
        self.assertIn("test-app-2", result2)
        self.assertIn("30 minutes", result1)
        self.assertIn("180 minutes", result2)

        # Test server parameter integration
        result_with_server = investigate_failures("app-123", server="production")
        self.assertIn('server="production"', result_with_server)


if __name__ == "__main__":
    unittest.main()
