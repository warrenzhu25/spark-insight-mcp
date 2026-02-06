"""
Tests for OutputFormatter behaviors.

Focus on JSON output and simple fallback when Rich is unavailable.
"""

import json
from types import SimpleNamespace

from spark_history_mcp.cli.formatters import OutputFormatter
from spark_history_mcp.models.spark_types import (
    ApplicationAttemptInfo,
    ApplicationInfo,
    JobData,
    StageData,
)


class TestOutputFormatterTable:
    def test_output_table_with_list_of_dicts(self, capsys):
        fmt = OutputFormatter(format_type="table")
        data = [
            {"col1": "a", "col2": 1},
            {"col1": "b", "col2": 2},
        ]
        fmt.output(data, title="List Table")
        out = capsys.readouterr().out
        assert "col1" in out and "col2" in out

    def test_output_table_with_single_dict(self, capsys):
        fmt = OutputFormatter(format_type="table")
        data = {"key": "value", "num": 42}
        fmt.output(data, title="KV Table")
        out = capsys.readouterr().out
        assert "Property" in out and "Value" in out


class TestOutputFormatterHuman:
    def test_format_comparison_result_and_app_summary_diff(self):
        fmt = OutputFormatter(format_type="human")
        comparison_data = {
            "applications": {
                "app1": {"id": "app-1", "name": "App One"},
                "app2": {"id": "app-2", "name": "App Two"},
            },
            "performance_comparison": {"stages": {"top_stage_differences": []}},
            "aggregated_overview": {
                "application_summary": {
                    "diff": {
                        "application_duration_minutes_change": "+10%",
                        "total_executor_runtime_minutes_change": "-5%",
                    },
                    "app1_summary": {
                        "application_duration_minutes": 12.34,
                        "total_executor_runtime_minutes": 11.0,
                    },
                    "app2_summary": {
                        "application_duration_minutes": 13.57,
                        "total_executor_runtime_minutes": 10.45,
                    },
                },
            },
        }
        # Should not raise
        fmt.output(comparison_data, title="Comparison")

    def test_format_stage_comparison_result(self):
        fmt = OutputFormatter(format_type="human")
        data = {
            "stage_comparison": {
                "stage1": {
                    "stage_id": 3,
                    "name": "Stage A",
                    "status": "COMPLETE",
                    "app_id": "app-1",
                },
                "stage2": {
                    "stage_id": 4,
                    "name": "Stage B",
                    "status": "COMPLETE",
                    "app_id": "app-2",
                },
                # include some ratio metrics to exercise summary table
                "duration_ratio": 1.2,
                "duration_ratio_change": "+20%",
            },
            "summary": {
                "significance_threshold": 0.1,
                "differences": [],
            },
        }
        fmt.output(data, title="Stage Compare")

    def test_format_timeline_and_executor_and_resource_results(self):
        fmt = OutputFormatter(format_type="human")
        timeline = {
            "app1_info": {},
            "app2_info": {},
            "timeline_comparison": {},
            "resource_efficiency": {},
        }
        executors = {
            "applications": {},
            "executor_comparison": {},
            "efficiency_metrics": {},
        }
        resources = {
            "applications": {},
            "resource_comparison": {},
        }
        # Should execute formatting branches without errors
        fmt.output(timeline, title="Timeline")
        fmt.output(executors, title="Executors")
        fmt.output(resources, title="Resources")

    def test_application_and_job_and_stage_objects(self):
        fmt = OutputFormatter(format_type="human")
        # Application with attempts
        attempt = ApplicationAttemptInfo(duration=120000, completed=True)
        app = ApplicationInfo(id="app-1", name="Demo", attempts=[attempt])
        fmt.output(app, title="App")
        # Job with times
        job = JobData(name="JobX", status="SUCCEEDED")
        fmt.output(job, title="Job")
        # Stage
        stage = StageData(
            status="COMPLETE", stage_id=1, num_tasks=10, name="S", details="D"
        )
        fmt.output(stage, title="Stage")

    def test_job_and_aggregated_stage_and_resource_comparison_results(self):
        fmt = OutputFormatter(format_type="human")
        job_comp = {
            "applications": {
                "app1": {
                    "id": "a1",
                    "job_stats": {"duration_ms": 1000},
                    "success_rate": 0.9,
                },
                "app2": {
                    "id": "a2",
                    "job_stats": {"duration_ms": 1500},
                    "success_rate": 0.95,
                },
            },
            "job_comparison": {},
            "timing_analysis": {},
        }
        fmt.output(job_comp, title="JobComp")

        agg_stage = {
            "applications": {"app1": {"id": "a1"}, "app2": {"id": "a2"}},
            "stage_comparison": {
                "task_time_ratio": 1.1,
                "task_time_ratio_change": "+10%",
            },
            "efficiency_analysis": {
                "app1_avg_tasks_per_stage": 10.0,
                "app2_avg_tasks_per_stage": 12.0,
            },
        }
        fmt.output(agg_stage, title="AggStage")

        res_comp = {
            "applications": {
                "app1": {
                    "id": "a1",
                    "cores_granted": 4,
                    "max_cores": 8,
                    "memory_per_executor_mb": 4096,
                },
                "app2": {
                    "id": "a2",
                    "cores_granted": 6,
                    "max_cores": 12,
                    "memory_per_executor_mb": 6144,
                },
            },
            "resource_comparison": {},
        }
        fmt.output(res_comp, title="Res")

    def test_comparison_result_full_with_recommendations(self):
        fmt = OutputFormatter(format_type="human")
        comparison = {
            "applications": {
                "app1": {"id": "a1", "name": "App One"},
                "app2": {"id": "a2", "name": "App Two"},
            },
            "performance_comparison": {
                "executor_comparison": {
                    "applications": {
                        "app1": {
                            "executor_metrics": {
                                "completed_tasks": 10,
                                "total_input_bytes": 1000,
                                "total_duration": 20,
                            }
                        },
                        "app2": {
                            "executor_metrics": {
                                "completed_tasks": 12,
                                "total_input_bytes": 2000,
                                "total_duration": 25,
                            }
                        },
                    },
                    "task_completion_ratio_change": "+5%",
                },
                "stage_comparison": {
                    "stage_comparison": {
                        "duration_ratio": 0.8,
                        "duration_ratio_change": "+10%",
                    }
                },
                "stages": {
                    "top_stage_differences": [
                        {
                            "stage_name": "A very long stage name that will be truncated for display",
                            "time_difference": {
                                "percentage": 15,
                                "slower_application": "app1",
                            },
                            "app1_stage": {"stage_id": 1, "duration_seconds": 5.0},
                            "app2_stage": {"stage_id": 2, "duration_seconds": 4.0},
                        }
                    ]
                },
            },
            "key_recommendations": [
                {"priority": "HIGH", "issue": "Test issue", "suggestion": "Do X"}
            ],
        }
        fmt.output(comparison, title="FullComp")

    def test_executor_comparison_result(self):
        fmt = OutputFormatter(format_type="human")
        exec_comp = {
            "applications": {"app1": {"id": "a1"}, "app2": {"id": "a2"}},
            "executor_comparison": {
                "applications": {
                    "app1": {"executor_metrics": {"total_executors": 5}},
                    "app2": {"executor_metrics": {"total_executors": 5}},
                },
                "task_completion_ratio_change": "+0.0%",
            },
            "efficiency_metrics": {
                "app1_tasks_per_executor": 1.0,
                "app2_tasks_per_executor": 1.0,
            },
            "efficiency_ratios": {"tasks_per_executor_ratio_change": "+0.0%"},
        }
        fmt.output(exec_comp, title="ExecComp")

    def test_stage_comparison_deep_metrics(self):
        fmt = OutputFormatter(format_type="human")
        stage_data = {
            "stage_comparison": {
                "stage1": {
                    "stage_id": 1,
                    "name": "Stage A",
                    "status": "COMPLETE",
                    "app_id": "a1",
                },
                "stage2": {
                    "stage_id": 2,
                    "name": "Stage B",
                    "status": "COMPLETE",
                    "app_id": "a2",
                },
            },
            "significant_differences": {
                "stage_metrics": {
                    "duration": {"stage1": 1000, "stage2": 1500, "change": "+50%"}
                },
                "task_distributions": {
                    "executor_run_time": {
                        "median": {"stage1": 10, "stage2": 5},
                        "p90": {"stage1": 15, "stage2": 8},
                        "p99": {"stage1": 20, "stage2": 9},
                    }
                },
                "executor_distributions": {
                    "memory_used": {"median": {"stage1": 1000000, "stage2": 2000000}}
                },
            },
            "summary": {"notes": "ok"},
        }
        fmt.output(stage_data, title="StageDeep")

    def test_timeline_comparison_full(self):
        fmt = OutputFormatter(format_type="human")
        timeline = {
            "app1_info": {
                "name": "App1",
                "duration_seconds": 120.0,
                "start_time": "2025-09-27T12:00:00Z",
                "end_time": "2025-09-27T12:02:00Z",
            },
            "app2_info": {
                "name": "App2",
                "duration_seconds": 100.0,
                "start_time": "2025-09-27T12:03:00Z",
                "end_time": "2025-09-27T12:04:40Z",
            },
            "timeline_comparison": [
                {
                    "interval": 1,
                    "timestamp_range": "2025-09-27 12:00:00 to 2025-09-27 12:00:10",
                    "app1": {"executor_count": 5},
                    "app2": {"executor_count": 3},
                    "differences": {"executor_count_diff": 2},
                }
            ],
            "resource_efficiency": {
                "app1": {
                    "efficiency_score": 0.8,
                    "peak_executor_count": 10,
                    "avg_executor_count": 6.5,
                },
                "app2": {
                    "efficiency_score": 0.7,
                    "peak_executor_count": 8,
                    "avg_executor_count": 5.0,
                },
            },
            "summary": {"note": "ok"},
            "recommendations": [
                {
                    "priority": "MEDIUM",
                    "issue": "Tune executors",
                    "suggestion": "Increase cores",
                }
            ],
        }
        fmt.output(timeline, title="TimelineFull")


class TestOutputFormatterJSON:
    def test_output_json_from_dict(self, capsys):
        fmt = OutputFormatter(format_type="json")
        data = {"a": 1, "b": "x"}
        fmt.output(data, title="ignored")
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert parsed == data

    def test_output_json_from_object_with_dict(self, capsys):
        fmt = OutputFormatter(format_type="json")
        obj = SimpleNamespace(foo=1, bar="z")
        fmt.output(obj)
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert parsed == {"foo": 1, "bar": "z"}

    def test_quiet_suppresses_non_json(self, capsys):
        fmt = OutputFormatter(format_type="table", quiet=True)
        fmt.output([1, 2, 3], title="Numbers")
        out = capsys.readouterr().out
        assert out.strip() == ""
