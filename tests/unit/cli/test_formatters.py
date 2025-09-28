"""
Tests for OutputFormatter behaviors.

Focus on JSON output and simple fallback when Rich is unavailable.
"""

import json
from types import SimpleNamespace
import pytest

from spark_history_mcp.cli.formatters import OutputFormatter


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
            "app_summary_diff": {
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
        }
        # Should not raise
        fmt.output(comparison_data, title="Comparison")

    def test_format_stage_comparison_result(self):
        fmt = OutputFormatter(format_type="human")
        data = {
            "stage_comparison": {
                "stage1": {"stage_id": 3, "name": "Stage A", "status": "COMPLETE", "app_id": "app-1"},
                "stage2": {"stage_id": 4, "name": "Stage B", "status": "COMPLETE", "app_id": "app-2"},
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
