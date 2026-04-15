"""
Tests for spark_history_mcp.tools.stage_aggregation module.
"""

from types import SimpleNamespace

from spark_history_mcp.models.spark_types import StageStatus
from spark_history_mcp.tools.stage_aggregation import (
    aggregate_stage_metrics,
    aggregate_stage_metrics_for_comparison,
    get_aggregated_field_names,
)


def make_stage(
    status=StageStatus.COMPLETE,
    executor_run_time=1000,
    executor_cpu_time=1_000_000_000,  # 1 second in nanoseconds
    jvm_gc_time=100,
    input_bytes=1024,
    output_bytes=512,
    shuffle_read_bytes=256,
    shuffle_write_bytes=128,
    memory_bytes_spilled=64,
    disk_bytes_spilled=32,
    num_tasks=10,
    num_failed_tasks=1,
    shuffle_fetch_wait_time=50,
    shuffle_write_time=100_000_000,  # 100ms in nanoseconds
    submission_time=None,
    completion_time=None,
):
    """Create a mock stage object for testing."""
    return SimpleNamespace(
        status=status,
        executor_run_time=executor_run_time,
        executor_cpu_time=executor_cpu_time,
        jvm_gc_time=jvm_gc_time,
        input_bytes=input_bytes,
        output_bytes=output_bytes,
        shuffle_read_bytes=shuffle_read_bytes,
        shuffle_write_bytes=shuffle_write_bytes,
        memory_bytes_spilled=memory_bytes_spilled,
        disk_bytes_spilled=disk_bytes_spilled,
        num_tasks=num_tasks,
        num_failed_tasks=num_failed_tasks,
        shuffle_fetch_wait_time=shuffle_fetch_wait_time,
        shuffle_write_time=shuffle_write_time,
        submission_time=submission_time,
        completion_time=completion_time,
    )


class TestAggregateStageMetrics:
    """Test the aggregate_stage_metrics function."""

    def test_empty_stages(self):
        """Test aggregation with no stages."""
        result = aggregate_stage_metrics([])

        assert result["total_stages"] == 0
        assert result["completed_stages"] == 0
        assert result["failed_stages"] == 0

    def test_single_stage(self):
        """Test aggregation with a single stage."""
        stages = [make_stage()]
        result = aggregate_stage_metrics(stages, include_duration=False)

        assert result["total_stages"] == 1
        assert result["completed_stages"] == 1
        assert result["failed_stages"] == 0
        assert result["executor_run_time"] == 1000
        assert result["input_bytes"] == 1024
        assert result["num_tasks"] == 10
        assert result["num_failed_tasks"] == 1

    def test_multiple_stages_aggregation(self):
        """Test that metrics are summed across multiple stages."""
        stages = [
            make_stage(input_bytes=1000, num_tasks=5),
            make_stage(input_bytes=2000, num_tasks=10),
            make_stage(input_bytes=3000, num_tasks=15),
        ]
        result = aggregate_stage_metrics(stages, include_duration=False)

        assert result["total_stages"] == 3
        assert result["completed_stages"] == 3
        assert result["input_bytes"] == 6000
        assert result["num_tasks"] == 30

    def test_stage_status_counting(self):
        """Test that completed and failed stages are counted correctly."""
        stages = [
            make_stage(status=StageStatus.COMPLETE),
            make_stage(status=StageStatus.COMPLETE),
            make_stage(status=StageStatus.FAILED),
            make_stage(status=StageStatus.PENDING),
        ]
        result = aggregate_stage_metrics(stages, include_duration=False)

        assert result["total_stages"] == 4
        assert result["completed_stages"] == 2
        assert result["failed_stages"] == 1

    def test_string_status_handling(self):
        """Test handling of string status values."""
        stages = [
            SimpleNamespace(
                status="COMPLETE",
                executor_run_time=100,
                input_bytes=100,
                num_tasks=1,
                num_failed_tasks=0,
                submission_time=None,
                completion_time=None,
            ),
            SimpleNamespace(
                status="COMPLETED",
                executor_run_time=100,
                input_bytes=100,
                num_tasks=1,
                num_failed_tasks=0,
                submission_time=None,
                completion_time=None,
            ),
        ]
        result = aggregate_stage_metrics(stages, include_duration=False)

        assert result["completed_stages"] == 2

    def test_none_values_handled(self):
        """Test that None values are treated as zero."""
        stage = SimpleNamespace(
            status=StageStatus.COMPLETE,
            executor_run_time=None,
            input_bytes=None,
            num_tasks=None,
            num_failed_tasks=None,
            submission_time=None,
            completion_time=None,
        )
        result = aggregate_stage_metrics([stage], include_duration=False)

        assert result["total_stages"] == 1
        # None values should be treated as 0
        assert result.get("executor_run_time", 0) == 0.0
        assert result.get("input_bytes", 0) == 0.0

    def test_dynamic_field_discovery(self):
        """Test that fields are discovered dynamically from the model."""
        result = aggregate_stage_metrics([make_stage()], include_duration=False)

        # Should include all major stage metrics
        assert "executor_run_time" in result
        assert "jvm_gc_time" in result
        assert "input_bytes" in result
        assert "output_bytes" in result
        assert "shuffle_read_bytes" in result
        assert "shuffle_write_bytes" in result
        assert "memory_bytes_spilled" in result
        assert "disk_bytes_spilled" in result


class TestAggregateStageMetricsForComparison:
    """Test the aggregate_stage_metrics_for_comparison function."""

    def test_empty_stages(self):
        """Test comparison format with no stages."""
        result = aggregate_stage_metrics_for_comparison([])

        assert result["total_stages"] == 0
        assert result["total_input_bytes"] == 0
        assert result["total_executor_run_time_ms"] == 0
        assert result["total_executor_cpu_time_ms"] == 0

    def test_key_format(self):
        """Test that output keys have the expected format with total_ prefix."""
        result = aggregate_stage_metrics_for_comparison([make_stage()])

        # Should have total_ prefixed keys
        assert "total_stages" in result
        assert "total_input_bytes" in result
        assert "total_output_bytes" in result
        assert "total_shuffle_read_bytes" in result
        assert "total_shuffle_write_bytes" in result
        assert "total_memory_spilled_bytes" in result
        assert "total_disk_spilled_bytes" in result
        assert "total_executor_run_time_ms" in result
        assert "total_executor_cpu_time_ms" in result
        assert "total_gc_time_ms" in result
        assert "total_tasks" in result
        assert "total_failed_tasks" in result

    def test_cpu_time_conversion(self):
        """Test that executor_cpu_time is converted from ns to ms."""
        # 1 billion nanoseconds = 1000 milliseconds
        stage = make_stage(executor_cpu_time=1_000_000_000)
        result = aggregate_stage_metrics_for_comparison([stage])

        assert result["total_executor_cpu_time_ms"] == 1000.0

    def test_value_aggregation(self):
        """Test that values are correctly aggregated."""
        stages = [
            make_stage(input_bytes=1000, executor_run_time=100),
            make_stage(input_bytes=2000, executor_run_time=200),
        ]
        result = aggregate_stage_metrics_for_comparison(stages)

        assert result["total_stages"] == 2
        assert result["total_input_bytes"] == 3000
        assert result["total_executor_run_time_ms"] == 300


class TestGetAggregatedFieldNames:
    """Test the get_aggregated_field_names function."""

    def test_returns_list(self):
        """Test that the function returns a list."""
        result = get_aggregated_field_names()
        assert isinstance(result, list)

    def test_contains_expected_fields(self):
        """Test that expected field names are included."""
        result = get_aggregated_field_names()

        # Should include key stage metrics
        assert "executor_run_time" in result
        assert "input_bytes" in result
        assert "num_tasks" in result

    def test_excludes_non_aggregatable(self):
        """Test that non-aggregatable fields are excluded."""
        result = get_aggregated_field_names()

        # Non-aggregatable fields should not be in the result
        # The get_aggregated_field_names function only returns fields where
        # aggregatable=True, so num_active_tasks and similar should be excluded
        assert "num_active_tasks" not in result
        assert "peak_execution_memory" not in result


class TestBackwardCompatibility:
    """Test backward compatibility with existing usage patterns."""

    def test_app_summary_fields_available(self):
        """Test that fields needed by summarize_app are available."""
        result = aggregate_stage_metrics([make_stage()], include_duration=False)

        # These fields are used by summarize_app
        assert "executor_run_time" in result
        assert "executor_cpu_time" in result
        assert "jvm_gc_time" in result
        assert "input_bytes" in result
        assert "output_bytes" in result
        assert "shuffle_read_bytes" in result
        assert "shuffle_write_bytes" in result
        assert "memory_bytes_spilled" in result
        assert "disk_bytes_spilled" in result
        assert "num_failed_tasks" in result
        assert "shuffle_fetch_wait_time" in result
        assert "shuffle_write_time" in result

    def test_comparison_format_matches_expected(self):
        """Test that comparison format matches what compare_app_stages_aggregated expects."""
        result = aggregate_stage_metrics_for_comparison([make_stage()])

        # Expected keys from the original _calculate_aggregated_stage_metrics
        expected_keys = {
            "total_stages",
            "total_input_bytes",
            "total_output_bytes",
            "total_shuffle_read_bytes",
            "total_shuffle_write_bytes",
            "total_memory_spilled_bytes",
            "total_disk_spilled_bytes",
            "total_executor_run_time_ms",
            "total_executor_cpu_time_ms",
            "total_gc_time_ms",
            "avg_stage_duration_ms",
            "total_tasks",
            "total_failed_tasks",
        }

        for key in expected_keys:
            assert key in result, f"Expected key {key} not found in result"
