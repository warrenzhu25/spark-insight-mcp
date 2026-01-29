"""
Tests for spark_history_mcp.tools.timelines module.
"""

from datetime import datetime
from types import SimpleNamespace

from spark_history_mcp.tools.timelines import (
    build_app_executor_timeline,
    build_stage_executor_timeline,
    merge_consecutive_intervals,
    merge_intervals,
)


class TestMergeConsecutiveIntervals:
    """Test the merge_consecutive_intervals function."""

    def test_empty_data(self):
        """Test handling of empty comparison data."""
        result = merge_consecutive_intervals([])
        assert result == []

    def test_single_entry(self):
        """Test handling of single entry."""
        data = [
            {
                "timestamp_range": "10:00 to 10:05",
                "differences": {"executor_count_diff": 2},
            }
        ]
        result = merge_consecutive_intervals(data)
        assert len(result) == 1
        assert result[0]["timestamp_range"] == "10:00 to 10:05"

    def test_merge_consecutive_same_diff(self):
        """Test merging consecutive intervals with same executor count diff."""
        data = [
            {
                "timestamp_range": "10:00 to 10:05",
                "differences": {"executor_count_diff": 2},
            },
            {
                "timestamp_range": "10:05 to 10:10",
                "differences": {"executor_count_diff": 2},
            },
            {
                "timestamp_range": "10:10 to 10:15",
                "differences": {"executor_count_diff": 2},
            },
        ]
        result = merge_consecutive_intervals(data)

        assert len(result) == 1
        assert result[0]["timestamp_range"] == "10:00 to 10:15"
        assert result[0]["differences"]["executor_count_diff"] == 2

    def test_no_merge_different_diff(self):
        """Test no merging when executor count diffs are different."""
        data = [
            {
                "timestamp_range": "10:00 to 10:05",
                "differences": {"executor_count_diff": 2},
            },
            {
                "timestamp_range": "10:05 to 10:10",
                "differences": {"executor_count_diff": 3},
            },
        ]
        result = merge_consecutive_intervals(data)

        assert len(result) == 2
        assert result[0]["timestamp_range"] == "10:00 to 10:05"
        assert result[1]["timestamp_range"] == "10:05 to 10:10"

    def test_mixed_merge_pattern(self):
        """Test mixed pattern of merge and no-merge."""
        data = [
            {
                "timestamp_range": "10:00 to 10:05",
                "differences": {"executor_count_diff": 2},
            },
            {
                "timestamp_range": "10:05 to 10:10",
                "differences": {"executor_count_diff": 2},
            },
            {
                "timestamp_range": "10:10 to 10:15",
                "differences": {"executor_count_diff": 3},
            },
            {
                "timestamp_range": "10:15 to 10:20",
                "differences": {"executor_count_diff": 3},
            },
        ]
        result = merge_consecutive_intervals(data)

        assert len(result) == 2
        assert result[0]["timestamp_range"] == "10:00 to 10:10"
        assert result[0]["differences"]["executor_count_diff"] == 2
        assert result[1]["timestamp_range"] == "10:10 to 10:20"
        assert result[1]["differences"]["executor_count_diff"] == 3

    def test_missing_differences_key(self):
        """Test handling of entries missing differences key."""
        data = [
            {
                "timestamp_range": "10:00 to 10:05",
                "differences": {"executor_count_diff": 2},
            },
            {
                "timestamp_range": "10:05 to 10:10"
                # Missing differences key
            },
        ]
        result = merge_consecutive_intervals(data)

        # Should not merge due to missing differences
        assert len(result) == 2


class TestMergeIntervals:
    """Test the generic merge_intervals function."""

    def test_default_key(self):
        """Test using default executor_count_diff key."""
        data = [
            {
                "timestamp_range": "10:00 to 10:05",
                "differences": {"executor_count_diff": 1},
            },
            {
                "timestamp_range": "10:05 to 10:10",
                "differences": {"executor_count_diff": 1},
            },
        ]
        result = merge_intervals(data)

        assert len(result) == 1
        assert result[0]["timestamp_range"] == "10:00 to 10:10"

    def test_custom_key(self):
        """Test using custom merge key."""
        data = [
            {"timestamp_range": "10:00 to 10:05", "differences": {"memory_diff": 512}},
            {"timestamp_range": "10:05 to 10:10", "differences": {"memory_diff": 512}},
        ]
        result = merge_intervals(data, same_key="memory_diff")

        assert len(result) == 1
        assert result[0]["timestamp_range"] == "10:00 to 10:10"

    def test_empty_data(self):
        """Test handling of empty data."""
        result = merge_intervals([])
        assert result == []


class TestBuildStageExecutorTimeline:
    """Test the build_stage_executor_timeline function."""

    def test_basic_stage_timeline(self):
        """Test basic stage timeline construction."""
        stage_start = datetime(2024, 1, 1, 10, 0, 0)
        stage_end = datetime(2024, 1, 1, 10, 5, 0)

        stage = SimpleNamespace(
            stage_id=1,
            attempt_id=0,
            name="Test Stage",
            submission_time=stage_start,
            completion_time=stage_end,
        )

        executors = [
            SimpleNamespace(
                id="exec-1",
                host_port="host1:7337",
                total_cores=4,
                max_memory=2 * 1024 * 1024 * 1024,  # 2GB in bytes
                add_time=datetime(2024, 1, 1, 10, 0, 0),
                remove_time=datetime(2024, 1, 1, 10, 3, 0),
            ),
            SimpleNamespace(
                id="exec-2",
                host_port="host2:7337",
                total_cores=4,
                max_memory=2 * 1024 * 1024 * 1024,
                add_time=datetime(2024, 1, 1, 10, 2, 0),
                remove_time=datetime(2024, 1, 1, 10, 5, 0),
            ),
        ]

        result = build_stage_executor_timeline(stage, executors, interval_minutes=1)

        assert "stage_info" in result
        assert "timeline" in result

        stage_info = result["stage_info"]
        assert stage_info["stage_id"] == 1
        assert stage_info["attempt_id"] == 0
        assert stage_info["name"] == "Test Stage"
        assert stage_info["duration_seconds"] == 300  # 5 minutes

        timeline = result["timeline"]
        assert len(timeline) == 5  # 5 one-minute intervals

        # First interval: only exec-1
        assert timeline[0]["active_executor_count"] == 1
        assert timeline[0]["total_cores"] == 4
        assert timeline[0]["total_memory_mb"] == 2048

        # Third interval: both executors (10:02-10:03)
        assert timeline[2]["active_executor_count"] == 2
        assert timeline[2]["total_cores"] == 8
        assert timeline[2]["total_memory_mb"] == 4096

    def test_stage_without_submission_time(self):
        """Test handling of stage without submission time."""
        stage = SimpleNamespace(
            stage_id=2, attempt_id=0, name="Bad Stage", submission_time=None
        )

        result = build_stage_executor_timeline(stage, [])

        assert "error" in result
        assert "Stage 2 has no submission time" in result["error"]
        assert result["stage_info"]["stage_id"] == 2
        assert result["timeline"] == []

    def test_stage_without_completion_time(self):
        """Test handling of stage without completion time (uses 24h default)."""
        stage_start = datetime(2024, 1, 1, 10, 0, 0)

        stage = SimpleNamespace(
            stage_id=3,
            attempt_id=0,
            name="Long Stage",
            submission_time=stage_start,
            completion_time=None,
        )

        executors = []

        # Use a smaller max_intervals to trigger truncation
        result = build_stage_executor_timeline(
            stage, executors, interval_minutes=60, max_intervals=2
        )

        # Should create timeline with truncation warning
        timeline = result["timeline"]
        # Should have 2 intervals plus truncation warning
        assert len(timeline) == 3
        assert "warning" in timeline[-1]
        assert "Timeline truncated" in timeline[-1]["warning"]

    def test_executors_without_timing(self):
        """Test handling of executors without timing information."""
        stage_start = datetime(2024, 1, 1, 10, 0, 0)
        stage_end = datetime(2024, 1, 1, 10, 2, 0)

        stage = SimpleNamespace(
            stage_id=4,
            attempt_id=0,
            name="Test Stage",
            submission_time=stage_start,
            completion_time=stage_end,
        )

        executors = [
            SimpleNamespace(
                id="exec-no-timing",
                host_port="host:7337",
                total_cores=None,
                max_memory=None,
                add_time=None,
                remove_time=None,
            )
        ]

        result = build_stage_executor_timeline(stage, executors, interval_minutes=1)

        timeline = result["timeline"]
        # Executor should be considered active for whole stage duration (due to None times)
        assert timeline[0]["active_executor_count"] == 1
        assert timeline[0]["total_cores"] == 0  # None cores becomes 0
        assert timeline[0]["total_memory_mb"] == 0  # None memory becomes 0

    def test_zero_duration_stage(self):
        """Test handling of stage with zero or negative duration."""
        stage_time = datetime(2024, 1, 1, 10, 0, 0)

        stage = SimpleNamespace(
            stage_id=5,
            attempt_id=0,
            name="Instant Stage",
            submission_time=stage_time,
            completion_time=stage_time,  # Same time = zero duration
        )

        result = build_stage_executor_timeline(stage, [], interval_minutes=1)

        timeline = result["timeline"]
        assert len(timeline) == 1  # Should create at least one interval
        assert (
            result["stage_info"]["duration_seconds"] == 60
        )  # Minimum interval duration


class TestBuildAppExecutorTimeline:
    """Test the build_app_executor_timeline function."""

    def test_basic_app_timeline(self):
        """Test basic application timeline construction."""
        app_start = datetime(2024, 1, 1, 10, 0, 0)
        app_end = datetime(2024, 1, 1, 10, 10, 0)

        app = SimpleNamespace(
            id="app-123",
            name="Test App",
            attempts=[SimpleNamespace(start_time=app_start, end_time=app_end)],
        )

        executors = [
            SimpleNamespace(
                id="exec-1",
                total_cores=4,
                max_memory=2 * 1024 * 1024 * 1024,
                add_time=datetime(2024, 1, 1, 10, 1, 0),
                remove_time=datetime(2024, 1, 1, 10, 8, 0),
            )
        ]

        stages = [
            SimpleNamespace(
                stage_id=1,
                name="Stage 1",
                submission_time=datetime(2024, 1, 1, 10, 2, 0),
                completion_time=datetime(2024, 1, 1, 10, 6, 0),
            )
        ]

        result = build_app_executor_timeline(app, executors, stages, interval_minutes=2)

        assert result is not None
        assert "app_info" in result
        assert "timeline" in result
        assert "summary" in result

        app_info = result["app_info"]
        assert app_info["app_id"] == "app-123"
        assert app_info["name"] == "Test App"
        assert app_info["duration_seconds"] == 600  # 10 minutes

        timeline = result["timeline"]
        assert len(timeline) == 5  # 10 minutes / 2-minute intervals

        # Check executor presence in timeline
        # First interval (10:00-10:02): executor added at 10:01, so it's active
        assert timeline[0]["active_executor_count"] == 1

        # Second interval (10:02-10:04): executor active, stage active
        assert timeline[1]["active_executor_count"] == 1
        assert timeline[1]["total_cores"] == 4
        assert timeline[1]["active_stages_count"] == 1

        summary = result["summary"]
        assert summary["total_executors"] == 1
        assert summary["total_stages"] == 1
        assert summary["peak_executor_count"] == 1
        assert summary["peak_cores"] == 4

    def test_app_without_attempts(self):
        """Test handling of app without attempts."""
        app = SimpleNamespace(attempts=[])

        result = build_app_executor_timeline(app, [], [])
        assert result is None

    def test_app_without_start_time(self):
        """Test handling of app attempt without start time."""
        app = SimpleNamespace(
            attempts=[SimpleNamespace(start_time=None, end_time=None)]
        )

        result = build_app_executor_timeline(app, [], [])
        assert result is None

    def test_app_without_end_time(self):
        """Test handling of app without end time (uses 24h default)."""
        app_start = datetime(2024, 1, 1, 10, 0, 0)

        app = SimpleNamespace(
            id="app-no-end",
            name="Long App",
            attempts=[SimpleNamespace(start_time=app_start, end_time=None)],
        )

        result = build_app_executor_timeline(app, [], [], interval_minutes=60)

        assert result is not None
        app_info = result["app_info"]
        assert app_info["end_time"] is None  # Original end_time was None
        # Duration should be 24 hours
        assert app_info["duration_seconds"] == 24 * 3600

    def test_timeline_events_sorting(self):
        """Test that timeline events are properly collected and sorted."""
        app_start = datetime(2024, 1, 1, 10, 0, 0)
        app_end = datetime(2024, 1, 1, 10, 10, 0)

        app = SimpleNamespace(
            id="app-events",
            name="Event App",
            attempts=[SimpleNamespace(start_time=app_start, end_time=app_end)],
        )

        # Executors with different add/remove times
        executors = [
            SimpleNamespace(
                id="exec-1",
                total_cores=2,
                max_memory=1024 * 1024 * 1024,
                add_time=datetime(2024, 1, 1, 10, 1, 0),
                remove_time=datetime(2024, 1, 1, 10, 5, 0),
            ),
            SimpleNamespace(
                id="exec-2",
                total_cores=2,
                max_memory=1024 * 1024 * 1024,
                add_time=datetime(2024, 1, 1, 10, 3, 0),
                remove_time=datetime(2024, 1, 1, 10, 8, 0),
            ),
        ]

        # Stages with different timing
        stages = [
            SimpleNamespace(
                stage_id=1,
                name="Early Stage",
                submission_time=datetime(2024, 1, 1, 10, 0, 30),
                completion_time=datetime(2024, 1, 1, 10, 2, 0),
            ),
            SimpleNamespace(
                stage_id=2,
                name="Late Stage",
                submission_time=datetime(2024, 1, 1, 10, 6, 0),
                completion_time=datetime(2024, 1, 1, 10, 9, 0),
            ),
        ]

        result = build_app_executor_timeline(app, executors, stages, interval_minutes=1)

        timeline = result["timeline"]

        # Verify different executor counts at different times
        # 10:00-10:01: 1 executor (exec-1 starts at 10:01), 1 stage
        assert timeline[0]["active_executor_count"] == 1
        assert timeline[0]["active_stages_count"] == 1

        # 10:03-10:04: 2 executors, no stages (early stage completed, late stage not started)
        assert timeline[3]["active_executor_count"] == 2
        assert timeline[3]["total_cores"] == 4
        assert timeline[3]["active_stages_count"] == 0

        # 10:06-10:07: 1 executor (exec-2), 1 stage (late stage active)
        assert timeline[6]["active_executor_count"] == 1
        assert timeline[6]["active_stages_count"] == 1

    def test_empty_timeline(self):
        """Test timeline with no executors or stages."""
        app_start = datetime(2024, 1, 1, 10, 0, 0)
        app_end = datetime(2024, 1, 1, 10, 2, 0)

        app = SimpleNamespace(
            id="app-empty",
            name="Empty App",
            attempts=[SimpleNamespace(start_time=app_start, end_time=app_end)],
        )

        result = build_app_executor_timeline(app, [], [], interval_minutes=1)

        timeline = result["timeline"]
        assert len(timeline) == 2

        for interval in timeline:
            assert interval["active_executor_count"] == 0
            assert interval["total_cores"] == 0
            assert interval["total_memory_mb"] == 0
            assert interval["active_stages_count"] == 0

        summary = result["summary"]
        assert summary["total_executors"] == 0
        assert summary["total_stages"] == 0
        assert summary["peak_executor_count"] == 0
        assert summary["avg_executor_count"] == 0.0

    def test_summary_calculations(self):
        """Test summary statistics calculations."""
        app_start = datetime(2024, 1, 1, 10, 0, 0)
        app_end = datetime(2024, 1, 1, 10, 6, 0)

        app = SimpleNamespace(
            id="app-summary",
            name="Summary App",
            attempts=[SimpleNamespace(start_time=app_start, end_time=app_end)],
        )

        # Varying executor counts over time
        executors = [
            SimpleNamespace(
                id=f"exec-{i}",
                total_cores=2,
                max_memory=1024 * 1024 * 1024,
                add_time=datetime(2024, 1, 1, 10, i, 0),
                remove_time=datetime(2024, 1, 1, 10, 4, 0),
            )
            for i in range(3)  # 3 executors added at different times
        ]

        result = build_app_executor_timeline(app, executors, [], interval_minutes=1)

        summary = result["summary"]
        assert summary["total_executors"] == 3
        assert summary["peak_executor_count"] == 3  # All active 10:02-10:04
        assert summary["peak_cores"] == 6  # 3 executors * 2 cores
        assert summary["peak_memory_mb"] == 3072  # 3 * 1024 MB

        # Average should be calculated across all intervals
        timeline = result["timeline"]
        expected_avg = sum(t["active_executor_count"] for t in timeline) / len(timeline)
        assert summary["avg_executor_count"] == expected_avg
