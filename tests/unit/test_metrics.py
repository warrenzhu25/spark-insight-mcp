"""
Tests for spark_history_mcp.tools.metrics module.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from spark_history_mcp.models.spark_types import StageStatus
from spark_history_mcp.tools.metrics import (
    compare_distributions,
    compare_numeric_maps,
    compute_utilization,
    summarize_app,
)


class TestSummarizeApp:
    """Test the summarize_app function."""

    def test_no_app_attempts(self):
        """Test handling app with no attempts."""
        app = SimpleNamespace(attempts=[], id="app-123")
        result = summarize_app(app, [], [])
        assert result["error"] == "No application attempts found"
        assert result["application_id"] == "app-123"

    def test_basic_app_summary(self):
        """Test basic application summary with minimal data."""
        app = SimpleNamespace(
            id="app-123",
            name="test-app",
            attempts=[SimpleNamespace(duration=60000)],  # 1 minute
            cores_per_executor=2,
        )
        stages = [
            SimpleNamespace(
                executor_run_time=30000,  # 30 seconds
                executor_cpu_time=25000000000,  # 25 seconds in ns
                jvm_gc_time=5000,  # 5 seconds
                input_bytes=1024 * 1024 * 1024,  # 1 GB
                output_bytes=512 * 1024 * 1024,  # 512 MB
                shuffle_read_bytes=256 * 1024 * 1024,  # 256 MB
                shuffle_write_bytes=128 * 1024 * 1024,  # 128 MB
                memory_bytes_spilled=64 * 1024 * 1024,  # 64 MB
                disk_bytes_spilled=32 * 1024 * 1024,  # 32 MB
                num_failed_tasks=2,
                num_tasks=10,
                status=StageStatus.COMPLETE,
                task_metrics_distributions=None,
            )
        ]
        executors = []

        result = summarize_app(app, stages, executors)

        assert result["application_id"] == "app-123"
        assert result["application_name"] == "test-app"
        assert result["application_duration_minutes"] == 1.0
        assert result["total_executor_runtime_minutes"] == 0.5
        assert result["executor_cpu_time_minutes"] == pytest.approx(0.417, rel=1e-2)
        assert result["jvm_gc_time_minutes"] == pytest.approx(0.083, rel=0.05)
        assert result["input_data_size_gb"] == 1.0
        assert result["output_data_size_gb"] == 0.5
        assert result["shuffle_read_size_gb"] == 0.25
        assert result["shuffle_write_size_gb"] == 0.125
        assert result["memory_spilled_gb"] == pytest.approx(0.063, rel=0.05)
        assert result["disk_spilled_gb"] == pytest.approx(0.031, rel=0.05)
        assert result["failed_tasks"] == 2
        assert result["total_stages"] == 1
        assert result["completed_stages"] == 1
        assert result["failed_stages"] == 0

    def test_multiple_stages_with_different_statuses(self):
        """Test summary with multiple stages having different statuses."""
        app = SimpleNamespace(
            id="app-456",
            name="multi-stage-app",
            attempts=[SimpleNamespace(duration=120000)],  # 2 minutes
            cores_per_executor=4,
        )
        stages = [
            SimpleNamespace(
                executor_run_time=45000,
                executor_cpu_time=40000000000,
                jvm_gc_time=2000,
                input_bytes=2 * 1024 * 1024 * 1024,
                output_bytes=1024 * 1024 * 1024,
                shuffle_read_bytes=512 * 1024 * 1024,
                shuffle_write_bytes=256 * 1024 * 1024,
                memory_bytes_spilled=0,
                disk_bytes_spilled=0,
                num_failed_tasks=0,
                num_tasks=20,
                status=StageStatus.COMPLETE,
                task_metrics_distributions=None,
            ),
            SimpleNamespace(
                executor_run_time=15000,
                executor_cpu_time=10000000000,
                jvm_gc_time=8000,
                input_bytes=0,
                output_bytes=0,
                shuffle_read_bytes=0,
                shuffle_write_bytes=0,
                memory_bytes_spilled=128 * 1024 * 1024,
                disk_bytes_spilled=64 * 1024 * 1024,
                num_failed_tasks=5,
                num_tasks=15,
                status=StageStatus.FAILED,
                task_metrics_distributions=None,
            ),
        ]
        executors = []

        result = summarize_app(app, stages, executors)

        assert result["application_duration_minutes"] == 2.0
        assert (
            result["total_executor_runtime_minutes"] == 1.0
        )  # 45000 + 15000 = 60000ms = 1min
        assert result["failed_tasks"] == 5
        assert result["total_stages"] == 2
        assert result["completed_stages"] == 1
        assert result["failed_stages"] == 1

    def test_executor_utilization_calculation(self):
        """Test executor utilization calculation with executor timing data."""
        start_time = datetime(2024, 1, 1, 10, 0, 0)
        end_time = datetime(2024, 1, 1, 10, 10, 0)  # 10 minutes later

        app = SimpleNamespace(
            id="app-789",
            name="utilization-test",
            attempts=[
                SimpleNamespace(
                    duration=600000,  # 10 minutes
                    start_time=start_time,
                    end_time=end_time,
                )
            ],
            cores_per_executor=2,
        )

        stages = [
            SimpleNamespace(
                executor_run_time=240000,  # 4 minutes total runtime
                executor_cpu_time=200000000000,
                jvm_gc_time=10000,
                input_bytes=0,
                output_bytes=0,
                shuffle_read_bytes=0,
                shuffle_write_bytes=0,
                memory_bytes_spilled=0,
                disk_bytes_spilled=0,
                num_failed_tasks=0,
                num_tasks=5,
                status=StageStatus.COMPLETE,
                task_metrics_distributions=None,
            )
        ]

        # 2 executors, each active for 5 minutes
        executors = [
            SimpleNamespace(
                add_time=datetime(2024, 1, 1, 10, 0, 0),
                remove_time=datetime(2024, 1, 1, 10, 5, 0),
            ),
            SimpleNamespace(
                add_time=datetime(2024, 1, 1, 10, 2, 0),
                remove_time=datetime(2024, 1, 1, 10, 7, 0),
            ),
        ]

        result = summarize_app(app, stages, executors)

        # Total executor time: 2 executors * 5 minutes * 2 cores = 20 core-minutes = 1,200,000 core-ms
        # Actual runtime: 240,000 ms = 4 minutes
        # Utilization: 240,000 / 1,200,000 * 100 = 20%
        assert result["executor_utilization_percent"] == 20.0

    def test_shuffle_metrics_from_distributions(self):
        """Test shuffle metrics extraction from task distributions."""
        app = SimpleNamespace(
            id="app-shuffle",
            name="shuffle-test",
            attempts=[SimpleNamespace(duration=300000)],
            cores_per_executor=1,
        )

        # Mock distributions with shuffle metrics
        shuffle_read_metrics = SimpleNamespace(
            fetch_wait_time=[
                1000000,
                2000000,
                3000000,
                4000000,
                5000000,
            ]  # ns, median = 3ms
        )
        shuffle_write_metrics = SimpleNamespace(
            write_time=[
                500000,
                1000000,
                1500000,
                2000000,
                2500000,
            ]  # ns, median = 1.5ms
        )
        distributions = SimpleNamespace(
            shuffle_read_metrics=shuffle_read_metrics,
            shuffle_write_metrics=shuffle_write_metrics,
        )

        stages = [
            SimpleNamespace(
                executor_run_time=60000,
                executor_cpu_time=50000000000,
                jvm_gc_time=1000,
                input_bytes=0,
                output_bytes=0,
                shuffle_read_bytes=0,
                shuffle_write_bytes=0,
                memory_bytes_spilled=0,
                disk_bytes_spilled=0,
                num_failed_tasks=0,
                num_tasks=100,  # 100 tasks
                status=StageStatus.COMPLETE,
                task_metrics_distributions=distributions,
            )
        ]
        executors = []

        result = summarize_app(app, stages, executors)

        # Shuffle fetch wait: 3ms * 100 tasks = 300ms = 0.005 minutes
        # Shuffle write time: 1.5ms * 100 tasks = 150ms = 0.0025 minutes
        assert (
            result["shuffle_read_wait_time_minutes"] == 0.01
        )  # rounded to 2 decimal places
        assert result["shuffle_write_time_minutes"] == 0.0

    def test_missing_attributes_handling(self):
        """Test handling of missing attributes with None or 0 defaults."""
        app = SimpleNamespace(
            id="app-missing", attempts=[SimpleNamespace(duration=None)]
        )

        stages = [
            SimpleNamespace(
                # Missing most attributes
                status=StageStatus.COMPLETE
            )
        ]
        executors = []

        result = summarize_app(app, stages, executors)

        assert result["application_duration_minutes"] == 0.0
        assert result["total_executor_runtime_minutes"] == 0.0
        assert result["executor_cpu_time_minutes"] == 0.0
        assert result["input_data_size_gb"] == 0.0
        assert result["failed_tasks"] == 0


class TestComputeUtilization:
    """Test the compute_utilization function."""

    def test_basic_utilization(self):
        """Test basic utilization calculation."""
        stages = [
            SimpleNamespace(executor_run_time=120000),  # 2 minutes
            SimpleNamespace(executor_run_time=180000),  # 3 minutes
        ]

        executors = [
            SimpleNamespace(
                add_time=datetime(2024, 1, 1, 10, 0, 0),
                remove_time=datetime(2024, 1, 1, 10, 10, 0),  # 10 minutes
            )
        ]

        # Total runtime: 5 minutes, total executor time: 10 minutes * 2 cores = 20 core-minutes
        # Utilization: 5 / 20 * 100 = 25%
        result = compute_utilization(stages, executors, executor_cores=2)
        assert result == 25.0

    def test_utilization_with_app_bounds(self):
        """Test utilization with app start/end bounds."""
        stages = [SimpleNamespace(executor_run_time=60000)]  # 1 minute

        # Executor without remove_time
        executors = [SimpleNamespace(add_time=datetime(2024, 1, 1, 10, 0, 0))]

        app_start = datetime(2024, 1, 1, 10, 0, 0).timestamp() * 1000
        app_end = datetime(2024, 1, 1, 10, 5, 0).timestamp() * 1000  # 5 minutes

        result = compute_utilization(
            stages, executors, executor_cores=1, app_start_end=(app_start, app_end)
        )

        # Runtime: 1 minute, executor time: 5 minutes * 1 core = 5 core-minutes
        # Utilization: 1 / 5 * 100 = 20%
        assert result == 20.0

    def test_zero_utilization_cases(self):
        """Test cases that should return zero utilization."""
        stages = [SimpleNamespace(executor_run_time=60000)]

        # No executors
        assert compute_utilization(stages, [], executor_cores=1) == 0.0

        # Zero cores
        executors = [
            SimpleNamespace(add_time=datetime.now(), remove_time=datetime.now())
        ]
        assert compute_utilization(stages, executors, executor_cores=0) == 0.0

        # Executor without timing info
        executors = [SimpleNamespace()]
        assert compute_utilization(stages, executors, executor_cores=1) == 0.0


class TestCompareNumericMaps:
    """Test the compare_numeric_maps function."""

    def test_basic_comparison(self):
        """Test basic numeric map comparison."""
        map1 = {"metric_a": 100, "metric_b": 50, "metric_c": 0}
        map2 = {"metric_a": 150, "metric_b": 50, "metric_c": 25}

        result = compare_numeric_maps(map1, map2, significance=0.1)

        # metric_a: 50% increase (significant)
        # metric_b: no change (excluded)
        # metric_c: infinite % increase (significant)
        assert "metric_a" in result["differences"]
        assert "metric_b" not in result["differences"]
        assert "metric_c" in result["differences"]

        assert result["differences"]["metric_a"]["before"] == 100
        assert result["differences"]["metric_a"]["after"] == 150
        assert result["differences"]["metric_a"]["absolute"] == 50
        assert result["differences"]["metric_a"]["percent"] == 50.0

    def test_significance_filtering(self):
        """Test significance threshold filtering."""
        map1 = {"small_change": 100, "big_change": 100}
        map2 = {"small_change": 105, "big_change": 200}  # 5% vs 100% change

        result = compare_numeric_maps(map1, map2, significance=0.1)  # 10% threshold

        # small_change: 5% (below threshold)
        # big_change: 100% (above threshold)
        assert "small_change" not in result["differences"]
        assert "big_change" in result["differences"]
        assert "small_change" in result["insignificant_keys"]
        assert "big_change" in result["significant_keys"]

    def test_exclude_keys(self):
        """Test excluding specific keys from comparison."""
        map1 = {"include_me": 100, "exclude_me": 50}
        map2 = {"include_me": 200, "exclude_me": 100}

        result = compare_numeric_maps(map1, map2, exclude=["exclude_me"])

        assert "include_me" in result["differences"]
        assert "exclude_me" not in result["differences"]
        assert "exclude_me" not in result["significant_keys"]
        assert "exclude_me" not in result["insignificant_keys"]

    def test_missing_keys(self):
        """Test handling of keys missing in one map."""
        map1 = {"only_in_1": 100, "common": 50}
        map2 = {"only_in_2": 200, "common": 75}

        result = compare_numeric_maps(map1, map2)

        # only_in_1: 100 -> 0 (treated as removed)
        # only_in_2: 0 -> 200 (treated as added)
        # common: 50 -> 75 (changed)
        assert "only_in_1" in result["differences"]
        assert "only_in_2" in result["differences"]
        assert "common" in result["differences"]

        assert result["differences"]["only_in_1"]["before"] == 100
        assert result["differences"]["only_in_1"]["after"] == 0
        assert result["differences"]["only_in_2"]["before"] == 0
        assert result["differences"]["only_in_2"]["after"] == 200

    @patch("spark_history_mcp.tools.metrics.get_config")
    def test_default_significance_from_config(self, mock_config):
        """Test using default significance threshold from config."""
        mock_config.return_value = SimpleNamespace(significance_threshold=0.2)

        map1 = {"metric": 100}
        map2 = {"metric": 115}  # 15% change

        result = compare_numeric_maps(map1, map2)  # No significance specified

        # With 20% threshold, 15% change should be insignificant
        assert "metric" not in result["differences"]
        assert result["significance_threshold"] == 0.2


class TestCompareDistributions:
    """Test the compare_distributions function."""

    def test_basic_distribution_comparison(self):
        """Test basic distribution comparison with median extraction."""
        # Mock distributions with nested metrics
        dist1 = SimpleNamespace(
            shuffle_read_metrics=SimpleNamespace(
                fetch_wait_time=[100, 200, 300, 400, 500]  # median = 300
            ),
            duration=[10, 20, 30, 40, 50],  # median = 30
        )

        dist2 = SimpleNamespace(
            shuffle_read_metrics=SimpleNamespace(
                fetch_wait_time=[200, 400, 600, 800, 1000]  # median = 600
            ),
            duration=[20, 40, 60, 80, 100],  # median = 60
        )

        fields = [
            ("shuffle_read_metrics.fetch_wait_time", "Shuffle Read Wait"),
            ("duration", "Task Duration"),
        ]

        result = compare_distributions(dist1, dist2, fields, significance=0.1)

        assert "Shuffle Read Wait" in result["metrics"]
        assert "Task Duration" in result["metrics"]

        shuffle_metric = result["metrics"]["Shuffle Read Wait"]
        assert shuffle_metric["before"] == 300
        assert shuffle_metric["after"] == 600
        assert shuffle_metric["percent"] == 100.0  # 100% increase
        assert shuffle_metric["significant"] is True

        duration_metric = result["metrics"]["Task Duration"]
        assert duration_metric["before"] == 30
        assert duration_metric["after"] == 60
        assert duration_metric["percent"] == 100.0
        assert duration_metric["significant"] is True

    def test_missing_metrics_handling(self):
        """Test handling of missing or invalid metrics."""
        dist1 = SimpleNamespace(
            valid_metric=[10, 20, 30, 40, 50],
            short_list=[10, 20],  # Too short for median
            missing_attr=None,
        )

        dist2 = SimpleNamespace(
            valid_metric=[20, 40, 60, 80, 100]
            # missing_attr and short_list entirely absent
        )

        fields = [
            ("valid_metric", "Valid Metric"),
            ("short_list", "Short List"),
            ("missing_attr", "Missing Attr"),
            ("nonexistent.path", "Non-existent"),
        ]

        result = compare_distributions(dist1, dist2, fields)

        # Only valid_metric should be present
        assert "Valid Metric" in result["metrics"]
        assert "Short List" not in result["metrics"]
        assert "Missing Attr" not in result["metrics"]
        assert "Non-existent" not in result["metrics"]

    def test_significance_determination(self):
        """Test significance determination in distribution comparison."""
        dist1 = SimpleNamespace(metric=[90, 95, 100, 105, 110])  # median = 100
        dist2 = SimpleNamespace(metric=[99, 102, 105, 108, 111])  # median = 105

        fields = [("metric", "Test Metric")]

        # 5% change with 10% threshold should be insignificant
        result = compare_distributions(dist1, dist2, fields, significance=0.1)

        metric = result["metrics"]["Test Metric"]
        assert metric["percent"] == 5.0
        assert metric["significant"] is False

        # Same change with 2% threshold should be significant
        result = compare_distributions(dist1, dist2, fields, significance=0.02)

        metric = result["metrics"]["Test Metric"]
        assert metric["significant"] is True

    @patch("spark_history_mcp.tools.metrics.get_config")
    def test_default_significance_from_config(self, mock_config):
        """Test using default significance threshold from config."""
        mock_config.return_value = SimpleNamespace(significance_threshold=0.15)

        dist1 = SimpleNamespace(metric=[100, 110, 120, 130, 140])  # median = 120
        dist2 = SimpleNamespace(metric=[110, 125, 140, 155, 170])  # median = 140

        fields = [("metric", "Test Metric")]
        result = compare_distributions(
            dist1, dist2, fields
        )  # No significance specified

        # ~16.7% change with 15% threshold should be significant
        metric = result["metrics"]["Test Metric"]
        assert metric["significant"] is True
        assert result["significance_threshold"] == 0.15
