import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from spark_history_mcp.models.spark_types import StageStatus


class Dist:
    def __init__(self, fetch_wait_ns: int, write_time_ns: int):
        self.shuffle_read_metrics = SimpleNamespace(
            fetch_wait_time=[0, 0, fetch_wait_ns]
        )
        self.shuffle_write_metrics = SimpleNamespace(write_time=[0, 0, write_time_ns])


def make_attempt(duration_ms: int, start: datetime):
    return SimpleNamespace(
        duration=duration_ms,
        start_time=start,
        end_time=start + timedelta(milliseconds=duration_ms),
    )


def make_stage():
    st = SimpleNamespace(
        executor_run_time=30000,
        executor_cpu_time=120_000_000_000,  # 120s
        jvm_gc_time=5000,
        input_bytes=1_073_741_824,  # 1 GiB
        output_bytes=0,
        shuffle_read_bytes=0,
        shuffle_write_bytes=0,
        memory_bytes_spilled=0,
        disk_bytes_spilled=0,
        num_failed_tasks=1,
        num_tasks=10,
        status=StageStatus.COMPLETE,
        task_metrics_distributions=Dist(1_000_000_000, 2_000_000_000),
    )
    return st


def make_executor(start: datetime, end: datetime):
    return SimpleNamespace(
        add_time=start,
        remove_time=end,
        total_cores=2,
        max_memory=1_073_741_824,  # 1 GiB
    )


@patch("spark_history_mcp.tools.application.fetch_executors")
@patch("spark_history_mcp.tools.application.fetch_stages")
@patch("spark_history_mcp.tools.application.fetch_app")
def test_get_app_summary_golden(
    mock_fetch_app, mock_fetch_stages, mock_fetch_executors
):
    from spark_history_mcp.tools.tools import get_app_summary

    now = datetime.now(timezone.utc)
    app = SimpleNamespace(
        name="TestApp", attempts=[make_attempt(60000, now)], cores_per_executor=2
    )
    stages = [make_stage()]
    executors = [
        make_executor(now, now + timedelta(seconds=60)),
        make_executor(now, now + timedelta(seconds=60)),
    ]

    mock_fetch_app.return_value = app
    mock_fetch_stages.return_value = stages
    mock_fetch_executors.return_value = executors

    out = get_app_summary("app-1")

    # Drop volatile fields
    out.pop("analysis_timestamp", None)

    with open("tests/fixtures/app_summary_simple.json") as f:
        expected = json.load(f)

    assert out == expected


@patch("spark_history_mcp.tools.tools.get_app_summary")
@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_compare_app_summaries_golden(mock_ctx, mock_get_app_summary):
    from spark_history_mcp.tools.tools import compare_app_summaries

    # app1 summary (subset of fields)
    app1 = {
        "application_id": "app-1",
        "application_duration_minutes": 1.0,
        "executor_utilization_percent": 12.5,
        "application_name": "ignored",
        "analysis_timestamp": "ignored",
    }

    # app2 summary with significant changes
    app2 = {
        "application_id": "app-2",
        "application_duration_minutes": 1.5,  # +50%
        "executor_utilization_percent": 25.0,  # +100%
        "application_name": "ignored",
        "analysis_timestamp": "ignored",
    }

    def side_effect(app_id, server=None):
        return app1 if app_id == "app-1" else app2

    mock_get_app_summary.side_effect = side_effect

    out = compare_app_summaries("app-1", "app-2", significance_threshold=0.1)

    with open("tests/fixtures/app_summary_compare_simple.json") as f:
        expected = json.load(f)

    assert out == expected
