"""
Shared fixtures and utilities for CLI tests.

This module provides common test fixtures, mock objects, and utilities
used across all CLI test modules.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

try:
    from click.testing import CliRunner

    CLI_AVAILABLE = True
except ImportError:
    CLI_AVAILABLE = False

from spark_history_mcp.models.spark_types import (
    ApplicationAttemptInfo,
    ApplicationInfo,
    ExecutorSummary,
    JobData,
    StageData,
)


@pytest.fixture
def cli_runner():
    """Click CLI test runner."""
    if not CLI_AVAILABLE:
        pytest.skip("CLI dependencies not available")
    return CliRunner()


@pytest.fixture
def mock_config_file():
    """Create a temporary config file for testing."""
    config_content = {
        "mcp": {
            "transports": ["stdio"],
            "address": "localhost",
            "port": "18888",
            "debug": False,
        },
        "servers": {
            "local": {
                "url": "http://localhost:18080",
                "default": True,
                "verify_ssl": True,
            },
            "production": {
                "url": "http://prod-spark:18080",
                "default": False,
                "verify_ssl": True,
            },
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        import yaml

        yaml.dump(config_content, f)
        yield Path(f.name)

    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def mock_spark_client():
    """Mock Spark REST client with common responses."""
    client = MagicMock()

    # Mock basic methods
    client.get_application.return_value = create_mock_application("app-123", "Test App")
    client.list_jobs.return_value = [create_mock_job(1, "Test Job")]
    client.list_stages.return_value = [create_mock_stage(1, "Test Stage")]
    client.list_applications.return_value = [
        create_mock_application("app-123", "Test App 1"),
        create_mock_application("app-456", "Test App 2"),
    ]

    return client


@pytest.fixture
def mock_tools_module():
    """Mock the tools module functions."""
    mock_tools = MagicMock()

    # Mock tool functions with typical return values
    mock_tools.compare_app_performance.return_value = {
        "applications": {"app1": {"id": "app-123"}, "app2": {"id": "app-456"}},
        "performance_comparison": {"stages": {"top_stage_differences": []}},
        "key_recommendations": [],
    }

    mock_tools.get_application_insights.return_value = {
        "application": {"id": "app-123", "name": "Test App"},
        "insights": {"bottlenecks": [], "recommendations": []},
    }

    mock_tools.compare_stages.return_value = {
        "summary": {"significance_threshold": 0.1},
        "stage_comparison": {},
        "recommendations": [],
    }

    return mock_tools


@pytest.fixture
def mock_comparison_context():
    """Mock comparison context file operations."""
    context_data = {"app_id1": "app-123", "app_id2": "app-456", "server": "local"}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(context_data, f)
        yield Path(f.name), context_data

    # Cleanup
    Path(f.name).unlink(missing_ok=True)


def create_mock_application(app_id: str, name: str) -> ApplicationInfo:
    """Create a mock ApplicationInfo object."""
    attempt = ApplicationAttemptInfo(
        attempt_id="1",
        start_time="2023-01-01T10:00:00.000Z",
        end_time="2023-01-01T11:00:00.000Z",
        last_updated="2023-01-01T11:00:00.000Z",
        duration=3600000,
        spark_user="test_user",
        completed=True,
    )

    return ApplicationInfo(id=app_id, name=name, attempts=[attempt])


def create_mock_job(job_id: int, name: str) -> JobData:
    """Create a mock JobData object."""
    return JobData(
        job_id=job_id,
        name=name,
        description=f"Test job {job_id}",
        status="SUCCEEDED",
        num_tasks=10,
        num_active_tasks=0,
        num_completed_tasks=10,
        num_skipped_tasks=0,
        num_failed_tasks=0,
        num_killed_tasks=0,
        num_active_stages=0,
        num_completed_stages=1,
        num_skipped_stages=0,
        num_failed_stages=0,
    )


def create_mock_stage(stage_id: int, name: str) -> StageData:
    """Create a mock StageData object."""
    return StageData(
        status="COMPLETE",
        stage_id=stage_id,
        attempt_id=0,
        num_tasks=5,
        num_active_tasks=0,
        num_complete_tasks=5,
        num_failed_tasks=0,
        num_killed_tasks=0,
        num_completed_indices=5,
        executor_run_time=180000,  # 3 minutes
        executor_cpu_time=150000,
        submission_time="2023-01-01T10:00:00.000Z",
        first_task_launched_time="2023-01-01T10:00:30.000Z",
        completion_time="2023-01-01T10:03:00.000Z",
        input_bytes=1000000,
        input_records=1000,
        output_bytes=500000,
        output_records=500,
        shuffle_read_bytes=0,
        shuffle_read_records=0,
        shuffle_write_bytes=0,
        shuffle_write_records=0,
        memory_bytes_spilled=0,
        disk_bytes_spilled=0,
        name=name,
        description=f"Test stage {stage_id}",
        scheduling_pool="default",
        rdd_ids=[],
        accumulables=[],
    )


def create_mock_executor(executor_id: str) -> ExecutorSummary:
    """Create a mock ExecutorSummary object."""
    return ExecutorSummary(
        id=executor_id,
        host_port="localhost:12345",
        is_active=True,
        rdd_blocks=0,
        memory_used=1000000,
        disk_used=0,
        total_cores=4,
        max_tasks=4,
        active_tasks=0,
        failed_tasks=0,
        completed_tasks=10,
        total_tasks=10,
        total_duration=180000,
        total_gc_time=5000,
        total_input_bytes=1000000,
        total_shuffle_read=0,
        total_shuffle_write=0,
        is_blacklisted=False,
        max_memory=2000000000,
        add_time="2023-01-01T10:00:00.000Z",
        executor_logs={},
    )


@pytest.fixture
def mock_output_formatter():
    """Mock output formatter for testing display logic."""
    formatter = MagicMock()
    formatter.format_type = "human"
    formatter.output.return_value = None  # Formatters typically don't return values
    return formatter


class CLITestCase:
    """Base class for CLI test cases with common utilities."""

    def assert_cli_success(self, result, expected_exit_code=0):
        """Assert that CLI command succeeded."""
        assert result.exit_code == expected_exit_code, (
            f"CLI Output: {result.output}\nException: {result.exception}"
        )

    def assert_cli_error(self, result, expected_message: str = None):
        """Assert that CLI command failed with expected message."""
        assert result.exit_code != 0
        if expected_message:
            assert expected_message in result.output

    def assert_output_contains(self, result, expected_text: str):
        """Assert that CLI output contains expected text."""
        assert expected_text in result.output

    def assert_json_output(self, result, expected_keys: list = None):
        """Assert that CLI output is valid JSON with expected keys."""
        try:
            output_data = json.loads(result.output)
            if expected_keys:
                for key in expected_keys:
                    assert key in output_data
            return output_data
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON: {result.output}")


@pytest.fixture
def cli_test_base():
    """Fixture providing CLI test utilities."""
    return CLITestCase()
