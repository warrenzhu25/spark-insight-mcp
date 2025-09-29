import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.models.spark_types import (
    ApplicationAttemptInfo,
    ApplicationEnvironmentInfo,
    ApplicationInfo,
    ExecutionData,
    ExecutorSummary,
    JobData,
    StageData,
    TaskMetricDistributions,
)
from spark_history_mcp.tools import (
    analyze_auto_scaling,
    analyze_failed_tasks,
    analyze_shuffle_skew,
    compare_stages,
    get_application,
)
from spark_history_mcp.tools.analysis import get_client_or_default
from spark_history_mcp.tools.application import list_applications
from spark_history_mcp.tools.jobs_stages import (
    get_stage,
    get_stage_task_summary,
    list_jobs,
    list_slowest_jobs,
    list_slowest_sql_queries,
    list_slowest_stages,
    list_stages,
)

# Additional imports for comparison and executor tools
try:
    from spark_history_mcp.tools_original import (
        _analyze_executor_performance_patterns,
    )
except ImportError:
    # Fallback if original tools are not available
    _analyze_executor_performance_patterns = None

# Import compare_stages from the refactored location
try:
    from spark_history_mcp.tools import compare_stages
except ImportError:
    compare_stages = None

# Import timeline comparison functions from the refactored location
try:
    from spark_history_mcp.tools import compare_stage_executor_timeline
except ImportError:
    compare_stage_executor_timeline = None

try:
    from spark_history_mcp.tools import compare_app_executor_timeline
except ImportError:
    compare_app_executor_timeline = None


class TestTools(unittest.TestCase):
    def setUp(self):
        # Create mock context
        self.mock_ctx = MagicMock()
        self.mock_lifespan_context = MagicMock()
        self.mock_ctx.request_context.lifespan_context = self.mock_lifespan_context

        # Create mock clients
        self.mock_client1 = MagicMock(spec=SparkRestClient)
        self.mock_client2 = MagicMock(spec=SparkRestClient)

        # Set up clients dictionary
        self.mock_lifespan_context.clients = {
            "server1": self.mock_client1,
            "server2": self.mock_client2,
        }

    def test_get_client_with_name(self):
        """Test getting a client by name"""
        self.mock_lifespan_context.default_client = self.mock_client1

        # Get client by name
        client = get_client_or_default(self.mock_ctx, "server2")

        # Should return the requested client
        self.assertEqual(client, self.mock_client2)

    def test_get_default_client(self):
        """Test getting the default client when no name is provided"""
        self.mock_lifespan_context.default_client = self.mock_client1

        # Get client without specifying name
        client = get_client_or_default(self.mock_ctx)

        # Should return the default client
        self.assertEqual(client, self.mock_client1)

    def test_get_client_not_found_with_default(self):
        """Test behavior when requested client is not found but default exists"""
        self.mock_lifespan_context.default_client = self.mock_client1

        # Get non-existent client
        client = get_client_or_default(self.mock_ctx, "non_existent_server")

        # Should fall back to default client
        self.assertEqual(client, self.mock_client1)

    def test_no_client_found(self):
        """Test error when no client is found and no default exists"""
        self.mock_lifespan_context.default_client = None

        # Try to get non-existent client with no default
        with self.assertRaises(ValueError) as context:
            get_client_or_default(self.mock_ctx, "non_existent_server")

        self.assertIn("No Spark client found", str(context.exception))

    def test_no_default_client(self):
        """Test error when no name is provided and no default exists"""
        self.mock_lifespan_context.default_client = None

        # Try to get default client when none exists
        with self.assertRaises(ValueError) as context:
            get_client_or_default(self.mock_ctx)

        self.assertIn("No Spark client found", str(context.exception))

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_slowest_jobs_empty(self, mock_get_client):
        """Test list_slowest_jobs when no jobs are found"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_jobs("app-123", n=3)

        # Verify results
        self.assertEqual(result, [])
        mock_client.list_jobs.assert_called_once_with(app_id="app-123", status=None)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_slowest_jobs_exclude_running(self, mock_get_client):
        """Test list_slowest_jobs excluding running jobs"""
        # Setup mock client and jobs
        mock_client = MagicMock()

        # Create mock jobs with different durations and statuses
        job1 = MagicMock(spec=JobData)
        job1.status = "RUNNING"
        job1.submission_time = datetime.now() - timedelta(minutes=10)
        job1.completion_time = None

        job2 = MagicMock(spec=JobData)
        job2.status = "SUCCEEDED"
        job2.submission_time = datetime.now() - timedelta(minutes=5)
        job2.completion_time = datetime.now() - timedelta(minutes=3)  # 2 min duration

        job3 = MagicMock(spec=JobData)
        job3.status = "SUCCEEDED"
        job3.submission_time = datetime.now() - timedelta(minutes=10)
        job3.completion_time = datetime.now() - timedelta(minutes=5)  # 5 min duration

        job4 = MagicMock(spec=JobData)
        job4.status = "FAILED"
        job4.submission_time = datetime.now() - timedelta(minutes=8)
        job4.completion_time = datetime.now() - timedelta(minutes=7)  # 1 min duration

        mock_client.list_jobs.return_value = [job1, job2, job3, job4]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=False (default)
        result = list_slowest_jobs("app-123", n=2)

        # Verify results - should return job3 and job2 (in that order)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], job3)  # Longest duration (5 min)
        self.assertEqual(result[1], job2)  # Second longest (2 min)

        # Running job (job1) should be excluded
        self.assertNotIn(job1, result)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_slowest_jobs_include_running(self, mock_get_client):
        """Test list_slowest_jobs including running jobs"""
        # Setup mock client and jobs
        mock_client = MagicMock()

        # Create mock jobs with different durations and statuses
        job1 = MagicMock(spec=JobData)
        job1.status = "RUNNING"
        job1.submission_time = datetime.now() - timedelta(
            minutes=20
        )  # Running for 20 min
        job1.completion_time = None

        job2 = MagicMock(spec=JobData)
        job2.status = "SUCCEEDED"
        job2.submission_time = datetime.now() - timedelta(minutes=5)
        job2.completion_time = datetime.now() - timedelta(minutes=3)  # 2 min duration

        job3 = MagicMock(spec=JobData)
        job3.status = "SUCCEEDED"
        job3.submission_time = datetime.now() - timedelta(minutes=10)
        job3.completion_time = datetime.now() - timedelta(minutes=5)  # 5 min duration

        mock_client.list_jobs.return_value = [job1, job2, job3]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=True
        result = list_slowest_jobs("app-123", include_running=True, n=2)

        # Verify results - should include the running job
        self.assertEqual(len(result), 2)
        # Running job should be included but will have duration 0 since completion_time is None
        # So job3 and job2 should be returned
        self.assertEqual(result[0], job3)
        self.assertEqual(result[1], job2)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_slowest_jobs_limit_results(self, mock_get_client):
        """Test list_slowest_jobs limits results to n"""
        # Setup mock client and jobs
        mock_client = MagicMock()

        # Create 5 mock jobs with different durations
        jobs = []
        for i in range(5):
            job = MagicMock(spec=JobData)
            job.status = "SUCCEEDED"
            job.submission_time = datetime.now() - timedelta(minutes=10)
            # Different completion times to create different durations
            job.completion_time = datetime.now() - timedelta(minutes=10 - i)
            jobs.append(job)

        mock_client.list_jobs.return_value = jobs
        mock_get_client.return_value = mock_client

        # Call the function with n=3
        result = list_slowest_jobs("app-123", n=3)

        # Verify results - should return only 3 jobs
        self.assertEqual(len(result), 3)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stage_with_attempt_id(self, mock_get_client):
        """Test get_stage with a specific attempt ID"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        mock_stage.task_metrics_distributions = None
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        mock_client.get_stage_attempt.return_value = mock_stage
        mock_get_client.return_value = mock_client

        # Call the function with attempt_id
        result = get_stage("app-123", stage_id=1, attempt_id=0)

        # Verify results
        self.assertEqual(result, mock_stage)
        mock_client.get_stage_attempt.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            attempt_id=0,
            details=False,
            with_summaries=False,
        )

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stage_without_attempt_id_single_stage(self, mock_get_client):
        """Test get_stage without attempt ID when a single stage is returned"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        mock_stage.task_metrics_distributions = None
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        mock_client.list_stage_attempts.return_value = mock_stage
        mock_get_client.return_value = mock_client

        # Call the function without attempt_id
        result = get_stage("app-123", stage_id=1)

        # Verify results
        self.assertEqual(result, mock_stage)
        mock_client.list_stage_attempts.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            details=False,
            with_summaries=False,
        )

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stage_without_attempt_id_multiple_stages(self, mock_get_client):
        """Test get_stage without attempt ID when multiple stages are returned"""
        # Setup mock client
        mock_client = MagicMock()

        # Create mock stages with different attempt IDs
        mock_stage1 = MagicMock(spec=StageData)
        mock_stage1.attempt_id = 0
        mock_stage1.task_metrics_distributions = None

        mock_stage2 = MagicMock(spec=StageData)
        mock_stage2.attempt_id = 1
        mock_stage2.task_metrics_distributions = None

        mock_client.list_stage_attempts.return_value = [mock_stage1, mock_stage2]
        mock_get_client.return_value = mock_client

        # Call the function without attempt_id
        result = get_stage("app-123", stage_id=1)

        # Verify results - should return the stage with highest attempt_id
        self.assertEqual(result, mock_stage2)
        mock_client.list_stage_attempts.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            details=False,
            with_summaries=False,
        )

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stage_with_summaries_missing_metrics(self, mock_get_client):
        """Test get_stage with summaries when metrics distributions are missing"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        # Set task_metrics_distributions to None to trigger the fetch
        mock_stage.task_metrics_distributions = None

        mock_summary = MagicMock(spec=TaskMetricDistributions)

        mock_client.get_stage_attempt.return_value = mock_stage
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        # Call the function with with_summaries=True
        result = get_stage("app-123", stage_id=1, attempt_id=0, with_summaries=True)

        # Verify results
        self.assertEqual(result, mock_stage)
        self.assertEqual(result.task_metrics_distributions, mock_summary)

        mock_client.get_stage_attempt.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            attempt_id=0,
            details=False,
            with_summaries=True,
        )

        mock_client.get_stage_task_summary.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            attempt_id=0,
        )

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stage_no_stages_found(self, mock_get_client):
        """Test get_stage when no stages are found"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_stage_attempts.return_value = []
        mock_get_client.return_value = mock_client

        with self.assertRaises(ValueError) as context:
            get_stage("app-123", stage_id=1)

        self.assertIn("No stage found with ID 1", str(context.exception))

    # Tests for get_application tool
    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_application_success(self, mock_get_client):
        """Test successful application retrieval"""
        # Setup mock client
        mock_client = MagicMock()
        mock_app = MagicMock(spec=ApplicationInfo)
        mock_app.id = "spark-app-123"
        mock_app.name = "Test Application"
        mock_client.get_application.return_value = mock_app
        mock_get_client.return_value = mock_client

        # Call the function
        result = get_application("spark-app-123")

        # Verify results
        self.assertEqual(result, mock_app)
        mock_client.get_application.assert_called_once_with("spark-app-123")
        mock_get_client.assert_called_once_with(unittest.mock.ANY, None)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_application_with_server(self, mock_get_client):
        """Test application retrieval with specific server"""
        # Setup mock client
        mock_client = MagicMock()
        mock_app = MagicMock(spec=ApplicationInfo)
        mock_client.get_application.return_value = mock_app
        mock_get_client.return_value = mock_client

        # Call the function with server
        get_application("spark-app-123", server="production")

        # Verify server parameter is passed
        mock_get_client.assert_called_once_with(unittest.mock.ANY, "production")

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_application_not_found(self, mock_get_client):
        """Test application retrieval when app doesn't exist"""
        # Setup mock client to raise exception
        mock_client = MagicMock()
        mock_client.get_application.side_effect = Exception("Application not found")
        mock_get_client.return_value = mock_client

        # Verify exception is propagated
        with self.assertRaises(Exception) as context:
            get_application("non-existent-app")

        self.assertIn("Application not found", str(context.exception))

    # Tests for list_jobs tool
    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_jobs_no_filter(self, mock_get_client):
        """Test job retrieval without status filter"""
        # Setup mock client
        mock_client = MagicMock()
        mock_jobs = [MagicMock(spec=JobData), MagicMock(spec=JobData)]
        mock_client.list_jobs.return_value = mock_jobs
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_jobs("spark-app-123")

        # Verify results
        self.assertEqual(result, mock_jobs)
        mock_client.list_jobs.assert_called_once_with(
            app_id="spark-app-123", status=None
        )

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_jobs_with_status_filter(self, mock_get_client):
        """Test job retrieval with status filter"""
        # Setup mock client
        mock_client = MagicMock()
        mock_jobs = [MagicMock(spec=JobData)]
        mock_jobs[0].status = "SUCCEEDED"
        mock_client.list_jobs.return_value = mock_jobs
        mock_get_client.return_value = mock_client

        # Call the function with status filter
        result = list_jobs("spark-app-123", status=["SUCCEEDED"])

        # Verify results
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "SUCCEEDED")

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_jobs_empty_result(self, mock_get_client):
        """Test job retrieval with empty result"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_jobs("spark-app-123")

        # Verify results
        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_jobs_status_filtering(self, mock_get_client):
        """Test job status filtering logic"""
        # Setup mock client
        mock_client = MagicMock()

        # Create jobs with different statuses
        job1 = MagicMock(spec=JobData)
        job1.status = "RUNNING"
        job2 = MagicMock(spec=JobData)
        job2.status = "SUCCEEDED"
        job3 = MagicMock(spec=JobData)
        job3.status = "FAILED"

        # Mock client to return only SUCCEEDED job when filtered
        mock_client.list_jobs.return_value = [job2]  # Only return SUCCEEDED job
        mock_get_client.return_value = mock_client

        # Test filtering for SUCCEEDED jobs
        result = list_jobs("spark-app-123", status=["SUCCEEDED"])

        # Should only return SUCCEEDED job
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "SUCCEEDED")

    # Tests for list_stages tool
    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stages_no_filter(self, mock_get_client):
        """Test stage retrieval without filters"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stages = [MagicMock(spec=StageData), MagicMock(spec=StageData)]
        mock_client.list_stages.return_value = mock_stages
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_stages("spark-app-123")

        # Verify results
        self.assertEqual(result, mock_stages)
        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123", status=None, with_summaries=False
        )

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stages_with_status_filter(self, mock_get_client):
        """Test stage retrieval with status filter"""
        # Setup mock client
        mock_client = MagicMock()

        # Create stages with different statuses
        stage1 = MagicMock(spec=StageData)
        stage1.status = "COMPLETE"
        stage2 = MagicMock(spec=StageData)
        stage2.status = "ACTIVE"
        stage3 = MagicMock(spec=StageData)
        stage3.status = "FAILED"

        # Mock client to return only COMPLETE stage when filtered
        mock_client.list_stages.return_value = [stage1]  # Only return COMPLETE stage
        mock_get_client.return_value = mock_client

        # Call with status filter
        result = list_stages("spark-app-123", status=["COMPLETE"])

        # Should only return COMPLETE stage
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "COMPLETE")

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stages_with_summaries(self, mock_get_client):
        """Test stage retrieval with summaries enabled"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stages = [MagicMock(spec=StageData)]
        mock_client.list_stages.return_value = mock_stages
        mock_get_client.return_value = mock_client

        # Call with summaries enabled
        list_stages("spark-app-123", with_summaries=True)

        # Verify summaries parameter is passed
        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123", status=None, with_summaries=True
        )

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stages_empty_result(self, mock_get_client):
        """Test stage retrieval with empty result"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_stages.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_stages("spark-app-123")

        # Verify results
        self.assertEqual(result, [])

    # Tests for get_stage_task_summary tool
    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stage_task_summary_success(self, mock_get_client):
        """Test successful stage task summary retrieval"""
        # Setup mock client
        mock_client = MagicMock()
        mock_summary = MagicMock(spec=TaskMetricDistributions)
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        # Call the function
        result = get_stage_task_summary("spark-app-123", 1, 0)

        # Verify results
        self.assertEqual(result, mock_summary)
        mock_client.get_stage_task_summary.assert_called_once_with(
            app_id="spark-app-123",
            stage_id=1,
            attempt_id=0,
            quantiles="0.05,0.25,0.5,0.75,0.95",
        )

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stage_task_summary_with_quantiles(self, mock_get_client):
        """Test stage task summary with custom quantiles"""
        # Setup mock client
        mock_client = MagicMock()
        mock_summary = MagicMock(spec=TaskMetricDistributions)
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        # Call with custom quantiles
        get_stage_task_summary("spark-app-123", 1, 0, quantiles="0.25,0.5,0.75")

        # Verify quantiles parameter is passed
        mock_client.get_stage_task_summary.assert_called_once_with(
            app_id="spark-app-123", stage_id=1, attempt_id=0, quantiles="0.25,0.5,0.75"
        )

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_stage_task_summary_not_found(self, mock_get_client):
        """Test stage task summary when stage doesn't exist"""
        # Setup mock client to raise exception
        mock_client = MagicMock()
        mock_client.get_stage_task_summary.side_effect = Exception("Stage not found")
        mock_get_client.return_value = mock_client

        # Verify exception is propagated
        with self.assertRaises(Exception) as context:
            get_stage_task_summary("spark-app-123", 999, 0)

        self.assertIn("Stage not found", str(context.exception))

    # Tests for list_slowest_stages tool
    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_slowest_stages_execution_time_vs_total_time(self, mock_get_client):
        """Test that list_slowest_stages prioritizes execution time over total stage duration"""
        mock_client = MagicMock()

        # Create Stage A: Longer total duration but shorter execution time
        stage_a = MagicMock(spec=StageData)
        stage_a.stage_id = 1
        stage_a.attempt_id = 0
        stage_a.name = "Stage A"
        stage_a.status = "COMPLETE"
        # Total duration: 10 minutes (submission to completion)
        stage_a.submission_time = datetime.now() - timedelta(minutes=10)
        stage_a.first_task_launched_time = datetime.now() - timedelta(
            minutes=5
        )  # 5 min delay
        stage_a.completion_time = datetime.now()
        # Execution time: 5 minutes (first_task_launched to completion)

        # Create Stage B: Shorter total duration but longer execution time
        stage_b = MagicMock(spec=StageData)
        stage_b.stage_id = 2
        stage_b.attempt_id = 0
        stage_b.name = "Stage B"
        stage_b.status = "COMPLETE"
        # Total duration: 8 minutes (submission to completion)
        stage_b.submission_time = datetime.now() - timedelta(minutes=8)
        stage_b.first_task_launched_time = datetime.now() - timedelta(
            minutes=7
        )  # 1 min delay
        stage_b.completion_time = datetime.now()
        # Execution time: 7 minutes (first_task_launched to completion)

        mock_client.list_stages.return_value = [stage_a, stage_b]
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_stages("app-123", n=2)

        # Verify results - Stage B should be first (longer execution time: 7 min vs 5 min)
        # even though Stage A has longer total duration (10 min vs 8 min)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], stage_b)  # Stage B first (7 min execution)
        self.assertEqual(result[1], stage_a)  # Stage A second (5 min execution)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_slowest_stages_exclude_running(self, mock_get_client):
        """Test that list_slowest_stages excludes running stages by default"""
        mock_client = MagicMock()

        # Create running stage with long execution time
        running_stage = MagicMock(spec=StageData)
        running_stage.stage_id = 1
        running_stage.attempt_id = 0
        running_stage.name = "Running Stage"
        running_stage.status = "RUNNING"
        running_stage.submission_time = datetime.now() - timedelta(minutes=20)
        running_stage.first_task_launched_time = datetime.now() - timedelta(minutes=15)
        running_stage.completion_time = None  # Still running

        # Create completed stage with shorter execution time
        completed_stage = MagicMock(spec=StageData)
        completed_stage.stage_id = 2
        completed_stage.attempt_id = 0
        completed_stage.name = "Completed Stage"
        completed_stage.status = "COMPLETE"
        completed_stage.submission_time = datetime.now() - timedelta(minutes=5)
        completed_stage.first_task_launched_time = datetime.now() - timedelta(minutes=4)
        completed_stage.completion_time = datetime.now()

        mock_client.list_stages.return_value = [running_stage, completed_stage]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=False (default)
        result = list_slowest_stages("app-123", include_running=False, n=2)

        # Should only return the completed stage
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], completed_stage)
        self.assertNotIn(running_stage, result)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_slowest_stages_include_running(self, mock_get_client):
        """Test that list_slowest_stages includes running stages when requested"""
        mock_client = MagicMock()

        # Create running stage
        running_stage = MagicMock(spec=StageData)
        running_stage.stage_id = 1
        running_stage.attempt_id = 0
        running_stage.name = "Running Stage"
        running_stage.status = "RUNNING"
        running_stage.submission_time = datetime.now() - timedelta(minutes=10)
        running_stage.first_task_launched_time = datetime.now() - timedelta(minutes=8)
        running_stage.completion_time = None

        # Create completed stage
        completed_stage = MagicMock(spec=StageData)
        completed_stage.stage_id = 2
        completed_stage.attempt_id = 0
        completed_stage.name = "Completed Stage"
        completed_stage.status = "COMPLETE"
        completed_stage.submission_time = datetime.now() - timedelta(minutes=5)
        completed_stage.first_task_launched_time = datetime.now() - timedelta(minutes=4)
        completed_stage.completion_time = datetime.now()

        mock_client.list_stages.return_value = [running_stage, completed_stage]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=True
        result = list_slowest_stages("app-123", include_running=True, n=2)

        # Should include both stages, but running stage will have duration 0
        # so completed stage should be first
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], completed_stage)  # Has actual duration
        self.assertEqual(result[1], running_stage)  # Duration 0

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_slowest_stages_missing_timestamps(self, mock_get_client):
        """Test list_slowest_stages handles stages with missing timestamps"""
        # Setup mock client
        mock_client = MagicMock()

        # Create stage with missing first_task_launched_time
        stage_missing_launch = MagicMock(spec=StageData)
        stage_missing_launch.stage_id = 1
        stage_missing_launch.attempt_id = 0
        stage_missing_launch.name = "Stage Missing Launch Time"
        stage_missing_launch.status = "COMPLETE"
        stage_missing_launch.submission_time = datetime.now() - timedelta(minutes=10)
        stage_missing_launch.first_task_launched_time = None
        stage_missing_launch.completion_time = datetime.now()

        # Create stage with missing completion_time
        stage_missing_completion = MagicMock(spec=StageData)
        stage_missing_completion.stage_id = 2
        stage_missing_completion.attempt_id = 0
        stage_missing_completion.name = "Stage Missing Completion Time"
        stage_missing_completion.status = "COMPLETE"
        stage_missing_completion.submission_time = datetime.now() - timedelta(minutes=5)
        stage_missing_completion.first_task_launched_time = datetime.now() - timedelta(
            minutes=4
        )
        stage_missing_completion.completion_time = None

        # Create valid stage
        valid_stage = MagicMock(spec=StageData)
        valid_stage.stage_id = 3
        valid_stage.attempt_id = 0
        valid_stage.name = "Valid Stage"
        valid_stage.status = "COMPLETE"
        valid_stage.submission_time = datetime.now() - timedelta(minutes=3)
        valid_stage.first_task_launched_time = datetime.now() - timedelta(minutes=2)
        valid_stage.completion_time = datetime.now()

        mock_client.list_stages.return_value = [
            stage_missing_launch,
            stage_missing_completion,
            valid_stage,
        ]
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_stages("app-123", n=3)

        # Should return valid stage first, others should have duration 0
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], valid_stage)  # Only one with valid duration

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_slowest_stages_empty_result(self, mock_get_client):
        """Test list_slowest_stages with no stages"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_stages.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_stages("app-123", n=5)

        # Should return empty list
        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_slowest_stages_limit_results(self, mock_get_client):
        """Test list_slowest_stages limits results to n"""
        # Setup mock client
        mock_client = MagicMock()

        # Create 5 stages with different execution times
        stages = []
        for i in range(5):
            stage = MagicMock(spec=StageData)
            stage.stage_id = i
            stage.attempt_id = 0
            stage.name = f"Stage {i}"
            stage.status = "COMPLETE"
            stage.submission_time = datetime.now() - timedelta(minutes=10)
            # Different execution times: 1, 2, 3, 4, 5 minutes
            stage.first_task_launched_time = datetime.now() - timedelta(minutes=i + 1)
            stage.completion_time = datetime.now()
            stages.append(stage)

        mock_client.list_stages.return_value = stages
        mock_get_client.return_value = mock_client

        # Call the function with n=3
        result = list_slowest_stages("app-123", n=3)

        # Should return only 3 stages (the ones with longest execution times)
        self.assertEqual(len(result), 3)
        # Should be sorted by execution time descending (5, 4, 3 minutes)
        self.assertEqual(result[0].stage_id, 4)  # 5 minutes
        self.assertEqual(result[1].stage_id, 3)  # 4 minutes
        self.assertEqual(result[2].stage_id, 2)  # 3 minutes

    # Tests for list_slowest_sql_queries tool
    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_slowest_sql_queries_success(self, mock_get_client):
        """Test successful SQL query retrieval and sorting"""
        # Setup mock client
        mock_client = MagicMock()

        # Create mock SQL executions with different durations
        sql1 = MagicMock(spec=ExecutionData)
        sql1.id = 1
        sql1.duration = 5000  # 5 seconds
        sql1.status = "COMPLETED"
        sql1.success_job_ids = [1, 2]
        sql1.failed_job_ids = []
        sql1.running_job_ids = []
        sql1.description = "Query 1"
        sql1.submission_time = datetime.now()
        sql1.plan_description = "Sample plan description"

        sql2 = MagicMock(spec=ExecutionData)
        sql2.id = 2
        sql2.duration = 10000  # 10 seconds
        sql2.status = "COMPLETED"
        sql2.success_job_ids = [3, 4]
        sql2.failed_job_ids = []
        sql2.running_job_ids = []
        sql2.description = "Query 2"
        sql2.submission_time = datetime.now()
        sql2.plan_description = "Sample plan description"

        sql3 = MagicMock(spec=ExecutionData)
        sql3.id = 3
        sql3.duration = 2000  # 2 seconds
        sql3.status = "COMPLETED"
        sql3.success_job_ids = [5]
        sql3.failed_job_ids = []
        sql3.running_job_ids = []
        sql3.description = "Query 3"
        sql3.submission_time = datetime.now()
        sql3.plan_description = "Sample plan description"

        mock_client.get_sql_list.return_value = [sql1, sql2, sql3]
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_sql_queries("spark-app-123", top_n=2)

        # Verify results are sorted by duration (descending)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].duration, 10000)  # Slowest first
        self.assertEqual(result[1].duration, 5000)  # Second slowest

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_slowest_sql_queries_exclude_running(self, mock_get_client):
        """Test SQL query retrieval excluding running queries"""
        # Setup mock client
        mock_client = MagicMock()

        # Create mock SQL executions with different statuses
        sql1 = MagicMock(spec=ExecutionData)
        sql1.id = 1
        sql1.duration = 5000
        sql1.status = "RUNNING"
        sql1.success_job_ids = []
        sql1.failed_job_ids = []
        sql1.running_job_ids = [1]
        sql1.description = "Running Query"
        sql1.submission_time = datetime.now()
        sql1.plan_description = "Running plan description"

        sql2 = MagicMock(spec=ExecutionData)
        sql2.id = 2
        sql2.duration = 10000
        sql2.status = "COMPLETED"
        sql2.success_job_ids = [2, 3]
        sql2.failed_job_ids = []
        sql2.running_job_ids = []
        sql2.description = "Completed Query"
        sql2.submission_time = datetime.now()
        sql2.plan_description = "Completed plan description"

        mock_client.get_sql_list.return_value = [sql1, sql2]
        mock_get_client.return_value = mock_client

        # Call the function (include_running=False by default)
        result = list_slowest_sql_queries("spark-app-123")

        # Should exclude running query
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "COMPLETED")

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_slowest_sql_queries_include_running(self, mock_get_client):
        """Test SQL query retrieval including running queries"""
        # Setup mock client
        mock_client = MagicMock()

        # Create mock SQL executions
        sql1 = MagicMock(spec=ExecutionData)
        sql1.id = 1
        sql1.duration = 5000
        sql1.status = "RUNNING"
        sql1.success_job_ids = []
        sql1.failed_job_ids = []
        sql1.running_job_ids = [1]
        sql1.description = "Running Query"
        sql1.submission_time = datetime.now()
        sql1.plan_description = "Running plan description"

        sql2 = MagicMock(spec=ExecutionData)
        sql2.id = 2
        sql2.duration = 10000
        sql2.status = "COMPLETED"
        sql2.success_job_ids = [2, 3]
        sql2.failed_job_ids = []
        sql2.running_job_ids = []
        sql2.description = "Completed Query"
        sql2.submission_time = datetime.now()
        sql2.plan_description = "Completed plan description"

        mock_client.get_sql_list.return_value = [sql1, sql2]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=True and top_n=2
        result = list_slowest_sql_queries(
            "spark-app-123", include_running=True, top_n=2
        )

        # Should include both queries
        self.assertEqual(len(result), 2)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_slowest_sql_queries_empty_result(self, mock_get_client):
        """Test SQL query retrieval with empty result"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.get_sql_list.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_sql_queries("spark-app-123")

        # Verify results
        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_get_slowest_sql_queries_limit(self, mock_get_client):
        """Test SQL query retrieval with limit"""
        # Setup mock client
        mock_client = MagicMock()

        # Create mock SQL executions
        sql_execs = []
        for i in range(10):
            sql = MagicMock(spec=ExecutionData)
            sql.id = i
            sql.duration = (10 - i) * 1000  # Decreasing durations
            sql.status = "COMPLETED"
            sql.success_job_ids = [i]
            sql.failed_job_ids = []
            sql.running_job_ids = []
            sql.description = f"Query {i}"
            sql.submission_time = datetime.now()
            sql.plan_description = f"Plan description for query {i}"
            sql_execs.append(sql)

        mock_client.get_sql_list.return_value = sql_execs
        mock_get_client.return_value = mock_client

        # Call the function with top_n=3
        result = list_slowest_sql_queries("spark-app-123", top_n=3)

        # Verify results - should return only 3 queries
        self.assertEqual(len(result), 3)
        # Should be sorted by duration (descending)
        self.assertEqual(result[0].duration, 10000)
        self.assertEqual(result[1].duration, 9000)
        self.assertEqual(result[2].duration, 8000)

    # Tests for list_applications tool
    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_no_filters(self, mock_get_client):
        """Test list_applications without any filters"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [MagicMock(spec=ApplicationInfo), MagicMock(spec=ApplicationInfo)]
        mock_apps[0].name = "Test App 1"
        mock_apps[1].name = "Test App 2"
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_applications()

        # Verify results
        self.assertEqual(result, mock_apps)
        mock_client.list_applications.assert_called_once_with(
            status=None,
            min_date=None,
            max_date=None,
            min_end_date=None,
            max_end_date=None,
            limit=None,
        )

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_with_existing_filters(self, mock_get_client):
        """Test list_applications with existing filters (backward compatibility)"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [MagicMock(spec=ApplicationInfo)]
        mock_apps[0].name = "Completed App"
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call with existing filters
        result = list_applications(
            status=["COMPLETED"], min_date="2023-01-01", limit=10, server="production"
        )

        # Verify results and that all parameters are passed through
        self.assertEqual(result, mock_apps)
        mock_client.list_applications.assert_called_once_with(
            status=["COMPLETED"],
            min_date="2023-01-01",
            max_date=None,
            min_end_date=None,
            max_end_date=None,
            limit=10,
        )
        mock_get_client.assert_called_once_with(unittest.mock.ANY, "production")

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_name_filter_contains(self, mock_get_client):
        """Test list_applications with name filtering using 'contains' search"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [
            MagicMock(spec=ApplicationInfo),
            MagicMock(spec=ApplicationInfo),
            MagicMock(spec=ApplicationInfo),
        ]
        mock_apps[0].name = "My ETL Job"
        mock_apps[1].name = "Data Processing Pipeline"
        mock_apps[2].name = "ETL Analytics Task"
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call with name filter (default "contains" search)
        result = list_applications(app_name="ETL")

        # Should return apps 0 and 2 (containing "ETL")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, "My ETL Job")
        self.assertEqual(result[1].name, "ETL Analytics Task")

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_name_filter_exact(self, mock_get_client):
        """Test list_applications with exact name matching"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [
            MagicMock(spec=ApplicationInfo),
            MagicMock(spec=ApplicationInfo),
        ]
        mock_apps[0].name = "My App"
        mock_apps[1].name = "My App Extended"
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call with exact name match
        result = list_applications(app_name="My App", search_type="exact")

        # Should return only exact match
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "My App")

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_name_filter_regex(self, mock_get_client):
        """Test list_applications with regex name matching"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [
            MagicMock(spec=ApplicationInfo),
            MagicMock(spec=ApplicationInfo),
            MagicMock(spec=ApplicationInfo),
        ]
        mock_apps[0].name = "Job_001_prod"
        mock_apps[1].name = "Job_002_dev"
        mock_apps[2].name = "Task_001_prod"
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call with regex pattern (jobs ending with _prod)
        result = list_applications(app_name=r"Job_\d+_prod$", search_type="regex")

        # Should return only Job_001_prod
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "Job_001_prod")

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_name_filter_case_insensitive(self, mock_get_client):
        """Test that name filtering is case insensitive"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [
            MagicMock(spec=ApplicationInfo),
            MagicMock(spec=ApplicationInfo),
        ]
        mock_apps[0].name = "MySQL Backup"
        mock_apps[1].name = "PostgreSQL Backup"
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call with lowercase search term
        result = list_applications(app_name="mysql", search_type="contains")

        # Should match case insensitively
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "MySQL Backup")

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_name_filter_empty_name(self, mock_get_client):
        """Test list_applications with apps that have empty/null names"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [
            MagicMock(spec=ApplicationInfo),
            MagicMock(spec=ApplicationInfo),
            MagicMock(spec=ApplicationInfo),
        ]
        mock_apps[0].name = "Named App"
        mock_apps[1].name = None
        mock_apps[2].name = ""
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call with name filter
        result = list_applications(app_name="Named", search_type="contains")

        # Should only match the app with an actual name
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "Named App")

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_name_filter_no_matches(self, mock_get_client):
        """Test list_applications when no apps match the name filter"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [MagicMock(spec=ApplicationInfo), MagicMock(spec=ApplicationInfo)]
        mock_apps[0].name = "App One"
        mock_apps[1].name = "App Two"
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call with non-matching name
        result = list_applications(app_name="NonExistent")

        # Should return empty list
        self.assertEqual(len(result), 0)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_combine_name_and_status_filters(self, mock_get_client):
        """Test combining name filtering with other filters"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [MagicMock(spec=ApplicationInfo)]
        mock_apps[0].name = "Production ETL Job"
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call with both status and name filters
        result = list_applications(
            status=["COMPLETED"], app_name="ETL", search_type="contains"
        )

        # Verify that server-side filtering is applied first
        mock_client.list_applications.assert_called_once_with(
            status=["COMPLETED"],
            min_date=None,
            max_date=None,
            min_end_date=None,
            max_end_date=None,
            limit=None,
        )

        # Then client-side name filtering is applied
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "Production ETL Job")

    def test_list_applications_invalid_search_type(self):
        """Test list_applications with invalid search_type parameter"""
        # Call with invalid search type should raise ValueError
        with self.assertRaises(ValueError) as context:
            list_applications(app_name="test", search_type="invalid")

        self.assertIn("search_type must be one of", str(context.exception))
        self.assertIn("invalid", str(context.exception))

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_invalid_regex(self, mock_get_client):
        """Test list_applications with invalid regex pattern"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [MagicMock(spec=ApplicationInfo)]
        mock_apps[0].name = "Test App"
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call with invalid regex pattern
        with self.assertRaises(Exception) as context:  # re.error gets wrapped
            list_applications(app_name="[invalid", search_type="regex")

        self.assertIn("Invalid regex pattern", str(context.exception))

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_no_name_parameter_returns_all(self, mock_get_client):
        """Test that when no app_name is provided, all apps are returned"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [MagicMock(spec=ApplicationInfo), MagicMock(spec=ApplicationInfo)]
        mock_apps[0].name = "App 1"
        mock_apps[1].name = "App 2"
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call without app_name parameter
        result = list_applications(status=["COMPLETED"])

        # Should return all apps without name filtering
        self.assertEqual(len(result), 2)
        self.assertEqual(result, mock_apps)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_list_applications_empty_app_name_returns_all(self, mock_get_client):
        """Test that empty app_name is treated as no filtering"""
        # Setup mock client
        mock_client = MagicMock()
        mock_apps = [MagicMock(spec=ApplicationInfo), MagicMock(spec=ApplicationInfo)]
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call with empty app_name
        result = list_applications(app_name="")

        # Should return all apps (empty string is falsy)
        self.assertEqual(len(result), 2)
        self.assertEqual(result, mock_apps)


class TestSparkInsightTools(unittest.TestCase):
    """Test suite for SparkInsight analysis tools"""

    def setUp(self):
        # Create mock context
        self.mock_ctx = MagicMock()
        self.mock_lifespan_context = MagicMock()
        self.mock_ctx.request_context.lifespan_context = self.mock_lifespan_context

        # Create mock client
        self.mock_client = MagicMock(spec=SparkRestClient)
        self.mock_lifespan_context.clients = {"default": self.mock_client}
        self.mock_lifespan_context.default_client = self.mock_client

        # Mock datetime.now() to return a consistent time
        self.mock_now = datetime(2024, 1, 1, 12, 0, 0)

    def _create_mock_application(self, app_id="app-123", name="Test App"):
        """Helper to create mock application"""
        mock_app = MagicMock(spec=ApplicationInfo)
        mock_app.id = app_id
        mock_app.name = name

        # Create mock attempt
        mock_attempt = MagicMock(spec=ApplicationAttemptInfo)
        mock_attempt.start_time = self.mock_now
        mock_attempt.end_time = self.mock_now + timedelta(minutes=30)
        mock_app.attempts = [mock_attempt]

        return mock_app

    def _create_mock_environment(self, spark_props=None):
        """Helper to create mock environment"""
        mock_env = MagicMock(spec=ApplicationEnvironmentInfo)
        if spark_props is None:
            spark_props = {
                "spark.dynamicAllocation.initialExecutors": "2",
                "spark.dynamicAllocation.maxExecutors": "10",
            }
        mock_env.spark_properties = [(k, v) for k, v in spark_props.items()]
        return mock_env

    def _create_mock_stage(
        self,
        stage_id,
        name="Test Stage",
        status="COMPLETE",
        executor_run_time=None,
        num_tasks=10,
        num_failed_tasks=0,
        shuffle_write_bytes=0,
        submission_time=None,
        completion_time=None,
        executor_metrics_distributions=None,
    ):
        """Helper to create mock stage"""
        mock_stage = MagicMock(spec=StageData)
        mock_stage.stage_id = stage_id
        mock_stage.attempt_id = 0
        mock_stage.name = name
        mock_stage.status = status
        mock_stage.executor_run_time = executor_run_time or 60000  # 1 minute in ms
        mock_stage.num_tasks = num_tasks
        mock_stage.num_failed_tasks = num_failed_tasks
        mock_stage.shuffle_write_bytes = shuffle_write_bytes
        mock_stage.submission_time = submission_time or self.mock_now
        mock_stage.completion_time = completion_time or (
            self.mock_now + timedelta(minutes=5)
        )
        mock_stage.executor_metrics_distributions = executor_metrics_distributions
        return mock_stage

    def _create_mock_executor(
        self,
        exec_id="1",
        host="worker1",
        failed_tasks=0,
        completed_tasks=10,
        is_active=True,
        total_cores=4,
        max_memory=1024 * 1024 * 1024,
        add_time=None,
        remove_time=None,
    ):
        """Helper to create mock executor"""
        mock_executor = MagicMock(spec=ExecutorSummary)
        mock_executor.id = exec_id
        mock_executor.host = host
        mock_executor.failed_tasks = failed_tasks
        mock_executor.completed_tasks = completed_tasks
        mock_executor.is_active = is_active
        mock_executor.total_cores = total_cores
        mock_executor.max_memory = max_memory
        mock_executor.add_time = add_time or self.mock_now
        mock_executor.remove_time = remove_time
        mock_executor.remove_reason = None
        return mock_executor

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_analyze_auto_scaling_success(self, mock_get_client):
        """Test successful auto-scaling analysis"""
        mock_get_client.return_value = self.mock_client

        # Setup mock data
        mock_app = self._create_mock_application()
        # Set initial executors to a value that should be changed
        mock_env = self._create_mock_environment(
            {
                "spark.dynamicAllocation.initialExecutors": "4",  # Will trigger recommendation to change to 2
                "spark.dynamicAllocation.maxExecutors": "10",  # Will trigger recommendation to change to 2
            }
        )

        # Create stages with different patterns
        stages = [
            self._create_mock_stage(
                1, "Stage 1", executor_run_time=120000, num_tasks=20
            ),
            self._create_mock_stage(
                2, "Stage 2", executor_run_time=180000, num_tasks=30
            ),
        ]

        self.mock_client.get_application.return_value = mock_app
        self.mock_client.get_environment.return_value = mock_env
        self.mock_client.list_stages.return_value = stages

        # Call the function
        result = analyze_auto_scaling("app-123")

        # Verify the result structure
        self.assertEqual(result["application_id"], "app-123")
        self.assertEqual(result["analysis_type"], "Auto-scaling Configuration")
        self.assertIn("recommendations", result)
        self.assertIn("initial_executors", result["recommendations"])
        self.assertIn("max_executors", result["recommendations"])
        self.assertIn("analysis_details", result)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_analyze_auto_scaling_no_stages(self, mock_get_client):
        """Test auto-scaling analysis with no stages"""
        mock_get_client.return_value = self.mock_client

        mock_app = self._create_mock_application()
        mock_env = self._create_mock_environment()

        self.mock_client.get_application.return_value = mock_app
        self.mock_client.get_environment.return_value = mock_env
        self.mock_client.list_stages.return_value = []

        # Call the function
        result = analyze_auto_scaling("app-123")

        # Should return error
        self.assertIn("error", result)
        self.assertEqual(result["application_id"], "app-123")

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_analyze_shuffle_skew_success(self, mock_get_client):
        """Test successful shuffle skew analysis"""
        mock_get_client.return_value = self.mock_client

        # Create stage with significant shuffle write
        mock_stage = self._create_mock_stage(
            1,
            "Shuffle Stage",
            shuffle_write_bytes=15 * 1024 * 1024 * 1024,  # 15 GB
        )

        # Create mock task summary with skew
        mock_task_summary = MagicMock(spec=TaskMetricDistributions)
        mock_task_summary.shuffle_write_bytes = [
            100 * 1024 * 1024,  # min: 100 MB
            500 * 1024 * 1024,  # 25th: 500 MB
            1024 * 1024 * 1024,  # median: 1 GB
            2048 * 1024 * 1024,  # 75th: 2 GB
            5120 * 1024 * 1024,  # max: 5 GB (5x median = high skew)
        ]

        self.mock_client.list_stages.return_value = [mock_stage]
        self.mock_client.get_stage_task_summary.return_value = mock_task_summary

        # Call the function
        result = analyze_shuffle_skew("app-123")

        # Verify the result
        self.assertEqual(result["application_id"], "app-123")
        self.assertEqual(result["analysis_type"], "Shuffle Skew Analysis")
        self.assertIn("skewed_stages", result)
        self.assertIn("recommendations", result)

        # Should detect skew (5 GB / 1 GB = 5.0 ratio > 2.0 threshold)
        self.assertEqual(len(result["skewed_stages"]), 1)
        self.assertEqual(result["skewed_stages"][0]["stage_id"], 1)
        self.assertEqual(result["skewed_stages"][0]["task_skew"]["skew_ratio"], 5.0)
        self.assertTrue(result["skewed_stages"][0]["task_skew"]["is_skewed"])

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_analyze_shuffle_skew_no_skew(self, mock_get_client):
        """Test shuffle skew analysis with no skew detected"""
        mock_get_client.return_value = self.mock_client

        # Create stage with small shuffle write (below threshold)
        mock_stage = self._create_mock_stage(
            1,
            "Small Shuffle Stage",
            shuffle_write_bytes=5 * 1024 * 1024 * 1024,  # 5 GB (below 10 GB threshold)
        )

        self.mock_client.list_stages.return_value = [mock_stage]

        # Call the function
        result = analyze_shuffle_skew("app-123")

        # Should not detect any skewed stages
        self.assertEqual(len(result["skewed_stages"]), 0)
        self.assertEqual(len(result["recommendations"]), 0)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_analyze_failed_tasks_success(self, mock_get_client):
        """Test successful failed task analysis"""
        mock_get_client.return_value = self.mock_client

        # Create stages with failures
        failed_stage = self._create_mock_stage(
            1, "Failed Stage", num_failed_tasks=5, num_tasks=20
        )
        good_stage = self._create_mock_stage(
            2, "Good Stage", num_failed_tasks=0, num_tasks=10
        )

        # Create executors with failures
        failed_executor = self._create_mock_executor(
            "1", "worker1", failed_tasks=3, completed_tasks=7
        )
        good_executor = self._create_mock_executor(
            "2", "worker2", failed_tasks=0, completed_tasks=10
        )

        self.mock_client.list_stages.return_value = [failed_stage, good_stage]
        self.mock_client.list_all_executors.return_value = [
            failed_executor,
            good_executor,
        ]

        # Call the function
        result = analyze_failed_tasks("app-123")

        # Verify the result
        self.assertEqual(result["application_id"], "app-123")
        self.assertEqual(result["analysis_type"], "Failed Task Analysis")
        self.assertIn("failed_stages", result)
        self.assertIn("problematic_executors", result)
        self.assertIn("recommendations", result)

        # Should detect failed stage and problematic executor
        self.assertEqual(len(result["failed_stages"]), 1)
        self.assertEqual(result["failed_stages"][0]["stage_id"], 1)
        self.assertEqual(result["failed_stages"][0]["failed_tasks"], 5)

        self.assertEqual(len(result["problematic_executors"]), 1)
        self.assertEqual(result["problematic_executors"][0]["executor_id"], "1")
        self.assertEqual(result["problematic_executors"][0]["failed_tasks"], 3)

    @patch("spark_history_mcp.tools.analysis.get_client_or_default")
    def test_analyze_failed_tasks_no_failures(self, mock_get_client):
        """Test failed task analysis with no failures"""
        mock_get_client.return_value = self.mock_client

        # Create stages and executors with no failures
        good_stage = self._create_mock_stage(1, "Good Stage", num_failed_tasks=0)
        good_executor = self._create_mock_executor("1", "worker1", failed_tasks=0)

        self.mock_client.list_stages.return_value = [good_stage]
        self.mock_client.list_all_executors.return_value = [good_executor]

        # Call the function
        result = analyze_failed_tasks("app-123")

        # Should not detect any failures
        self.assertEqual(len(result["failed_stages"]), 0)
        self.assertEqual(len(result["problematic_executors"]), 0)
        self.assertEqual(len(result["recommendations"]), 0)
