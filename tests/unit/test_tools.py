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
from spark_history_mcp.tools.tools import (
    _analyze_executor_performance_patterns,
    analyze_auto_scaling,
    analyze_executor_utilization,
    analyze_failed_tasks,
    analyze_shuffle_skew,
    compare_app_executor_timeline,
    compare_app_performance,
    compare_stage_executor_timeline,
    compare_stages,
    get_application,
    get_application_insights,
    get_client_or_default,
    get_stage,
    get_stage_dependency_from_sql_plan,
    get_stage_task_summary,
    list_applications,
    list_jobs,
    list_slowest_jobs,
    list_slowest_sql_queries,
    list_slowest_stages,
    list_stages,
)


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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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
        mock_client.list_jobs.assert_called_once_with(app_id="app-123")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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
            status=["COMPLETED"],
            min_date="2023-01-01",
            limit=10,
            server="production"
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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
            status=["COMPLETED"],
            app_name="ETL",
            search_type="contains"
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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
                "spark.dynamicAllocation.maxExecutors": "10"
            }
        mock_env.spark_properties = [(k, v) for k, v in spark_props.items()]
        return mock_env

    def _create_mock_stage(self, stage_id, name="Test Stage", status="COMPLETE",
                          executor_run_time=None, num_tasks=10, num_failed_tasks=0,
                          shuffle_write_bytes=0, submission_time=None, completion_time=None,
                          executor_metrics_distributions=None):
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
        mock_stage.completion_time = completion_time or (self.mock_now + timedelta(minutes=5))
        mock_stage.executor_metrics_distributions = executor_metrics_distributions
        return mock_stage

    def _create_mock_executor(self, exec_id="1", host="worker1", failed_tasks=0,
                             completed_tasks=10, is_active=True, total_cores=4,
                             max_memory=1024*1024*1024, add_time=None, remove_time=None):
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_analyze_auto_scaling_success(self, mock_get_client):
        """Test successful auto-scaling analysis"""
        mock_get_client.return_value = self.mock_client

        # Setup mock data
        mock_app = self._create_mock_application()
        mock_env = self._create_mock_environment()

        # Create stages with different patterns
        stages = [
            self._create_mock_stage(1, "Stage 1", executor_run_time=120000, num_tasks=20),
            self._create_mock_stage(2, "Stage 2", executor_run_time=180000, num_tasks=30),
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_analyze_shuffle_skew_success(self, mock_get_client):
        """Test successful shuffle skew analysis"""
        mock_get_client.return_value = self.mock_client

        # Create stage with significant shuffle write
        mock_stage = self._create_mock_stage(
            1, "Shuffle Stage",
            shuffle_write_bytes=15 * 1024 * 1024 * 1024  # 15 GB
        )

        # Create mock task summary with skew
        mock_task_summary = MagicMock(spec=TaskMetricDistributions)
        mock_task_summary.shuffle_write_bytes = [
            100 * 1024 * 1024,      # min: 100 MB
            500 * 1024 * 1024,      # 25th: 500 MB
            1024 * 1024 * 1024,     # median: 1 GB
            2048 * 1024 * 1024,     # 75th: 2 GB
            5120 * 1024 * 1024      # max: 5 GB (5x median = high skew)
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_analyze_shuffle_skew_no_skew(self, mock_get_client):
        """Test shuffle skew analysis with no skew detected"""
        mock_get_client.return_value = self.mock_client

        # Create stage with small shuffle write (below threshold)
        mock_stage = self._create_mock_stage(
            1, "Small Shuffle Stage",
            shuffle_write_bytes=5 * 1024 * 1024 * 1024  # 5 GB (below 10 GB threshold)
        )

        self.mock_client.list_stages.return_value = [mock_stage]

        # Call the function
        result = analyze_shuffle_skew("app-123")

        # Should not detect any skewed stages
        self.assertEqual(len(result["skewed_stages"]), 0)
        self.assertEqual(len(result["recommendations"]), 0)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_analyze_failed_tasks_success(self, mock_get_client):
        """Test successful failed task analysis"""
        mock_get_client.return_value = self.mock_client

        # Create stages with failures
        failed_stage = self._create_mock_stage(1, "Failed Stage", num_failed_tasks=5, num_tasks=20)
        good_stage = self._create_mock_stage(2, "Good Stage", num_failed_tasks=0, num_tasks=10)

        # Create executors with failures
        failed_executor = self._create_mock_executor("1", "worker1", failed_tasks=3, completed_tasks=7)
        good_executor = self._create_mock_executor("2", "worker2", failed_tasks=0, completed_tasks=10)

        self.mock_client.list_stages.return_value = [failed_stage, good_stage]
        self.mock_client.list_all_executors.return_value = [failed_executor, good_executor]

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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
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

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_analyze_executor_utilization_success(self, mock_get_client):
        """Test successful executor utilization analysis"""
        mock_get_client.return_value = self.mock_client

        mock_app = self._create_mock_application()

        # Create executors with different lifecycles
        executor1 = self._create_mock_executor(
            "1", "worker1",
            add_time=self.mock_now,
            remove_time=self.mock_now + timedelta(minutes=20)
        )
        executor2 = self._create_mock_executor(
            "2", "worker2",
            add_time=self.mock_now + timedelta(minutes=10),
            remove_time=None  # Still active
        )

        self.mock_client.get_application.return_value = mock_app
        self.mock_client.list_all_executors.return_value = [executor1, executor2]

        # Call the function
        result = analyze_executor_utilization("app-123")

        # Verify the result
        self.assertEqual(result["application_id"], "app-123")
        self.assertEqual(result["analysis_type"], "Executor Utilization Analysis")
        self.assertIn("timeline", result)
        self.assertIn("summary", result)
        self.assertIn("recommendations", result)

        # Should have utilization metrics
        self.assertIn("peak_executors", result["summary"])
        self.assertIn("average_executors", result["summary"])
        self.assertIn("utilization_efficiency_percent", result["summary"])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_analyze_executor_utilization_no_attempts(self, mock_get_client):
        """Test executor utilization analysis with no application attempts"""
        mock_get_client.return_value = self.mock_client

        mock_app = self._create_mock_application()
        mock_app.attempts = []  # No attempts

        self.mock_client.get_application.return_value = mock_app

        # Call the function
        result = analyze_executor_utilization("app-123")

        # Should return error
        self.assertIn("error", result)
        self.assertEqual(result["application_id"], "app-123")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    @patch("spark_history_mcp.tools.tools.datetime")
    def test_get_application_insights_success(self, mock_datetime, mock_get_client):
        """Test comprehensive application insights analysis"""
        mock_datetime.now.return_value = self.mock_now
        mock_get_client.return_value = self.mock_client

        mock_app = self._create_mock_application()
        self.mock_client.get_application.return_value = mock_app

        # Call the function
        result = get_application_insights("app-123")

        # Verify the result structure
        self.assertEqual(result["application_id"], "app-123")
        self.assertEqual(result["application_name"], "Test App")
        self.assertEqual(result["analysis_type"], "Comprehensive SparkInsight Analysis")
        self.assertIn("analyses", result)
        self.assertIn("summary", result)
        self.assertIn("recommendations", result)

        # Should include all analyses by default
        expected_analyses = ["auto_scaling", "shuffle_skew", "failed_tasks", "executor_utilization"]
        for analysis in expected_analyses:
            self.assertIn(analysis, result["analyses"])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    @patch("spark_history_mcp.tools.tools.datetime")
    def test_get_application_insights_selective(self, mock_datetime, mock_get_client):
        """Test application insights with selective analysis"""
        mock_datetime.now.return_value = self.mock_now
        mock_get_client.return_value = self.mock_client

        mock_app = self._create_mock_application()
        self.mock_client.get_application.return_value = mock_app

        # Call with only specific analyses enabled
        result = get_application_insights(
            "app-123",
            include_auto_scaling=True,
            include_shuffle_skew=False,
            include_failed_tasks=True,
            include_executor_utilization=False
        )

        # Should only include requested analyses
        self.assertIn("auto_scaling", result["analyses"])
        self.assertNotIn("shuffle_skew", result["analyses"])
        self.assertIn("failed_tasks", result["analyses"])
        self.assertNotIn("executor_utilization", result["analyses"])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_analyze_shuffle_skew_task_summary_error(self, mock_get_client):
        """Test shuffle skew analysis when task summary fetch fails"""
        mock_get_client.return_value = self.mock_client

        # Create stage with shuffle write
        mock_stage = self._create_mock_stage(
            1, "Shuffle Stage",
            shuffle_write_bytes=15 * 1024 * 1024 * 1024  # 15 GB
        )

        self.mock_client.list_stages.return_value = [mock_stage]
        # Make task summary fetch fail
        self.mock_client.get_stage_task_summary.side_effect = Exception("Task summary not available")

        # Call the function
        result = analyze_shuffle_skew("app-123")

        # Should handle the error gracefully and not include failed stages
        self.assertEqual(len(result["skewed_stages"]), 0)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_analyze_shuffle_skew_executor_level(self, mock_get_client):
        """Test shuffle skew analysis with executor-level skew"""
        mock_get_client.return_value = self.mock_client

        # Create mock executor metrics distributions with skew
        mock_exec_metrics = MagicMock()
        mock_exec_metrics.shuffle_write = [
            500 * 1024 * 1024,      # min: 500 MB
            800 * 1024 * 1024,      # 25th: 800 MB
            1024 * 1024 * 1024,     # median: 1 GB
            1536 * 1024 * 1024,     # 75th: 1.5 GB
            4096 * 1024 * 1024      # max: 4 GB (4x median = high skew)
        ]

        # Create stage with significant shuffle write and executor skew
        mock_stage = self._create_mock_stage(
            1, "Executor Skew Stage",
            shuffle_write_bytes=15 * 1024 * 1024 * 1024,  # 15 GB
            executor_metrics_distributions=mock_exec_metrics
        )

        # Create mock task summary with no skew (to test executor-only skew)
        mock_task_summary = MagicMock(spec=TaskMetricDistributions)
        mock_task_summary.shuffle_write_bytes = [
            1000 * 1024 * 1024,     # min: 1000 MB
            1100 * 1024 * 1024,     # 25th: 1100 MB
            1200 * 1024 * 1024,     # median: 1200 MB
            1300 * 1024 * 1024,     # 75th: 1300 MB
            1400 * 1024 * 1024      # max: 1400 MB (1.17 ratio < 2.0 threshold)
        ]

        self.mock_client.list_stages.return_value = [mock_stage]
        self.mock_client.get_stage_task_summary.return_value = mock_task_summary

        # Call the function
        result = analyze_shuffle_skew("app-123")

        # Verify the result
        self.assertEqual(result["application_id"], "app-123")
        self.assertEqual(len(result["skewed_stages"]), 1)

        skewed_stage = result["skewed_stages"][0]
        self.assertEqual(skewed_stage["stage_id"], 1)

        # Should have no task skew but executor skew
        self.assertIsNotNone(skewed_stage["task_skew"])
        self.assertFalse(skewed_stage["task_skew"]["is_skewed"])

        self.assertIsNotNone(skewed_stage["executor_skew"])
        self.assertTrue(skewed_stage["executor_skew"]["is_skewed"])
        self.assertEqual(skewed_stage["executor_skew"]["skew_ratio"], 4.0)

        # Should have resource allocation recommendation
        recommendations = result["recommendations"]
        resource_rec_found = any(
            rec.get("type") == "resource_allocation"
            for rec in recommendations
        )
        self.assertTrue(resource_rec_found)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_analyze_failed_tasks_host_concentration(self, mock_get_client):
        """Test failed task analysis detecting host-specific issues"""
        mock_get_client.return_value = self.mock_client

        # Create stage with failures
        failed_stage = self._create_mock_stage(1, "Failed Stage", num_failed_tasks=10)

        # Create executors where most failures are on one host
        executor1 = self._create_mock_executor("1", "problematic-host", failed_tasks=8, completed_tasks=2)
        executor2 = self._create_mock_executor("2", "good-host", failed_tasks=1, completed_tasks=9)
        executor3 = self._create_mock_executor("3", "another-good-host", failed_tasks=1, completed_tasks=9)

        self.mock_client.list_stages.return_value = [failed_stage]
        self.mock_client.list_all_executors.return_value = [executor1, executor2, executor3]

        # Call the function
        result = analyze_failed_tasks("app-123")

        # Should detect host concentration issue
        # 8 failures on problematic-host > 50% of total 10 failures
        recommendations = result["recommendations"]
        host_issue_found = any(
            rec.get("type") == "infrastructure" and "concentration" in rec.get("issue", "")
            for rec in recommendations
        )
        self.assertTrue(host_issue_found)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_analyze_executor_utilization_low_efficiency(self, mock_get_client):
        """Test executor utilization analysis detecting low efficiency"""
        mock_get_client.return_value = self.mock_client

        mock_app = self._create_mock_application()

        # Create scenario with low utilization efficiency (high peak, low average)
        # Executor 1: Active for 5 minutes
        executor1 = self._create_mock_executor(
            "1", "worker1",
            add_time=self.mock_now,
            remove_time=self.mock_now + timedelta(minutes=5)
        )
        # Executor 2: Active for full 30 minutes
        executor2 = self._create_mock_executor(
            "2", "worker2",
            add_time=self.mock_now,
            remove_time=None
        )
        # Executor 3: Active for only 2 minutes (short burst)
        executor3 = self._create_mock_executor(
            "3", "worker3",
            add_time=self.mock_now + timedelta(minutes=10),
            remove_time=self.mock_now + timedelta(minutes=12)
        )

        self.mock_client.get_application.return_value = mock_app
        self.mock_client.list_all_executors.return_value = [executor1, executor2, executor3]

        # Call the function
        result = analyze_executor_utilization("app-123")

        # Should detect low efficiency and recommend optimization
        recommendations = result["recommendations"]
        efficiency_issue_found = any(
            rec.get("type") == "resource_efficiency" and "Low executor utilization" in rec.get("issue", "")
            for rec in recommendations
        )

        # The efficiency calculation should trigger the recommendation if < 70%
        if result["summary"]["utilization_efficiency_percent"] < 70:
            self.assertTrue(efficiency_issue_found)


class TestExecutorPerformanceAnalysis(unittest.TestCase):
    """Test suite for executor performance analysis functionality"""

    def _create_mock_executor_summary(self, exec_id, task_time=5000, failed_tasks=0,
                                     succeeded_tasks=10, memory_spilled=0,
                                     shuffle_read=0, shuffle_write=0):
        """Helper to create mock executor summary data"""
        mock_executor = MagicMock()
        mock_executor.task_time = task_time
        mock_executor.failed_tasks = failed_tasks
        mock_executor.succeeded_tasks = succeeded_tasks
        mock_executor.memory_bytes_spilled = memory_spilled
        mock_executor.shuffle_read = shuffle_read
        mock_executor.shuffle_write = shuffle_write
        return mock_executor

    def test_analyze_executor_performance_patterns_basic(self):
        """Test basic executor performance pattern analysis"""
        # Create mock executor summaries
        exec_summary1 = {
            "1": self._create_mock_executor_summary("1", task_time=5000, succeeded_tasks=10),
            "2": self._create_mock_executor_summary("2", task_time=6000, succeeded_tasks=12)
        }
        exec_summary2 = {
            "1": self._create_mock_executor_summary("1", task_time=8000, succeeded_tasks=8),
            "2": self._create_mock_executor_summary("2", task_time=10000, succeeded_tasks=15)
        }

        # Call the function
        result = _analyze_executor_performance_patterns(exec_summary1, exec_summary2)

        # Verify structure
        self.assertIn("app1_executor_metrics", result)
        self.assertIn("app2_executor_metrics", result)
        self.assertIn("comparative_analysis", result)
        self.assertIn("insights", result)
        self.assertIn("recommendations", result)

        # Verify app1 metrics
        app1_metrics = result["app1_executor_metrics"]
        self.assertEqual(app1_metrics["total_executors"], 2)
        self.assertEqual(app1_metrics["avg_task_time"], 5500)  # (5000 + 6000) / 2
        self.assertEqual(app1_metrics["avg_succeeded_tasks"], 11)  # (10 + 12) / 2

        # Verify app2 metrics
        app2_metrics = result["app2_executor_metrics"]
        self.assertEqual(app2_metrics["total_executors"], 2)
        self.assertEqual(app2_metrics["avg_task_time"], 9000)  # (8000 + 10000) / 2
        self.assertEqual(app2_metrics["avg_succeeded_tasks"], 11.5)  # (8 + 15) / 2

        # Verify comparative analysis
        comparison = result["comparative_analysis"]
        self.assertEqual(comparison["executor_count_ratio"], 1.0)  # Same number of executors
        self.assertAlmostEqual(comparison["task_time_ratio"], 9000/5500, places=1)  # App2 is slower

    def test_analyze_executor_performance_patterns_with_failures(self):
        """Test executor performance analysis with task failures"""
        exec_summary1 = {
            "1": self._create_mock_executor_summary("1", failed_tasks=2, succeeded_tasks=8)
        }
        exec_summary2 = {
            "1": self._create_mock_executor_summary("1", failed_tasks=6, succeeded_tasks=4)
        }

        result = _analyze_executor_performance_patterns(exec_summary1, exec_summary2)

        # Verify failure analysis
        app1_metrics = result["app1_executor_metrics"]
        app2_metrics = result["app2_executor_metrics"]

        self.assertEqual(app1_metrics["executors_with_failures"], 1)
        self.assertEqual(app2_metrics["executors_with_failures"], 1)
        self.assertEqual(app1_metrics["avg_failed_tasks"], 2)
        self.assertEqual(app2_metrics["avg_failed_tasks"], 6)

        # Should generate insights about failure differences
        comparison = result["comparative_analysis"]
        self.assertEqual(comparison["failure_rate_ratio"], 3.0)  # App2 has 3x more failures

    def test_analyze_executor_performance_patterns_memory_spill(self):
        """Test executor performance analysis with memory spill"""
        exec_summary1 = {
            "1": self._create_mock_executor_summary("1", memory_spilled=100*1024*1024),  # 100 MB
            "2": self._create_mock_executor_summary("2", memory_spilled=0)
        }
        exec_summary2 = {
            "1": self._create_mock_executor_summary("1", memory_spilled=500*1024*1024),  # 500 MB
            "2": self._create_mock_executor_summary("2", memory_spilled=300*1024*1024)   # 300 MB
        }

        result = _analyze_executor_performance_patterns(exec_summary1, exec_summary2)

        # Verify memory spill analysis
        app1_metrics = result["app1_executor_metrics"]
        app2_metrics = result["app2_executor_metrics"]

        self.assertEqual(app1_metrics["executors_with_spill"], 1)
        self.assertEqual(app2_metrics["executors_with_spill"], 2)
        self.assertEqual(app1_metrics["total_memory_spilled"], 100*1024*1024)
        self.assertEqual(app2_metrics["total_memory_spilled"], 800*1024*1024)  # 500 + 300

        # Should detect high memory spill ratio
        comparison = result["comparative_analysis"]
        self.assertEqual(comparison["memory_spill_ratio"], 8.0)  # 800MB / 100MB

        # Should generate memory-related recommendations
        insights = result["insights"]
        recommendations = result["recommendations"]

        memory_insight_found = any("memory spill" in insight.lower() for insight in insights)
        memory_rec_found = any("memory" in rec.lower() for rec in recommendations)

        self.assertTrue(memory_insight_found)
        self.assertTrue(memory_rec_found)

    def test_analyze_executor_performance_patterns_empty_data(self):
        """Test executor performance analysis with empty data"""
        result = _analyze_executor_performance_patterns({}, {})

        # Should handle empty data gracefully
        self.assertEqual(result["analysis"], "No executor data available for comparison")

    def test_analyze_executor_performance_patterns_one_empty(self):
        """Test executor performance analysis with one empty dataset"""
        exec_summary1 = {
            "1": self._create_mock_executor_summary("1", task_time=5000)
        }
        exec_summary2 = {}

        result = _analyze_executor_performance_patterns(exec_summary1, exec_summary2)

        # Should handle mixed empty data
        self.assertIn("app1_executor_metrics", result)
        self.assertIn("app2_executor_metrics", result)

        app1_metrics = result["app1_executor_metrics"]
        app2_metrics = result["app2_executor_metrics"]

        self.assertEqual(app1_metrics["total_executors"], 1)
        self.assertEqual(app2_metrics["total_executors"], 0)

    def test_analyze_executor_performance_patterns_reliability_insights(self):
        """Test reliability insights generation"""
        # Create scenario where app2 has > 2x more executor failures than app1
        exec_summary1 = {
            "1": self._create_mock_executor_summary("1", failed_tasks=1, succeeded_tasks=9),
            "2": self._create_mock_executor_summary("2", failed_tasks=0, succeeded_tasks=10),
            "3": self._create_mock_executor_summary("3", failed_tasks=0, succeeded_tasks=10),
            "4": self._create_mock_executor_summary("4", failed_tasks=0, succeeded_tasks=10)
        }
        exec_summary2 = {
            "1": self._create_mock_executor_summary("1", failed_tasks=5, succeeded_tasks=5),
            "2": self._create_mock_executor_summary("2", failed_tasks=4, succeeded_tasks=6),
            "3": self._create_mock_executor_summary("3", failed_tasks=3, succeeded_tasks=7),
            "4": self._create_mock_executor_summary("4", failed_tasks=0, succeeded_tasks=10)
        }

        result = _analyze_executor_performance_patterns(exec_summary1, exec_summary2)

        # App1: 1 executor with failures out of 4 = 25%
        # App2: 3 executors with failures out of 4 = 75%
        # 75% > 25% * 2 = 50% so should trigger recommendation
        comparison = result["comparative_analysis"]
        reliability = comparison["reliability_comparison"]

        self.assertEqual(reliability["app1_failure_percentage"], 25.0)
        self.assertEqual(reliability["app2_failure_percentage"], 75.0)

        # Should generate infrastructure recommendation since 75% > 50%
        recommendations = result["recommendations"]
        infra_rec_found = any("infrastructure" in rec.lower() for rec in recommendations)
        self.assertTrue(infra_rec_found)


class TestCompareAppPerformance(unittest.TestCase):
    """Test suite for compare_app_performance tool"""

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
        mock_app.attempts = [MagicMock()]
        mock_app.attempts[0].start_time = self.mock_now
        mock_app.attempts[0].end_time = self.mock_now + timedelta(minutes=30)
        mock_app.attempts[0].duration = 30 * 60 * 1000  # 30 minutes in milliseconds

        # Add attributes that compare_app_performance might access
        mock_app.cores_granted = 8
        mock_app.memory_per_executor_mb = 1024
        mock_app.max_executors = 10

        return mock_app

    def _create_mock_stage(self, stage_id, name="Test Stage", duration_ms=60000,
                          num_tasks=10, num_failed_tasks=0, memory_spilled_bytes=0,
                          shuffle_write_bytes=0, executor_summary=None):
        """Helper to create mock stage"""
        mock_stage = MagicMock(spec=StageData)
        mock_stage.stage_id = stage_id
        mock_stage.attempt_id = 0
        mock_stage.name = name
        mock_stage.status = "COMPLETE"
        mock_stage.executor_run_time = duration_ms
        mock_stage.num_tasks = num_tasks
        mock_stage.num_failed_tasks = num_failed_tasks
        mock_stage.memory_bytes_spilled = memory_spilled_bytes  # Fix attribute name
        mock_stage.shuffle_write_bytes = shuffle_write_bytes
        mock_stage.submission_time = self.mock_now
        mock_stage.completion_time = self.mock_now + timedelta(milliseconds=duration_ms)
        mock_stage.executor_summary = executor_summary or {}

        # Add additional attributes that compare_app_performance might access
        mock_stage.input_bytes = 0
        mock_stage.output_bytes = 0
        mock_stage.shuffle_read_bytes = 0
        mock_stage.disk_bytes_spilled = 0
        mock_stage.num_active_tasks = 0
        mock_stage.num_complete_tasks = num_tasks - num_failed_tasks
        mock_stage.first_task_launched_time = self.mock_now

        # Add attributes for compare_stages function
        mock_stage.task_metrics_distributions = None  # Will be set by client.get_stage_task_summary
        mock_stage.executor_metrics_distributions = None  # Mock executor distributions

        return mock_stage

    def _create_mock_job(self, job_id, name="Test Job", status="SUCCEEDED",
                        submission_time=None, completion_time=None):
        """Helper to create mock job"""
        mock_job = MagicMock(spec=JobData)
        mock_job.job_id = job_id
        mock_job.name = name
        mock_job.status = status
        mock_job.submission_time = submission_time or self.mock_now
        mock_job.completion_time = completion_time or (self.mock_now + timedelta(minutes=5))
        return mock_job

    def _create_mock_executor_summary_data(self):
        """Helper to create mock executor summary data"""
        mock_executor = MagicMock()
        mock_executor.task_time = 5000
        mock_executor.failed_tasks = 1
        mock_executor.succeeded_tasks = 9
        mock_executor.memory_bytes_spilled = 100*1024*1024  # 100 MB
        mock_executor.shuffle_read = 50*1024*1024  # 50 MB
        mock_executor.shuffle_write = 75*1024*1024  # 75 MB
        return {"1": mock_executor}

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    @patch("spark_history_mcp.tools.tools.get_executor_summary")
    def test_compare_app_performance_basic_success(self, mock_executor_summary, mock_get_client):
        """Test basic successful application performance comparison"""
        mock_get_client.return_value = self.mock_client

        # Setup mock applications
        app1 = self._create_mock_application("app-123", "App One")
        app2 = self._create_mock_application("app-456", "App Two")

        # Setup mock jobs
        jobs1 = [self._create_mock_job(1, "Job 1", completion_time=self.mock_now + timedelta(minutes=3))]
        jobs2 = [self._create_mock_job(1, "Job 1", completion_time=self.mock_now + timedelta(minutes=5))]

        # Setup mock stages with different performance characteristics
        executor_summary1 = self._create_mock_executor_summary_data()
        executor_summary2 = self._create_mock_executor_summary_data()
        executor_summary2["1"].task_time = 8000  # Slower in app2

        stage1 = self._create_mock_stage(1, "Stage 1", duration_ms=180000, executor_summary=executor_summary1)  # 3 min
        stage2 = self._create_mock_stage(1, "Stage 1", duration_ms=300000, executor_summary=executor_summary2)  # 5 min

        stages1 = [stage1]
        stages2 = [stage2]

        # Setup mock client returns
        self.mock_client.get_application.side_effect = [app1, app2]
        self.mock_client.list_jobs.side_effect = [jobs1, jobs2]
        self.mock_client.list_stages.side_effect = [stages1, stages2]

        # Setup mock executor summary tool returns
        mock_executor_summary.side_effect = [executor_summary1, executor_summary2]

        # Call the function
        result = compare_app_performance("app-123", "app-456")

        # Verify basic structure
        self.assertIn("applications", result)
        self.assertIn("aggregated_overview", result)
        self.assertIn("stage_deep_dive", result)
        self.assertIn("recommendations", result)

        # Verify application info
        applications = result["applications"]
        self.assertEqual(applications["app1"]["id"], "app-123")
        self.assertEqual(applications["app2"]["id"], "app-456")

        # Verify aggregated overview structure
        overview = result["aggregated_overview"]
        self.assertIn("application_summary", overview)
        self.assertIn("job_performance", overview)
        self.assertIn("stage_metrics", overview)
        self.assertIn("executor_performance", overview)

        # Verify stage deep dive structure
        deep_dive = result["stage_deep_dive"]
        self.assertIn("analysis_parameters", deep_dive)
        self.assertIn("top_stage_differences", deep_dive)
        self.assertIn("stage_summary", deep_dive)

        # Verify stage comparison includes executor analysis instead of raw data
        if deep_dive["top_stage_differences"]:
            stage_comparison = deep_dive["top_stage_differences"][0]
            self.assertIn("executor_analysis", stage_comparison)
            self.assertNotIn("app1_executor_details", stage_comparison)
            self.assertNotIn("app2_executor_details", stage_comparison)

            # Verify executor analysis structure
            executor_analysis = stage_comparison["executor_analysis"]
            self.assertIn("app1_executor_metrics", executor_analysis)
            self.assertIn("app2_executor_metrics", executor_analysis)
            self.assertIn("comparative_analysis", executor_analysis)
            self.assertIn("insights", executor_analysis)
            self.assertIn("recommendations", executor_analysis)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    @patch("spark_history_mcp.tools.tools.get_executor_summary")
    def test_compare_app_performance_stage_matching(self, mock_executor_summary, mock_get_client):
        """Test stage matching logic in compare_app_performance"""
        mock_get_client.return_value = self.mock_client

        # Setup applications
        app1 = self._create_mock_application("app-123", "App One")
        app2 = self._create_mock_application("app-456", "App Two")

        # Setup jobs
        jobs1 = [self._create_mock_job(1)]
        jobs2 = [self._create_mock_job(1)]

        # Setup stages with similar names but different performance
        executor_summary = self._create_mock_executor_summary_data()
        stage1_fast = self._create_mock_stage(1, "Data Loading Stage", duration_ms=120000, executor_summary=executor_summary)
        stage1_slow = self._create_mock_stage(2, "Processing Stage", duration_ms=60000, executor_summary=executor_summary)

        stage2_fast = self._create_mock_stage(1, "Data Loading Step", duration_ms=180000, executor_summary=executor_summary)  # Similar name, slower
        stage2_slow = self._create_mock_stage(2, "Processing Step", duration_ms=90000, executor_summary=executor_summary)   # Similar name, slower

        stages1 = [stage1_fast, stage1_slow]
        stages2 = [stage2_fast, stage2_slow]

        # Setup mock returns
        self.mock_client.get_application.side_effect = [app1, app2]
        self.mock_client.list_jobs.side_effect = [jobs1, jobs2]
        self.mock_client.list_stages.side_effect = [stages1, stages2]
        mock_executor_summary.return_value = executor_summary

        # Call function
        result = compare_app_performance("app-123", "app-456", top_n=2)

        # Verify stage matching worked
        deep_dive = result["stage_deep_dive"]
        stage_differences = deep_dive["top_stage_differences"]

        self.assertEqual(len(stage_differences), 2)

        # Should match stages by name similarity
        stage_names_matched = [
            (comp["app1_stage"]["name"], comp["app2_stage"]["name"])
            for comp in stage_differences
        ]

        # Should have matched similar names
        matched_names = [name_pair for name_pair in stage_names_matched]
        self.assertEqual(len(matched_names), 2)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    @patch("spark_history_mcp.tools.tools.get_executor_summary")
    def test_compare_app_performance_recommendations_generation(self, mock_executor_summary, mock_get_client):
        """Test that compare_app_performance generates appropriate recommendations"""
        mock_get_client.return_value = self.mock_client

        # Setup applications
        app1 = self._create_mock_application("app-123", "Fast App")
        app2 = self._create_mock_application("app-456", "Slow App")

        # Setup jobs with significant time difference
        jobs1 = [self._create_mock_job(1, completion_time=self.mock_now + timedelta(minutes=2))]  # Fast
        jobs2 = [self._create_mock_job(1, completion_time=self.mock_now + timedelta(minutes=10))]  # Slow

        # Setup stages with large time difference (should trigger recommendations)
        executor_summary1 = self._create_mock_executor_summary_data()
        executor_summary2 = self._create_mock_executor_summary_data()
        executor_summary2["1"].task_time = 15000  # 3x slower
        executor_summary2["1"].memory_bytes_spilled = 500*1024*1024  # 5x more spill

        stage1 = self._create_mock_stage(1, "Critical Stage", duration_ms=60000, executor_summary=executor_summary1)   # 1 min
        stage2 = self._create_mock_stage(1, "Critical Stage", duration_ms=300000, executor_summary=executor_summary2)  # 5 min (4 min difference > 60s threshold)

        stages1 = [stage1]
        stages2 = [stage2]

        # Setup mock returns
        self.mock_client.get_application.side_effect = [app1, app2]
        self.mock_client.list_jobs.side_effect = [jobs1, jobs2]
        self.mock_client.list_stages.side_effect = [stages1, stages2]
        mock_executor_summary.side_effect = [executor_summary1, executor_summary2]

        # Call function
        result = compare_app_performance("app-123", "app-456")

        # Verify recommendations were generated
        recommendations = result["recommendations"]
        self.assertGreater(len(recommendations), 0)

        # Should have stage performance recommendation due to large time difference
        stage_perf_rec = any(
            rec.get("type") == "stage_performance"
            for rec in recommendations
        )
        self.assertTrue(stage_perf_rec)

        # Should have recommendations sorted by priority
        priorities = [rec.get("priority", "low") for rec in recommendations]
        priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        priority_values = [priority_order.get(p, 3) for p in priorities]
        self.assertEqual(priority_values, sorted(priority_values))

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_app_performance_error_handling(self, mock_get_client):
        """Test error handling in compare_app_performance"""
        mock_get_client.return_value = self.mock_client

        # Setup mock client to raise exception for second app
        app1 = self._create_mock_application("app-123", "App One")
        self.mock_client.get_application.side_effect = [app1, Exception("App not found")]

        # Call function
        result = compare_app_performance("app-123", "nonexistent-app")

        # Should return error structure
        self.assertIn("error", result)
        self.assertIn("applications", result)
        self.assertEqual(result["applications"]["app1"]["id"], "app-123")
        self.assertEqual(result["applications"]["app2"]["id"], "nonexistent-app")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    @patch("spark_history_mcp.tools.tools.get_executor_summary")
    def test_compare_app_performance_no_matching_stages(self, mock_executor_summary, mock_get_client):
        """Test compare_app_performance when no stages can be matched"""
        mock_get_client.return_value = self.mock_client

        # Setup applications
        app1 = self._create_mock_application("app-123", "App One")
        app2 = self._create_mock_application("app-456", "App Two")

        # Setup jobs
        jobs1 = [self._create_mock_job(1)]
        jobs2 = [self._create_mock_job(1)]

        # Setup stages with completely different names
        executor_summary = self._create_mock_executor_summary_data()
        stages1 = [self._create_mock_stage(1, "Data Ingestion Pipeline", executor_summary=executor_summary)]
        stages2 = [self._create_mock_stage(1, "Machine Learning Training", executor_summary=executor_summary)]

        # Setup mock returns
        self.mock_client.get_application.side_effect = [app1, app2]
        self.mock_client.list_jobs.side_effect = [jobs1, jobs2]
        self.mock_client.list_stages.side_effect = [stages1, stages2]
        mock_executor_summary.return_value = executor_summary

        # Call function with high similarity threshold
        result = compare_app_performance("app-123", "app-456", similarity_threshold=0.8)

        # Should still return valid structure but with no stage comparisons
        deep_dive = result["stage_deep_dive"]
        self.assertEqual(len(deep_dive["top_stage_differences"]), 0)
        self.assertEqual(deep_dive["analysis_parameters"]["matched_stages"], 0)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_stages_basic_success(self, mock_get_client):
        """Test basic successful stage comparison"""
        mock_get_client.return_value = self.mock_client

        # Create mock stages with different metrics to trigger significance threshold
        stage1 = self._create_mock_stage(
            stage_id=1,
            name="Stage 1",
            num_tasks=100,
            memory_spilled_bytes=1000,
            duration_ms=10000  # 10 seconds in ms
        )
        stage2 = self._create_mock_stage(
            stage_id=2,
            name="Stage 2",
            num_tasks=150,  # 50% increase
            memory_spilled_bytes=2000,  # 100% increase
            duration_ms=15000  # 15 seconds in ms
        )

        # Mock the client method calls
        self.mock_client.get_stage_attempt.side_effect = [stage1, stage2]
        self.mock_client.get_stage_task_summary.side_effect = [
            MagicMock(),  # Mock task distributions for stage1
            MagicMock()   # Mock task distributions for stage2
        ]

        # Call the function
        result = compare_stages("app-123", "app-456", 1, 2)

        # Verify basic structure
        self.assertIn("stage_comparison", result)
        self.assertIn("significant_differences", result)
        self.assertIn("summary", result)

        # Verify stage comparison info
        stage_comp = result["stage_comparison"]
        self.assertEqual(stage_comp["stage1"]["app_id"], "app-123")
        self.assertEqual(stage_comp["stage1"]["stage_id"], 1)
        self.assertEqual(stage_comp["stage2"]["app_id"], "app-456")
        self.assertEqual(stage_comp["stage2"]["stage_id"], 2)

        # Verify significance threshold is set
        self.assertEqual(result["summary"]["significance_threshold"], 0.2)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_stages_error_handling(self, mock_get_client):
        """Test error handling in compare_stages"""
        mock_get_client.return_value = self.mock_client

        # Setup mock client to raise exception
        self.mock_client.get_stage_attempt.side_effect = Exception("Stage not found")

        # Call function
        result = compare_stages("app-123", "app-456", 1, 2)

        # Should return error structure
        self.assertIn("error", result)
        self.assertIn("stages", result)
        self.assertEqual(result["stages"]["stage1"]["app_id"], "app-123")
        self.assertEqual(result["stages"]["stage1"]["stage_id"], 1)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_stages_no_significant_differences(self, mock_get_client):
        """Test compare_stages when no differences meet significance threshold"""
        mock_get_client.return_value = self.mock_client

        # Create mock stages with very similar metrics (below default 0.2 threshold)
        stage1 = self._create_mock_stage(
            stage_id=1,
            name="Stage 1",
            num_tasks=100,
            memory_spilled_bytes=1000
        )
        stage2 = self._create_mock_stage(
            stage_id=2,
            name="Stage 2",
            num_tasks=105,  # Only 5% increase (below 20% threshold)
            memory_spilled_bytes=1050  # Only 5% increase
        )

        # Mock the client method calls
        self.mock_client.get_stage_attempt.side_effect = [stage1, stage2]
        self.mock_client.get_stage_task_summary.side_effect = [
            MagicMock(),
            MagicMock()
        ]

        # Call the function
        result = compare_stages("app-123", "app-456", 1, 2)

        # Should have empty significant_differences since changes are below threshold
        self.assertEqual(len(result["significant_differences"]), 0)
        self.assertEqual(result["summary"]["total_differences_found"], 0)


class TestCompareStageExecutorTimeline(unittest.TestCase):
    """Test cases for compare_stage_executor_timeline function"""

    def _create_mock_stage(self, stage_id, name, submission_time=None, completion_time=None):
        """Helper to create mock stage data"""
        stage = MagicMock()
        stage.stage_id = stage_id
        stage.attempt_id = 0
        stage.name = name
        stage.submission_time = submission_time or datetime(2024, 1, 1, 10, 0, 0)
        stage.completion_time = completion_time
        return stage

    def _create_mock_executor(self, executor_id, add_time=None, remove_time=None, cores=4, memory_mb=8192):
        """Helper to create mock executor data"""
        executor = MagicMock()
        executor.id = executor_id
        executor.host_port = f"executor-{executor_id}:7337"
        executor.add_time = add_time
        executor.remove_time = remove_time
        executor.total_cores = cores
        executor.max_memory = memory_mb * 1024 * 1024  # Convert MB to bytes
        return executor

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_stage_executor_timeline_basic_success(self, mock_get_client):
        """Test basic successful timeline comparison"""
        # Mock client
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Create mock stages
        start_time1 = datetime(2024, 1, 1, 10, 0, 0)
        end_time1 = datetime(2024, 1, 1, 10, 5, 0)
        start_time2 = datetime(2024, 1, 1, 11, 0, 0)
        end_time2 = datetime(2024, 1, 1, 11, 10, 0)

        stage1 = self._create_mock_stage(1, "Stage 1", start_time1, end_time1)
        stage2 = self._create_mock_stage(2, "Stage 2", start_time2, end_time2)

        # Create mock executors
        executor1_1 = self._create_mock_executor("1", start_time1, end_time1, cores=4, memory_mb=4096)
        executor1_2 = self._create_mock_executor("2", start_time1, end_time1, cores=4, memory_mb=4096)

        executor2_1 = self._create_mock_executor("1", start_time2, end_time2, cores=2, memory_mb=2048)
        executor2_2 = self._create_mock_executor("2", start_time2, end_time2, cores=2, memory_mb=2048)
        executor2_3 = self._create_mock_executor("3", start_time2, end_time2, cores=2, memory_mb=2048)

        # Mock client responses
        mock_client.get_stage_attempt.side_effect = [stage1, stage2]
        mock_client.list_all_executors.side_effect = [
            [executor1_1, executor1_2],
            [executor2_1, executor2_2, executor2_3]
        ]

        # Call the function
        result = compare_stage_executor_timeline("app-123", "app-456", 1, 2, interval_minutes=1)

        # Verify basic structure
        self.assertIn("app1_info", result)
        self.assertIn("app2_info", result)
        self.assertIn("timeline_comparison", result)
        self.assertIn("summary", result)

        # Verify app info
        self.assertEqual(result["app1_info"]["app_id"], "app-123")
        self.assertEqual(result["app2_info"]["app_id"], "app-456")

        # Verify timeline comparison has data
        self.assertGreater(len(result["timeline_comparison"]), 0)

        # Verify summary statistics
        self.assertIn("total_intervals", result["summary"])
        self.assertIn("intervals_with_executor_differences", result["summary"])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_stage_executor_timeline_different_intervals(self, mock_get_client):
        """Test timeline comparison with different interval settings"""
        # Mock client
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Create 10-minute stages
        start_time = datetime(2024, 1, 1, 10, 0, 0)
        end_time = datetime(2024, 1, 1, 10, 10, 0)

        stage1 = self._create_mock_stage(1, "Stage 1", start_time, end_time)
        stage2 = self._create_mock_stage(2, "Stage 2", start_time, end_time)

        # Create mock executors
        executor1 = self._create_mock_executor("1", start_time, end_time)
        executor2 = self._create_mock_executor("1", start_time, end_time)

        # Mock client responses
        mock_client.get_stage_attempt.side_effect = [stage1, stage2]
        mock_client.list_all_executors.side_effect = [[executor1], [executor2]]

        # Test with 2-minute intervals
        result = compare_stage_executor_timeline("app-123", "app-456", 1, 2, interval_minutes=2)

        # Should have 5 intervals (10 minutes / 2 minutes each)
        self.assertEqual(result["comparison_config"]["interval_minutes"], 2)
        expected_intervals = 5
        self.assertEqual(len(result["timeline_comparison"]), expected_intervals)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_stage_executor_timeline_no_submission_time(self, mock_get_client):
        """Test handling of stages without submission time"""
        # Mock client
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Create stage without submission time
        stage1 = self._create_mock_stage(1, "Stage 1", submission_time=None)
        stage2 = self._create_mock_stage(2, "Stage 2", submission_time=datetime(2024, 1, 1, 10, 0, 0))

        # Mock client responses
        mock_client.get_stage_attempt.side_effect = [stage1, stage2]
        mock_client.list_all_executors.side_effect = [[], []]

        # Call the function
        result = compare_stage_executor_timeline("app-123", "app-456", 1, 2)

        # Should handle the error gracefully
        self.assertIn("summary", result)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_stage_executor_timeline_executor_timing_variations(self, mock_get_client):
        """Test executor timeline with varying add/remove times"""
        # Mock client
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Create 5-minute stages
        start_time = datetime(2024, 1, 1, 10, 0, 0)
        end_time = datetime(2024, 1, 1, 10, 5, 0)

        stage1 = self._create_mock_stage(1, "Stage 1", start_time, end_time)
        stage2 = self._create_mock_stage(2, "Stage 2", start_time, end_time)

        # Create executors with different timing patterns
        # App1: executor added/removed during stage
        executor1_1 = self._create_mock_executor("1",
                                                datetime(2024, 1, 1, 10, 1, 0),  # Added 1 min after stage start
                                                datetime(2024, 1, 1, 10, 4, 0))  # Removed 1 min before stage end

        # App2: executor present for entire stage
        executor2_1 = self._create_mock_executor("1", start_time, end_time)

        # Mock client responses
        mock_client.get_stage_attempt.side_effect = [stage1, stage2]
        mock_client.list_all_executors.side_effect = [[executor1_1], [executor2_1]]

        # Call the function with 1-minute intervals
        result = compare_stage_executor_timeline("app-123", "app-456", 1, 2, interval_minutes=1)

        # Should show differences in executor allocation over time
        self.assertGreater(len(result["timeline_comparison"]), 0)

        # Check that some intervals show executor differences
        executor_diffs = [interval["differences"]["executor_count_diff"]
                         for interval in result["timeline_comparison"]]
        self.assertTrue(any(diff != 0 for diff in executor_diffs))

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_stage_executor_timeline_error_handling(self, mock_get_client):
        """Test error handling in compare_stage_executor_timeline"""
        # Mock client that raises an exception
        mock_client = MagicMock()
        mock_client.get_stage_attempt.side_effect = Exception("Client error")
        mock_get_client.return_value = mock_client

        # Call the function
        result = compare_stage_executor_timeline("app-123", "app-456", 1, 2)

        # Should return error information
        self.assertIn("error", result)
        self.assertIn("Client error", result["error"])
        self.assertEqual(result["app1_id"], "app-123")
        self.assertEqual(result["app2_id"], "app-456")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_stage_executor_timeline_uncompleted_stages(self, mock_get_client):
        """Test handling of uncompleted stages"""
        # Mock client
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Create stages - one completed, one not
        start_time = datetime(2024, 1, 1, 10, 0, 0)
        completed_time = datetime(2024, 1, 1, 10, 5, 0)

        stage1 = self._create_mock_stage(1, "Completed Stage", start_time, completed_time)
        stage2 = self._create_mock_stage(2, "Running Stage", start_time, completion_time=None)

        # Create mock executors
        executor1 = self._create_mock_executor("1", start_time, completed_time)
        executor2 = self._create_mock_executor("1", start_time, remove_time=None)  # Still running

        # Mock client responses
        mock_client.get_stage_attempt.side_effect = [stage1, stage2]
        mock_client.list_all_executors.side_effect = [[executor1], [executor2]]

        # Call the function
        result = compare_stage_executor_timeline("app-123", "app-456", 1, 2)

        # Should handle uncompleted stage (defaults to 24h duration)
        self.assertIn("timeline_comparison", result)
        self.assertFalse(result["summary"]["stages_overlap"])  # One stage incomplete


class TestCompareAppExecutorTimeline(unittest.TestCase):
    """Test cases for compare_app_executor_timeline function"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = MagicMock()

    def _create_mock_application(self, app_id, name, start_time, end_time):
        """Create a mock application with attempts"""
        app = MagicMock()
        app.id = app_id
        app.name = name

        # Mock attempts
        attempt = MagicMock()
        attempt.start_time = start_time
        attempt.end_time = end_time
        app.attempts = [attempt]

        return app

    def _create_mock_executor(self, executor_id, add_time, remove_time=None, cores=4, memory_bytes=4096*1024*1024):
        """Create a mock executor"""
        executor = MagicMock()
        executor.id = executor_id
        executor.add_time = add_time
        executor.remove_time = remove_time
        executor.total_cores = cores
        executor.max_memory = memory_bytes
        return executor

    def _create_mock_stage(self, stage_id, name, submission_time, completion_time=None):
        """Create a mock stage"""
        stage = MagicMock()
        stage.stage_id = stage_id
        stage.name = name
        stage.submission_time = submission_time
        stage.completion_time = completion_time
        return stage

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_app_executor_timeline_basic_success(self, mock_get_client):
        """Test basic successful application timeline comparison"""
        mock_get_client.return_value = self.mock_client

        # Create mock applications
        start_time1 = datetime(2024, 1, 1, 10, 0, 0)
        end_time1 = datetime(2024, 1, 1, 10, 30, 0)
        start_time2 = datetime(2024, 1, 1, 11, 0, 0)
        end_time2 = datetime(2024, 1, 1, 11, 20, 0)

        app1 = self._create_mock_application("app-123", "ETL Job v1", start_time1, end_time1)
        app2 = self._create_mock_application("app-456", "ETL Job v2", start_time2, end_time2)

        # Create mock executors
        executor1_1 = self._create_mock_executor("1", start_time1, end_time1, cores=4, memory_bytes=4096*1024*1024)
        executor1_2 = self._create_mock_executor("2", start_time1, end_time1, cores=4, memory_bytes=4096*1024*1024)

        executor2_1 = self._create_mock_executor("1", start_time2, end_time2, cores=8, memory_bytes=8192*1024*1024)
        executor2_2 = self._create_mock_executor("2", start_time2, end_time2, cores=8, memory_bytes=8192*1024*1024)
        executor2_3 = self._create_mock_executor("3", start_time2, end_time2, cores=8, memory_bytes=8192*1024*1024)

        # Create mock stages
        stage1_1 = self._create_mock_stage(1, "Stage 1-1", start_time1, start_time1 + timedelta(minutes=15))
        stage1_2 = self._create_mock_stage(2, "Stage 1-2", start_time1 + timedelta(minutes=10), end_time1)

        stage2_1 = self._create_mock_stage(1, "Stage 2-1", start_time2, start_time2 + timedelta(minutes=10))
        stage2_2 = self._create_mock_stage(2, "Stage 2-2", start_time2 + timedelta(minutes=5), end_time2)

        # Mock client responses
        self.mock_client.get_application.side_effect = [app1, app2]
        self.mock_client.list_all_executors.side_effect = [
            [executor1_1, executor1_2],
            [executor2_1, executor2_2, executor2_3]
        ]
        self.mock_client.list_stages.side_effect = [
            [stage1_1, stage1_2],
            [stage2_1, stage2_2]
        ]

        # Call the function
        result = compare_app_executor_timeline("app-123", "app-456", interval_minutes=5)

        # Verify the result structure
        self.assertIn("app1_info", result)
        self.assertIn("app2_info", result)
        self.assertIn("comparison_config", result)
        self.assertIn("timeline_comparison", result)
        self.assertIn("resource_efficiency", result)
        self.assertIn("summary", result)
        self.assertIn("recommendations", result)
        self.assertIn("key_differences", result)

        # Check app info
        self.assertEqual(result["app1_info"]["app_id"], "app-123")
        self.assertEqual(result["app1_info"]["name"], "ETL Job v1")
        self.assertEqual(result["app2_info"]["app_id"], "app-456")
        self.assertEqual(result["app2_info"]["name"], "ETL Job v2")

        # Check configuration
        self.assertEqual(result["comparison_config"]["interval_minutes"], 5)
        self.assertEqual(result["comparison_config"]["analysis_type"], "App-Level Executor Timeline Comparison")

        # Check that timeline comparison has data
        self.assertGreater(len(result["timeline_comparison"]), 0)

        # Check resource efficiency data
        self.assertIn("app1", result["resource_efficiency"])
        self.assertIn("app2", result["resource_efficiency"])
        self.assertIn("peak_executor_count", result["resource_efficiency"]["app1"])
        self.assertIn("avg_executor_count", result["resource_efficiency"]["app1"])

        # Verify client was called correctly
        self.mock_client.get_application.assert_any_call("app-123")
        self.mock_client.get_application.assert_any_call("app-456")
        self.mock_client.list_all_executors.assert_any_call(app_id="app-123")
        self.mock_client.list_all_executors.assert_any_call(app_id="app-456")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_app_executor_timeline_no_attempts(self, mock_get_client):
        """Test handling applications with no attempts"""
        mock_get_client.return_value = self.mock_client

        # Create mock applications without attempts
        app1 = MagicMock()
        app1.id = "app-123"
        app1.attempts = []

        app2 = MagicMock()
        app2.id = "app-456"
        app2.attempts = [MagicMock()]  # Only app2 has attempts

        self.mock_client.get_application.side_effect = [app1, app2]

        # Call the function
        result = compare_app_executor_timeline("app-123", "app-456")

        # Should return error
        self.assertIn("error", result)
        self.assertEqual(result["error"], "One or both applications have no attempts")
        self.assertIn("applications", result)
        self.assertFalse(result["applications"]["app1"]["has_attempts"])
        self.assertTrue(result["applications"]["app2"]["has_attempts"])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_app_executor_timeline_no_start_time(self, mock_get_client):
        """Test handling applications with no start time"""
        mock_get_client.return_value = self.mock_client

        # Create mock applications
        app1 = MagicMock()
        app1.id = "app-123"
        attempt1 = MagicMock()
        attempt1.start_time = None  # No start time
        attempt1.end_time = datetime(2024, 1, 1, 10, 30, 0)
        app1.attempts = [attempt1]

        app2 = self._create_mock_application("app-456", "App2",
                                             datetime(2024, 1, 1, 11, 0, 0),
                                             datetime(2024, 1, 1, 11, 20, 0))

        self.mock_client.get_application.side_effect = [app1, app2]
        self.mock_client.list_all_executors.side_effect = [[], []]
        self.mock_client.list_stages.side_effect = [[], []]

        # Call the function
        result = compare_app_executor_timeline("app-123", "app-456")

        # Should return error about building timeline
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Could not build timeline for one or both applications")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_app_executor_timeline_efficiency_analysis(self, mock_get_client):
        """Test efficiency analysis and recommendations"""
        mock_get_client.return_value = self.mock_client

        # Create applications with different efficiency patterns
        start_time1 = datetime(2024, 1, 1, 10, 0, 0)
        end_time1 = datetime(2024, 1, 1, 10, 60, 0)  # 1 hour
        start_time2 = datetime(2024, 1, 1, 11, 0, 0)
        end_time2 = datetime(2024, 1, 1, 11, 30, 0)  # 30 minutes (faster)

        app1 = self._create_mock_application("app-123", "Slow App", start_time1, end_time1)
        app2 = self._create_mock_application("app-456", "Fast App", start_time2, end_time2)

        # App1: Lower efficiency (few executors, long duration)
        executor1_1 = self._create_mock_executor("1", start_time1, end_time1, cores=2)
        executor1_2 = self._create_mock_executor("2", start_time1, end_time1, cores=2)

        # App2: Higher efficiency (more executors, shorter duration)
        executor2_1 = self._create_mock_executor("1", start_time2, end_time2, cores=4)
        executor2_2 = self._create_mock_executor("2", start_time2, end_time2, cores=4)
        executor2_3 = self._create_mock_executor("3", start_time2, end_time2, cores=4)
        executor2_4 = self._create_mock_executor("4", start_time2, end_time2, cores=4)

        # Mock client responses
        self.mock_client.get_application.side_effect = [app1, app2]
        self.mock_client.list_all_executors.side_effect = [
            [executor1_1, executor1_2],
            [executor2_1, executor2_2, executor2_3, executor2_4]
        ]
        self.mock_client.list_stages.side_effect = [[], []]

        # Call the function
        result = compare_app_executor_timeline("app-123", "app-456")

        # Check that recommendations were generated
        self.assertIn("recommendations", result)

        # Should detect performance difference (App1 takes longer)
        performance_recs = [r for r in result["recommendations"] if r.get("type") == "performance"]
        self.assertGreater(len(performance_recs), 0, "Should detect performance difference")

        # Should detect resource allocation difference
        resource_recs = [r for r in result["recommendations"] if r.get("type") == "resource_allocation"]

        # Check efficiency scores
        self.assertIn("efficiency_score", result["resource_efficiency"]["app1"])
        self.assertIn("efficiency_score", result["resource_efficiency"]["app2"])

        # Check key differences
        self.assertIn("duration_difference_seconds", result["key_differences"])
        self.assertLess(result["key_differences"]["duration_difference_seconds"], 0)  # App2 is faster

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_app_executor_timeline_different_intervals(self, mock_get_client):
        """Test with different interval settings"""
        mock_get_client.return_value = self.mock_client

        start_time = datetime(2024, 1, 1, 10, 0, 0)
        end_time = datetime(2024, 1, 1, 10, 10, 0)  # 10 minutes

        app1 = self._create_mock_application("app-123", "App1", start_time, end_time)
        app2 = self._create_mock_application("app-456", "App2", start_time, end_time)

        # Create simple executor setup
        executor1 = self._create_mock_executor("1", start_time, end_time)
        executor2 = self._create_mock_executor("1", start_time, end_time)

        self.mock_client.get_application.side_effect = [app1, app2]
        self.mock_client.list_all_executors.side_effect = [[executor1], [executor2]]
        self.mock_client.list_stages.side_effect = [[], []]

        # Test with 2-minute intervals
        result = compare_app_executor_timeline("app-123", "app-456", interval_minutes=2)

        # Should have fewer intervals (10 minutes / 2 minute intervals = 5 intervals)
        self.assertEqual(result["comparison_config"]["interval_minutes"], 2)
        self.assertLessEqual(len(result["timeline_comparison"]), 6)  # Allow for rounding

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_app_executor_timeline_error_handling(self, mock_get_client):
        """Test error handling in compare_app_executor_timeline"""
        mock_get_client.return_value = self.mock_client

        # Simulate an exception in get_application
        self.mock_client.get_application.side_effect = Exception("Connection failed")

        result = compare_app_executor_timeline("app-123", "app-456")

        # Should return error information
        self.assertIn("error", result)
        self.assertIn("Failed to compare app executor timelines", result["error"])
        self.assertEqual(result["app1_id"], "app-123")
        self.assertEqual(result["app2_id"], "app-456")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_dependency_from_sql_plan_success(self, mock_get_client):
        """Test successful stage dependency analysis from SQL plan"""
        mock_get_client.return_value = self.mock_client

        # Mock SQL execution data
        mock_execution = MagicMock()
        mock_execution.id = 1
        mock_execution.description = "Test SQL Query"
        mock_execution.status = "COMPLETED"
        mock_execution.duration = 10000
        mock_execution.success_job_ids = [1, 2]
        mock_execution.running_job_ids = []
        mock_execution.failed_job_ids = []

        # Mock job data
        job1 = MagicMock(spec=JobData)
        job1.job_id = 1
        job1.name = "Job 1"
        job1.status = "SUCCEEDED"
        job1.stage_ids = [1, 2]
        job1.submission_time = datetime(2024, 1, 1, 10, 0, 0)
        job1.completion_time = datetime(2024, 1, 1, 10, 2, 0)

        job2 = MagicMock(spec=JobData)
        job2.job_id = 2
        job2.name = "Job 2"
        job2.status = "SUCCEEDED"
        job2.stage_ids = [3, 4]
        job2.submission_time = datetime(2024, 1, 1, 10, 2, 30)
        job2.completion_time = datetime(2024, 1, 1, 10, 4, 0)

        # Mock stage data
        stage1 = MagicMock(spec=StageData)
        stage1.stage_id = 1
        stage1.name = "Stage 1"
        stage1.status = "COMPLETE"
        stage1.num_tasks = 10
        stage1.attempt_id = 0
        stage1.submission_time = datetime(2024, 1, 1, 10, 0, 0)
        stage1.completion_time = datetime(2024, 1, 1, 10, 1, 0)

        stage2 = MagicMock(spec=StageData)
        stage2.stage_id = 2
        stage2.name = "Stage 2"
        stage2.status = "COMPLETE"
        stage2.num_tasks = 8
        stage2.attempt_id = 0
        stage2.submission_time = datetime(2024, 1, 1, 10, 1, 30)
        stage2.completion_time = datetime(2024, 1, 1, 10, 2, 0)

        stage3 = MagicMock(spec=StageData)
        stage3.stage_id = 3
        stage3.name = "Stage 3"
        stage3.status = "COMPLETE"
        stage3.num_tasks = 12
        stage3.attempt_id = 0
        stage3.submission_time = datetime(2024, 1, 1, 10, 2, 30)
        stage3.completion_time = datetime(2024, 1, 1, 10, 3, 30)

        stage4 = MagicMock(spec=StageData)
        stage4.stage_id = 4
        stage4.name = "Stage 4"
        stage4.status = "COMPLETE"
        stage4.num_tasks = 6
        stage4.attempt_id = 0
        stage4.submission_time = datetime(2024, 1, 1, 10, 3, 45)
        stage4.completion_time = datetime(2024, 1, 1, 10, 4, 0)

        # Set up mock client responses
        self.mock_client.get_sql_execution.return_value = mock_execution
        self.mock_client.list_jobs.return_value = [job1, job2]
        self.mock_client.list_stages.return_value = [stage1, stage2, stage3, stage4]

        # Call the function
        result = get_stage_dependency_from_sql_plan("app-123", execution_id=1)

        # Verify the result structure
        self.assertEqual(result["app_id"], "app-123")
        self.assertEqual(result["execution_id"], 1)
        self.assertEqual(result["sql_description"], "Test SQL Query")
        self.assertEqual(result["execution_status"], "COMPLETED")
        self.assertEqual(result["total_jobs"], 2)
        self.assertEqual(result["total_stages"], 4)

        # Verify stage dependencies structure
        self.assertIn("stage_dependencies", result)
        self.assertIn("execution_timeline", result)
        self.assertIn("critical_path", result)
        self.assertIn("stage_job_mapping", result)
        self.assertIn("analysis_metadata", result)

        # Verify timeline is ordered chronologically
        timeline = result["execution_timeline"]
        self.assertEqual(len(timeline), 4)
        self.assertEqual(timeline[0]["stage_id"], 1)  # First stage
        self.assertEqual(timeline[-1]["stage_id"], 4)  # Last stage

        # Verify stage-job mapping
        mapping = result["stage_job_mapping"]
        self.assertEqual(len(mapping[1]), 1)  # Stage 1 in 1 job
        self.assertEqual(mapping[1][0]["job_id"], 1)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_dependency_from_sql_plan_no_execution_id(self, mock_get_client):
        """Test stage dependency analysis without specifying execution_id"""
        mock_get_client.return_value = self.mock_client

        # Mock multiple SQL executions
        execution1 = MagicMock()
        execution1.id = 1
        execution1.duration = 5000
        execution1.description = "Short Query"
        execution1.status = "COMPLETED"
        execution1.success_job_ids = [1]
        execution1.running_job_ids = []
        execution1.failed_job_ids = []

        execution2 = MagicMock()
        execution2.id = 2
        execution2.duration = 15000  # Longest duration
        execution2.description = "Long Query"
        execution2.status = "COMPLETED"
        execution2.success_job_ids = [2, 3]
        execution2.running_job_ids = []
        execution2.failed_job_ids = []

        self.mock_client.get_sql_list.return_value = [execution1, execution2]

        # Mock job and stage data for the longest execution
        job2 = MagicMock(spec=JobData)
        job2.job_id = 2
        job2.name = "Job 2"
        job2.status = "SUCCEEDED"
        job2.stage_ids = [2]
        job2.submission_time = datetime(2024, 1, 1, 10, 0, 0)
        job2.completion_time = datetime(2024, 1, 1, 10, 5, 0)

        stage2 = MagicMock(spec=StageData)
        stage2.stage_id = 2
        stage2.name = "Stage 2"
        stage2.status = "COMPLETE"
        stage2.num_tasks = 20
        stage2.attempt_id = 0
        stage2.submission_time = datetime(2024, 1, 1, 10, 0, 0)
        stage2.completion_time = datetime(2024, 1, 1, 10, 5, 0)

        self.mock_client.list_jobs.return_value = [job2]
        self.mock_client.list_stages.return_value = [stage2]

        # Call without execution_id - should pick longest duration execution
        result = get_stage_dependency_from_sql_plan("app-123")

        # Should have picked execution 2 (longest duration)
        self.assertEqual(result["execution_id"], 2)
        self.assertEqual(result["sql_description"], "Long Query")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_dependency_from_sql_plan_no_sql_executions(self, mock_get_client):
        """Test when no SQL executions are found"""
        mock_get_client.return_value = self.mock_client
        self.mock_client.get_sql_list.return_value = []

        result = get_stage_dependency_from_sql_plan("app-123")

        self.assertIn("error", result)
        self.assertEqual(result["error"], "No SQL executions found for application")
        self.assertEqual(result["app_id"], "app-123")
        self.assertEqual(result["stage_dependencies"], {})

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_dependency_from_sql_plan_no_jobs(self, mock_get_client):
        """Test when SQL execution has no associated jobs"""
        mock_get_client.return_value = self.mock_client

        # Mock execution with no job IDs
        mock_execution = MagicMock()
        mock_execution.id = 1
        mock_execution.description = "Query with no jobs"
        mock_execution.status = "FAILED"
        mock_execution.success_job_ids = []
        mock_execution.running_job_ids = []
        mock_execution.failed_job_ids = []

        self.mock_client.get_sql_execution.return_value = mock_execution

        result = get_stage_dependency_from_sql_plan("app-123", execution_id=1)

        self.assertIn("error", result)
        self.assertEqual(result["error"], "No jobs found for SQL execution")
        self.assertEqual(result["execution_id"], 1)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_dependency_from_sql_plan_exception_handling(self, mock_get_client):
        """Test exception handling in get_stage_dependency_from_sql_plan"""
        mock_get_client.return_value = self.mock_client

        # Simulate an exception
        self.mock_client.get_sql_execution.side_effect = Exception("API Error")

        result = get_stage_dependency_from_sql_plan("app-123", execution_id=1)

        self.assertIn("error", result)
        self.assertIn("Failed to analyze stage dependencies", result["error"])
        self.assertEqual(result["execution_id"], 1)
