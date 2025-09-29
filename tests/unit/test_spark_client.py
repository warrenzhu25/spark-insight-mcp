import unittest
from unittest.mock import MagicMock, patch

import requests

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import ServerConfig


class TestSparkClient(unittest.TestCase):
    def setUp(self):
        self.server_config = ServerConfig(url="http://spark-history-server:18080")
        self.client = SparkRestClient(self.server_config)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_list_applications(self, mock_get):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": "app-20230101123456-0001",
                "name": "Test Spark App",
                "coresGranted": 8,
                "maxCores": 16,
                "coresPerExecutor": 2,
                "memoryPerExecutorMB": 4096,
                "attempts": [
                    {
                        "attemptId": "1",
                        "startTime": "2023-01-01T12:34:56.789GMT",
                        "endTime": "2023-01-01T13:34:56.789GMT",
                        "lastUpdated": "2023-01-01T13:34:56.789GMT",
                        "duration": 3600000,
                        "sparkUser": "spark",
                        "appSparkVersion": "3.3.0",
                        "completed": True,
                    }
                ],
            }
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Call the method
        apps = self.client.list_applications(status=["COMPLETED"], limit=10)

        mock_get.assert_called_once_with(
            "http://spark-history-server:18080/api/v1/applications",
            params={"status": ["COMPLETED"], "limit": 10},
            headers={"Accept": "application/json"},
            auth=None,
            timeout=30,
            verify=True,
            proxies=None,
        )

        self.assertEqual(len(apps), 1)
        self.assertEqual(apps[0].id, "app-20230101123456-0001")
        self.assertEqual(apps[0].name, "Test Spark App")
        self.assertEqual(apps[0].cores_granted, 8)
        self.assertEqual(len(apps[0].attempts), 1)
        self.assertEqual(apps[0].attempts[0].attempt_id, "1")
        self.assertEqual(apps[0].attempts[0].spark_user, "spark")
        self.assertTrue(apps[0].attempts[0].completed)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_list_applications_with_filters(self, mock_get):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": "app-20230101123456-0001",
                "name": "Test Spark App",
                "attempts": [
                    {
                        "attemptId": "1",
                        "startTime": "2023-01-01T12:34:56.789GMT",
                        "endTime": "2023-01-01T13:34:56.789GMT",
                        "lastUpdated": "2023-01-01T13:34:56.789GMT",
                        "duration": 3600000,
                        "sparkUser": "spark",
                        "completed": True,
                    }
                ],
            }
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Call the method with various filters
        apps = self.client.list_applications(
            status=["COMPLETED"], min_date="2023-01-01", max_date="2023-01-02", limit=5
        )

        # Assertions
        mock_get.assert_called_once_with(
            "http://spark-history-server:18080/api/v1/applications",
            params={
                "status": ["COMPLETED"],
                "minDate": "2023-01-01",
                "maxDate": "2023-01-02",
                "limit": 5,
            },
            headers={"Accept": "application/json"},
            auth=None,
            timeout=30,
            verify=True,
            proxies=None,
        )

        self.assertEqual(len(apps), 1)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_list_applications_empty_response(self, mock_get):
        # Setup mock response with empty list
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Call the method
        apps = self.client.list_applications()

        # Assertions
        mock_get.assert_called_once()
        self.assertEqual(len(apps), 0)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_fallback_behavior(self, mock_get):
        # First request fails with 404
        error_response = MagicMock()
        error_response.status_code = 404
        error_response.text = "no such app"
        http_error = requests.exceptions.HTTPError(response=error_response)
        error_response.raise_for_status.side_effect = http_error

        # Second request succeeds
        success_response = MagicMock()
        success_response.json.return_value = {"key": "value"}
        success_response.raise_for_status.return_value = None

        # Configure mock to return different responses
        mock_get.side_effect = [error_response, success_response]

        # Call method that should trigger EMR fallback
        result = self.client._get("applications/app-123/jobs")

        # Verify both URLs were tried
        mock_get.assert_any_call(
            "http://spark-history-server:18080/api/v1/applications/app-123/jobs",
            params=None,
            headers={"Accept": "application/json"},
            auth=None,
            timeout=30,
            verify=True,
            proxies=self.client.proxies,  # Use actual proxies value
        )
        mock_get.assert_any_call(
            "http://spark-history-server:18080/api/v1/applications/app-123/1/jobs",
            params=None,
            headers={"Accept": "application/json"},
            auth=None,
            timeout=30,
            verify=True,
            proxies=self.client.proxies,  # Use actual proxies value
        )

        # Verify we got the success response
        self.assertEqual(result, {"key": "value"})

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_fallback_fail(self, mock_get):
        # Create 404 response
        error_response = MagicMock()
        error_response.status_code = 404
        error_response.text = "no such app"
        http_error = requests.exceptions.HTTPError(response=error_response)
        error_response.raise_for_status.side_effect = http_error

        # Both requests fail
        mock_get.side_effect = [error_response, error_response]

        # Call method and expect exception
        with self.assertRaises(requests.exceptions.HTTPError):
            self.client._get("applications/app-123/jobs")

        # Verify both URLs were tried
        self.assertEqual(mock_get.call_count, 2)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_proxy_configuration(self, mock_get):
        # Test with proxy enabled
        client = SparkRestClient(
            ServerConfig(url="http://spark-history-server:18080", use_proxy=True)
        )
        self.assertEqual(
            client.proxies,
            {"http": "socks5h://localhost:8157", "https": "socks5h://localhost:8157"},
        )

        # Test with proxy disabled
        client = SparkRestClient(
            ServerConfig(url="http://spark-history-server:18080", use_proxy=False)
        )
        self.assertIsNone(client.proxies)

    def test_url_modification(self):
        """Test the URL modification logic for different URL patterns"""
        test_cases = [
            # Test case 1: Standard URL that can be modified
            {
                "input": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/allexecutors",
                "expected": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/1/allexecutors",
            },
            # Test case 2: URL that already has an attempt number (should not be modified)
            {
                "input": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/2/allexecutors",
                "expected": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/2/allexecutors",
            },
            # Test case 3: URL without applications path (should not be modified)
            {
                "input": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/metrics",
                "expected": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/metrics",
            },
            # Test case 4: URL with another endpoint after application ID
            {
                "input": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/stages",
                "expected": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/1/stages",
            },
        ]

        for test_case in test_cases:
            input_url = test_case["input"]
            expected_url = test_case["expected"]
            modified_url = self.client._modify_url(input_url)
            self.assertEqual(
                modified_url,
                expected_url,
                f"Failed to correctly modify URL.\nInput: {input_url}\nExpected: {expected_url}\nGot: {modified_url}",
            )

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_cache_get_application(self, mock_get):
        """Test that get_application caches results."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": "app-123",
            "name": "Test App",
            "attempts": [
                {
                    "attemptId": "1",
                    "startTime": "2023-01-01T12:34:56.789GMT",
                    "endTime": "2023-01-01T13:34:56.789GMT",
                    "lastUpdated": "2023-01-01T13:34:56.789GMT",
                    "duration": 3600000,
                    "sparkUser": "spark",
                    "completed": True,
                }
            ],
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Clear cache before test
        self.client.clear_cache()

        # First call - should hit the API
        app1 = self.client.get_application("app-123")
        self.assertEqual(app1.id, "app-123")
        self.assertEqual(mock_get.call_count, 1)

        # Second call with same app_id - should use cache
        app2 = self.client.get_application("app-123")
        self.assertEqual(app2.id, "app-123")
        self.assertEqual(mock_get.call_count, 1)  # Still 1, not 2

        # Verify both returns are the same object (cached)
        self.assertIs(app1, app2)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_cache_list_jobs(self, mock_get):
        """Test that list_jobs caches results with tuple conversion."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "jobId": 1,
                "name": "Job 1",
                "status": "SUCCEEDED",
                "numTasks": 10,
                "numActiveTasks": 0,
                "numCompletedTasks": 10,
                "numSkippedTasks": 0,
                "numFailedTasks": 0,
                "numKilledTasks": 0,
                "numCompletedIndices": 10,
                "numActiveStages": 0,
                "numCompletedStages": 1,
                "numSkippedStages": 0,
                "numFailedStages": 0,
                "stageIds": [1],
            }
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Clear cache before test
        self.client.clear_cache()

        from spark_history_mcp.models.spark_types import JobExecutionStatus

        # First call
        jobs1 = self.client.list_jobs("app-123", status=[JobExecutionStatus.SUCCEEDED])
        self.assertEqual(len(jobs1), 1)
        self.assertEqual(mock_get.call_count, 1)

        # Second call with same parameters - should use cache
        jobs2 = self.client.list_jobs("app-123", status=[JobExecutionStatus.SUCCEEDED])
        self.assertEqual(len(jobs2), 1)
        self.assertEqual(mock_get.call_count, 1)  # Still 1

    def test_clear_cache(self):
        """Test that clear_cache clears all cached methods."""
        # Clear and get initial cache info
        self.client.clear_cache()
        cache_info = self.client.cache_info()

        # All methods should have 0 hits and 0 cache size
        for method_name, info in cache_info.items():
            self.assertEqual(info["hits"], 0, f"{method_name} should have 0 hits")
            self.assertEqual(
                info["currsize"], 0, f"{method_name} should have 0 cache size"
            )

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_cache_info(self, mock_get):
        """Test that cache_info returns correct statistics."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": "app-123",
            "name": "Test App",
            "attempts": [
                {
                    "attemptId": "1",
                    "startTime": "2023-01-01T12:34:56.789GMT",
                    "endTime": "2023-01-01T13:34:56.789GMT",
                    "lastUpdated": "2023-01-01T13:34:56.789GMT",
                    "duration": 3600000,
                    "sparkUser": "spark",
                    "completed": True,
                }
            ],
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Clear cache
        self.client.clear_cache()

        # Make some cached calls
        self.client.get_application("app-123")
        self.client.get_application("app-123")  # Cache hit
        self.client.get_application("app-456")  # Cache miss

        # Get cache info
        cache_info = self.client.cache_info()

        # Verify get_application has correct stats
        app_cache = cache_info["get_application"]
        self.assertEqual(app_cache["hits"], 1)  # One hit on second call
        self.assertEqual(app_cache["misses"], 2)  # Two misses (app-123 first, app-456)
        self.assertEqual(app_cache["currsize"], 2)  # Two items cached
        self.assertEqual(app_cache["maxsize"], 1000)  # Default maxsize
