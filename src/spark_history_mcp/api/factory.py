"""
Factory for creating Spark REST clients.
"""

from spark_history_mcp.api.emr_persistent_ui_client import EMRPersistentUIClient
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import ServerConfig


def create_spark_client(server_config: ServerConfig) -> SparkRestClient:
    """
    Create and initialize a Spark REST client based on configuration.

    Handles both standard Spark History Servers and EMR Persistent UIs.

    Args:
        server_config: Configuration for the server

    Returns:
        An initialized SparkRestClient
    """
    if server_config.emr_cluster_arn:
        # Create EMR client
        emr_client = EMRPersistentUIClient(server_config)

        # Initialize EMR client (create persistent UI, get presigned URL, setup session)
        base_url, session = emr_client.initialize()

        # Create a modified server config with the base URL
        emr_server_config = server_config.model_copy()
        emr_server_config.url = base_url

        # Create SparkRestClient with the session
        spark_client = SparkRestClient(emr_server_config)
        spark_client.session = session  # Use the authenticated session

        return spark_client
    else:
        # Regular Spark REST client
        return SparkRestClient(server_config)
