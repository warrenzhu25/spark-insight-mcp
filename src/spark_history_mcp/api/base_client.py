"""
Base client for Spark History Server API.
"""

from typing import Any, Dict, Optional

import requests

from spark_history_mcp.config.config import ServerConfig


class BaseApiClient:
    """Base class for API clients handling sessions and requests."""

    def __init__(self, server_config: ServerConfig):
        self.config = server_config
        self.timeout = server_config.timeout
        self.verify_ssl = server_config.verify_ssl
        self.session = requests.Session()

        # Configure proxies if available
        if hasattr(server_config, "use_proxy") and server_config.use_proxy:
            proxy_url = server_config.proxy_url
            self.session.proxies = {"http": proxy_url, "https": proxy_url}

    def _make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> requests.Response:
        """Make an HTTP request using the session."""
        if headers is None:
            headers = {}

        # Default JSON accept header
        if "Accept" not in headers:
            headers["Accept"] = "application/json"

        # Add auth token if available
        if self.config.auth and self.config.auth.token:
            headers["Authorization"] = f"Bearer {self.config.auth.token}"

        response = self.session.request(
            method=method,
            url=url,
            params=params,
            headers=headers,
            timeout=self.timeout,
            verify=self.verify_ssl,
            **kwargs
        )
        return response
