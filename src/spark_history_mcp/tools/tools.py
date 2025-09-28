"""Legacy monolithic tools file for backward compatibility.

This module re-exports all tools from the new modular structure to maintain
backward compatibility with existing tests and imports.
"""

# Import everything from the modular tools
from spark_history_mcp.tools.analysis import *  # noqa: F401,F403
from spark_history_mcp.tools.application import *  # noqa: F401,F403
from spark_history_mcp.tools.comparisons import *  # noqa: F401,F403
from spark_history_mcp.tools.executors import *  # noqa: F401,F403
from spark_history_mcp.tools.jobs_stages import *  # noqa: F401,F403

# Import common utilities that might be used by tests
from spark_history_mcp.tools.common import get_client_or_default, get_config  # noqa: F401

# Import the MCP app instance for tests that patch it
from spark_history_mcp.core.app import mcp  # noqa: F401