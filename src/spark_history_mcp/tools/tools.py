"""Legacy monolithic tools file for backward compatibility.

This module re-exports all tools from the new modular structure to maintain
backward compatibility with existing tests and imports.
"""

# Import everything from the modular tools
from .analysis import *  # noqa: F401,F403
from .application import *  # noqa: F401,F403
from .comparisons import *  # noqa: F401,F403
from .comparisons import (  # noqa: F401
    _compare_environments,
    _compare_sql_execution_plans,
)
from .executors import *  # noqa: F401,F403
from .jobs_stages import *  # noqa: F401,F403
from .jobs_stages import _build_dependencies_from_dag_data  # noqa: F401

# Import common utilities that might be used by tests
from .common import get_client_or_default, get_config  # noqa: F401

# Import the MCP app instance for tests that patch it
from ..core.app import mcp  # noqa: F401
