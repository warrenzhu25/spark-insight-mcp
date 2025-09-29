"""Legacy tool implementations retained for backwards compatibility.

This module re-exports the refactored tool functions that now live in
``spark_history_mcp.tools``. Importing from here continues to work for older
callers while the new modules host the actual implementations.
"""

from spark_history_mcp.tools.analysis import *  # noqa: F401,F403
from spark_history_mcp.tools.application import *  # noqa: F401,F403
from spark_history_mcp.tools.comparisons import *  # noqa: F401,F403
from spark_history_mcp.tools.executors import *  # noqa: F401,F403
from spark_history_mcp.tools.jobs_stages import *  # noqa: F401,F403

# Populate __all__ with anything we just re-exported (skip helper modules)
__all__ = [
    name
    for name in globals()
    if not name.startswith("_") and name not in {"typing", "mcp"}
]
