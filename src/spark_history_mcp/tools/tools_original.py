"""Legacy tool implementations retained for backwards compatibility.

This module re-exports the refactored tool functions that now live in
``spark_history_mcp.tools``. Importing from here continues to work for older
callers while the new modules host the actual implementations.
"""

from spark_history_mcp.tools import analysis as _analysis
from spark_history_mcp.tools import application as _application
from spark_history_mcp.tools import comparisons as _comparisons
from spark_history_mcp.tools import executors as _executors
from spark_history_mcp.tools import jobs_stages as _jobs_stages
from spark_history_mcp.tools.analysis import *  # noqa: F401,F403
from spark_history_mcp.tools.application import *  # noqa: F401,F403
from spark_history_mcp.tools.comparisons import *  # noqa: F401,F403
from spark_history_mcp.tools.executors import *  # noqa: F401,F403
from spark_history_mcp.tools.jobs_stages import *  # noqa: F401,F403

__all__ = []
for _module in (
    _application,
    _jobs_stages,
    _executors,
    _analysis,
    _comparisons,
):
    __all__.extend(getattr(_module, "__all__", []))
