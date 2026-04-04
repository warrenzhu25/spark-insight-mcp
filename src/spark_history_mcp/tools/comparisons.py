"""
Legacy comparison tools file for backward compatibility.

This module now re-exports all comparison functionality from the modularized
comparison_modules package. All new development should occur in those modules.
"""

# Re-export all functions from modular sub-packages
from .comparison_modules.core import *  # noqa: F401,F403
from .comparison_modules.environment import *  # noqa: F401,F403
from .comparison_modules.executors import *  # noqa: F401,F403
from .comparison_modules.stages import *  # noqa: F401,F403
from .comparison_modules.utils import (  # noqa: F401
    _compare_environments,
    _compare_sql_execution_plans,
    calculate_safe_ratio,
    filter_significant_metrics,
    sort_comparison_data,
)
