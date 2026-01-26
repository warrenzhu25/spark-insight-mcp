"""
Modular formatter components for Spark History Server MCP CLI.

This package contains the formatter modules that were extracted from the monolithic
formatters.py file to improve code organization and maintainability:

- base.py: Core OutputFormatter class and basic infrastructure
- basic.py: Basic data formatting (applications, jobs, stages, lists)
- comparison.py: Comparison result formatting methods
- utils.py: Utility functions (metric formatters, display names, progress)

The original formatters.py file is preserved unchanged to maintain backward compatibility.
New development should use these modular components where appropriate.

For Phase 2 of refactoring, the modules provide a cleaner structure while delegating
complex operations to the original implementation to avoid circular imports.
"""

from .base import OutputFormatter
from .utils import create_progress

# Export main classes for use
__all__ = ["OutputFormatter", "create_progress"]
