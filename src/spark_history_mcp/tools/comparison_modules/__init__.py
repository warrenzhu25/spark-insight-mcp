"""
Application comparison tools for MCP server.

This package contains the refactored comparison functionality organized into focused modules:

- core: Main comparison functions (compare_app_performance, compare_app_summaries)
- stages: Stage-specific comparisons (find_top_stage_differences, compare_stages)
- executors: Executor and timeline comparisons
- environment: Resource and environment comparisons
- utils: Common utilities and helper functions

The original tools/comparisons.py file maintains backward compatibility by re-exporting
the original monolithic implementation.
"""

# This package provides the modular refactored implementation
# The original tools/comparisons.py imports from here as needed for backward compatibility
