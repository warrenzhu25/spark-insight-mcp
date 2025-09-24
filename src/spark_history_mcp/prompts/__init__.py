"""
Spark History Server MCP Prompts

This module provides reusable prompt templates for analyzing Spark applications.
These prompts guide AI agents in performing structured analysis and generating
comprehensive insights from Spark History Server data.

Prompt Categories:
- Performance Analysis: Analyze slow applications, investigate bottlenecks
- Troubleshooting: Debug failures, memory issues, configuration problems
- Optimization: Resource allocation, auto-scaling, query performance
- Reporting: Performance reports, executive summaries, trend analysis
"""

from spark_history_mcp.core.app import mcp

# Import all prompt modules to register them with FastMCP
from spark_history_mcp.prompts import (
    performance,
    troubleshooting,
    optimization,
    reporting,
)

__all__ = [
    "performance",
    "troubleshooting",
    "optimization",
    "reporting",
]