"""
Backward-compatible entry point for CLI formatters.

This module now exports the refactored, modular OutputFormatter from the
formatter_modules package. All new formatting logic should be added there.
"""

from .formatter_modules import OutputFormatter, create_progress
from .formatter_modules.comparison import console

__all__ = ["OutputFormatter", "create_progress", "console"]
