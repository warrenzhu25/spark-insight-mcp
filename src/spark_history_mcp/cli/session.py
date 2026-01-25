"""
Session management for CLI numbered app references.

Provides functions to save and load numbered app references from apps list
so users can use shorthand like `1`, `2` in subsequent commands.
"""

import json
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

# Session timeout in seconds (1 hour)
SESSION_TIMEOUT_SECONDS = 3600


def get_session_dir() -> Path:
    """Get the session directory for storing CLI state."""
    config_dir = Path.home() / ".config" / "spark-history-mcp"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def get_app_refs_file() -> Path:
    """Get path to the app references session file."""
    return get_session_dir() / "app-refs-session.json"


def save_app_refs(app_mapping: Dict[int, str], server: Optional[str] = None) -> None:
    """
    Save app references mapping to session file.

    Args:
        app_mapping: Dictionary mapping numbers (1, 2, 3...) to app IDs
        server: Optional server name used for the apps
    """
    session_file = get_app_refs_file()
    session_data = {
        "app_mapping": {str(k): v for k, v in app_mapping.items()},
        "server": server,
        "timestamp": time.time(),
    }
    with open(session_file, "w") as f:
        json.dump(session_data, f, indent=2)


def load_app_refs() -> Optional[Tuple[Dict[int, str], Optional[str]]]:
    """
    Load app references from session file.

    Returns:
        Tuple of (app_mapping, server) if valid session exists and not stale,
        None if session doesn't exist or is stale (>1 hour old)
    """
    session_file = get_app_refs_file()
    if not session_file.exists():
        return None

    try:
        with open(session_file, "r") as f:
            session_data = json.load(f)

        # Check for staleness
        timestamp = session_data.get("timestamp", 0)
        if time.time() - timestamp > SESSION_TIMEOUT_SECONDS:
            return None

        # Convert string keys back to int
        app_mapping = {
            int(k): v for k, v in session_data.get("app_mapping", {}).items()
        }
        server = session_data.get("server")

        return app_mapping, server

    except (json.JSONDecodeError, KeyError, ValueError):
        return None


def resolve_number_ref(num: int) -> Optional[str]:
    """
    Resolve a number reference to an app ID.

    Args:
        num: The number reference (1, 2, 3...)

    Returns:
        The app ID if found, None otherwise
    """
    result = load_app_refs()
    if result is None:
        return None

    app_mapping, _ = result
    return app_mapping.get(num)


def is_number_ref(identifier: str) -> bool:
    """
    Check if an identifier is a valid number reference.

    Valid number refs: "1", "2", "10", etc.
    Not valid: "0", "-1", "01", "1a", "app-1", empty string

    Args:
        identifier: The identifier to check

    Returns:
        True if identifier is a positive integer string without leading zeros
    """
    if not identifier:
        return False

    # Must be all digits
    if not identifier.isdigit():
        return False

    # Must be a positive integer (no leading zeros except for "0" itself)
    if len(identifier) > 1 and identifier[0] == "0":
        return False

    # Must be positive (not zero)
    try:
        num = int(identifier)
        return num > 0
    except ValueError:
        return False


def format_session_hint(count: int) -> str:
    """
    Format a hint message about using number references.

    Args:
        count: Number of apps saved in session

    Returns:
        Formatted hint string
    """
    if count == 0:
        return ""

    if count == 1:
        return "\nTip: Use number 1 to reference this app. Example: apps show 1"

    return (
        f"\nTip: Use numbers 1-{count} to reference these apps. "
        f"Example: apps compare 1 2"
    )


def clear_app_refs() -> None:
    """Clear the app references session file."""
    session_file = get_app_refs_file()
    if session_file.exists():
        session_file.unlink()
