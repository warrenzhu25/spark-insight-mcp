"""
Disk-based cache for Spark History Server API responses.

Stores JSON files in ~/.cache/spark-history-mcp/ keyed by SHA256 hash.
History server data for completed apps is immutable, making disk caching safe.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, Optional, Tuple

CACHE_DIR = Path.home() / ".cache" / "spark-history-mcp"


def _key_to_path(key: Tuple[Any, ...]) -> Path:
    raw = json.dumps(key, sort_keys=True, default=str)
    h = hashlib.sha256(raw.encode()).hexdigest()
    return CACHE_DIR / f"{h}.json"


def disk_get(key: Tuple[Any, ...]) -> Optional[str]:
    """Read cached JSON string for *key*, or None if missing."""
    path = _key_to_path(key)
    try:
        return path.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return None


def disk_set(key: Tuple[Any, ...], data: str) -> None:
    """Write *data* (JSON string) to disk for *key*."""
    path = _key_to_path(key)
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(data, encoding="utf-8")
    except OSError:
        pass  # non-fatal; in-process cache still works


def clear_cache() -> int:
    """Remove all cached files. Returns the number of files removed."""
    if not CACHE_DIR.exists():
        return 0
    count = sum(1 for _ in CACHE_DIR.glob("*.json"))
    shutil.rmtree(CACHE_DIR, ignore_errors=True)
    return count
