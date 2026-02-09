"""
Cleanup tools for managing Spark event logs.

Provides MCP tools for identifying and deleting event log files from GCS
for short-running or name-matched Spark applications.
"""

import fnmatch
import logging
import re
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from ..core.app import mcp
from .common import get_client_or_default

logger = logging.getLogger(__name__)

_DURATION_UNITS = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}
_DURATION_RE = re.compile(r"^(\d+)([smhd])$")
_GCS_DELETE_BATCH_SIZE = 500


def _parse_duration_to_ms(duration_str: str) -> int:
    """Parse a duration string like '30s', '5m', '2h', '1d' to milliseconds.

    Raises:
        ValueError: If the duration string is not in a valid format.
    """
    match = _DURATION_RE.match(duration_str.strip())
    if not match:
        raise ValueError(
            f"Invalid duration format: '{duration_str}'. "
            "Use a number followed by s/m/h/d (e.g. '30s', '5m', '2h', '1d')."
        )
    value = int(match.group(1))
    unit = match.group(2)
    return value * _DURATION_UNITS[unit]


def _format_duration_human(ms: int) -> str:
    """Convert milliseconds to a human-readable string like '2m 30s'."""
    if ms < 0:
        return "0s"
    total_seconds = ms // 1000
    if total_seconds < 60:
        return f"{total_seconds}s"
    if total_seconds < 3600:
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes}m {seconds}s" if seconds else f"{minutes}m"
    if total_seconds < 86400:
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    return f"{days}d {hours}h" if hours else f"{days}d"


def _gcs_list_files(gcs_dir: str) -> List[str]:
    """List files in a GCS directory using gcloud storage.

    Raises:
        RuntimeError: If gcloud is not found, access is denied, or the command fails.
    """
    gcs_dir = gcs_dir.rstrip("/")
    try:
        result = subprocess.run(  # noqa: S603
            ["gcloud", "storage", "ls", f"{gcs_dir}/"],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=120,
        )
    except FileNotFoundError as e:
        raise RuntimeError(
            "gcloud CLI not found. Install the Google Cloud SDK: "
            "https://cloud.google.com/sdk/docs/install"
        ) from e
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(
            f"Timed out listing GCS files in {gcs_dir} (120s timeout)"
        ) from e

    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to list GCS files in {gcs_dir}: {result.stderr.strip()}"
        )

    return [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]


def _gcs_delete_files(file_paths: List[str]) -> Tuple[int, List[str]]:
    """Delete files from GCS using gcloud storage, batching to avoid arg limits.

    Returns:
        Tuple of (deleted_count, list of error messages).
    """
    deleted = 0
    errors: List[str] = []

    for i in range(0, len(file_paths), _GCS_DELETE_BATCH_SIZE):
        batch = file_paths[i : i + _GCS_DELETE_BATCH_SIZE]
        try:
            result = subprocess.run(  # noqa: S603, S607
                ["gcloud", "storage", "rm"] + batch,
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode == 0:
                deleted += len(batch)
            else:
                errors.append(
                    f"Batch {i // _GCS_DELETE_BATCH_SIZE + 1} failed: "
                    f"{result.stderr.strip()}"
                )
        except FileNotFoundError:
            errors.append("gcloud CLI not found")
            break
        except subprocess.TimeoutExpired:
            errors.append(f"Batch {i // _GCS_DELETE_BATCH_SIZE + 1} timed out (300s)")

    return deleted, errors


def _match_gcs_files(
    app_ids: List[str], gcs_files: List[str]
) -> Tuple[List[str], List[str]]:
    """Match GCS file paths to application IDs.

    For each app ID, finds GCS files containing that ID as a substring.
    Handles attempt suffixes (_1, _2) and extensions (.zstd, .lz4, etc.).

    Returns:
        Tuple of (matched_file_paths, app_ids_not_found_in_gcs).
    """
    matched_files: List[str] = []
    not_found: List[str] = []

    for app_id in app_ids:
        found = [f for f in gcs_files if app_id in f]
        if found:
            matched_files.extend(found)
        else:
            not_found.append(app_id)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_files: List[str] = []
    for f in matched_files:
        if f not in seen:
            seen.add(f)
            unique_files.append(f)

    return unique_files, not_found


@mcp.tool()
def delete_event_logs(
    gcs_dir: str,
    server: Optional[str] = None,
    max_duration: Optional[str] = None,
    name_pattern: Optional[str] = None,
    limit: int = 1000,
    dry_run: bool = True,
) -> Dict[str, Any]:
    """
    Delete Spark event logs from GCS for short-running or name-matched applications.

    Identifies applications from the Spark History Server matching the given filters,
    finds their corresponding event log files in GCS, and deletes them.

    SAFETY: dry_run defaults to True. Set dry_run=False to actually delete files.

    At least one of max_duration or name_pattern must be provided.

    Args:
        gcs_dir: GCS directory containing event logs (must start with 'gs://')
        server: Optional server name to use (uses default if not specified)
        max_duration: Delete logs for apps shorter than this duration (e.g. '30s', '5m', '2h', '1d')
        name_pattern: Delete logs matching this app name pattern (glob-style, e.g. 'my_etl_*')
        limit: Max number of applications to fetch from SHS (default: 1000)
        dry_run: If True (default), only report what would be deleted without deleting

    Returns:
        Dictionary with matched applications, GCS files, and deletion results
    """
    # Validate inputs
    if not gcs_dir.startswith("gs://"):
        raise ValueError(f"gcs_dir must start with 'gs://', got: '{gcs_dir}'")

    if not max_duration and not name_pattern:
        raise ValueError(
            "At least one of max_duration or name_pattern must be provided"
        )

    max_duration_ms: Optional[int] = None
    if max_duration:
        max_duration_ms = _parse_duration_to_ms(max_duration)

    # Get applications from Spark History Server
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server)
    applications = client.list_applications(limit=limit)

    # Filter applications by duration and/or name pattern (AND logic)
    matched_apps: List[Dict[str, Any]] = []
    for app in applications:
        attempts = list(getattr(app, "attempts", None) or [])
        latest = attempts[-1] if attempts else None
        duration_ms = getattr(latest, "duration", 0) or 0 if latest else 0
        app_name = getattr(app, "name", "") or ""

        # Duration filter: app ran LESS than max_duration
        if max_duration_ms is not None:
            if duration_ms >= max_duration_ms:
                continue

        # Name filter: glob match (case-insensitive)
        if name_pattern:
            if not fnmatch.fnmatch(app_name.lower(), name_pattern.lower()):
                continue

        matched_apps.append(
            {
                "app_id": app.id,
                "name": app_name,
                "duration_ms": duration_ms,
                "duration_human": _format_duration_human(duration_ms),
            }
        )

    # List GCS files
    gcs_files = _gcs_list_files(gcs_dir)

    # Match files to app IDs
    app_ids = [a["app_id"] for a in matched_apps]
    matched_gcs_files, not_found_ids = _match_gcs_files(app_ids, gcs_files)

    result: Dict[str, Any] = {
        "action": "dry_run" if dry_run else "deleted",
        "gcs_dir": gcs_dir,
        "filters": {
            "max_duration": max_duration,
            "name_pattern": name_pattern,
        },
        "matched_applications": matched_apps,
        "gcs_files_matched": matched_gcs_files,
        "gcs_files_not_found": not_found_ids,
        "summary": {
            "total_apps_scanned": len(applications),
            "apps_matched": len(matched_apps),
            "gcs_files_found": len(matched_gcs_files),
            "gcs_files_deleted": 0,
        },
        "errors": [],
    }

    if not dry_run and matched_gcs_files:
        deleted_count, errors = _gcs_delete_files(matched_gcs_files)
        result["summary"]["gcs_files_deleted"] = deleted_count
        result["errors"] = errors

    return result
