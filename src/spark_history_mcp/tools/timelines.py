"""
Timeline helpers to build stage-level and app-level executor timelines
and to merge consecutive intervals with identical diffs.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, List

from .common import DEFAULT_INTERVAL_MINUTES, MAX_INTERVALS


def merge_consecutive_intervals(comparison_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge consecutive entries that have the same executor_count_diff.

    Expects each entry to have "differences" with key "executor_count_diff"
    and interval start/end fields.
    """
    if not comparison_data:
        return []

    merged: List[Dict[str, Any]] = []
    current = comparison_data[0].copy()

    for entry in comparison_data[1:]:
        if (
            entry.get("differences", {}).get("executor_count_diff")
            == current.get("differences", {}).get("executor_count_diff")
        ):
            # Extend current time range to this entry's end
            current["timestamp_range"] = f"{current['timestamp_range'].split(' to ')[0]} to {entry['timestamp_range'].split(' to ')[1]}"
        else:
            merged.append(current)
            current = entry.copy()

    merged.append(current)
    return merged


def merge_intervals(items: List[Dict[str, Any]], same_key: str = "executor_count_diff") -> List[Dict[str, Any]]:
    """Generic consecutive interval merge by comparing values under differences[same_key].

    This generalizes merge_consecutive_intervals for broader reuse.
    """
    if not items:
        return []
    merged: List[Dict[str, Any]] = []
    current = items[0].copy()
    for entry in items[1:]:
        if (
            entry.get("differences", {}).get(same_key)
            == current.get("differences", {}).get(same_key)
        ):
            current["timestamp_range"] = f"{current['timestamp_range'].split(' to ')[0]} to {entry['timestamp_range'].split(' to ')[1]}"
        else:
            merged.append(current)
            current = entry.copy()
    merged.append(current)
    return merged


def build_stage_executor_timeline(
    stage,
    executors: List[Any],
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
    max_intervals: int = MAX_INTERVALS,
) -> Dict[str, Any]:
    """Build executor timeline for a single stage given executors.

    Returns a dict with keys: stage_info, timeline.
    """
    if not stage.submission_time:
        return {
            "error": f"Stage {getattr(stage, 'stage_id', 'unknown')} has no submission time",
            "stage_info": {
                "stage_id": getattr(stage, "stage_id", None),
                "attempt_id": getattr(stage, "attempt_id", 0),
                "name": getattr(stage, "name", "unknown"),
                "submission_time": None,
                "completion_time": None,
                "duration_seconds": 0,
            },
            "timeline": [],
        }

    stage_start = stage.submission_time
    stage_end = stage.completion_time or stage_start + timedelta(hours=24)

    if stage_end <= stage_start:
        stage_end = stage_start + timedelta(minutes=interval_minutes)

    timeline: List[Dict[str, Any]] = []
    current_time = stage_start
    interval_count = 0

    while current_time < stage_end and interval_count < max_intervals:
        interval_end = current_time + timedelta(minutes=interval_minutes)
        if interval_end >= stage_end:
            interval_end = stage_end

        active_executors = []
        total_cores = 0
        total_memory = 0

        for executor in executors:
            executor_start = executor.add_time or stage_start
            executor_end = executor.remove_time or stage_end

            if executor_start <= interval_end and executor_end >= current_time:
                active_executors.append(
                    {
                        "id": executor.id,
                        "host_port": executor.host_port,
                        "cores": executor.total_cores or 0,
                        "memory_mb": (executor.max_memory / (1024 * 1024)) if executor.max_memory else 0,
                    }
                )
                total_cores += executor.total_cores or 0
                total_memory += (executor.max_memory / (1024 * 1024)) if executor.max_memory else 0

        timeline.append(
            {
                "timestamp": current_time.isoformat(),
                "interval_start": current_time.isoformat(),
                "interval_end": interval_end.isoformat(),
                "active_executor_count": len(active_executors),
                "total_cores": total_cores,
                "total_memory_mb": total_memory,
                "active_executors": active_executors,
            }
        )

        if interval_end >= stage_end:
            break

        current_time = interval_end
        interval_count += 1

    if interval_count >= max_intervals:
        timeline.append(
            {
                "warning": f"Timeline truncated at {max_intervals} intervals to prevent excessive memory usage",
                "stage_duration_hours": (stage_end - stage_start).total_seconds() / 3600,
                "interval_minutes": interval_minutes,
            }
        )

    return {
        "stage_info": {
            "stage_id": getattr(stage, "stage_id", None),
            "attempt_id": getattr(stage, "attempt_id", 0),
            "name": getattr(stage, "name", "unknown"),
            "submission_time": stage_start.isoformat() if stage_start else None,
            "completion_time": stage_end.isoformat() if stage.completion_time else None,
            "duration_seconds": (stage_end - stage_start).total_seconds() if stage_start else 0,
        },
        "timeline": timeline,
    }


def build_app_executor_timeline(
    app,
    executors: List[Any],
    stages: List[Any],
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
    max_intervals: int = MAX_INTERVALS,
) -> Dict[str, Any] | None:
    """Build executor timeline for a whole application given executors and stages.

    Returns a dict with keys: app_info, timeline, summary or None if insufficient data.
    """
    if not app.attempts:
        return None

    start_time = app.attempts[0].start_time
    end_time = app.attempts[0].end_time or (start_time and start_time + timedelta(hours=24))
    if not start_time:
        return None

    timeline_events = []
    for executor in executors:
        if executor.add_time:
            timeline_events.append({
                "timestamp": executor.add_time,
                "type": "executor_add",
                "executor_id": executor.id,
                "cores": executor.total_cores or 0,
                "memory_mb": (executor.max_memory / (1024 * 1024)) if executor.max_memory else 0,
            })
        if executor.remove_time:
            timeline_events.append({
                "timestamp": executor.remove_time,
                "type": "executor_remove",
                "executor_id": executor.id,
            })

    for stage in stages:
        if stage.submission_time:
            timeline_events.append({
                "timestamp": stage.submission_time,
                "type": "stage_start",
                "stage_id": stage.stage_id,
                "name": stage.name,
            })
        if stage.completion_time:
            timeline_events.append({
                "timestamp": stage.completion_time,
                "type": "stage_end",
                "stage_id": stage.stage_id,
            })

    timeline_events.sort(key=lambda x: x["timestamp"])  # for future use

    timeline: List[Dict[str, Any]] = []
    current_time = start_time
    interval_count = 0

    while current_time < end_time and interval_count < max_intervals:
        interval_end = current_time + timedelta(minutes=interval_minutes)
        if interval_end > end_time:
            interval_end = end_time

        active_executor_count = 0
        total_cores = 0
        total_memory_mb = 0
        active_stages = set()

        for executor in executors:
            executor_start = executor.add_time or start_time
            executor_end = executor.remove_time or end_time
            if executor_start <= interval_end and executor_end >= current_time:
                active_executor_count += 1
                total_cores += executor.total_cores or 0
                total_memory_mb += (executor.max_memory / (1024 * 1024)) if executor.max_memory else 0

        for stage in stages:
            stage_start = stage.submission_time
            stage_end = stage.completion_time or end_time
            if stage_start and stage_start <= interval_end and stage_end >= current_time:
                active_stages.add(stage.stage_id)

        timeline.append({
            "interval_start": current_time.isoformat(),
            "interval_end": interval_end.isoformat(),
            "active_executor_count": active_executor_count,
            "total_cores": total_cores,
            "total_memory_mb": total_memory_mb,
            "active_stages_count": len(active_stages),
        })

        current_time = interval_end
        interval_count += 1

    return {
        "app_info": {
            "app_id": getattr(app, "id", None),
            "name": app.name,
            "start_time": start_time.isoformat() if start_time else None,
            "end_time": end_time.isoformat() if app.attempts[0].end_time else None,
            "duration_seconds": (end_time - start_time).total_seconds() if start_time and end_time else 0,
        },
        "timeline": timeline,
        "summary": {
            "total_executors": len(executors),
            "total_stages": len(stages),
            "peak_executor_count": max((t["active_executor_count"] for t in timeline), default=0),
            "avg_executor_count": (sum(t["active_executor_count"] for t in timeline) / len(timeline)) if timeline else 0,
            "peak_cores": max((t["total_cores"] for t in timeline), default=0),
            "peak_memory_mb": max((t["total_memory_mb"] for t in timeline), default=0),
        }
    }
