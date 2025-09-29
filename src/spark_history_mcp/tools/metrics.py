"""
Metrics helpers for summarizing applications and comparing numeric maps.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..models.spark_types import StageStatus

from .common import bytes_to_gb, get_config, ms_to_min, ns_to_min


def summarize_app(app, stages, executors) -> Dict[str, Any]:
    """Build the application summary dict used by get_app_summary.

    Keeps behavior aligned with current tools.get_app_summary implementation.
    """
    if not app.attempts:
        return {"error": "No application attempts found", "application_id": getattr(app, 'id', None)}

    attempt = app.attempts[-1]

    total_runtime_min = ms_to_min(getattr(attempt, 'duration', 0))

    total_executor_runtime_ms = sum(getattr(stage, 'executor_run_time', 0) or 0 for stage in stages)
    total_executor_runtime_min = ms_to_min(total_executor_runtime_ms)

    total_executor_cpu_time_ns = sum(getattr(stage, 'executor_cpu_time', 0) or 0 for stage in stages)
    total_executor_cpu_time_min = ns_to_min(total_executor_cpu_time_ns)

    total_gc_time_ms = sum(getattr(stage, 'jvm_gc_time', 0) or 0 for stage in stages)
    total_gc_time_min = ms_to_min(total_gc_time_ms)

    if getattr(attempt, 'end_time', None) and getattr(attempt, 'start_time', None):
        app_end_time_ms = attempt.end_time.timestamp() * 1000
    else:
        app_end_time_ms = None

    total_executor_time_ms = 0
    executor_cores = getattr(app, 'cores_per_executor', 1) or 1
    for executor in executors:
        if getattr(executor, 'add_time', None):
            add_time_ms = executor.add_time.timestamp() * 1000
            if getattr(executor, 'remove_time', None):
                remove_time_ms = executor.remove_time.timestamp() * 1000
            elif app_end_time_ms:
                remove_time_ms = app_end_time_ms
            else:
                continue
            total_executor_time_ms += (remove_time_ms - add_time_ms)

    executor_utilization = 0.0
    if total_executor_time_ms > 0:
        executor_utilization = (total_executor_runtime_ms / (total_executor_time_ms * executor_cores)) * 100

    total_input_gb = bytes_to_gb(sum(getattr(stage, 'input_bytes', 0) or 0 for stage in stages))
    total_output_gb = bytes_to_gb(sum(getattr(stage, 'output_bytes', 0) or 0 for stage in stages))
    total_shuffle_read_gb = bytes_to_gb(sum(getattr(stage, 'shuffle_read_bytes', 0) or 0 for stage in stages))
    total_shuffle_write_gb = bytes_to_gb(sum(getattr(stage, 'shuffle_write_bytes', 0) or 0 for stage in stages))
    total_memory_spilled_gb = bytes_to_gb(sum(getattr(stage, 'memory_bytes_spilled', 0) or 0 for stage in stages))
    total_disk_spilled_gb = bytes_to_gb(sum(getattr(stage, 'disk_bytes_spilled', 0) or 0 for stage in stages))

    total_shuffle_fetch_wait_time_ns = 0
    total_shuffle_write_time_ns = 0
    total_failed_tasks = sum(getattr(stage, 'num_failed_tasks', 0) or 0 for stage in stages)

    for stage in stages:
        dist = getattr(stage, 'task_metrics_distributions', None)
        if not dist:
            continue

        if (
            getattr(dist, 'shuffle_read_metrics', None)
            and getattr(dist.shuffle_read_metrics, 'fetch_wait_time', None)
            and len(dist.shuffle_read_metrics.fetch_wait_time) >= 3
        ):
            median_fetch_wait = dist.shuffle_read_metrics.fetch_wait_time[2]
            num_tasks = getattr(stage, 'num_tasks', 0) or 0
            total_shuffle_fetch_wait_time_ns += median_fetch_wait * num_tasks

        if (
            getattr(dist, 'shuffle_write_metrics', None)
            and getattr(dist.shuffle_write_metrics, 'write_time', None)
            and len(dist.shuffle_write_metrics.write_time) >= 3
        ):
            median_write_time = dist.shuffle_write_metrics.write_time[2]
            num_tasks = getattr(stage, 'num_tasks', 0) or 0
            total_shuffle_write_time_ns += median_write_time * num_tasks

    shuffle_fetch_wait_min = ns_to_min(total_shuffle_fetch_wait_time_ns)
    shuffle_write_time_min = ns_to_min(total_shuffle_write_time_ns)

    summary = {
        "application_id": getattr(app, 'id', None),
        "application_name": getattr(app, 'name', None),
        "analysis_timestamp": datetime.now().isoformat(),
        "application_duration_minutes": round(total_runtime_min, 2),
        "total_executor_runtime_minutes": round(total_executor_runtime_min, 2),
        "executor_cpu_time_minutes": round(total_executor_cpu_time_min, 2),
        "jvm_gc_time_minutes": round(total_gc_time_min, 2),
        "executor_utilization_percent": round(executor_utilization, 2),
        "input_data_size_gb": round(total_input_gb, 3),
        "output_data_size_gb": round(total_output_gb, 3),
        "shuffle_read_size_gb": round(total_shuffle_read_gb, 3),
        "shuffle_write_size_gb": round(total_shuffle_write_gb, 3),
        "memory_spilled_gb": round(total_memory_spilled_gb, 3),
        "disk_spilled_gb": round(total_disk_spilled_gb, 3),
        "shuffle_read_wait_time_minutes": round(shuffle_fetch_wait_min, 2),
        "shuffle_write_time_minutes": round(shuffle_write_time_min, 2),
        "failed_tasks": total_failed_tasks,
        "total_stages": len(stages),
        "completed_stages": len([s for s in stages if getattr(s, 'status', None) == StageStatus.COMPLETE]),
        "failed_stages": len([s for s in stages if getattr(s, 'status', None) == StageStatus.FAILED]),
    }
    return summary


def compute_utilization(
    stages: Iterable[Any],
    executors: Iterable[Any],
    executor_cores: int,
    app_start_end: Optional[Tuple[Optional[float], Optional[float]]] = None,
) -> float:
    """Compute executor utilization percentage.

    executor_cores should be the cores per executor. app_start_end may contain
    (start_ms, end_ms) to bound active time when executor.remove_time missing.
    """
    total_executor_runtime_ms = sum(getattr(s, "executor_run_time", 0) or 0 for s in stages)
    total_executor_time_ms = 0
    app_end_ms = app_start_end[1] if app_start_end else None
    for e in executors:
        if getattr(e, "add_time", None):
            add_ms = e.add_time.timestamp() * 1000
            if getattr(e, "remove_time", None):
                rm_ms = e.remove_time.timestamp() * 1000
            elif app_end_ms:
                rm_ms = app_end_ms
            else:
                continue
            total_executor_time_ms += (rm_ms - add_ms)
    if total_executor_time_ms <= 0 or not executor_cores:
        return 0.0
    return (total_executor_runtime_ms / (total_executor_time_ms * executor_cores)) * 100.0


def compare_numeric_maps(
    map1: Dict[str, float | int],
    map2: Dict[str, float | int],
    *,
    significance: Optional[float] = None,
    exclude: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Compare two numeric maps and return significant differences only.

    Returns dict with keys: differences (per-key with before/after/change/percent),
    significant_keys, insignificant_keys.
    """
    cfg = get_config()
    sig = significance if significance is not None else cfg.significance_threshold
    ex = set(exclude or [])

    diffs: Dict[str, Any] = {}
    significant_keys: List[str] = []
    insignificant_keys: List[str] = []

    for key in sorted(set(map1.keys()) | set(map2.keys())):
        if key in ex:
            continue
        v1 = float(map1.get(key, 0) or 0)
        v2 = float(map2.get(key, 0) or 0)
        if v1 == v2:
            continue
        abs_change = v2 - v1
        pct = None if v1 == 0 else ((v2 - v1) / v1)
        entry = {
            "before": v1,
            "after": v2,
            "absolute": abs_change,
            "percent": None if pct is None else round(pct * 100.0, 2),
        }
        if pct is None or abs(pct) >= sig:
            diffs[key] = entry
            significant_keys.append(key)
        else:
            insignificant_keys.append(key)

    return {
        "differences": diffs,
        "significant_keys": significant_keys,
        "insignificant_keys": insignificant_keys,
        "significance_threshold": sig,
    }


def compare_distributions(
    dist1: Any,
    dist2: Any,
    fields: List[Tuple[str, str]],
    *,
    significance: Optional[float] = None,
) -> Dict[str, Any]:
    """Compare selected distribution fields by median if available.

    fields is a list of (path, label) where path can be like
    "shuffle_read_metrics.fetch_wait_time".
    """
    cfg = get_config()
    sig = significance if significance is not None else cfg.significance_threshold

    def median_from_path(obj: Any, path: str) -> Optional[float]:
        try:
            parts = path.split(".")
            cur = obj
            for p in parts:
                cur = getattr(cur, p)
            if isinstance(cur, (list, tuple)) and len(cur) >= 3:
                return float(cur[2])
        except Exception:
            return None
        return None

    result: Dict[str, Any] = {
        "significance_threshold": sig,
        "metrics": {},
    }
    for path, label in fields:
        m1 = median_from_path(dist1, path)
        m2 = median_from_path(dist2, path)
        if m1 is None and m2 is None:
            continue
        v1 = float(m1 or 0)
        v2 = float(m2 or 0)
        pct = None if v1 == 0 else ((v2 - v1) / v1)
        significant = pct is None or abs(pct) >= sig
        result["metrics"][label] = {
            "before": v1,
            "after": v2,
            "percent": None if pct is None else round(pct * 100.0, 2),
            "significant": significant,
        }
    return result
