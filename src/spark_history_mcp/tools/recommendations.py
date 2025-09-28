"""
Recommendation utilities: normalization, deduplication, prioritization, and rule scaffolding.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from .common import get_config


PRIORITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}


def normalize(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure standard keys exist for a recommendation dict."""
    return {
        "type": rec.get("type", "general"),
        "priority": rec.get("priority", "low"),
        "issue": rec.get("issue", ""),
        "suggestion": rec.get("suggestion", ""),
        **{k: v for k, v in rec.items() if k not in {"type", "priority", "issue", "suggestion"}},
    }


def dedupe(recs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate recommendations using (type, issue, suggestion) identity."""
    seen: set[Tuple[str, str, str]] = set()
    result: List[Dict[str, Any]] = []
    for r in recs:
        n = normalize(r)
        key = (n.get("type", ""), n.get("issue", ""), n.get("suggestion", ""))
        if key in seen:
            continue
        seen.add(key)
        result.append(n)
    return result


def prioritize(recs: List[Dict[str, Any]], top_n: int = 5) -> List[Dict[str, Any]]:
    """Sort recommendations by priority and return top N of critical/high/medium."""
    filtered = [r for r in recs if r.get("priority") in {"critical", "high", "medium"}]
    filtered.sort(key=lambda x: PRIORITY_ORDER.get(x.get("priority", "low"), 3))
    return filtered[:top_n]


# Rule engine scaffolding (optional extension point)
Rule = Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]


def apply_rules(context: Dict[str, Any], rules: Optional[List[Rule]] = None) -> List[Dict[str, Any]]:
    """Apply registered rules to context and return list of recommendations."""
    if not rules:
        return []
    out: List[Dict[str, Any]] = []
    for rule in rules:
        try:
            rec = rule(context)
            if rec:
                out.append(normalize(rec))
        except Exception:
            # Rules are best-effort; skip on error
            continue
    return out


# Built-in example rules
def rule_resource_allocation(ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    app1 = ctx.get("app1")
    app2 = ctx.get("app2")
    if not app1 or not app2:
        return None
    try:
        if getattr(app1, "cores_granted", None) and getattr(app2, "cores_granted", None):
            core_ratio = app2.cores_granted / app1.cores_granted
            if core_ratio > 1.5 or core_ratio < 0.67:
                slower_app = "app1" if core_ratio > 1.5 else "app2"
                faster_app = "app2" if core_ratio > 1.5 else "app1"
                return {
                    "type": "resource_allocation",
                    "priority": "medium",
                    "issue": f"Significant core allocation difference (ratio: {core_ratio:.2f})",
                    "suggestion": f"Consider equalizing core allocation - {slower_app} has fewer cores than {faster_app}",
                }
        if getattr(app1, "memory_per_executor_mb", None) and getattr(app2, "memory_per_executor_mb", None):
            memory_ratio = app2.memory_per_executor_mb / app1.memory_per_executor_mb
            if memory_ratio > 1.5 or memory_ratio < 0.67:
                return {
                    "type": "resource_allocation",
                    "priority": "medium",
                    "issue": f"Significant memory per executor difference (ratio: {memory_ratio:.2f})",
                    "suggestion": "Review memory allocation settings between applications",
                }
    except Exception:
        return None
    return None


def rule_large_stage_diff(ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    cfg = get_config()
    comps = ctx.get("detailed_comparisons") or []
    if not comps:
        return None
    try:
        large = [
            c for c in comps
            if c.get("time_difference", {}).get("absolute_seconds", 0) > int(cfg.large_stage_diff_seconds)
        ]
        if large:
            slower = large[0].get("time_difference", {}).get("slower_application", "an app")
            return {
                "type": "stage_performance",
                "priority": "high",
                "issue": f"Found {len(large)} stages with >{cfg.large_stage_diff_seconds}s time difference",
                "suggestion": f"Investigate {slower} for potential performance issues in specific stages",
            }
    except Exception:
        return None
    return None


def default_rules() -> List[Rule]:
    return [rule_resource_allocation, rule_large_stage_diff]

