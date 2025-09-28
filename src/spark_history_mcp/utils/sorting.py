"""
Sorting utilities for Spark metrics comparison data.

Provides consistent sorting logic for MCP tools and CLI formatters
to sort metrics by difference ratios, percentage changes, and other criteria.
"""

from typing import Any, Dict


def extract_percentage_value(change_str: str) -> float:
    """
    Extract numeric percentage from change string for sorting.

    Args:
        change_str: String like "+45.2%", "-12.5%", "N/A", "+∞", "-∞"

    Returns:
        Absolute percentage value as float, or 0.0 for non-numeric values
    """
    if change_str in ["N/A", "+∞", "-∞"]:
        return 0.0  # Treat as no change for sorting
    try:
        # Remove +/- and % signs, convert to float
        return abs(float(change_str.replace("+", "").replace("%", "")))
    except (ValueError, AttributeError):
        return 0.0


def sort_metrics_by_change(metrics_dict: Dict[str, Any], reverse: bool = True) -> Dict[str, Any]:
    """
    Sort a metrics dictionary by percentage change values in descending order.

    Looks for keys ending with '_change' and sorts by their percentage values.

    Args:
        metrics_dict: Dictionary containing metrics with '_change' suffix keys
        reverse: If True, sort in descending order (largest changes first)

    Returns:
        New dictionary with metrics sorted by change percentage
    """
    if not metrics_dict:
        return metrics_dict

    # Separate change metrics from other metrics
    change_metrics = {}
    other_metrics = {}

    for key, value in metrics_dict.items():
        if key.endswith('_change') and isinstance(value, str):
            change_metrics[key] = value
        else:
            other_metrics[key] = value

    # Sort change metrics by percentage value
    def get_change_sort_key(item):
        _, change_str = item
        return extract_percentage_value(change_str)

    sorted_change_items = sorted(change_metrics.items(), key=get_change_sort_key, reverse=reverse)

    # Build result dictionary with sorted change metrics first, then other metrics
    result = {}
    for key, value in sorted_change_items:
        result[key] = value
    result.update(other_metrics)

    return result


def sort_metrics_by_ratio(metrics_dict: Dict[str, Any], reverse: bool = True) -> Dict[str, Any]:
    """
    Sort a metrics dictionary by ratio values in descending order.

    Looks for keys ending with '_ratio' and sorts by their numeric values.

    Args:
        metrics_dict: Dictionary containing metrics with '_ratio' suffix keys
        reverse: If True, sort in descending order (largest ratios first)

    Returns:
        New dictionary with metrics sorted by ratio values
    """
    if not metrics_dict:
        return metrics_dict

    # Separate ratio metrics from other metrics
    ratio_metrics = {}
    other_metrics = {}

    for key, value in metrics_dict.items():
        if key.endswith('_ratio') and isinstance(value, (int, float)):
            ratio_metrics[key] = value
        else:
            other_metrics[key] = value

    # Sort ratio metrics by numeric value
    def get_ratio_sort_key(item):
        _, ratio_value = item
        # Convert to difference from 1.0 for sorting (larger differences = more significant)
        return abs(ratio_value - 1.0) if isinstance(ratio_value, (int, float)) else 0.0

    sorted_ratio_items = sorted(ratio_metrics.items(), key=get_ratio_sort_key, reverse=reverse)

    # Build result dictionary with sorted ratio metrics first, then other metrics
    result = {}
    for key, value in sorted_ratio_items:
        result[key] = value
    result.update(other_metrics)

    return result


def sort_mixed_metrics(metrics_dict: Dict[str, Any], reverse: bool = True) -> Dict[str, Any]:
    """
    Sort a mixed metrics dictionary containing both change percentages and ratios.

    Prioritizes change metrics, then ratio metrics, then other metrics.

    Args:
        metrics_dict: Dictionary containing various metrics types
        reverse: If True, sort in descending order (most significant first)

    Returns:
        New dictionary with metrics sorted by significance
    """
    if not metrics_dict:
        return metrics_dict

    change_metrics = {}
    ratio_metrics = {}
    other_metrics = {}

    for key, value in metrics_dict.items():
        if key.endswith('_change') and isinstance(value, str):
            change_metrics[key] = value
        elif key.endswith('_ratio') and isinstance(value, (int, float)):
            ratio_metrics[key] = value
        else:
            other_metrics[key] = value

    # Sort each category
    sorted_change = sort_metrics_by_change(change_metrics, reverse)
    sorted_ratio = sort_metrics_by_ratio(ratio_metrics, reverse)

    # Combine in priority order: changes first, then ratios, then others
    result = {}
    result.update(sorted_change)
    result.update(sorted_ratio)
    result.update(other_metrics)

    return result


def sort_comparison_data(data: Dict[str, Any], sort_key: str = "mixed") -> Dict[str, Any]:
    """
    Sort comparison data structure containing multiple metric dictionaries.

    Args:
        data: Comparison data structure from MCP tools
        sort_key: Type of sorting - "change", "ratio", or "mixed"

    Returns:
        New data structure with sorted metric dictionaries
    """
    if not isinstance(data, dict):
        return data

    # Create a copy to avoid modifying the original
    result = data.copy()

    # Common keys that contain sortable metrics
    metric_keys = [
        'diff', 'executor_comparison', 'efficiency_ratios', 'stage_comparison',
        'job_comparison', 'resource_comparison', 'comparison_metrics'
    ]

    sort_func = {
        "change": sort_metrics_by_change,
        "ratio": sort_metrics_by_ratio,
        "mixed": sort_mixed_metrics
    }.get(sort_key, sort_mixed_metrics)

    # Sort any metric dictionaries found
    for key in metric_keys:
        if key in result and isinstance(result[key], dict):
            result[key] = sort_func(result[key])

    return result
