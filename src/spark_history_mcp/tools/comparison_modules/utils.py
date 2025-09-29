"""
Common utilities and helper functions for comparison tools.

This module contains shared utilities used across comparison modules including
context management, client resolution, calculation helpers, and data processing utilities.
"""

from typing import Any, Dict, Optional

from ...core.app import mcp
from .. import analysis as analysis_tools


def get_mcp_context() -> Optional[object]:
    """Return the active MCP request context if available."""
    try:
        return mcp.get_context()
    except ValueError:
        # Allow tools to run in unit tests where no request context is active.
        return None


def resolve_client(server: Optional[str]) -> Any:
    """Return a Spark client using the legacy analysis accessor.

    The tests patch ``spark_history_mcp.tools.analysis.get_client_or_default`` to
    inject mock clients, so we delegate through that module instead of importing
    the helper directly. When no MCP request context is available we surface a
    clear error unless the patched helper returns a client (as in unit tests).
    """
    ctx = get_mcp_context()
    try:
        return analysis_tools.get_client_or_default(ctx, server)
    except AttributeError as exc:
        if ctx is None:
            raise ValueError(
                "Spark MCP context is not available outside of a request."
            ) from exc
        raise


def calculate_safe_ratio(val1: float, val2: float) -> float:
    """Calculate ratio of val2/val1 with safe handling of zero divisors."""
    if val1 == 0:
        return float("inf") if val2 > 0 else 1.0
    return val2 / val1


def filter_significant_metrics(
    metrics: Dict[str, Any], threshold: float, show_only_significant: bool = True
) -> Dict[str, Any]:
    """Filter out metrics below significance threshold."""
    if not show_only_significant or not metrics:
        return metrics

    filtered = {}
    for key, value in metrics.items():
        if key.endswith("_ratio"):
            # For ratios, check if significantly different from 1.0
            if isinstance(value, (int, float)):
                if abs(value - 1.0) >= threshold:
                    filtered[key] = value
        elif key.endswith("_percent_change"):
            # For percent changes, check if above threshold
            if isinstance(value, (int, float)) and abs(value) >= threshold * 100:
                filtered[key] = value
        else:
            # Include non-ratio metrics
            filtered[key] = value

    return filtered


def sort_comparison_data(
    data: Dict[str, Any], sort_key: str = "ratio"
) -> Dict[str, Any]:
    """Sort comparison data by specified key for consistent output."""
    if not isinstance(data, dict) or sort_key not in data:
        return data

    # For now, just return as-is. Could implement more sophisticated sorting logic.
    return data


def calculate_stage_duration(stage) -> float:
    """Calculate stage duration in seconds."""
    if stage.completion_time and stage.submission_time:
        return (stage.completion_time - stage.submission_time).total_seconds()
    return 0.0


def _compare_environments(
    app1_env, app2_env, filter_auto_generated: bool = True
) -> Dict[str, Any]:
    """Compare environment configurations between applications."""
    comparison = {
        "spark_properties": {"different": [], "app1_only": [], "app2_only": []},
        "system_properties": {"different": [], "app1_only": [], "app2_only": []},
        "jvm_info": {},
        "summary": {},
    }

    # Compare Spark properties
    if app1_env.spark_properties and app2_env.spark_properties:
        app1_spark = {k: v for k, v in app1_env.spark_properties}
        app2_spark = {k: v for k, v in app2_env.spark_properties}

        # Filter auto-generated properties if requested
        if filter_auto_generated:
            auto_gen_patterns = [
                "spark.app.id",
                "spark.driver.host",
                "spark.driver.port",
            ]
            app1_spark = {
                k: v
                for k, v in app1_spark.items()
                if not any(pattern in k for pattern in auto_gen_patterns)
            }
            app2_spark = {
                k: v
                for k, v in app2_spark.items()
                if not any(pattern in k for pattern in auto_gen_patterns)
            }

        all_keys = set(app1_spark.keys()) | set(app2_spark.keys())
        for key in sorted(all_keys):
            if key in app1_spark and key in app2_spark:
                if app1_spark[key] != app2_spark[key]:
                    comparison["spark_properties"]["different"].append(
                        {
                            "property": key,
                            "app1_value": app1_spark[key],
                            "app2_value": app2_spark[key],
                        }
                    )
            elif key in app1_spark:
                comparison["spark_properties"]["app1_only"].append(
                    {"property": key, "value": app1_spark[key]}
                )
            else:
                comparison["spark_properties"]["app2_only"].append(
                    {"property": key, "value": app2_spark[key]}
                )

    # Compare system properties (similar logic)
    if app1_env.system_properties and app2_env.system_properties:
        app1_system = {k: v for k, v in app1_env.system_properties}
        app2_system = {k: v for k, v in app2_env.system_properties}

        all_keys = set(app1_system.keys()) | set(app2_system.keys())
        for key in sorted(all_keys):
            if key in app1_system and key in app2_system:
                if app1_system[key] != app2_system[key]:
                    comparison["system_properties"]["different"].append(
                        {
                            "property": key,
                            "app1_value": app1_system[key],
                            "app2_value": app2_system[key],
                        }
                    )
            elif key in app1_system:
                comparison["system_properties"]["app1_only"].append(
                    {"property": key, "value": app1_system[key]}
                )
            else:
                comparison["system_properties"]["app2_only"].append(
                    {"property": key, "value": app2_system[key]}
                )

    # Compare JVM information
    if hasattr(app1_env, "runtime") and hasattr(app2_env, "runtime"):
        comparison["jvm_info"] = {
            "java_version": {
                "app1": getattr(app1_env.runtime, "java_version", "Unknown"),
                "app2": getattr(app2_env.runtime, "java_version", "Unknown"),
            },
            "java_home": {
                "app1": getattr(app1_env.runtime, "java_home", "Unknown"),
                "app2": getattr(app2_env.runtime, "java_home", "Unknown"),
            },
            "scala_version": {
                "app1": getattr(app1_env.runtime, "scala_version", "Unknown"),
                "app2": getattr(app2_env.runtime, "scala_version", "Unknown"),
            },
        }

    # Create summary
    comparison["summary"] = {
        "total_spark_differences": len(comparison["spark_properties"]["different"]),
        "spark_app1_only": len(comparison["spark_properties"]["app1_only"]),
        "spark_app2_only": len(comparison["spark_properties"]["app2_only"]),
        "total_system_differences": len(comparison["system_properties"]["different"]),
        "system_app1_only": len(comparison["system_properties"]["app1_only"]),
        "system_app2_only": len(comparison["system_properties"]["app2_only"]),
    }

    return comparison


def _compare_sql_execution_plans(client, app_id1: str, app_id2: str) -> Dict[str, Any]:
    """Compare SQL execution plans between applications."""
    try:
        # Get SQL queries for both applications
        sql1 = client.list_sql_queries(app_id=app_id1, details=False)
        sql2 = client.list_sql_queries(app_id=app_id2, details=False)

        return {
            "app1_queries": len(sql1) if sql1 else 0,
            "app2_queries": len(sql2) if sql2 else 0,
            "comparison": "SQL query comparison would require detailed analysis",
        }
    except Exception as e:
        return {
            "error": f"Failed to compare SQL plans: {str(e)}",
            "app1_queries": 0,
            "app2_queries": 0,
        }
