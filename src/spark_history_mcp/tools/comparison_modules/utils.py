"""
Common utilities and helper functions for comparison tools.

This module contains shared utilities used across comparison modules including
context management, client resolution, calculation helpers, and data processing utilities.
"""

import os
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

    def _normalize_env_value(key: str, value):
        if value is None:
            return None

        val = str(value)
        if not val:
            return val

        lower_val = val.lower()
        if key in {"spark.app.id", "spark.app.startTime"}:
            return "<auto>"

        if "spark.app.id" in lower_val:
            return "<auto>"

        if "spark.kubernetes" in key or "kubernetes" in lower_val:
            if "pod" in lower_val or "container" in lower_val:
                return "<auto:k8s_id>"

        if f"{os.sep}tmp" in val or "temp" in lower_val:
            return "<auto:temp_path>"

        if "://" not in val and "/" in val and "file:" not in lower_val:
            if "/var/lib" in val or "/data" in val or "/mnt" in val:
                return "<auto:path>"

        # UUID patterns
        if len(val) >= 32:
            uuid_chars = "0123456789abcdef-"
            if all(c in uuid_chars for c in lower_val) and lower_val.count("-") in {
                4,
                5,
            }:
                return "<auto:uuid>"

        # Spark app ids
        if lower_val.startswith("spark-") and any(ch.isdigit() for ch in lower_val):
            return "<auto:spark_id>"

        # IP address
        ip_parts = val.split(".")
        if len(ip_parts) == 4 and all(p.isdigit() for p in ip_parts):
            return "<auto:ip>"

        # Host:port
        if ":" in val:
            host, _, port = val.rpartition(":")
            if port.isdigit():
                return "<auto:hostport>"

        # Epoch/timestamp-ish
        if val.isdigit() and len(val) >= 10:
            return "<auto:timestamp>"

        if "timestamp" in lower_val or "time" in lower_val:
            if any(ch.isdigit() for ch in lower_val):
                return "<auto:timestamp>"

        return value

    def _normalize_props(props):
        if not props:
            return {}
        return {k: _normalize_env_value(k, v) for k, v in props.items()}

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
                "spark.app.startTime",
                "spark.app.submitTime",
                "spark.driver.host",
                "spark.driver.port",
                "spark.driver.appUIAddress",
                "spark.yarn.app.",
                "spark.yarn.credentials",
                "spark.yarn.historyServer",
                "spark.ui.",
                "spark.repl.",
                "spark.jars",
                "spark.submit.",
                "spark.master",
                "spark.launcher.",
                "spark.history.fs.logDirectory",
                "spark.dataproc.metrics.listener",
                "spark.executorEnv.",
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

        if filter_auto_generated:
            app1_spark = _normalize_props(app1_spark)
            app2_spark = _normalize_props(app2_spark)

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

        # Add counts for convenience while keeping full lists
        spark_props = comparison["spark_properties"]
        spark_props["total_different"] = len(spark_props["different"])
        spark_props["app1_only_count"] = len(spark_props["app1_only"])
        spark_props["app2_only_count"] = len(spark_props["app2_only"])

    # Compare system properties (similar logic)
    if app1_env.system_properties and app2_env.system_properties:
        app1_system = {k: v for k, v in app1_env.system_properties}
        app2_system = {k: v for k, v in app2_env.system_properties}
        if filter_auto_generated:
            app1_system = _normalize_props(app1_system)
            app2_system = _normalize_props(app2_system)

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

        # Add counts for convenience while keeping full lists
        sys_props = comparison["system_properties"]
        sys_props["total_different"] = len(sys_props["different"])
        sys_props["app1_only_count"] = len(sys_props["app1_only"])
        sys_props["app2_only_count"] = len(sys_props["app2_only"])

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
    spark_props = comparison["spark_properties"]
    sys_props = comparison["system_properties"]
    comparison["summary"] = {
        "total_spark_differences": spark_props.get("total_different", 0),
        "spark_app1_only": spark_props.get("app1_only_count", 0),
        "spark_app2_only": spark_props.get("app2_only_count", 0),
        "total_system_differences": sys_props.get("total_different", 0),
        "system_app1_only": sys_props.get("app1_only_count", 0),
        "system_app2_only": sys_props.get("app2_only_count", 0),
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
