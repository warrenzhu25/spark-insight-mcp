from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch


def make_app(id: str, name: str, cores: int = 0, mem_mb: int = 0):
    return SimpleNamespace(id=id, name=name, cores_granted=cores, memory_per_executor_mb=mem_mb)


def make_stage(stage_id: int, name: str, seconds: int):
    now = datetime.utcnow()
    return SimpleNamespace(
        stage_id=stage_id,
        name=name,
        status="COMPLETE",
        submission_time=now,
        completion_time=now + timedelta(seconds=seconds),
        num_tasks=10,
    )


@patch("spark_history_mcp.tools.tools._compare_sql_execution_plans")
@patch("spark_history_mcp.tools.tools._compare_environments")
@patch("spark_history_mcp.tools.tools.find_top_stage_differences")
@patch("spark_history_mcp.tools.tools.compare_app_stages_aggregated")
@patch("spark_history_mcp.tools.tools.compare_app_executors")
@patch("spark_history_mcp.tools.tools.fetch_app")
@patch("spark_history_mcp.tools.tools.get_client_or_default")
@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_compare_app_performance_schema_and_rules(
    mock_ctx,
    mock_get_client,
    mock_fetch_app,
    mock_executors,
    mock_stages_agg,
    mock_find_top,
    mock_env,
    mock_sql,
):
    from spark_history_mcp.tools.tools import compare_app_performance

    mock_ctx.return_value = SimpleNamespace()
    mock_get_client.return_value = SimpleNamespace()

    # Apps: large core and memory differences to trigger resource rules
    mock_fetch_app.side_effect = [make_app("a1", "app1", cores=100, mem_mb=4096), make_app("a2", "app2", cores=50, mem_mb=2048)]

    # Aggregated comparisons return recommendations
    mock_executors.return_value = {"recommendations": [{"type": "exec", "priority": "high", "issue": "x", "suggestion": "y"}]}
    mock_stages_agg.return_value = {"recommendations": [{"type": "stage", "priority": "medium", "issue": "x", "suggestion": "y"}]}

    # Stage deep-dive: include large time difference to trigger stage diff rule
    mock_find_top.return_value = {
        "applications": {"app1": {"id": "a1"}, "app2": {"id": "a2"}},
        "top_stage_differences": [
            {
                "time_difference": {"absolute_seconds": 120, "slower_application": "app2"},
            }
        ],
    }

    mock_env.return_value = {"ok": True}
    mock_sql.return_value = {"ok": True}

    out = compare_app_performance("a1", "a2")
    assert out["schema_version"] == 1
    assert "applications" in out and "performance_comparison" in out
    assert out["key_recommendations"], "Expected some recommendations after rules"
    # Ensure our staged recommendations flow in and are prioritized
    types = [r.get("type") for r in out["key_recommendations"]]
    assert any(t in {"exec", "stage", "resource_allocation", "stage_performance"} for t in types)


@patch("spark_history_mcp.tools.tools.fetch_stages")
@patch("spark_history_mcp.tools.tools.fetch_app")
@patch("spark_history_mcp.tools.tools.get_client_or_default")
@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_find_top_stage_differences_matches(mock_ctx, mock_get_client, mock_fetch_app, mock_fetch_stages):
    from spark_history_mcp.tools.tools import find_top_stage_differences

    mock_ctx.return_value = SimpleNamespace()
    mock_get_client.return_value = SimpleNamespace()
    mock_fetch_app.side_effect = [make_app("a1", "app1"), make_app("a2", "app2")]

    st1 = [make_stage(1, "Filter at MapPartitions", 30)]
    st2 = [make_stage(2, "filter at mappartitions", 60)]
    mock_fetch_stages.side_effect = [st1, st2]

    out = find_top_stage_differences("a1", "a2", top_n=1, similarity_threshold=0.6)
    assert "top_stage_differences" in out
    assert len(out["top_stage_differences"]) == 1
