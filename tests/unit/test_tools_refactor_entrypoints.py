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




@patch("spark_history_mcp.tools.fetchers.fetch_stages")
@patch("spark_history_mcp.tools.fetchers.fetch_app")
@patch("spark_history_mcp.tools.common.get_client_or_default")
@patch("spark_history_mcp.tools.common.get_active_mcp_context")
def test_find_top_stage_differences_matches(mock_ctx, mock_get_client, mock_fetch_app, mock_fetch_stages):
    from spark_history_mcp.tools.tools import find_top_stage_differences

    mock_ctx.return_value = SimpleNamespace()

    # Create a proper mock client with the methods that fetchers expect
    mock_client = SimpleNamespace()
    mock_client.get_application = lambda app_id: make_app(app_id, f"app-{app_id}")
    mock_client.list_stages = lambda app_id, status=None, with_summaries=False: []
    mock_get_client.return_value = mock_client

    mock_fetch_app.side_effect = [make_app("a1", "app1"), make_app("a2", "app2")]

    st1 = [make_stage(1, "Filter at MapPartitions", 30)]
    st2 = [make_stage(2, "filter at mappartitions", 60)]
    mock_fetch_stages.side_effect = [st1, st2]

    out = find_top_stage_differences("a1", "a2", top_n=1, similarity_threshold=0.6)
    assert "top_stage_differences" in out
    assert len(out["top_stage_differences"]) == 1
