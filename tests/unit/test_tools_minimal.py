"""
Minimal unit tests for spark_history_mcp.tools.tools to raise coverage.

These tests focus on argument plumbing, validation branches, and simple
aggregation logic with fully mocked clients and MCP context.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


def make_ctx(client):
    class Lifespan:
        def __init__(self, c):
            self.default_client = c
            self.clients = {"default": c, "local": c}

    class Req:
        def __init__(self, c):
            self.lifespan_context = Lifespan(c)

    return SimpleNamespace(request_context=Req(client))


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_get_client_or_default_variants(mock_get_context):
    from spark_history_mcp.tools.tools import get_client_or_default

    c = MagicMock()
    mock_get_context.return_value = make_ctx(c)

    ctx = mock_get_context()
    # default
    assert get_client_or_default(ctx) is c
    # named match
    assert get_client_or_default(ctx, "local") is c
    # error when no default and missing server
    bad_ctx = make_ctx(c)
    bad_ctx.request_context.lifespan_context.default_client = None
    bad_ctx.request_context.lifespan_context.clients = {}
    with pytest.raises(ValueError):
        get_client_or_default(bad_ctx)


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_list_applications_filters_and_search_types(mock_get_context):
    from spark_history_mcp.tools.tools import list_applications
    c = MagicMock()
    # Two apps with names
    app1 = SimpleNamespace(id="a1", name="Demo App One")
    app2 = SimpleNamespace(id="a2", name="Other")
    c.list_applications.return_value = [app1, app2]
    mock_get_context.return_value = make_ctx(c)

    # No name filter returns all
    apps = list_applications(limit=5)
    assert apps == [app1, app2]

    # Contains filter
    apps = list_applications(app_name="demo", search_type="contains")
    assert [a.id for a in apps] == ["a1"]

    # Exact filter
    apps = list_applications(app_name="Other", search_type="exact")
    assert [a.id for a in apps] == ["a2"]

    # Regex invalid raises
    with pytest.raises(Exception):
        list_applications(app_name="[bad", search_type="regex")

    # Invalid search_type raises ValueError
    with pytest.raises(ValueError):
        list_applications(app_name="x", search_type="unknown")


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_list_jobs_status_conversion(mock_get_context):
    from spark_history_mcp.tools.tools import list_jobs
    c = MagicMock()
    c.list_jobs.return_value = []
    mock_get_context.return_value = make_ctx(c)
    list_jobs(app_id="app-1", status=["SUCCEEDED", "FAILED"])  # should not raise
    # Ensure conversion happened by checking call kwargs type
    args, kwargs = c.list_jobs.call_args
    assert kwargs["app_id"] == "app-1"
    # status entries should be Enums; just ensure we got a list of two entries
    assert len(kwargs["status"]) == 2


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_list_slowest_jobs_and_empty(mock_get_context):
    from spark_history_mcp.tools.tools import list_slowest_jobs
    c = MagicMock()
    now = datetime.now()
    j1 = SimpleNamespace(
        status="SUCCEEDED",
        submission_time=now,
        completion_time=now + timedelta(seconds=5),
    )
    j2 = SimpleNamespace(
        status="RUNNING",
        submission_time=now,
        completion_time=None,
    )
    c.list_jobs.return_value = [j1, j2]
    mock_get_context.return_value = make_ctx(c)

    # exclude running by default → only j1 considered
    slow = list_slowest_jobs(app_id="a", n=1)
    assert slow == [j1]

    # No jobs case
    c.list_jobs.return_value = []
    assert list_slowest_jobs(app_id="a") == []


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_list_stages_and_slowest_stages(mock_get_context):
    from spark_history_mcp.tools.tools import list_stages, list_slowest_stages
    c = MagicMock()
    mock_get_context.return_value = make_ctx(c)
    list_stages(app_id="a", status=["COMPLETE"], with_summaries=True)
    # ensure enums converted and param names passed through
    args, kwargs = c.list_stages.call_args
    assert kwargs["with_summaries"] is True

    # Slowest stages
    now = datetime.now()
    s1 = SimpleNamespace(
        status="COMPLETE",
        completion_time=now + timedelta(seconds=10),
        first_task_launched_time=now,
    )
    s2 = SimpleNamespace(
        status="RUNNING",
        completion_time=None,
        first_task_launched_time=None,
    )
    c.list_stages.return_value = [s1, s2]
    slow = list_slowest_stages(app_id="a", n=1)
    assert slow == [s1]


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_get_executor_and_summary(mock_get_context):
    from spark_history_mcp.tools.tools import get_executor, get_executor_summary
    c = MagicMock()
    # executor list
    exec1 = SimpleNamespace(
        id="1",
        is_active=True,
        disk_used=100,
        completed_tasks=2,
        failed_tasks=0,
        total_duration=5,
        total_gc_time=1,
        total_input_bytes=1024,
        total_shuffle_read=2048,
        total_shuffle_write=4096,
        memory_metrics=SimpleNamespace(
            used_on_heap_storage_memory=10, used_off_heap_storage_memory=20
        ),
    )
    exec2 = SimpleNamespace(
        id="2",
        is_active=False,
        disk_used=200,
        completed_tasks=1,
        failed_tasks=1,
        total_duration=3,
        total_gc_time=2,
        total_input_bytes=512,
        total_shuffle_read=1024,
        total_shuffle_write=2048,
        memory_metrics=SimpleNamespace(
            used_on_heap_storage_memory=5, used_off_heap_storage_memory=5
        ),
    )
    c.list_all_executors.return_value = [exec1, exec2]
    mock_get_context.return_value = make_ctx(c)

    # get_executor by id
    assert get_executor("app", "2") is exec2
    assert get_executor("app", "x") is None

    # summary aggregation
    summary = get_executor_summary("app")
    assert summary["total_executors"] == 2
    assert summary["active_executors"] == 1
    assert summary["memory_used"] == 40
    assert summary["disk_used"] == 300
    assert summary["completed_tasks"] == 3
    assert summary["failed_tasks"] == 1
    assert summary["total_duration"] == 8
    assert summary["total_gc_time"] == 3
    assert summary["total_input_bytes"] == 1536
    assert summary["total_shuffle_read"] == 3072
    assert summary["total_shuffle_write"] == 6144


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_analyze_failed_tasks(mock_get_context):
    from spark_history_mcp.tools.tools import analyze_failed_tasks
    c = MagicMock()
    # stages with failures
    st1 = SimpleNamespace(stage_id=1, attempt_id=0, name='s1', num_failed_tasks=5, num_tasks=10, status='COMPLETE')
    st2 = SimpleNamespace(stage_id=2, attempt_id=0, name='s2', num_failed_tasks=0, num_tasks=10, status='COMPLETE')
    c.list_stages.return_value = [st1, st2]
    # executors with failures; hosts used for concentration check
    ex1 = SimpleNamespace(id='1', host='h1', failed_tasks=6, completed_tasks=4, remove_reason=None, is_active=True)
    ex2 = SimpleNamespace(id='2', host='h1', failed_tasks=5, completed_tasks=5, remove_reason=None, is_active=False)
    c.list_all_executors.return_value = [ex1, ex2]
    mock_get_context.return_value = make_ctx(c)

    res = analyze_failed_tasks('app')
    assert res['application_id'] == 'app'
    assert res['failed_stages'][0]['stage_id'] == 1
    assert res['summary']['total_failed_tasks'] >= 5
    assert isinstance(res['recommendations'], list)


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_analyze_executor_utilization_errors(mock_get_context):
    from spark_history_mcp.tools.tools import analyze_executor_utilization
    c = MagicMock()
    # No attempts
    app = SimpleNamespace(attempts=[])
    c.get_application.return_value = app
    c.list_all_executors.return_value = []
    mock_get_context.return_value = make_ctx(c)
    res = analyze_executor_utilization('app')
    assert 'error' in res

    # Missing times
    attempt = SimpleNamespace(start_time=None, end_time=None)
    app2 = SimpleNamespace(attempts=[attempt])
    c.get_application.return_value = app2
    res = analyze_executor_utilization('app')
    assert 'error' in res


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_compare_stage_executor_timeline_basic(mock_get_context):
    from spark_history_mcp.tools.tools import compare_stage_executor_timeline
    c = MagicMock()
    # stages
    now = datetime.now()
    st = SimpleNamespace(stage_id=1, attempt_id=0, name='s', submission_time=now, completion_time=now + timedelta(minutes=2))
    c.get_stage_attempt.side_effect = [st, st]
    # executors
    ex = SimpleNamespace(id='e1', host_port='h:1', total_cores=4, max_memory=1024*1024*1024, add_time=now, remove_time=now + timedelta(minutes=1))
    c.list_all_executors.side_effect = [[ex], [ex]]
    mock_get_context.return_value = make_ctx(c)
    res = compare_stage_executor_timeline('a1','a2',1,2, interval_minutes=1)
    assert 'timeline_comparison' in res
    assert res['comparison_config']['merged_intervals_shown'] >= 1


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_compare_app_executor_timeline_basic(mock_get_context):
    from spark_history_mcp.tools.tools import compare_app_executor_timeline
    c = MagicMock()
    now = datetime.now()
    attempt = SimpleNamespace(start_time=now, end_time=now + timedelta(minutes=3))
    app = SimpleNamespace(name='A', attempts=[attempt])
    c.get_application.side_effect = [app, app]
    ex = SimpleNamespace(id='e1', total_cores=2, max_memory=512*1024*1024, add_time=now, remove_time=now + timedelta(minutes=2))
    c.list_all_executors.side_effect = [[ex], [ex]]
    st = SimpleNamespace(stage_id=1, name='s', submission_time=now, completion_time=now + timedelta(minutes=1))
    c.list_stages.side_effect = [[st], [st]]
    mock_get_context.return_value = make_ctx(c)
    res = compare_app_executor_timeline('a1','a2', interval_minutes=1)
    assert 'timeline_comparison' in res
    assert res['app1_info']['name'] == 'A'


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_find_top_stage_differences_no_stages(mock_get_context):
    from spark_history_mcp.tools.tools import find_top_stage_differences
    c = MagicMock()
    c.get_application.side_effect = [SimpleNamespace(name='A'), SimpleNamespace(name='B')]
    c.list_stages.side_effect = [[], []]
    mock_get_context.return_value = make_ctx(c)
    res = find_top_stage_differences('a1','a2')
    assert 'error' in res
