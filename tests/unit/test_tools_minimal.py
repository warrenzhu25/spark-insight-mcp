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


def test_truncate_plan_description():
    from spark_history_mcp.tools.tools import truncate_plan_description
    short = "abc"
    assert truncate_plan_description(short, 10) == short
    long = "line1\nline2\n" + ("x" * 100)
    out = truncate_plan_description(long, 10)
    assert out.endswith("... [truncated]")


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_list_slowest_sql_queries_basic(mock_get_context):
    from spark_history_mcp.tools.tools import list_slowest_sql_queries
    c = MagicMock()
    now = datetime.now()
    # two executions, one running, one completed
    e1 = SimpleNamespace(
        id=1,
        duration=100,
        description='Q1',
        status='COMPLETED',
        submission_time=now,
        plan_description='PLAN\n' + ('x' * 100),
        success_job_ids=[1],
        failed_job_ids=[],
        running_job_ids=[]
    )
    e2 = SimpleNamespace(
        id=2,
        duration=200,
        description='Q2',
        status='RUNNING',
        submission_time=now,
        plan_description='RUN',
        success_job_ids=[],
        failed_job_ids=[],
        running_job_ids=[2]
    )
    c.get_sql_list.side_effect = [[e1, e2], []]
    mock_get_context.return_value = make_ctx(c)

    # Exclude RUNNING, should return only e1 when top_n=1
    res = list_slowest_sql_queries('app', top_n=1, page_size=10, include_running=False)
    assert len(res) == 1
    assert res[0].id == 1

    # Include running and top 1 should be id=2 with duration longer
    # reset side_effect for subsequent call
    c.get_sql_list.side_effect = [[e1, e2], []]
    res2 = list_slowest_sql_queries('app', top_n=1, page_size=10, include_running=True)
    assert len(res2) == 1 and res2[0].id == 2

    # No plan description if disabled
    c.get_sql_list.side_effect = [[e1], []]
    res3 = list_slowest_sql_queries('app', top_n=1, page_size=10, include_plan_description=False, include_running=True)
    assert res3[0].plan_description == ''


def test_build_dependencies_from_dag_data_and_get_stage_dependency():
    from spark_history_mcp.tools.tools import _build_dependencies_from_dag_data, get_stage_dependency_from_sql_plan
    # DAG data with nodes/edges and stage references: node 0 -> node 1 maps to stage 1 -> 2
    dag = {
        'stage_task_references': [{'stage_id': 1}, {'stage_id': 2}],
        'dagVizData': {
            'nodes': [{'name': 'stage 1'}, {'name': 'stage 2'}],
            'edges': [{'from': 0, 'to': 1}]
        }
    }
    deps = _build_dependencies_from_dag_data(dag)
    assert 1 in deps and 2 in deps
    assert any(p['stage_id'] == 1 for p in deps[2]['parents'])

    # Now test full API path using mocked client methods
    c = MagicMock()
    # Execution record with id and job ids
    exec_rec = SimpleNamespace(id=99, duration=10, running_job_ids=[1], success_job_ids=[2], failed_job_ids=[])
    c.get_sql_list.return_value = [exec_rec]
    # HTML methods
    c.get_sql_execution_html.return_value = '<html></html>'
    c.extract_dag_data_from_html.return_value = dag
    # Jobs and stages
    job1 = SimpleNamespace(job_id=1, name='job1', status='SUCCEEDED', submission_time=datetime.now(), completion_time=datetime.now(), stage_ids=[1])
    job2 = SimpleNamespace(job_id=2, name='job2', status='SUCCEEDED', submission_time=datetime.now(), completion_time=datetime.now(), stage_ids=[2])
    c.list_jobs.return_value = [job1, job2]
    st1 = SimpleNamespace(stage_id=1, name='s1', submission_time=datetime.now(), completion_time=datetime.now())
    st2 = SimpleNamespace(stage_id=2, name='s2', submission_time=datetime.now(), completion_time=datetime.now())
    c.list_stages.return_value = [st1, st2]
    with patch("spark_history_mcp.tools.tools.mcp.get_context", return_value=make_ctx(c)):
        out = get_stage_dependency_from_sql_plan('app')
    assert isinstance(out, dict)


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_get_app_summary_basic(mock_get_context):
    from spark_history_mcp.tools.tools import get_app_summary
    c = MagicMock()
    mock_get_context.return_value = make_ctx(c)
    # Application with one attempt
    now = datetime.now()
    attempt = SimpleNamespace(start_time=now, end_time=now + timedelta(minutes=5), duration=5*60*1000)
    app = SimpleNamespace(name='AppX', attempts=[attempt], cores_per_executor=2)
    c.get_application.return_value = app
    # Stages with minimal numeric attributes used by summary
    st1 = SimpleNamespace(
        status='COMPLETE', executor_run_time=60_000, executor_cpu_time=30_000_000_000,
        jvm_gc_time=5_000, input_bytes=10_000_000, output_bytes=5_000_000,
        shuffle_read_bytes=2_000_000, shuffle_write_bytes=1_000_000,
        memory_bytes_spilled=0, disk_bytes_spilled=0, num_failed_tasks=0,
        num_tasks=10, task_metrics_distributions=None
    )
    c.list_stages.return_value = [st1]
    # Executors
    ex1 = SimpleNamespace(add_time=now, remove_time=now + timedelta(minutes=4), total_cores=2, max_memory=512*1024*1024)
    c.list_all_executors.return_value = [ex1]

    out = get_app_summary('app')
    assert out['application_id'] == 'app'
    assert out['application_name'] == 'AppX'
    assert out['application_duration_minutes'] > 0
    assert 'input_data_size_gb' in out


@patch('spark_history_mcp.tools.tools.get_app_summary')
@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_compare_app_summaries_basic(mock_get_context, mock_get_app_summary):
    from spark_history_mcp.tools.tools import compare_app_summaries
    mock_get_context.return_value = make_ctx(MagicMock())
    s1 = {
        'application_id': 'a1', 'application_name': 'A1', 'analysis_timestamp': 't',
        'application_duration_minutes': 10.0, 'total_executor_runtime_minutes': 5.0,
        'executor_cpu_time_minutes': 3.0
    }
    s2 = {
        'application_id': 'a2', 'application_name': 'A2', 'analysis_timestamp': 't',
        'application_duration_minutes': 20.0, 'total_executor_runtime_minutes': 4.0,
        'executor_cpu_time_minutes': 6.0
    }
    mock_get_app_summary.side_effect = [s1, s2]
    out = compare_app_summaries('a1','a2', significance_threshold=0.05)
    assert 'app1_summary' in out and 'app2_summary' in out and 'diff' in out
    assert any(k.endswith('_change') for k in out['diff'].keys())


@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_compare_stages_basic(mock_get_context):
    from spark_history_mcp.tools.tools import compare_stages
    c = MagicMock()
    mock_get_context.return_value = make_ctx(c)
    # Stages with names, status, and times to compute duration
    now = datetime.now()
    st1 = SimpleNamespace(name='S1', status='COMPLETE', completion_time=now + timedelta(seconds=20), first_task_launched_time=now, task_metrics_distributions=None, executor_metrics_distributions=None)
    st2 = SimpleNamespace(name='S2', status='COMPLETE', completion_time=now + timedelta(seconds=30), first_task_launched_time=now, task_metrics_distributions=None, executor_metrics_distributions=None)
    c.get_stage_attempt.side_effect = [st1, st2]
    out = compare_stages('a1','a2', 1, 2, significance_threshold=0.1)
    assert 'stage_comparison' in out
    # Should include a duration difference
    diffs = out.get('significant_differences', {}).get('stage_metrics', {})
    assert 'duration_seconds' in diffs



@patch("spark_history_mcp.tools.tools.mcp.get_context")
def test_get_stage_task_summary(mock_get_context):
    from spark_history_mcp.tools.tools import get_stage_task_summary
    c = MagicMock()
    mock_get_context.return_value = make_ctx(c)
    # Mock return value (can be any object; function passes through)
    summary_obj = SimpleNamespace(quantiles=[0.5, 0.9], duration=[10, 20])
    c.get_stage_task_summary.return_value = summary_obj

    out = get_stage_task_summary(app_id='app', stage_id=7, attempt_id=2)
    assert out is summary_obj
    args, kwargs = c.get_stage_task_summary.call_args
    assert kwargs['app_id'] == 'app' and kwargs['stage_id'] == 7 and kwargs['attempt_id'] == 2
