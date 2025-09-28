import os
from types import SimpleNamespace

from spark_history_mcp.tools.common import get_config
from spark_history_mcp.tools.matching import match_stages
from spark_history_mcp.tools.metrics import compare_numeric_maps, compare_distributions
from spark_history_mcp.tools.timelines import merge_intervals


def make_stage(name, submit=None, complete=None):
    return SimpleNamespace(name=name, submission_time=submit, completion_time=complete)


def test_matching_by_name_only():
    s1 = [make_stage("Filter at MapPartitions"), make_stage("Shuffle Sort")]
    s2 = [make_stage("filter at mappartitions"), make_stage("Other")] 
    matches = match_stages(s1, s2, similarity_threshold=0.7)
    assert matches, "Expected at least one match"
    assert matches[0].stage1.name.lower().startswith("filter")
    assert matches[0].similarity >= 0.7


def test_compare_numeric_maps_filters_by_significance():
    m1 = {"a": 100, "b": 200}
    m2 = {"a": 110, "b": 202}  # a=+10%, b=+1%
    out = compare_numeric_maps(m1, m2, significance=0.05)
    assert "a" in out["differences"]
    assert "b" not in out["differences"], "b change below threshold should be filtered"


class DistObj:
    def __init__(self, fetch_wait=None, write_time=None):
        self.shuffle_read_metrics = SimpleNamespace(fetch_wait_time=fetch_wait)
        self.shuffle_write_metrics = SimpleNamespace(write_time=write_time)


def test_compare_distributions_uses_median():
    d1 = DistObj(fetch_wait=[0, 1, 10], write_time=[0, 1, 5])  # medians 10, 5
    d2 = DistObj(fetch_wait=[0, 1, 20], write_time=[0, 1, 10])  # medians 20, 10
    out = compare_distributions(
        d1,
        d2,
        fields=[("shuffle_read_metrics.fetch_wait_time", "fetch_wait"), ("shuffle_write_metrics.write_time", "write")],
        significance=0.5,  # 50%
    )
    assert out["metrics"]["fetch_wait"]["significant"] is True  # 100% increase
    assert out["metrics"]["write"]["significant"] is True


def test_merge_intervals_collapses_consecutive_same_diff():
    items = [
        {"timestamp_range": "t1 to t2", "differences": {"executor_count_diff": 0}},
        {"timestamp_range": "t2 to t3", "differences": {"executor_count_diff": 0}},
        {"timestamp_range": "t3 to t4", "differences": {"executor_count_diff": 1}},
    ]
    merged = merge_intervals(items)
    assert len(merged) == 2
    assert merged[0]["timestamp_range"].endswith("t3")


def test_toolconfig_env_override(monkeypatch):
    monkeypatch.setenv("SHS_STAGE_MATCH_SIMILARITY", "0.9")
    cfg = get_config()
    assert abs(cfg.stage_match_similarity - 0.9) < 1e-6
