from types import SimpleNamespace

from spark_history_mcp.tools.comparison_modules.utils import _compare_environments


def _make_env(spark_props=None, system_props=None):
    env = SimpleNamespace()
    env.spark_properties = [(k, v) for k, v in (spark_props or {}).items()]
    env.system_properties = [(k, v) for k, v in (system_props or {}).items()]
    return env


def test_compare_environments_filters_auto_generated_keys():
    app1_env = _make_env(
        spark_props={
            "spark.app.id": "spark-111",
            "spark.driver.host": "driver-1",
            "spark.sql.shuffle.partitions": "200",
        }
    )
    app2_env = _make_env(
        spark_props={
            "spark.app.id": "spark-222",
            "spark.driver.host": "driver-2",
            "spark.sql.shuffle.partitions": "200",
        }
    )

    result = _compare_environments(app1_env, app2_env, filter_auto_generated=True)
    spark_props = result["spark_properties"]

    assert spark_props["total_different"] == 0
    assert spark_props["different"] == []


def test_compare_environments_normalizes_k8s_and_temp_values():
    app1_env = _make_env(
        spark_props={
            "spark.kubernetes.executor.pod.name": "executor-pod-aaa",
        },
        system_props={
            "java.io.tmpdir": "temp-dir/spark-aaa",
        },
    )
    app2_env = _make_env(
        spark_props={
            "spark.kubernetes.executor.pod.name": "executor-pod-bbb",
        },
        system_props={
            "java.io.tmpdir": "temp-dir/spark-bbb",
        },
    )

    result = _compare_environments(app1_env, app2_env, filter_auto_generated=True)

    assert result["spark_properties"]["total_different"] == 0
    assert result["spark_properties"]["different"] == []
    assert result["system_properties"]["total_different"] == 0
    assert result["system_properties"]["different"] == []


def test_compare_environments_keeps_meaningful_differences():
    app1_env = _make_env(
        spark_props={"spark.sql.shuffle.partitions": "200"},
    )
    app2_env = _make_env(
        spark_props={"spark.sql.shuffle.partitions": "500"},
    )

    result = _compare_environments(app1_env, app2_env, filter_auto_generated=True)
    spark_props = result["spark_properties"]

    assert spark_props["total_different"] == 1
    assert spark_props["different"][0]["property"] == "spark.sql.shuffle.partitions"
