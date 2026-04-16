from spark_history_mcp.cli.formatter_modules import OutputFormatter
from spark_history_mcp.cli.formatter_modules import comparison as comparison_module
from spark_history_mcp.cli.formatter_modules.comparison import (
    format_environment_comparison_result,
    format_performance_metrics,
    format_top_metrics_differences,
)


class DummyConsole:
    def __init__(self) -> None:
        self.printed = []

    def print(self, obj=None) -> None:
        if obj is not None:
            self.printed.append(obj)


def _build_overview():
    return {
        "executor_comparison": {
            "task_completion_ratio_change": "+20.0%",
            "applications": {
                "app1": {
                    "executor_metrics": {
                        "completed_tasks": 100,
                        "total_input_bytes": 1024,
                        "total_duration": 10000,
                        "total_shuffle_read": 2048,
                    }
                },
                "app2": {
                    "executor_metrics": {
                        "completed_tasks": 120,
                        "total_input_bytes": 2048,
                        "total_duration": 12000,
                        "total_shuffle_read": 4096,
                    }
                },
            },
        }
    }


def _get_metric_cells(table):
    return list(table.columns[0]._cells)


def test_performance_metrics_default_shows_key_metrics_only(monkeypatch):
    dummy_console = DummyConsole()
    monkeypatch.setattr(comparison_module, "console", dummy_console)

    formatter = OutputFormatter(show_all_metrics=False)
    format_performance_metrics(formatter, _build_overview())

    assert dummy_console.printed
    table = dummy_console.printed[-1]
    metric_cells = _get_metric_cells(table)

    assert metric_cells == ["Completed Tasks", "Input Data", "Total Duration"]
    assert "Shuffle Read" not in metric_cells


def test_performance_metrics_show_all_includes_extra_metrics(monkeypatch):
    dummy_console = DummyConsole()
    monkeypatch.setattr(comparison_module, "console", dummy_console)

    formatter = OutputFormatter(show_all_metrics=True)
    format_performance_metrics(formatter, _build_overview())

    assert dummy_console.printed
    table = dummy_console.printed[-1]
    metric_cells = _get_metric_cells(table)

    assert "Completed Tasks" in metric_cells
    assert "Input Data" in metric_cells
    assert "Total Duration" in metric_cells
    assert "Shuffle Read" in metric_cells


def test_top_metric_differences_table(monkeypatch):
    dummy_console = DummyConsole()
    monkeypatch.setattr(comparison_module, "console", dummy_console)

    formatter = OutputFormatter()
    format_top_metrics_differences(
        formatter,
        [
            {
                "metric": "duration_ms",
                "left": 1000,
                "right": 2000,
                "percent_change": 100.0,
            },
            {
                "metric": "total_input_bytes",
                "left": 1024,
                "right": 2048,
                "percent_change": 100.0,
            },
        ],
    )

    table = dummy_console.printed[-1]
    metric_cells = _get_metric_cells(table)
    assert "duration_ms" in metric_cells
    assert "total_input_bytes" in metric_cells


def test_environment_comparison_table(monkeypatch):
    dummy_console = DummyConsole()
    monkeypatch.setattr(comparison_module, "console", dummy_console)

    formatter = OutputFormatter()
    format_environment_comparison_result(
        formatter,
        {
            "spark_properties": {
                "different": {"spark.executor.memory": {"app1": "2g", "app2": "4g"}},
                "app1_only": {},
                "app2_only": {},
                "performance_impact_analysis": [],
            },
            "runtime_environment": {"differences": []},
            "system_properties": {"key_differences": {}},
        },
    )

    table = dummy_console.printed[0]
    assert "Spark Properties Differences" in str(table.title)


def test_environment_comparison_omits_identical_jvm_info(monkeypatch):
    dummy_console = DummyConsole()
    monkeypatch.setattr(comparison_module, "console", dummy_console)

    formatter = OutputFormatter()
    format_environment_comparison_result(
        formatter,
        {
            "jvm_info": {
                "java_version": {"app1": "17.0.12", "app2": "17.0.12"},
                "java_home": {"app1": "/opt/java/openjdk", "app2": "/opt/java/openjdk"},
                "scala_version": {"app1": "2.12.18", "app2": "2.12.18"},
            },
            "spark_properties": {
                "different": {"spark.executor.memory": {"app1": "2g", "app2": "4g"}},
                "app1_only": {},
                "app2_only": {},
                "performance_impact_analysis": [],
            },
            "system_properties": {"different": []},
        },
    )

    printed_titles = [
        str(obj.title) for obj in dummy_console.printed if hasattr(obj, "title")
    ]
    assert "JVM Information" not in printed_titles
    assert "Spark Properties Differences" in printed_titles


def test_environment_comparison_filters_spark_app_name(monkeypatch):
    dummy_console = DummyConsole()
    monkeypatch.setattr(comparison_module, "console", dummy_console)

    formatter = OutputFormatter()
    format_environment_comparison_result(
        formatter,
        {
            "spark_properties": {
                "different": {
                    "spark.app.name": {"app1": "job-a", "app2": "job-b"},
                    "spark.executor.memory": {"app1": "2g", "app2": "4g"},
                },
                "app1_only": {},
                "app2_only": {},
                "performance_impact_analysis": [],
            },
            "system_properties": {"different": []},
        },
    )

    spark_table = next(obj for obj in dummy_console.printed if hasattr(obj, "title"))
    property_cells = list(spark_table.columns[0]._cells)
    assert "spark.app.name" not in property_cells
    assert "spark.executor.memory" in property_cells


def test_environment_comparison_summary_ignores_filtered_spark_app_name(monkeypatch):
    dummy_console = DummyConsole()
    monkeypatch.setattr(comparison_module, "console", dummy_console)

    formatter = OutputFormatter()
    format_environment_comparison_result(
        formatter,
        {
            "spark_properties": {
                "different": {
                    "spark.app.name": {"app1": "job-a", "app2": "job-b"},
                },
                "total_different": 1,
                "app1_only_count": 0,
                "app2_only_count": 0,
            },
            "system_properties": {"different": [], "total_different": 0},
        },
    )

    printed_text = [obj for obj in dummy_console.printed if isinstance(obj, str)]
    assert all("Spark props:" not in line for line in printed_text)
    assert all("more differences" not in line for line in printed_text)


def test_environment_comparison_filters_sun_java_command(monkeypatch):
    dummy_console = DummyConsole()
    monkeypatch.setattr(comparison_module, "console", dummy_console)

    formatter = OutputFormatter()
    format_environment_comparison_result(
        formatter,
        {
            "spark_properties": {"different": {}},
            "system_properties": {
                "different": [
                    {
                        "property": "sun.java.command",
                        "app1_value": "cmd-a",
                        "app2_value": "cmd-b",
                    },
                    {
                        "property": "java.io.tmpdir",
                        "app1_value": "/var/data/a",
                        "app2_value": "/var/data/b",
                    },
                ],
                "total_different": 2,
            },
        },
    )

    system_table = next(obj for obj in dummy_console.printed if hasattr(obj, "title"))
    property_cells = list(system_table.columns[0]._cells)
    assert "sun.java.command" not in property_cells
    assert "java.io.tmpdir" in property_cells


def test_environment_comparison_summary_ignores_filtered_sun_java_command(monkeypatch):
    dummy_console = DummyConsole()
    monkeypatch.setattr(comparison_module, "console", dummy_console)

    formatter = OutputFormatter()
    format_environment_comparison_result(
        formatter,
        {
            "spark_properties": {"different": {}, "total_different": 0},
            "system_properties": {
                "different": [
                    {
                        "property": "sun.java.command",
                        "app1_value": "cmd-a",
                        "app2_value": "cmd-b",
                    }
                ],
                "total_different": 1,
            },
        },
    )

    printed_text = [obj for obj in dummy_console.printed if isinstance(obj, str)]
    assert all("System props:" not in line for line in printed_text)
    assert all("more differences" not in line for line in printed_text)


def test_format_number_with_commas():
    from spark_history_mcp.cli.formatter_modules.comparison import (
        _format_number_with_commas,
    )

    assert _format_number_with_commas(10000) == "10,000"
    assert _format_number_with_commas(9999) == "9999"
    assert _format_number_with_commas(1) == "1"
    assert _format_number_with_commas(0) == "0"

    assert _format_number_with_commas(10000.0) == "10,000"
    assert _format_number_with_commas(123.45) == "123.45"

    assert _format_number_with_commas(-10000) == "-10,000"
    assert _format_number_with_commas(-15000.5) == "-15,000.5"
