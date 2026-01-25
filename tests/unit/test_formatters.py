from spark_history_mcp.cli import formatters as formatters_module
from spark_history_mcp.cli.formatters import OutputFormatter


class DummyConsole:
    def __init__(self) -> None:
        self.printed = []

    def print(self, obj) -> None:
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
    monkeypatch.setattr(formatters_module, "console", dummy_console)

    formatter = OutputFormatter(show_all_metrics=False)
    formatter._format_performance_metrics(_build_overview())

    assert dummy_console.printed
    table = dummy_console.printed[-1]
    metric_cells = _get_metric_cells(table)

    assert metric_cells == ["Completed Tasks", "Input Data", "Total Duration"]
    assert "Shuffle Read" not in metric_cells


def test_performance_metrics_show_all_includes_extra_metrics(monkeypatch):
    dummy_console = DummyConsole()
    monkeypatch.setattr(formatters_module, "console", dummy_console)

    formatter = OutputFormatter(show_all_metrics=True)
    formatter._format_performance_metrics(_build_overview())

    assert dummy_console.printed
    table = dummy_console.printed[-1]
    metric_cells = _get_metric_cells(table)

    assert "Completed Tasks" in metric_cells
    assert "Input Data" in metric_cells
    assert "Total Duration" in metric_cells
    assert "Shuffle Read" in metric_cells
