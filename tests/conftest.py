import pytest


@pytest.fixture(autouse=True)
def _disable_compact_tool_output(monkeypatch):
    """Keep tool output full for unit tests to avoid brittle expectations."""
    monkeypatch.setenv("SHS_COMPACT_TOOL_OUTPUT", "false")
