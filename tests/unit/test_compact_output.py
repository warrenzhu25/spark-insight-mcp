"""Tests for compact output infrastructure."""

from __future__ import annotations

from unittest.mock import patch

from spark_history_mcp.tools.common import (
    ToolConfig,
    _compact_dict,
    compact_dict,
    strip_applications_metadata,
)
from spark_history_mcp.tools.recommendations import compact_recommendation


class TestCompactDict:
    """Tests for _compact_dict and compact_dict."""

    def test_truncates_recommendations(self):
        cfg = ToolConfig(compact_recommendations_limit=2)
        data = {
            "recommendations": [
                {"priority": "high", "issue": "a", "suggestion": "b"},
                {"priority": "medium", "issue": "c", "suggestion": "d"},
                {"priority": "low", "issue": "e", "suggestion": "f"},
            ],
        }
        result = _compact_dict(data, cfg)
        assert len(result["recommendations"]) == 2
        assert result["_recommendations_truncated"]["total"] == 3
        assert result["_recommendations_truncated"]["returned"] == 2

    def test_no_truncation_metadata_when_within_limit(self):
        cfg = ToolConfig(compact_recommendations_limit=10)
        data = {"recommendations": [{"priority": "high", "issue": "a"}]}
        result = _compact_dict(data, cfg)
        assert len(result["recommendations"]) == 1
        assert "_recommendations_truncated" not in result

    def test_truncates_timeline(self):
        cfg = ToolConfig(compact_timeline_limit=2)
        data = {
            "timeline": [{"t": 1}, {"t": 2}, {"t": 3}, {"t": 4}],
            "summary": {"total": 4},
        }
        result = _compact_dict(data, cfg)
        assert len(result["timeline"]) == 2
        assert result["_timeline_truncated"]["total"] == 4

    def test_recurses_into_nested_dicts(self):
        cfg = ToolConfig(compact_recommendations_limit=1)
        data = {
            "nested": {
                "recommendations": [
                    {"priority": "high", "issue": "a"},
                    {"priority": "low", "issue": "b"},
                ],
            },
        }
        result = _compact_dict(data, cfg)
        assert len(result["nested"]["recommendations"]) == 1

    def test_compact_dict_disabled(self):
        data = {"recommendations": [1, 2, 3]}
        result = compact_dict(data, compact=False)
        assert result is data

    def test_compact_dict_enabled(self):
        with patch(
            "spark_history_mcp.tools.common.get_config",
            return_value=ToolConfig(
                compact_tool_output=True, compact_recommendations_limit=1
            ),
        ):
            data = {"recommendations": [{"a": 1}, {"b": 2}]}
            result = compact_dict(data)
            assert len(result["recommendations"]) == 1


class TestStripApplicationsMetadata:
    def test_strips_applications_key(self):
        data = {
            "applications": {"app1": {}, "app2": {}},
            "metrics": {"cpu": 0.5},
        }
        result = strip_applications_metadata(data)
        assert "applications" not in result
        assert result["metrics"] == {"cpu": 0.5}

    def test_passthrough_non_dict(self):
        assert strip_applications_metadata([1, 2]) == [1, 2]


class TestCompactRecommendation:
    def test_keeps_only_essential_fields(self):
        rec = {
            "type": "resource_allocation",
            "priority": "high",
            "issue": "Too many cores",
            "suggestion": "Reduce cores",
            "source_analysis": "auto_scaling",
            "recommendation_type": "scaling",
        }
        result = compact_recommendation(rec)
        assert set(result.keys()) == {"priority", "issue", "suggestion"}
        assert result["priority"] == "high"

    def test_defaults_for_missing_fields(self):
        result = compact_recommendation({})
        assert result["priority"] == "low"
        assert result["issue"] == ""
        assert result["suggestion"] == ""
