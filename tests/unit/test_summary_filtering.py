from unittest.mock import patch


@patch("spark_history_mcp.tools.comparison_modules.core.compare_app_stages_aggregated")
@patch("spark_history_mcp.tools.get_app_summary")
@patch("spark_history_mcp.tools.mcp.get_context")
def test_compare_app_summaries_always_shows_top_5(
    mock_ctx, mock_get_app_summary, mock_stage_agg
):
    from spark_history_mcp.tools import compare_app_summaries

    # Mock app summaries with 6 metrics that have very small changes (< 1%)
    # but some are slightly larger than others.
    app1 = {
        "application_id": "app-1",
        "metric1": 100.0,
        "metric2": 100.0,
        "metric3": 100.0,
        "metric4": 100.0,
        "metric5": 100.0,
        "metric6": 100.0,
        "application_name": "app1",
        "analysis_timestamp": "timestamp",
    }

    app2 = {
        "application_id": "app-2",
        "metric1": 100.9,  # +0.9%
        "metric2": 100.8,  # +0.8%
        "metric3": 100.7,  # +0.7%
        "metric4": 100.6,  # +0.6%
        "metric5": 100.5,  # +0.5%
        "metric6": 100.4,  # +0.4%
        "application_name": "app2",
        "analysis_timestamp": "timestamp",
    }

    def side_effect(app_id, server=None):
        return app1 if app_id == "app-1" else app2

    mock_get_app_summary.side_effect = side_effect
    mock_stage_agg.return_value = {"error": "disabled"}

    # Default threshold is 0.1 (10%). All metrics are way below this.
    # Previously, diff would be empty. Now it should have 5 entries.
    out = compare_app_summaries("app-1", "app-2", significance_threshold=0.1)

    diff = out.get("diff", {})
    # Should contain top 5 metrics (metric1 to metric5)
    assert len(diff) == 5
    assert "metric1_change" in diff
    assert "metric2_change" in diff
    assert "metric3_change" in diff
    assert "metric4_change" in diff
    assert "metric5_change" in diff
    assert "metric6_change" not in diff

    # Verify the actual values
    assert diff["metric1_change"] == "+0.9%"
    assert diff["metric5_change"] == "+0.5%"

    # Also verify filtering_summary
    filtering = out.get("filtering_summary", {})
    assert filtering["forced_top_metrics"] == 5
    assert filtering["significant_metrics"] == 5
