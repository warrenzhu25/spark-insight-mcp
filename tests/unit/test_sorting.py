"""
Tests for spark_history_mcp.utils.sorting module.
"""

import copy

from spark_history_mcp.utils.sorting import (
    extract_percentage_value,
    sort_comparison_data,
    sort_metrics_by_change,
    sort_metrics_by_ratio,
    sort_mixed_metrics,
)


class TestExtractPercentageValue:
    """Test the extract_percentage_value function."""

    def test_positive_percentage(self):
        """Test extracting positive percentage values."""
        assert extract_percentage_value("+45.2%") == 45.2
        assert extract_percentage_value("+100%") == 100.0
        assert extract_percentage_value("+0%") == 0.0

    def test_negative_percentage(self):
        """Test extracting negative percentage values (returns absolute)."""
        assert extract_percentage_value("-25.5%") == 25.5
        assert extract_percentage_value("-100%") == 100.0
        assert extract_percentage_value("-0%") == 0.0

    def test_percentage_without_sign(self):
        """Test extracting percentage values without +/- sign."""
        assert extract_percentage_value("50%") == 50.0
        assert extract_percentage_value("0%") == 0.0
        assert extract_percentage_value("123.45%") == 123.45

    def test_special_values(self):
        """Test handling of special string values."""
        assert extract_percentage_value("N/A") == 0.0
        assert extract_percentage_value("+∞") == 0.0
        assert extract_percentage_value("-∞") == 0.0

    def test_invalid_values(self):
        """Test handling of invalid or malformed values."""
        assert extract_percentage_value("invalid") == 0.0
        assert extract_percentage_value("") == 0.0
        assert extract_percentage_value("abc%") == 0.0
        assert (
            extract_percentage_value("++50%") == 50.0
        )  # Actually valid after stripping +

    def test_numeric_without_percent(self):
        """Test handling of numeric values without percent sign."""
        assert extract_percentage_value("45.2") == 45.2
        assert extract_percentage_value("-25.5") == 25.5
        assert extract_percentage_value("+100") == 100.0

    def test_non_string_input(self):
        """Test handling of non-string input."""
        assert extract_percentage_value(None) == 0.0
        # Note: The function expects string input, but should handle edge cases gracefully


class TestSortMetricsByChange:
    """Test the sort_metrics_by_change function."""

    def test_empty_dict(self):
        """Test handling of empty dictionary."""
        result = sort_metrics_by_change({})
        assert result == {}

    def test_no_change_metrics(self):
        """Test dictionary with no _change suffix keys."""
        data = {"metric_a": 100, "metric_b": 200, "metric_ratio": 1.5}
        result = sort_metrics_by_change(data)
        assert result == data  # Should return unchanged

    def test_basic_change_sorting(self):
        """Test basic sorting of change metrics."""
        data = {
            "small_change": "+5%",
            "large_change": "+50%",
            "medium_change": "+25%",
            "other_metric": 100,
        }
        result = sort_metrics_by_change(data)

        # Should be sorted by change magnitude (descending)
        keys = list(result.keys())
        assert keys[0] == "large_change"
        assert keys[1] == "medium_change"
        assert keys[2] == "small_change"
        assert keys[3] == "other_metric"

    def test_negative_changes(self):
        """Test sorting with negative changes (sorted by absolute value)."""
        data = {
            "big_decrease_change": "-75%",
            "small_increase_change": "+10%",
            "small_decrease_change": "-5%",
            "other_metric": "value",
        }
        result = sort_metrics_by_change(data)

        keys = list(result.keys())
        assert keys[0] == "big_decrease_change"  # 75% (largest absolute)
        assert keys[1] == "small_increase_change"  # 10%
        assert keys[2] == "small_decrease_change"  # 5%
        assert keys[3] == "other_metric"

    def test_special_change_values(self):
        """Test sorting with special change values."""
        data = {
            "infinite_change": "+∞",
            "na_change": "N/A",
            "normal_change": "+30%",
            "zero_change": "+0%",
        }
        result = sort_metrics_by_change(data)

        # Special values should be treated as 0.0 and sorted together
        keys = list(result.keys())
        assert keys[0] == "normal_change"  # 30% (only non-zero change)

    def test_ascending_sort(self):
        """Test sorting in ascending order."""
        data = {"large_change": "+50%", "small_change": "+5%", "medium_change": "+25%"}
        result = sort_metrics_by_change(data, reverse=False)

        keys = list(result.keys())
        assert keys[0] == "small_change"
        assert keys[1] == "medium_change"
        assert keys[2] == "large_change"

    def test_non_string_change_values(self):
        """Test handling of non-string values for _change keys."""
        data = {
            "string_change": "+25%",
            "numeric_change": 25,  # Not a string, should be in other_metrics
            "other_metric": "value",
        }
        result = sort_metrics_by_change(data)

        # Only string_change should be sorted as a change metric
        keys = list(result.keys())
        assert keys[0] == "string_change"
        assert "numeric_change" in result
        assert "other_metric" in result


class TestSortMetricsByRatio:
    """Test the sort_metrics_by_ratio function."""

    def test_empty_dict(self):
        """Test handling of empty dictionary."""
        result = sort_metrics_by_ratio({})
        assert result == {}

    def test_no_ratio_metrics(self):
        """Test dictionary with no _ratio suffix keys."""
        data = {"metric_a": 100, "metric_b": 200, "metric_change": "+25%"}
        result = sort_metrics_by_ratio(data)
        assert result == data

    def test_basic_ratio_sorting(self):
        """Test basic sorting of ratio metrics by deviation from 1.0."""
        data = {
            "small_ratio": 1.1,  # deviation: 0.1
            "large_ratio": 2.0,  # deviation: 1.0
            "medium_ratio": 1.5,  # deviation: 0.5
            "other_metric": "value",
        }
        result = sort_metrics_by_ratio(data)

        # Should be sorted by deviation from 1.0 (descending)
        keys = list(result.keys())
        assert keys[0] == "large_ratio"
        assert keys[1] == "medium_ratio"
        assert keys[2] == "small_ratio"
        assert keys[3] == "other_metric"

    def test_ratios_below_one(self):
        """Test sorting with ratios below 1.0."""
        data = {
            "small_decrease_ratio": 0.9,  # deviation: 0.1
            "large_decrease_ratio": 0.5,  # deviation: 0.5
            "increase_ratio": 1.3,  # deviation: 0.3
        }
        result = sort_metrics_by_ratio(data)

        keys = list(result.keys())
        assert keys[0] == "large_decrease_ratio"  # 0.5 deviation
        assert keys[1] == "increase_ratio"  # 0.3 deviation
        assert keys[2] == "small_decrease_ratio"  # 0.1 deviation

    def test_ratio_of_one(self):
        """Test handling of ratio exactly equal to 1.0."""
        data = {
            "no_change_ratio": 1.0,
            "big_change_ratio": 2.0,
            "small_change_ratio": 1.1,
        }
        result = sort_metrics_by_ratio(data)

        keys = list(result.keys())
        assert keys[0] == "big_change_ratio"
        assert keys[1] == "small_change_ratio"
        assert keys[2] == "no_change_ratio"  # Should be last (no deviation)

    def test_ascending_sort(self):
        """Test sorting in ascending order."""
        data = {"large_ratio": 2.0, "small_ratio": 1.1, "medium_ratio": 1.5}
        result = sort_metrics_by_ratio(data, reverse=False)

        keys = list(result.keys())
        assert keys[0] == "small_ratio"
        assert keys[1] == "medium_ratio"
        assert keys[2] == "large_ratio"

    def test_non_numeric_ratio_values(self):
        """Test handling of non-numeric values for _ratio keys."""
        data = {
            "numeric_ratio": 1.5,
            "string_ratio": "1.5",  # String, should be in other_metrics
            "other_metric": 100,
        }
        result = sort_metrics_by_ratio(data)

        # Only numeric_ratio should be sorted as a ratio metric
        keys = list(result.keys())
        assert keys[0] == "numeric_ratio"
        assert "string_ratio" in result
        assert "other_metric" in result


class TestSortMixedMetrics:
    """Test the sort_mixed_metrics function."""

    def test_empty_dict(self):
        """Test handling of empty dictionary."""
        result = sort_mixed_metrics({})
        assert result == {}

    def test_mixed_metrics_prioritization(self):
        """Test that change metrics come before ratio metrics before others."""
        data = {
            "other_metric": 100,
            "medium_ratio": 1.5,
            "large_change": "+50%",
            "small_ratio": 1.1,
            "small_change": "+10%",
            "another_metric": "value",
        }
        result = sort_mixed_metrics(data)

        keys = list(result.keys())

        # Changes should come first (sorted by magnitude)
        assert keys[0] == "large_change"
        assert keys[1] == "small_change"

        # Then ratios (sorted by deviation from 1.0)
        assert keys[2] == "medium_ratio"
        assert keys[3] == "small_ratio"

        # Then other metrics (original order preserved)
        assert "other_metric" in keys[4:]
        assert "another_metric" in keys[4:]

    def test_only_change_metrics(self):
        """Test with only change metrics."""
        data = {"big_change": "+75%", "small_change": "+5%", "medium_change": "+25%"}
        result = sort_mixed_metrics(data)

        keys = list(result.keys())
        assert keys == ["big_change", "medium_change", "small_change"]

    def test_only_ratio_metrics(self):
        """Test with only ratio metrics."""
        data = {"big_ratio": 2.0, "small_ratio": 1.05, "medium_ratio": 1.3}
        result = sort_mixed_metrics(data)

        keys = list(result.keys())
        assert keys == ["big_ratio", "medium_ratio", "small_ratio"]

    def test_only_other_metrics(self):
        """Test with only other metrics (no change or ratio)."""
        data = {"metric_c": 300, "metric_a": 100, "metric_b": 200}
        result = sort_mixed_metrics(data)

        # Should preserve original order for other metrics
        assert result == data

    def test_ascending_sort(self):
        """Test mixed sorting in ascending order."""
        data = {
            "large_change": "+50%",
            "small_change": "+10%",
            "large_ratio": 2.0,
            "small_ratio": 1.1,
        }
        result = sort_mixed_metrics(data, reverse=False)

        keys = list(result.keys())

        # Changes first (ascending by magnitude)
        assert keys[0] == "small_change"
        assert keys[1] == "large_change"

        # Ratios second (ascending by deviation)
        assert keys[2] == "small_ratio"
        assert keys[3] == "large_ratio"


class TestSortComparisonData:
    """Test the sort_comparison_data function."""

    def test_non_dict_input(self):
        """Test handling of non-dictionary input."""
        assert sort_comparison_data("not a dict") == "not a dict"
        assert sort_comparison_data([1, 2, 3]) == [1, 2, 3]
        assert sort_comparison_data(None) is None

    def test_empty_dict(self):
        """Test handling of empty dictionary."""
        result = sort_comparison_data({})
        assert result == {}

    def test_sort_root_level_metrics(self):
        """Test sorting of metrics at root level."""
        data = {
            "diff": {
                "large_change": "+50%",
                "small_change": "+10%",
                "other_metric": 100,
            },
            "executor_comparison": {"big_ratio": 2.0, "small_ratio": 1.1},
            "unrelated_data": "value",
        }

        result = sort_comparison_data(data, sort_key="mixed")

        # Check that diff was sorted (changes first)
        diff_keys = list(result["diff"].keys())
        assert diff_keys[0] == "large_change"
        assert diff_keys[1] == "small_change"
        assert diff_keys[2] == "other_metric"

        # Check that executor_comparison was sorted
        exec_keys = list(result["executor_comparison"].keys())
        assert exec_keys[0] == "big_ratio"
        assert exec_keys[1] == "small_ratio"

        # Unrelated data should remain unchanged
        assert result["unrelated_data"] == "value"

    def test_sort_nested_paths(self):
        """Test sorting of metrics at nested paths."""
        data = {
            "app_summary_diff": {
                "diff": {
                    "medium_change": "+25%",
                    "large_change": "+75%",
                    "small_change": "+5%",
                },
                "other_data": "preserved",
            },
            "performance_comparison": {
                "executors": {"executor_ratio": 1.5, "memory_ratio": 2.0},
                "stages": {"stage_change": "+30%", "duration_change": "+10%"},
            },
        }

        result = sort_comparison_data(data, sort_key="mixed")

        # Check nested diff sorting
        nested_diff_keys = list(result["app_summary_diff"]["diff"].keys())
        assert nested_diff_keys[0] == "large_change"
        assert nested_diff_keys[1] == "medium_change"
        assert nested_diff_keys[2] == "small_change"

        # Check nested executors sorting
        exec_keys = list(result["performance_comparison"]["executors"].keys())
        assert exec_keys[0] == "memory_ratio"  # Higher deviation from 1.0
        assert exec_keys[1] == "executor_ratio"

        # Check nested stages sorting
        stage_keys = list(result["performance_comparison"]["stages"].keys())
        assert stage_keys[0] == "stage_change"  # 30% > 10%
        assert stage_keys[1] == "duration_change"

    def test_different_sort_keys(self):
        """Test different sort key options."""
        data = {
            "diff": {
                "metric_change": "+25%",  # Proper _change suffix
                "metric_ratio": 1.5,  # Proper _ratio suffix
                "other_metric": 100,
            }
        }

        # Test change sorting
        change_result = sort_comparison_data(data, sort_key="change")
        change_keys = list(change_result["diff"].keys())
        assert change_keys[0] == "metric_change"

        # Test ratio sorting
        ratio_result = sort_comparison_data(data, sort_key="ratio")
        ratio_keys = list(ratio_result["diff"].keys())
        assert ratio_keys[0] == "metric_ratio"

        # Test mixed sorting (default)
        mixed_result = sort_comparison_data(data, sort_key="mixed")
        mixed_keys = list(mixed_result["diff"].keys())
        assert mixed_keys[0] == "metric_change"  # Changes have priority
        assert mixed_keys[1] == "metric_ratio"

    def test_invalid_sort_key(self):
        """Test handling of invalid sort key (falls back to mixed)."""
        data = {
            "diff": {"change_metric": "+25%", "ratio_metric": 1.5, "other_metric": 100}
        }

        result = sort_comparison_data(data, sort_key="invalid")

        # Should fall back to mixed sorting
        keys = list(result["diff"].keys())
        assert keys[0] == "change_metric"  # Changes first in mixed sorting
        assert keys[1] == "ratio_metric"  # Ratios second
        assert keys[2] == "other_metric"  # Others last

    def test_deep_copy_behavior(self):
        """Test that original data is not modified."""
        original_data = {
            "diff": {
                "small_change": "+10%",  # Put small first so sorting will change order
                "large_change": "+50%",
            }
        }

        # Create a copy to compare against
        expected_original = copy.deepcopy(original_data)

        # Sort the data
        result = sort_comparison_data(original_data)

        # Original should be unchanged
        assert original_data == expected_original

        # Result should be different (sorted)
        result_keys = list(result["diff"].keys())
        original_keys = list(original_data["diff"].keys())
        assert (
            result_keys != original_keys
        )  # large_change should come first after sorting

    def test_missing_nested_paths(self):
        """Test handling of missing nested paths."""
        data = {
            "app_summary_diff": {
                # Missing 'diff' key
                "other_data": "value"
            },
            "performance_comparison": {
                # Missing 'executors' and 'stages' keys
                "other_data": "value"
            },
        }

        # Should not crash and return data unchanged
        result = sort_comparison_data(data)
        assert result == data

    def test_non_dict_values_in_metric_keys(self):
        """Test handling of non-dict values for expected metric keys."""
        data = {
            "diff": "not a dict",  # Should be ignored for sorting
            "executor_comparison": ["not", "a", "dict"],
            "valid_diff": {"change_metric": "+25%"},
        }

        result = sort_comparison_data(data)

        # Non-dict values should be preserved as-is
        assert result["diff"] == "not a dict"
        assert result["executor_comparison"] == ["not", "a", "dict"]

        # Valid dict should be processed
        assert "change_metric" in result["valid_diff"]
