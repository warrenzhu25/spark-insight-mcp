"""
Tests for spark_history_mcp.utils.model_fields module.
"""

from typing import Optional, Sequence

from pydantic import BaseModel

from spark_history_mcp.models.spark_types import StageData, TaskMetricDistributions
from spark_history_mcp.utils.model_fields import (
    DEFAULT_EXCLUDE_FIELDS,
    NANOSECOND_FIELDS,
    NS_TO_MS_FACTOR,
    ComparableField,
    get_comparable_numeric_fields,
    get_distribution_fields,
    get_field_value,
    get_quantile_value,
    is_nested_model,
    is_numeric_type,
    is_sequence_of_numeric,
)


class TestIsNumericType:
    """Test the is_numeric_type function."""

    def test_int_type(self):
        """Test that int is recognized as numeric."""
        assert is_numeric_type(int) is True

    def test_float_type(self):
        """Test that float is recognized as numeric."""
        assert is_numeric_type(float) is True

    def test_optional_int(self):
        """Test that Optional[int] is recognized as numeric."""
        assert is_numeric_type(Optional[int]) is True

    def test_optional_float(self):
        """Test that Optional[float] is recognized as numeric."""
        assert is_numeric_type(Optional[float]) is True

    def test_str_type(self):
        """Test that str is not recognized as numeric."""
        assert is_numeric_type(str) is False

    def test_list_type(self):
        """Test that list is not recognized as numeric."""
        assert is_numeric_type(list) is False

    def test_sequence_float(self):
        """Test that Sequence[float] is not recognized as plain numeric."""
        assert is_numeric_type(Sequence[float]) is False


class TestIsSequenceOfNumeric:
    """Test the is_sequence_of_numeric function."""

    def test_sequence_float(self):
        """Test that Sequence[float] is recognized."""
        assert is_sequence_of_numeric(Sequence[float]) is True

    def test_sequence_int(self):
        """Test that Sequence[int] is recognized."""
        assert is_sequence_of_numeric(Sequence[int]) is True

    def test_list_float(self):
        """Test that list[float] is recognized."""
        assert is_sequence_of_numeric(list[float]) is True

    def test_optional_sequence_float(self):
        """Test that Optional[Sequence[float]] is recognized."""
        assert is_sequence_of_numeric(Optional[Sequence[float]]) is True

    def test_plain_int(self):
        """Test that plain int is not a sequence."""
        assert is_sequence_of_numeric(int) is False

    def test_sequence_str(self):
        """Test that Sequence[str] is not recognized."""
        assert is_sequence_of_numeric(Sequence[str]) is False


class TestIsNestedModel:
    """Test the is_nested_model function."""

    def test_base_model_subclass(self):
        """Test that BaseModel subclass is recognized."""

        class TestModel(BaseModel):
            value: int

        assert is_nested_model(TestModel) is True

    def test_optional_base_model(self):
        """Test that Optional[BaseModel] is recognized."""

        class TestModel(BaseModel):
            value: int

        assert is_nested_model(Optional[TestModel]) is True

    def test_non_model_types(self):
        """Test that non-model types are not recognized."""
        assert is_nested_model(int) is False
        assert is_nested_model(str) is False
        assert is_nested_model(list) is False


class TestGetComparableNumericFields:
    """Test the get_comparable_numeric_fields function."""

    def test_extract_from_stage_data(self):
        """Test extraction of numeric fields from StageData."""
        fields = get_comparable_numeric_fields(StageData)

        # Should have fields
        assert len(fields) > 0

        # All should be ComparableField
        for field in fields:
            assert isinstance(field, ComparableField)
            assert field.is_sequence is False

        # Check some known fields exist
        field_names = {f.name for f in fields}
        assert "num_tasks" in field_names
        assert "executor_run_time" in field_names
        assert "input_bytes" in field_names

    def test_excludes_identifiers(self):
        """Test that identifier fields are excluded."""
        fields = get_comparable_numeric_fields(StageData)
        field_names = {f.name for f in fields}

        # These should be excluded
        assert "stage_id" not in field_names
        assert "attempt_id" not in field_names
        assert "resource_profile_id" not in field_names

    def test_nanosecond_fields_have_scale_factor(self):
        """Test that nanosecond fields have appropriate scale factor."""
        fields = get_comparable_numeric_fields(StageData)
        fields_by_name = {f.name: f for f in fields}

        # executor_cpu_time should have ns to ms scale factor
        if "executor_cpu_time" in fields_by_name:
            assert fields_by_name["executor_cpu_time"].scale_factor == NS_TO_MS_FACTOR

    def test_caching(self):
        """Test that results are cached."""
        fields1 = get_comparable_numeric_fields(StageData)
        fields2 = get_comparable_numeric_fields(StageData)

        # Should be same object due to caching
        assert fields1 is fields2

    def test_custom_exclude_fields(self):
        """Test custom exclude fields."""

        # Create a simple test model
        class SimpleModel(BaseModel):
            metric_a: int
            metric_b: int
            excluded_field: int

        # Clear cache before testing
        get_comparable_numeric_fields.cache_clear()

        fields = get_comparable_numeric_fields(
            SimpleModel, exclude_fields=frozenset({"excluded_field"})
        )
        field_names = {f.name for f in fields}

        assert "metric_a" in field_names
        assert "metric_b" in field_names
        assert "excluded_field" not in field_names


class TestGetDistributionFields:
    """Test the get_distribution_fields function."""

    def test_extract_from_task_metric_distributions(self):
        """Test extraction of distribution fields from TaskMetricDistributions."""
        fields = get_distribution_fields(TaskMetricDistributions, include_nested=False)

        # Should have fields
        assert len(fields) > 0

        # All should be sequences
        for field in fields:
            assert isinstance(field, ComparableField)
            assert field.is_sequence is True
            assert field.nested_path is None

        # Check some known fields exist
        field_names = {f.name for f in fields}
        assert "duration" in field_names
        assert "executor_run_time" in field_names
        assert "jvm_gc_time" in field_names

    def test_include_nested_fields(self):
        """Test extraction with nested fields included."""
        fields = get_distribution_fields(TaskMetricDistributions, include_nested=True)

        # Should have more fields with nested
        nested_fields = [f for f in fields if f.nested_path is not None]
        assert len(nested_fields) > 0

        # Check for shuffle metrics
        field_names = {f.name for f in fields}
        # These come from nested models
        assert any("shuffle_read_metrics" in name for name in field_names)
        assert any("shuffle_write_metrics" in name for name in field_names)

    def test_excludes_quantiles(self):
        """Test that quantiles field is excluded."""
        fields = get_distribution_fields(TaskMetricDistributions)
        field_names = {f.name for f in fields}

        assert "quantiles" not in field_names

    def test_caching(self):
        """Test that results are cached."""
        fields1 = get_distribution_fields(TaskMetricDistributions, include_nested=True)
        fields2 = get_distribution_fields(TaskMetricDistributions, include_nested=True)

        # Should be same object due to caching
        assert fields1 is fields2


class TestGetFieldValue:
    """Test the get_field_value function."""

    def test_simple_field_extraction(self):
        """Test extracting a simple field value."""

        class SimpleModel(BaseModel):
            value: int = 100

        obj = SimpleModel()
        field = ComparableField(name="value")

        assert get_field_value(obj, field) == 100.0

    def test_with_scale_factor(self):
        """Test field extraction with scale factor."""

        class SimpleModel(BaseModel):
            nanoseconds: int = 1_000_000_000  # 1 second in ns

        obj = SimpleModel()
        field = ComparableField(name="nanoseconds", scale_factor=NS_TO_MS_FACTOR)

        # Should convert to milliseconds
        assert get_field_value(obj, field) == 1000.0

    def test_missing_field(self):
        """Test extraction of missing field returns default."""

        class SimpleModel(BaseModel):
            other: int = 1

        obj = SimpleModel()
        field = ComparableField(name="missing")

        assert get_field_value(obj, field) == 0.0
        assert get_field_value(obj, field, default=42.0) == 42.0

    def test_none_value(self):
        """Test that None value returns default."""

        class SimpleModel(BaseModel):
            nullable_field: Optional[int] = None

        obj = SimpleModel()
        field = ComparableField(name="nullable_field")

        assert get_field_value(obj, field) == 0.0

    def test_nested_field_extraction(self):
        """Test extracting nested field value."""

        class NestedModel(BaseModel):
            read_bytes: int = 1000

        class ParentModel(BaseModel):
            shuffle_read_metrics: Optional[NestedModel] = None

        obj = ParentModel(shuffle_read_metrics=NestedModel())
        field = ComparableField(
            name="shuffle_read_metrics_read_bytes",
            nested_path="shuffle_read_metrics",
        )

        assert get_field_value(obj, field) == 1000.0

    def test_nested_field_parent_none(self):
        """Test nested field when parent is None."""

        class NestedModel(BaseModel):
            read_bytes: int = 1000

        class ParentModel(BaseModel):
            shuffle_read_metrics: Optional[NestedModel] = None

        obj = ParentModel()  # shuffle_read_metrics is None
        field = ComparableField(
            name="shuffle_read_metrics_read_bytes",
            nested_path="shuffle_read_metrics",
        )

        assert get_field_value(obj, field) == 0.0


class TestGetQuantileValue:
    """Test the get_quantile_value function."""

    def test_simple_quantile_extraction(self):
        """Test extracting a quantile value from a distribution."""

        class SimpleDistModel(BaseModel):
            duration: Sequence[float] = [0.0, 100.0, 200.0, 300.0, 400.0]

        obj = SimpleDistModel()
        field = ComparableField(name="duration", is_sequence=True)

        # Median (index 2)
        assert get_quantile_value(obj, field, 2) == 200.0

        # P95 (index 4)
        assert get_quantile_value(obj, field, 4) == 400.0

    def test_with_scale_factor(self):
        """Test quantile extraction with scale factor."""

        class SimpleDistModel(BaseModel):
            nanoseconds: Sequence[float] = [0.0, 1_000_000.0, 2_000_000.0]

        obj = SimpleDistModel()
        field = ComparableField(
            name="nanoseconds", is_sequence=True, scale_factor=NS_TO_MS_FACTOR
        )

        # 2_000_000 ns = 2 ms
        assert get_quantile_value(obj, field, 2) == 2.0

    def test_index_out_of_bounds(self):
        """Test that out of bounds index returns default."""

        class SimpleDistModel(BaseModel):
            duration: Sequence[float] = [100.0, 200.0]

        obj = SimpleDistModel()
        field = ComparableField(name="duration", is_sequence=True)

        assert get_quantile_value(obj, field, 10) == 0.0

    def test_none_values(self):
        """Test handling of None distribution."""

        class SimpleDistModel(BaseModel):
            duration: Optional[Sequence[float]] = None

        obj = SimpleDistModel()
        field = ComparableField(name="duration", is_sequence=True)

        assert get_quantile_value(obj, field, 2) == 0.0

    def test_nested_quantile_extraction(self):
        """Test extracting quantile from nested distribution."""

        class NestedDistModel(BaseModel):
            read_bytes: Sequence[float] = [0.0, 500.0, 1000.0, 1500.0, 2000.0]

        class ParentDistModel(BaseModel):
            shuffle_read_metrics: Optional[NestedDistModel] = None

        obj = ParentDistModel(shuffle_read_metrics=NestedDistModel())
        field = ComparableField(
            name="shuffle_read_metrics_read_bytes",
            is_sequence=True,
            nested_path="shuffle_read_metrics",
        )

        assert get_quantile_value(obj, field, 2) == 1000.0


class TestComparableFieldNamedTuple:
    """Test the ComparableField NamedTuple."""

    def test_default_values(self):
        """Test default values for ComparableField."""
        field = ComparableField(name="test_field")

        assert field.name == "test_field"
        assert field.is_sequence is False
        assert field.nested_path is None
        assert field.scale_factor == 1.0

    def test_all_values(self):
        """Test ComparableField with all values set."""
        field = ComparableField(
            name="test_field",
            is_sequence=True,
            nested_path="parent",
            scale_factor=0.001,
        )

        assert field.name == "test_field"
        assert field.is_sequence is True
        assert field.nested_path == "parent"
        assert field.scale_factor == 0.001


class TestConstants:
    """Test module constants."""

    def test_default_exclude_fields(self):
        """Test that default exclude fields contains expected values."""
        assert "stage_id" in DEFAULT_EXCLUDE_FIELDS
        assert "attempt_id" in DEFAULT_EXCLUDE_FIELDS
        assert "resource_profile_id" in DEFAULT_EXCLUDE_FIELDS
        assert "status" in DEFAULT_EXCLUDE_FIELDS
        assert "quantiles" in DEFAULT_EXCLUDE_FIELDS

    def test_nanosecond_fields(self):
        """Test that nanosecond fields contains expected values."""
        assert "executor_cpu_time" in NANOSECOND_FIELDS
        assert "executor_deserialize_cpu_time" in NANOSECOND_FIELDS
        assert "shuffle_write_time" in NANOSECOND_FIELDS

    def test_ns_to_ms_factor(self):
        """Test nanosecond to millisecond conversion factor."""
        assert NS_TO_MS_FACTOR == 1.0 / 1_000_000.0
        # 1 second = 1_000_000_000 ns = 1000 ms
        assert 1_000_000_000 * NS_TO_MS_FACTOR == 1000.0


class TestIntegrationWithRealModels:
    """Integration tests with real Spark model classes."""

    def test_stage_data_numeric_fields_complete(self):
        """Verify StageData numeric field extraction is complete."""
        fields = get_comparable_numeric_fields(StageData)
        field_names = {f.name for f in fields}

        # Key metrics should be present
        expected_metrics = {
            "num_tasks",
            "num_active_tasks",
            "num_complete_tasks",
            "num_failed_tasks",
            "executor_run_time",
            "executor_cpu_time",
            "jvm_gc_time",
            "input_bytes",
            "input_records",
            "output_bytes",
            "output_records",
            "shuffle_read_bytes",
            "shuffle_read_records",
            "shuffle_write_bytes",
            "shuffle_write_records",
            "memory_bytes_spilled",
            "disk_bytes_spilled",
        }

        for metric in expected_metrics:
            assert metric in field_names, f"Expected metric {metric} not found"

    def test_task_metric_distributions_fields_complete(self):
        """Verify TaskMetricDistributions distribution field extraction."""
        fields = get_distribution_fields(TaskMetricDistributions, include_nested=False)
        field_names = {f.name for f in fields}

        # Key distribution metrics should be present
        expected_metrics = {
            "duration",
            "executor_run_time",
            "executor_cpu_time",
            "jvm_gc_time",
            "result_size",
            "memory_bytes_spilled",
            "disk_bytes_spilled",
        }

        for metric in expected_metrics:
            assert metric in field_names, f"Expected metric {metric} not found"

    def test_nested_distribution_fields(self):
        """Test that nested distribution fields are extracted."""
        fields = get_distribution_fields(TaskMetricDistributions, include_nested=True)
        field_names = {f.name for f in fields}

        # Should include nested shuffle metrics
        shuffle_read_fields = [n for n in field_names if "shuffle_read" in n]
        shuffle_write_fields = [n for n in field_names if "shuffle_write" in n]

        assert len(shuffle_read_fields) > 0, "No shuffle read fields found"
        assert len(shuffle_write_fields) > 0, "No shuffle write fields found"
