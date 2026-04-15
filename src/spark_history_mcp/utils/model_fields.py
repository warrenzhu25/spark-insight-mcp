"""
Dynamic field extraction utilities for Pydantic models.

This module provides functions for dynamically extracting comparable numeric fields
from Pydantic models, enabling automatic discovery of fields for comparison operations.
"""

import collections.abc
from functools import lru_cache
from typing import (
    Any,
    FrozenSet,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Type,
    Union,
    get_args,
    get_origin,
)

from pydantic import BaseModel


class ComparableField(NamedTuple):
    """Represents a field that can be compared between models."""

    name: str
    """Field name as defined in the model."""

    is_sequence: bool = False
    """Whether this is a sequence/distribution field (e.g., Sequence[float])."""

    nested_path: Optional[str] = None
    """For nested metrics, the parent field name (e.g., 'shuffle_read_metrics')."""

    scale_factor: float = 1.0
    """Scale factor to apply when extracting values (e.g., for ns to ms conversion)."""


# Fields that are identifiers or metadata, not comparable metrics
DEFAULT_EXCLUDE_FIELDS: FrozenSet[str] = frozenset(
    {
        # Identifiers
        "stage_id",
        "attempt_id",
        "resource_profile_id",
        "job_id",
        "task_id",
        "id",
        "index",
        "attempt",
        "partition_id",
        # Metadata/status fields
        "status",
        "name",
        "description",
        "details",
        "scheduling_pool",
        "failure_reason",
        # Time fields that aren't durations
        "submission_time",
        "first_task_launched_time",
        "completion_time",
        "launch_time",
        "result_fetch_start",
        # Quantiles array (metadata, not a metric)
        "quantiles",
    }
)

# Fields that are stored in nanoseconds and should be converted to milliseconds
NANOSECOND_FIELDS: FrozenSet[str] = frozenset(
    {
        "executor_cpu_time",
        "executor_deserialize_cpu_time",
        "shuffle_write_time",
    }
)

# Scale factor for nanosecond to millisecond conversion
NS_TO_MS_FACTOR: float = 1.0 / 1_000_000.0


def is_numeric_type(type_hint: Any) -> bool:
    """
    Check if a type hint represents a numeric type.

    Handles int, float, and Optional variants.

    Args:
        type_hint: A type annotation to check

    Returns:
        True if the type represents a numeric value
    """
    origin = get_origin(type_hint)

    # Handle Optional[X] which is Union[X, None]
    if origin is Union:
        args = get_args(type_hint)
        # Filter out NoneType and check remaining types
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1:
            return is_numeric_type(non_none_args[0])
        return False

    # Direct numeric types
    if type_hint in (int, float):
        return True

    return False


def is_sequence_of_numeric(type_hint: Any) -> bool:
    """
    Check if a type hint represents a sequence of numeric values.

    Handles Sequence[float], Sequence[int], list[float], etc.
    and their Optional variants.

    Args:
        type_hint: A type annotation to check

    Returns:
        True if the type represents a sequence of numeric values
    """
    origin = get_origin(type_hint)

    # Handle Optional[X] which is Union[X, None]
    if origin is Union:
        args = get_args(type_hint)
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1:
            return is_sequence_of_numeric(non_none_args[0])
        return False

    # Check for Sequence, list, tuple origins
    # Note: get_origin(Sequence[X]) returns collections.abc.Sequence, not typing.Sequence
    if origin in (collections.abc.Sequence, Sequence, list, tuple):
        args = get_args(type_hint)
        if args:
            return is_numeric_type(args[0])

    return False


def is_nested_model(type_hint: Any) -> bool:
    """
    Check if a type hint represents a nested Pydantic model.

    Args:
        type_hint: A type annotation to check

    Returns:
        True if the type represents a nested BaseModel
    """
    origin = get_origin(type_hint)

    # Handle Optional[X]
    if origin is Union:
        args = get_args(type_hint)
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1:
            return is_nested_model(non_none_args[0])
        return False

    # Check if it's a BaseModel subclass
    try:
        return isinstance(type_hint, type) and issubclass(type_hint, BaseModel)
    except TypeError:
        return False


def _get_inner_type(type_hint: Any) -> Any:
    """Extract the inner type from Optional or Union types."""
    origin = get_origin(type_hint)
    if origin is Union:
        args = get_args(type_hint)
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1:
            return non_none_args[0]
    return type_hint


@lru_cache(maxsize=32)
def get_comparable_numeric_fields(
    model_class: Type[BaseModel],
    exclude_fields: Optional[FrozenSet[str]] = None,
) -> List[ComparableField]:
    """
    Extract all comparable numeric fields from a Pydantic model.

    Uses model introspection to find fields with int or float types,
    excluding identifier fields.

    Args:
        model_class: A Pydantic BaseModel class
        exclude_fields: Optional set of field names to exclude (defaults to
                        DEFAULT_EXCLUDE_FIELDS)

    Returns:
        List of ComparableField objects representing numeric fields
    """
    if exclude_fields is None:
        exclude_fields = DEFAULT_EXCLUDE_FIELDS

    fields: List[ComparableField] = []

    for field_name, field_info in model_class.model_fields.items():
        if field_name in exclude_fields:
            continue

        type_hint = field_info.annotation
        if type_hint is None:
            continue

        if is_numeric_type(type_hint):
            # Determine scale factor for nanosecond fields
            scale = NS_TO_MS_FACTOR if field_name in NANOSECOND_FIELDS else 1.0
            fields.append(
                ComparableField(
                    name=field_name,
                    is_sequence=False,
                    nested_path=None,
                    scale_factor=scale,
                )
            )

    return fields


@lru_cache(maxsize=32)
def get_distribution_fields(
    model_class: Type[BaseModel],
    exclude_fields: Optional[FrozenSet[str]] = None,
    include_nested: bool = True,
) -> List[ComparableField]:
    """
    Extract distribution fields (Sequence[float]) from a Pydantic model.

    Optionally includes fields from nested models.

    Args:
        model_class: A Pydantic BaseModel class
        exclude_fields: Optional set of field names to exclude
        include_nested: Whether to include fields from nested models

    Returns:
        List of ComparableField objects representing distribution fields
    """
    if exclude_fields is None:
        exclude_fields = DEFAULT_EXCLUDE_FIELDS

    fields: List[ComparableField] = []

    for field_name, field_info in model_class.model_fields.items():
        if field_name in exclude_fields:
            continue

        type_hint = field_info.annotation
        if type_hint is None:
            continue

        if is_sequence_of_numeric(type_hint):
            # Determine scale factor for nanosecond fields
            scale = NS_TO_MS_FACTOR if field_name in NANOSECOND_FIELDS else 1.0
            fields.append(
                ComparableField(
                    name=field_name,
                    is_sequence=True,
                    nested_path=None,
                    scale_factor=scale,
                )
            )
        elif include_nested and is_nested_model(type_hint):
            # Extract fields from nested model
            inner_type = _get_inner_type(type_hint)
            nested_fields = _get_nested_distribution_fields(
                inner_type, field_name, exclude_fields
            )
            fields.extend(nested_fields)

    return fields


def _get_nested_distribution_fields(
    model_class: Type[BaseModel],
    parent_field: str,
    exclude_fields: FrozenSet[str],
) -> List[ComparableField]:
    """
    Extract distribution fields from a nested model.

    Args:
        model_class: The nested Pydantic BaseModel class
        parent_field: Name of the parent field containing this model
        exclude_fields: Set of field names to exclude

    Returns:
        List of ComparableField objects with nested_path set
    """
    fields: List[ComparableField] = []

    for field_name, field_info in model_class.model_fields.items():
        if field_name in exclude_fields:
            continue

        type_hint = field_info.annotation
        if type_hint is None:
            continue

        if is_sequence_of_numeric(type_hint):
            # Create a descriptive name combining parent and nested field
            full_name = f"{parent_field}_{field_name}"
            scale = NS_TO_MS_FACTOR if field_name in NANOSECOND_FIELDS else 1.0
            fields.append(
                ComparableField(
                    name=full_name,
                    is_sequence=True,
                    nested_path=parent_field,
                    scale_factor=scale,
                )
            )

    return fields


def get_field_value(
    obj: Any,
    field: ComparableField,
    default: float = 0.0,
) -> float:
    """
    Extract a field value from an object, handling nested paths and scale factors.

    Args:
        obj: The object to extract the value from
        field: The ComparableField describing how to extract the value
        default: Default value if the field is not found or None

    Returns:
        The extracted and scaled value, or the default
    """
    try:
        if field.nested_path:
            # Extract nested field name from full name
            # e.g., "shuffle_read_metrics_read_bytes" -> "read_bytes"
            nested_field_name = field.name[len(field.nested_path) + 1 :]
            parent = getattr(obj, field.nested_path, None)
            if parent is None:
                return default
            value = getattr(parent, nested_field_name, None)
        else:
            value = getattr(obj, field.name, None)

        if value is None:
            return default

        return float(value) * field.scale_factor

    except (TypeError, ValueError, AttributeError):
        return default


def get_quantile_value(
    obj: Any,
    field: ComparableField,
    quantile_index: int,
    default: float = 0.0,
) -> float:
    """
    Extract a specific quantile value from a distribution field.

    Args:
        obj: The object containing the distribution
        field: The ComparableField describing the distribution field
        quantile_index: Index into the quantiles array (e.g., 2 for median)
        default: Default value if not found

    Returns:
        The quantile value, scaled appropriately
    """
    try:
        if field.nested_path:
            # Extract nested field name from full name
            nested_field_name = field.name[len(field.nested_path) + 1 :]
            parent = getattr(obj, field.nested_path, None)
            if parent is None:
                return default
            values = getattr(parent, nested_field_name, None)
        else:
            values = getattr(obj, field.name, None)

        if values is None or not isinstance(values, (list, tuple, Sequence)):
            return default

        if quantile_index >= len(values):
            return default

        value = values[quantile_index]
        if value is None:
            return default

        return float(value) * field.scale_factor

    except (TypeError, ValueError, AttributeError, IndexError):
        return default
