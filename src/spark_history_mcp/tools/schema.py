"""
Optional schema models for outputs. Enable validation via ToolConfig.debug_validate_schema.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class CompareAppPerformanceOutput(BaseModel):
    schema_version: int = Field(default=1)
    applications: Dict[str, Any]
    performance_comparison: Dict[str, Any]
    environment_comparison: Dict[str, Any]
    key_recommendations: List[Dict[str, Any]]
    # These fields are omitted in compact mode
    stage_deep_dive: Optional[Dict[str, Any]] = None
    recommendations: Optional[List[Dict[str, Any]]] = None


def validate_output(
    model_cls: type[BaseModel], data: Dict[str, Any], enabled: bool
) -> Dict[str, Any]:
    if not enabled:
        return data
    # Model validation raises on errors; return dict form on success
    return model_cls.model_validate(data).model_dump()
