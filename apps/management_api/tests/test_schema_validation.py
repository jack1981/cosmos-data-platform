from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.schemas.pipeline_spec import PipelineSpecDocument


def test_valid_linear_pipeline_schema() -> None:
    spec = PipelineSpecDocument.model_validate(
        {
            "name": "demo",
            "execution_mode": "streaming",
            "stages": [
                {"stage_id": "s1", "name": "Source", "stage_template": "builtin.identity"},
                {"stage_id": "s2", "name": "Transform", "stage_template": "builtin.uppercase"},
            ],
            "edges": [{"source": "s1", "target": "s2"}],
        }
    )
    assert len(spec.stages) == 2
    assert len(spec.edges) == 1


def test_invalid_fan_out_pipeline_schema() -> None:
    with pytest.raises(ValidationError):
        PipelineSpecDocument.model_validate(
            {
                "name": "invalid",
                "execution_mode": "streaming",
                "stages": [
                    {"stage_id": "s1", "name": "A", "stage_template": "builtin.identity"},
                    {"stage_id": "s2", "name": "B", "stage_template": "builtin.identity"},
                    {"stage_id": "s3", "name": "C", "stage_template": "builtin.identity"},
                ],
                "edges": [
                    {"source": "s1", "target": "s2"},
                    {"source": "s1", "target": "s3"},
                ],
            }
        )
