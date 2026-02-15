from __future__ import annotations

import pytest
from app.schemas.pipeline_spec import PipelineSpecDocument
from pydantic import ValidationError


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


def test_dataset_mode_allows_fan_out_schema() -> None:
    spec = PipelineSpecDocument.model_validate(
        {
            "name": "dataset-fan-out",
            "data_model": "dataset",
            "execution_mode": "batch",
            "stages": [
                {
                    "stage_id": "s1",
                    "name": "A",
                    "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                },
                {
                    "stage_id": "s2",
                    "name": "B",
                    "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                },
                {
                    "stage_id": "s3",
                    "name": "C",
                    "python_import_path": "app.services.dataset_stage_fixtures:JoinDatasetStage",
                },
            ],
            "edges": [
                {"source": "s1", "target": "s3"},
                {"source": "s2", "target": "s3"},
            ],
        }
    )
    assert spec.data_model == "dataset"
    assert len(spec.edges) == 2


def test_dataset_mode_rejects_cycles() -> None:
    with pytest.raises(ValidationError):
        PipelineSpecDocument.model_validate(
            {
                "name": "dataset-cycle",
                "data_model": "dataset",
                "execution_mode": "batch",
                "stages": [
                    {
                        "stage_id": "s1",
                        "name": "A",
                        "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                    },
                    {
                        "stage_id": "s2",
                        "name": "B",
                        "python_import_path": "app.services.dataset_stage_fixtures:JoinDatasetStage",
                    },
                ],
                "edges": [
                    {"source": "s1", "target": "s2"},
                    {"source": "s2", "target": "s1"},
                ],
            }
        )


def test_dataset_mode_accepts_runtime_storage_options() -> None:
    spec = PipelineSpecDocument.model_validate(
        {
            "name": "dataset-storage",
            "data_model": "dataset",
            "execution_mode": "batch",
            "stages": [
                {
                    "stage_id": "s1",
                    "name": "Reader",
                    "python_import_path": "app.services.dataset_stages:LanceReaderStage",
                    "params": {"uri": "s3://bucket/in.lance"},
                }
            ],
            "runtime": {
                "ray_mode": "local",
                "work_dir": "/tmp/pipelineforge-datasets",
                "storage_options": {"s3": {"region_name": "us-west-2"}},
            },
        }
    )
    assert spec.runtime.storage_options == {"s3": {"region_name": "us-west-2"}}
    assert spec.runtime.work_dir == "/tmp/pipelineforge-datasets"
