"""Tests for xenna_adapter and distributed_executor modules."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
from app.schemas.pipeline_spec import PipelineSpecDocument
from app.services.dataset_types import DatasetRef, DatasetRuntimeContext, DatasetStage
from app.services.xenna_adapter import DatasetStageAdapter


class StubDatasetStage(DatasetStage):
    """Minimal DatasetStage for testing."""

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        super().__init__(params or {})
        self.setup_called = False
        self.run_calls: list[dict[str, DatasetRef]] = []

    def setup(self, ctx: DatasetRuntimeContext) -> None:
        self.setup_called = True

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        self.run_calls.append(inputs)
        uris = "+".join(ref.uri for ref in inputs.values())
        return DatasetRef(uri=f"output://{uris}")


def _make_ctx() -> DatasetRuntimeContext:
    return DatasetRuntimeContext(
        io_config=None,
        pipeline_io=None,
        storage_options={},
        ray_mode="local",
        ray_address=None,
        work_dir="/tmp/test",
    )


class TestDatasetStageAdapter:
    def test_required_resources(self) -> None:
        stage = StubDatasetStage()
        adapter = DatasetStageAdapter(stage, _make_ctx(), "test_stage", cpus=2.0, gpus=1.0)
        res = adapter.required_resources
        assert res.cpus == 2.0
        assert res.gpus == 1.0

    def test_stage_batch_size(self) -> None:
        stage = StubDatasetStage()
        adapter = DatasetStageAdapter(stage, _make_ctx(), "test_stage", batch_size=4)
        assert adapter.stage_batch_size == 4

    def test_setup_delegates_to_dataset_stage(self) -> None:
        stage = StubDatasetStage()
        adapter = DatasetStageAdapter(stage, _make_ctx(), "test_stage")
        adapter.setup(worker_metadata={})
        assert stage.setup_called

    def test_setup_on_node_is_noop(self) -> None:
        stage = StubDatasetStage()
        adapter = DatasetStageAdapter(stage, _make_ctx(), "test_stage")
        adapter.setup_on_node(node_info={}, worker_metadata={})

    def test_process_data_with_dict_inputs(self) -> None:
        stage = StubDatasetStage()
        ctx = _make_ctx()
        adapter = DatasetStageAdapter(stage, ctx, "test_stage")

        inputs = [
            {"upstream_a": DatasetRef(uri="lance://a")},
            {"upstream_b": DatasetRef(uri="lance://b")},
        ]
        results = adapter.process_data(inputs)

        assert len(results) == 2
        assert results[0].uri == "output://lance://a"
        assert results[1].uri == "output://lance://b"
        assert len(stage.run_calls) == 2

    def test_process_data_with_dataset_ref_inputs(self) -> None:
        stage = StubDatasetStage()
        ctx = _make_ctx()
        adapter = DatasetStageAdapter(stage, ctx, "test_stage")

        inputs = [DatasetRef(uri="lance://direct")]
        results = adapter.process_data(inputs)

        assert len(results) == 1
        assert results[0].uri == "output://lance://direct"
        assert "__source__" in stage.run_calls[0]

    def test_process_data_with_string_inputs(self) -> None:
        stage = StubDatasetStage()
        ctx = _make_ctx()
        adapter = DatasetStageAdapter(stage, ctx, "test_stage")

        inputs = ["raw_string_input"]
        results = adapter.process_data(inputs)

        assert len(results) == 1
        assert "__source__" in stage.run_calls[0]
        assert stage.run_calls[0]["__source__"].uri == "raw_string_input"

    def test_repr(self) -> None:
        stage = StubDatasetStage()
        adapter = DatasetStageAdapter(stage, _make_ctx(), "my_stage")
        assert "my_stage" in repr(adapter)
        assert "StubDatasetStage" in repr(adapter)

    def test_default_resources(self) -> None:
        stage = StubDatasetStage()
        adapter = DatasetStageAdapter(stage, _make_ctx(), "test_stage")
        res = adapter.required_resources
        assert res.cpus == 1.0
        assert res.gpus == 0.0


class TestDistributedExecutorImportGuard:
    def test_run_distributed_pipeline_raises_without_xenna(self) -> None:
        from app.services.distributed_executor import run_distributed_pipeline

        spec = PipelineSpecDocument.model_validate(
            {
                "name": "test",
                "data_model": "dataset",
                "stages": [
                    {
                        "stage_id": "s1",
                        "name": "Stage 1",
                        "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                        "params": {"uri": "lance://test"},
                    }
                ],
                "io": {"source": {"kind": "dataset_uri", "uri": "lance://in"}, "sink": {"kind": "none"}},
            }
        )

        with patch("app.services.distributed_executor.is_xenna_available", return_value=False):
            with pytest.raises(ImportError, match="cosmos-xenna is not installed"):
                run_distributed_pipeline(spec, lambda _: None)


class TestRunnerDistributedMode:
    def test_should_use_distributed_never(self) -> None:
        from app.services.runner import PipelineRunnerService

        spec = PipelineSpecDocument.model_validate(
            {
                "name": "test",
                "data_model": "dataset",
                "stages": [
                    {
                        "stage_id": "s1",
                        "name": "Stage",
                        "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                        "params": {"uri": "lance://a"},
                    }
                ],
                "io": {"source": {"kind": "dataset_uri", "uri": "lance://in"}, "sink": {"kind": "none"}},
                "runtime": {"distributed_mode": "never"},
            }
        )
        assert PipelineRunnerService._should_use_distributed(spec) is False

    def test_should_use_distributed_always_without_xenna(self) -> None:
        from app.services.runner import PipelineRunnerService

        spec = PipelineSpecDocument.model_validate(
            {
                "name": "test",
                "data_model": "dataset",
                "stages": [
                    {
                        "stage_id": "s1",
                        "name": "Stage",
                        "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                        "params": {"uri": "lance://a"},
                    }
                ],
                "io": {"source": {"kind": "dataset_uri", "uri": "lance://in"}, "sink": {"kind": "none"}},
                "runtime": {"distributed_mode": "always"},
            }
        )
        with patch("app.services.runner.is_xenna_available", return_value=False):
            assert PipelineRunnerService._should_use_distributed(spec) is False

    def test_should_use_distributed_auto_with_xenna(self) -> None:
        from app.services.runner import PipelineRunnerService

        spec = PipelineSpecDocument.model_validate(
            {
                "name": "test",
                "data_model": "dataset",
                "stages": [
                    {
                        "stage_id": "s1",
                        "name": "Left",
                        "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                        "params": {"uri": "lance://a"},
                    },
                    {
                        "stage_id": "s2",
                        "name": "Right",
                        "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                        "params": {"uri": "lance://b"},
                    },
                    {
                        "stage_id": "s3",
                        "name": "Join",
                        "python_import_path": "app.services.dataset_stage_fixtures:JoinDatasetStage",
                    },
                ],
                "edges": [{"source": "s1", "target": "s3"}, {"source": "s2", "target": "s3"}],
                "io": {"source": {"kind": "dataset_uri", "uri": "lance://in"}, "sink": {"kind": "none"}},
                "runtime": {"distributed_mode": "auto"},
            }
        )
        with patch("app.services.runner.is_xenna_available", return_value=True):
            assert PipelineRunnerService._should_use_distributed(spec) is True

    def test_distributed_mode_defaults_to_never(self) -> None:
        spec = PipelineSpecDocument.model_validate(
            {
                "name": "test",
                "data_model": "dataset",
                "stages": [
                    {
                        "stage_id": "s1",
                        "name": "Stage",
                        "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                        "params": {"uri": "lance://a"},
                    }
                ],
                "io": {"source": {"kind": "dataset_uri", "uri": "lance://in"}, "sink": {"kind": "none"}},
            }
        )
        assert spec.runtime.distributed_mode == "never"
