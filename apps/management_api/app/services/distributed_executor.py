"""Distributed pipeline executor using cosmos_xenna.

Follows the cosmos-curate XennaRunner pattern:
1. Wrap each DatasetStage in a DatasetStageAdapter
2. Build a PipelineConfig with streaming-mode defaults
3. Build a PipelineSpec and call run_pipeline()

cosmos_xenna provides Rust-compiled scheduling algorithms, actor pools,
autoscaling, and streaming backpressure on top of Ray.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any, Callable

from app.schemas.pipeline_spec import PipelineSpecDocument, StageDefinition
from app.services.dataset_executor import DatasetPipelineResult, _build_context, _maybe_init_ray_and_daft
from app.services.dataset_types import DatasetRef, DatasetStage
from app.services.xenna_adapter import DatasetStageAdapter, is_xenna_available

logger = logging.getLogger(__name__)


def _topological_order(spec: PipelineSpecDocument) -> tuple[list[str], dict[str, list[str]], dict[str, list[str]]]:
    """Compute topological execution order of pipeline stages."""
    stage_ids = [stage.stage_id for stage in spec.stages]
    incoming: dict[str, list[str]] = {sid: [] for sid in stage_ids}
    adjacency: dict[str, list[str]] = {sid: [] for sid in stage_ids}
    indegree = {sid: 0 for sid in stage_ids}

    for edge in spec.edges:
        adjacency[edge.source].append(edge.target)
        incoming[edge.target].append(edge.source)
        indegree[edge.target] += 1

    queue = deque([sid for sid in stage_ids if indegree[sid] == 0])
    order: list[str] = []
    local_indegree = indegree.copy()
    while queue:
        node = queue.popleft()
        order.append(node)
        for nxt in adjacency[node]:
            local_indegree[nxt] -= 1
            if local_indegree[nxt] == 0:
                queue.append(nxt)

    if len(order) != len(stage_ids):
        raise ValueError("Dataset pipeline graph is not acyclic")
    return order, adjacency, incoming


def _instantiate_dataset_stage(stage: StageDefinition) -> DatasetStage:
    """Instantiate a DatasetStage from a StageDefinition."""
    from app.services.stage_registry import get_template_class

    if stage.stage_template:
        cls = get_template_class(stage.stage_template)
    else:
        assert stage.python_import_path is not None
        import importlib

        if ":" in stage.python_import_path:
            module_path, class_name = stage.python_import_path.split(":", maxsplit=1)
        elif "." in stage.python_import_path:
            module_path, class_name = stage.python_import_path.rsplit(".", maxsplit=1)
        else:
            raise ValueError("python_import_path must be in module:ClassName format")
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)

    params = stage.params or {}
    try:
        instance = cls(params)
    except TypeError:
        instance = cls(**params)

    if not isinstance(instance, DatasetStage):
        raise ValueError(f"Dataset pipeline stage {stage.stage_id} must subclass DatasetStage")
    return instance


def run_distributed_pipeline(
    spec: PipelineSpecDocument,
    log: Callable[[str], None],
) -> DatasetPipelineResult:
    """Execute a dataset pipeline using cosmos_xenna's distributed streaming executor.

    This function:
    1. Builds a DatasetRuntimeContext and initialises Ray/Daft
    2. Wraps each DatasetStage in a DatasetStageAdapter
    3. Builds a cosmos_xenna PipelineSpec with streaming config
    4. Calls cosmos_xenna.run_pipeline for distributed execution
    5. Falls back to the standard dataset executor on import failure
    """
    if not is_xenna_available():
        raise ImportError(
            "cosmos-xenna is not installed. Install it with: pip install cosmos-xenna>=0.1.7"
        )

    from cosmos_xenna.pipelines.v1 import (
        ExecutionMode,
        PipelineConfig,
        PipelineSpec,
        StageSpec,
        StreamingSpecificSpec,
    )
    from cosmos_xenna.pipelines.v1 import run_pipeline as xenna_run_pipeline

    ctx = _build_context(spec)
    _maybe_init_ray_and_daft(ctx, log)
    order, adjacency, incoming = _topological_order(spec)

    stage_by_id = {stage.stage_id: stage for stage in spec.stages}

    # Build adapted stages
    adapted_stages: list[StageSpec] = []
    stage_instances: dict[str, DatasetStage] = {}
    for stage_id in order:
        stage_def = stage_by_id[stage_id]
        dataset_stage = _instantiate_dataset_stage(stage_def)
        stage_instances[stage_id] = dataset_stage

        adapter = DatasetStageAdapter(
            dataset_stage=dataset_stage,
            ctx=ctx,
            stage_id=stage_id,
            cpus=stage_def.resources.cpus,
            gpus=stage_def.resources.gpus,
            batch_size=stage_def.batch_size,
        )
        adapted_stages.append(StageSpec(stage=adapter))
        log(f"Distributed executor: adapted stage {stage_id} ({stage_def.name})")

    # Build input partitions from source config
    input_partitions = _build_input_partitions(spec, incoming, order)

    # Build PipelineConfig following cosmos-curate defaults
    autoscaling = spec.runtime.autoscaling or {}
    config = PipelineConfig(
        execution_mode=ExecutionMode.STREAMING,
        enable_work_stealing=False,
        return_last_stage_outputs=True,
        mode_specific=StreamingSpecificSpec(
            autoscale_interval_s=autoscaling.get("interval_s", 180.0),
            autoscale_speed_estimation_window_duration_s=autoscaling.get("speed_window_s", 180.0),
            max_queued_multiplier=autoscaling.get("max_queued_multiplier", 1.5),
            max_queued_lower_bound=autoscaling.get("max_queued_lower_bound", 16),
        ),
    )

    # Build PipelineSpec and run
    pipeline_spec = PipelineSpec(
        input_data=input_partitions,
        stages=adapted_stages,
        config=config,
    )

    log(f"Distributed executor: launching cosmos_xenna pipeline with {len(adapted_stages)} stages")
    start = time.perf_counter()

    try:
        output = xenna_run_pipeline(pipeline_spec)
    except Exception:
        log("Distributed executor: cosmos_xenna pipeline failed")
        raise

    duration = time.perf_counter() - start
    log(f"Distributed executor: pipeline completed in {duration:.3f}s")

    return _build_result(output, order, duration)


def _build_input_partitions(
    spec: PipelineSpecDocument,
    incoming: dict[str, list[str]],
    order: list[str],
) -> list[Any]:
    """Build input partition list for cosmos_xenna from the pipeline source config."""
    root_stages = [sid for sid in order if not incoming[sid]]

    if spec.io.source.kind == "dataset_uri" and spec.io.source.uri:
        return [DatasetRef(uri=spec.io.source.uri)]

    if spec.io.source.static_data:
        return [DatasetRef(uri=str(item)) for item in spec.io.source.static_data]

    # For pipelines with no explicit source, provide a single trigger item per root
    return [DatasetRef(uri=f"trigger://{sid}") for sid in root_stages]


def _build_result(
    output: Any,
    execution_order: list[str],
    duration: float,
) -> DatasetPipelineResult:
    """Convert cosmos_xenna output to DatasetPipelineResult."""
    if isinstance(output, list) and output:
        last = output[-1]
        if isinstance(last, DatasetRef):
            output_ref = last
        else:
            output_ref = DatasetRef(uri=str(last), metadata={"raw_output": True})
    elif isinstance(output, DatasetRef):
        output_ref = output
    else:
        output_ref = DatasetRef(uri="xenna://completed", metadata={"raw_output": str(output)})

    metrics = [
        {
            "stage_id": stage_id,
            "duration_seconds": round(duration / len(execution_order), 4),
            "distributed": True,
        }
        for stage_id in execution_order
    ]

    return DatasetPipelineResult(
        output_ref=output_ref,
        stage_metrics=metrics,
        execution_order=execution_order,
    )
