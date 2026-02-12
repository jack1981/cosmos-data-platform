from __future__ import annotations

import os
from typing import Any, Callable

from app.schemas.pipeline_spec import PipelineSpecDocument


def try_run_with_xenna(
    spec: PipelineSpecDocument,
    stage_instances: list[Any],
    input_data: list[Any],
    log: Callable[[str], None],
) -> tuple[bool, list[Any]]:
    try:
        import cosmos_xenna.pipelines.v1 as pipelines_v1  # type: ignore
    except Exception as exc:  # pragma: no cover - environment-dependent
        log(f"Xenna runtime unavailable in API container, using simulated runner ({exc}).")
        return False, []

    if not stage_instances:
        log("No stage instances to execute.")
        return True, input_data

    xenna_stages = []
    for instance in stage_instances:
        if not isinstance(instance, pipelines_v1.Stage):
            log("At least one stage is not a Xenna Stage subclass; using simulated runner.")
            return False, []
        xenna_stages.append(pipelines_v1.StageSpec(instance))

    mode_map = {
        "streaming": pipelines_v1.ExecutionMode.STREAMING,
        "batch": pipelines_v1.ExecutionMode.BATCH,
        "serving": pipelines_v1.ExecutionMode.SERVING,
    }

    config = pipelines_v1.PipelineConfig(
        execution_mode=mode_map[spec.execution_mode],
        return_last_stage_outputs=True,
        logging_interval_s=30.0,
    )

    if spec.runtime.ray_mode:
        os.environ["RAY_MODE"] = spec.runtime.ray_mode
    if spec.runtime.ray_address:
        os.environ["RAY_ADDRESS"] = spec.runtime.ray_address

    pipeline_spec = pipelines_v1.PipelineSpec(
        input_data=input_data,
        stages=xenna_stages,
        config=config,
    )

    result = pipelines_v1.run_pipeline(pipeline_spec)
    return True, result or []
