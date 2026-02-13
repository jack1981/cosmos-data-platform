from __future__ import annotations

import importlib
import json
import os
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass
from inspect import signature
from types import ModuleType
from typing import Any, Callable

from app.schemas.pipeline_spec import PipelineSpecDocument, StageDefinition
from app.services.dataset_types import DatasetRef, DatasetRuntimeContext, DatasetStage

_RUNTIME_INIT_LOCK = threading.Lock()


@dataclass
class DatasetPipelineResult:
    output_ref: Any
    stage_metrics: list[dict[str, Any]]
    execution_order: list[str]


def _load_module(module_path: str) -> ModuleType:
    return importlib.import_module(module_path)


def _load_stage_class(python_import_path: str) -> type[Any]:
    if ":" in python_import_path:
        module_path, class_name = python_import_path.split(":", maxsplit=1)
    elif "." in python_import_path:
        module_path, class_name = python_import_path.rsplit(".", maxsplit=1)
    else:
        raise ValueError("python_import_path must be in module:ClassName format")

    module = _load_module(module_path)
    return getattr(module, class_name)


def _instantiate_dataset_stage(stage: StageDefinition) -> Any:
    from app.services.stage_registry import get_template_class

    instance: Any
    if stage.stage_template:
        cls = get_template_class(stage.stage_template)
        params = stage.params or {}
        try:
            instance = cls(params)
        except TypeError:
            instance = cls(**params)
    else:
        assert stage.python_import_path is not None
        cls = _load_stage_class(stage.python_import_path)
        params = stage.params or {}
        try:
            instance = cls(params)
        except TypeError:
            instance = cls(**params)

    if not isinstance(instance, DatasetStage):
        raise ValueError(f"Dataset pipeline stage {stage.stage_id} must subclass DatasetStage")

    return instance


def _load_storage_options(spec: PipelineSpecDocument) -> dict[str, Any]:
    storage_options = dict(spec.runtime.storage_options or {})
    if storage_options:
        return storage_options

    env_storage_options = os.environ.get("PIPELINEFORGE_DATASET_STORAGE_OPTIONS_JSON") or os.environ.get(
        "XENNA_DATASET_STORAGE_OPTIONS_JSON"
    )
    if not env_storage_options:
        return {}
    try:
        parsed = json.loads(env_storage_options)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _build_daft_io_config(storage_options: dict[str, Any]) -> Any:
    try:
        import daft  # type: ignore
    except ImportError:
        return None

    io_module = getattr(daft, "io", None)
    if io_module is None:
        return None

    io_config_cls = getattr(io_module, "IOConfig", None)
    if io_config_cls is None:
        return None

    s3_options = storage_options.get("s3")
    if isinstance(s3_options, dict):
        s3_config_cls = getattr(io_module, "S3Config", None)
        if s3_config_cls is not None:
            try:
                accepted_s3_keys = set(signature(s3_config_cls).parameters.keys())
            except (TypeError, ValueError):
                accepted_s3_keys = set(s3_options.keys())
            s3_kwargs = {key: value for key, value in s3_options.items() if key in accepted_s3_keys}
            try:
                return io_config_cls(s3=s3_config_cls(**s3_kwargs))
            except TypeError:
                return None

    try:
        return io_config_cls()
    except TypeError:
        return None


def _build_context(spec: PipelineSpecDocument) -> Any:
    storage_options = _load_storage_options(spec)
    daft_io_config = _build_daft_io_config(storage_options)

    ray_mode = spec.runtime.ray_mode or os.environ.get("RAY_MODE", "local")
    ray_address = spec.runtime.ray_address or os.environ.get("RAY_ADDRESS")
    work_dir = spec.runtime.work_dir or tempfile.mkdtemp(
        prefix=f"pipelineforge-dataset-{spec.pipeline_id or 'pipeline'}-"
    )

    return DatasetRuntimeContext(
        io_config=daft_io_config,
        pipeline_io=spec.io,
        storage_options=storage_options,
        ray_mode=ray_mode,
        ray_address=ray_address,
        work_dir=work_dir,
    )


def _is_ray_client_address(address: str | None) -> bool:
    if not address:
        return False
    return address.startswith("ray://")


def _extract_daft_runner_setters(daft_module: Any) -> tuple[Callable[..., Any] | None, Callable[..., Any] | None]:
    set_runner_ray = getattr(daft_module, "set_runner_ray", None)
    set_runner_native = getattr(daft_module, "set_runner_native", None)
    if callable(set_runner_ray) and callable(set_runner_native):
        return set_runner_ray, set_runner_native

    context_module = getattr(daft_module, "context", None)
    if context_module is None:
        return (
            set_runner_ray if callable(set_runner_ray) else None,
            set_runner_native if callable(set_runner_native) else None,
        )

    if not callable(set_runner_ray):
        set_runner_ray = getattr(context_module, "set_runner_ray", None)
    if not callable(set_runner_native):
        set_runner_native = getattr(context_module, "set_runner_native", None)

    return (
        set_runner_ray if callable(set_runner_ray) else None,
        set_runner_native if callable(set_runner_native) else None,
    )


def _is_runner_already_configured_error(exc: Exception) -> bool:
    return "Cannot set runner more than once" in str(exc)


def _maybe_init_ray_and_daft(ctx: Any, log: Callable[[str], None]) -> None:
    with _RUNTIME_INIT_LOCK:
        force_native_runner = _is_ray_client_address(ctx.ray_address)

        if force_native_runner:
            # Ray client mode (ray://) is flaky for Daft actor bootstrap in this API-threaded executor.
            log(f"Dataset runtime detected Ray client address ({ctx.ray_address}); preferring Daft native runner.")
        else:
            try:
                import ray

                if not ray.is_initialized():
                    if ctx.ray_address:
                        ray.init(address=ctx.ray_address, ignore_reinit_error=True)
                    elif ctx.ray_mode == "local":
                        ray.init(ignore_reinit_error=True)
                    else:
                        ray.init(address="auto", ignore_reinit_error=True)
                    log(f"Dataset runtime initialized Ray (mode={ctx.ray_mode}).")
            except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:  # pragma: no cover
                log(f"Dataset runtime skipped Ray initialization ({exc}).")

        try:
            import daft  # type: ignore

            set_runner_ray, set_runner_native = _extract_daft_runner_setters(daft)

            if force_native_runner and callable(set_runner_native):
                try:
                    set_runner_native()
                    log("Dataset runtime configured Daft native runner.")
                except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                    if _is_runner_already_configured_error(exc):
                        log("Dataset runtime reused existing Daft runner.")
                    else:
                        log(f"Dataset runtime skipped Daft runner setup ({exc}).")
                return

            if callable(set_runner_ray):
                try:
                    if ctx.ray_address:
                        set_runner_ray(ctx.ray_address)
                    else:
                        set_runner_ray()
                    log("Dataset runtime configured Daft Ray runner.")
                    return
                except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                    if _is_runner_already_configured_error(exc):
                        log("Dataset runtime reused existing Daft runner.")
                        return
                    if callable(set_runner_native):
                        try:
                            set_runner_native()
                            log(f"Dataset runtime fell back to Daft native runner after Ray-runner failure ({exc}).")
                            return
                        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as native_exc:
                            if _is_runner_already_configured_error(native_exc):
                                log("Dataset runtime reused existing Daft runner.")
                                return
                            log(f"Dataset runtime skipped Daft runner setup ({native_exc}).")
                            return
                    log(f"Dataset runtime skipped Daft runner setup ({exc}).")
                    return

            if callable(set_runner_native):
                try:
                    set_runner_native()
                    log("Dataset runtime configured Daft native runner (Ray-runner API unavailable).")
                except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                    if _is_runner_already_configured_error(exc):
                        log("Dataset runtime reused existing Daft runner.")
                    else:
                        log(f"Dataset runtime skipped Daft runner setup ({exc}).")
                return

            log("Dataset runtime could not find Daft runner APIs; using Daft defaults.")
        except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:  # pragma: no cover
            log(f"Dataset runtime skipped Daft runner setup ({exc}).")


def _topological_order(spec: PipelineSpecDocument) -> tuple[list[str], dict[str, list[str]], dict[str, list[str]]]:
    stage_ids = [stage.stage_id for stage in spec.stages]
    incoming: dict[str, list[str]] = {stage_id: [] for stage_id in stage_ids}
    adjacency: dict[str, list[str]] = {stage_id: [] for stage_id in stage_ids}
    indegree = {stage_id: 0 for stage_id in stage_ids}

    for edge in spec.edges:
        adjacency[edge.source].append(edge.target)
        incoming[edge.target].append(edge.source)
        indegree[edge.target] += 1

    queue = deque([stage_id for stage_id in stage_ids if indegree[stage_id] == 0])
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


def run_dataset_pipeline(
    spec: PipelineSpecDocument,
    log: Callable[[str], None],
) -> DatasetPipelineResult:
    ctx = _build_context(spec)
    _maybe_init_ray_and_daft(ctx, log)
    order, adjacency, incoming = _topological_order(spec)

    stage_by_id = {stage.stage_id: stage for stage in spec.stages}
    stage_instances = {stage_id: _instantiate_dataset_stage(stage_by_id[stage_id]) for stage_id in order}

    for stage_id in order:
        stage_instances[stage_id].setup(ctx)

    outputs: dict[str, Any] = {}
    metrics: list[dict[str, Any]] = []

    for stage_id in order:
        upstream_ids = incoming[stage_id]
        stage_inputs = {upstream_id: outputs[upstream_id] for upstream_id in upstream_ids}

        stage_start = time.perf_counter()
        output_ref = stage_instances[stage_id].run(ctx, stage_inputs)
        stage_duration = time.perf_counter() - stage_start

        if not isinstance(output_ref, DatasetRef):
            raise ValueError(f"Dataset stage {stage_id} must return DatasetRef")

        outputs[stage_id] = output_ref
        metrics.append(
            {
                "stage_id": stage_id,
                "duration_seconds": round(stage_duration, 4),
                "input_count": len(stage_inputs),
                "output_uri": output_ref.uri,
                "output_format": output_ref.format,
            }
        )
        log(f"Dataset stage {stage_id} completed in {stage_duration:.3f}s -> {output_ref.uri}")

    leaves = [stage_id for stage_id, downstream in adjacency.items() if not downstream]
    if len(leaves) != 1:
        raise ValueError("Dataset pipeline execution requires exactly one leaf stage")
    leaf_id = leaves[0]

    return DatasetPipelineResult(output_ref=outputs[leaf_id], stage_metrics=metrics, execution_order=order)
