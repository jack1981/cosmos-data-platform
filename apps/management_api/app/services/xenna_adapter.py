"""Adapter bridging PipelineForge DatasetStage to cosmos_xenna's Stage base class.

Follows the cosmos-curate integration pattern: each DatasetStage is wrapped in a
DatasetStageAdapter that implements cosmos_xenna's Stage protocol, enabling
distributed execution via Xenna's streaming executor with actor pools, autoscaling,
and backpressure.
"""

from __future__ import annotations

import logging
from typing import Any

from app.services.dataset_types import DatasetRef, DatasetRuntimeContext, DatasetStage

logger = logging.getLogger(__name__)

try:
    from cosmos_xenna.pipelines.v1 import Resources, Stage

    _XENNA_AVAILABLE = True
except ImportError:  # pragma: no cover
    _XENNA_AVAILABLE = False

    class Stage:  # type: ignore[no-redef]
        """Stub when cosmos_xenna is not installed."""

    class Resources:  # type: ignore[no-redef]
        """Stub when cosmos_xenna is not installed."""

        def __init__(self, **kwargs: Any) -> None:
            for k, v in kwargs.items():
                setattr(self, k, v)


def is_xenna_available() -> bool:
    """Return True if cosmos_xenna is importable."""
    return _XENNA_AVAILABLE


class DatasetStageAdapter(Stage):
    """Wraps a PipelineForge DatasetStage for cosmos_xenna execution.

    Each adapter instance holds:
    - A DatasetStage implementation (the actual transform logic)
    - A DatasetRuntimeContext (shared execution context)
    - Resource/batch configuration from the StageDefinition
    """

    def __init__(
        self,
        dataset_stage: DatasetStage,
        ctx: DatasetRuntimeContext,
        stage_id: str,
        cpus: float = 1.0,
        gpus: float = 0.0,
        batch_size: int = 1,
    ) -> None:
        self._stage = dataset_stage
        self._ctx = ctx
        self._stage_id = stage_id
        self._cpus = cpus
        self._gpus = gpus
        self._batch_size = batch_size

    @property
    def required_resources(self) -> Resources:
        return Resources(cpus=self._cpus, gpus=self._gpus)

    @property
    def stage_batch_size(self) -> int:
        return self._batch_size

    def setup_on_node(self, node_info: Any, worker_metadata: Any) -> None:
        """Called once per node. DatasetStages are stateless — no-op."""

    def setup(self, worker_metadata: Any) -> None:
        """Called once per worker. Delegates to DatasetStage.setup."""
        self._stage.setup(self._ctx)

    def process_data(self, in_data: list[Any]) -> list[Any]:
        """Process a batch of items through the wrapped DatasetStage.

        Each item in ``in_data`` is expected to be a dict mapping upstream
        stage IDs to DatasetRef instances (matching the DatasetStage.run signature).
        """
        results: list[Any] = []
        for item in in_data:
            if isinstance(item, dict):
                inputs = item
            elif isinstance(item, DatasetRef):
                inputs = {"__source__": item}
            else:
                inputs = {"__source__": DatasetRef(uri=str(item))}

            result = self._stage.run(self._ctx, inputs)
            results.append(result)
        return results

    def __repr__(self) -> str:
        return f"DatasetStageAdapter(stage_id={self._stage_id!r}, stage={self._stage.__class__.__name__})"
