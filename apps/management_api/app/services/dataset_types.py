from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DatasetRef:
    """Reference to a materialized dataset artifact."""

    uri: str
    format: str = "lance"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DatasetRuntimeContext:
    """Execution context shared by dataset-mode pipeline stages."""

    io_config: Any
    pipeline_io: Any
    storage_options: dict[str, Any]
    ray_mode: str
    ray_address: str | None
    work_dir: str


class DatasetStage(ABC):
    """Base class for dataset/dataframe stages."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = params

    def setup(self, ctx: DatasetRuntimeContext) -> None:
        del ctx

    @abstractmethod
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef: ...
