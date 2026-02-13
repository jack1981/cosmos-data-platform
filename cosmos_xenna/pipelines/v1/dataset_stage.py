from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class DatasetRef:
    """Reference to a materialized dataset artifact."""

    uri: str
    format: str = "lance"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DatasetRuntimeContext:
    """Execution context shared by dataset pipeline stages."""

    io_config: Any
    pipeline_io: Any
    storage_options: dict[str, Any]
    ray_mode: str
    ray_address: Optional[str]
    work_dir: str


class DatasetStage(ABC):
    """Base class for dataset/dataframe stages."""

    def __init__(self, params: dict[str, Any]) -> None:
        self.params = params

    def setup(self, ctx: DatasetRuntimeContext) -> None:
        del ctx
        return

    @abstractmethod
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        """Run stage logic and return a materialized dataset reference."""
