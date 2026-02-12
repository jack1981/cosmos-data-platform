from __future__ import annotations

from collections import deque
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class ResourceRequirements(BaseModel):
    cpus: float = Field(default=1.0, ge=0)
    gpus: float = Field(default=0.0, ge=0)
    memory_mb: int | None = Field(default=None, ge=1)


class StageParamsSchema(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)


class StageDefinition(BaseModel):
    stage_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=255)
    python_import_path: str | None = None
    stage_template: str | None = None
    resources: ResourceRequirements = Field(default_factory=ResourceRequirements)
    batch_size: int = Field(default=1, ge=1)
    concurrency_hint: int = Field(default=1, ge=1)
    retries: int = Field(default=0, ge=0)
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_stage_source(self) -> "StageDefinition":
        if bool(self.python_import_path) == bool(self.stage_template):
            raise ValueError("Exactly one of python_import_path or stage_template must be set")
        return self


class PipelineEdge(BaseModel):
    source: str
    target: str


class SourceConfig(BaseModel):
    kind: Literal["inline", "queue", "dataset_uri"] = "inline"
    static_data: list[Any] = Field(default_factory=list)
    uri: str | None = None


class SinkConfig(BaseModel):
    kind: Literal["none", "queue", "artifact_uri"] = "none"
    uri: str | None = None


class IOConfig(BaseModel):
    source: SourceConfig = Field(default_factory=SourceConfig)
    sink: SinkConfig = Field(default_factory=SinkConfig)


class RuntimeConfig(BaseModel):
    ray_address: str | None = None
    autoscaling: dict[str, Any] = Field(default_factory=dict)
    retry_policy: dict[str, Any] = Field(default_factory=dict)


class ObservabilityConfig(BaseModel):
    log_level: Literal["DEBUG", "INFO", "WARN", "ERROR"] = "INFO"
    metrics_enabled: bool = True
    tracing_enabled: bool = False


class MetadataLinks(BaseModel):
    datasets: list[str] = Field(default_factory=list)
    models: list[str] = Field(default_factory=list)


class PipelineSpecDocument(BaseModel):
    pipeline_id: str | None = None
    name: str = Field(min_length=1, max_length=255)
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    owners: list[str] = Field(default_factory=list)
    team_ids: list[str] = Field(default_factory=list)
    execution_mode: Literal["streaming", "batch", "serving"] = "streaming"
    stages: list[StageDefinition] = Field(default_factory=list)
    edges: list[PipelineEdge] = Field(default_factory=list)
    io: IOConfig = Field(default_factory=IOConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    metadata_links: MetadataLinks = Field(default_factory=MetadataLinks)

    @model_validator(mode="after")
    def validate_linear_chain(self) -> "PipelineSpecDocument":
        if not self.stages:
            raise ValueError("Pipeline must define at least one stage")

        stage_ids = [stage.stage_id for stage in self.stages]
        if len(stage_ids) != len(set(stage_ids)):
            raise ValueError("Stage IDs must be unique")

        if not self.edges and len(self.stages) > 1:
            self.edges = [
                PipelineEdge(source=self.stages[idx].stage_id, target=self.stages[idx + 1].stage_id)
                for idx in range(len(self.stages) - 1)
            ]

        if len(self.stages) == 1:
            self.edges = []
            return self

        if len(self.edges) != len(self.stages) - 1:
            raise ValueError("Linear pipelines require exactly N-1 edges")

        valid_nodes = set(stage_ids)
        indegree = {stage_id: 0 for stage_id in stage_ids}
        outdegree = {stage_id: 0 for stage_id in stage_ids}

        for edge in self.edges:
            if edge.source not in valid_nodes or edge.target not in valid_nodes:
                raise ValueError("All edges must reference known stage IDs")
            indegree[edge.target] += 1
            outdegree[edge.source] += 1

        roots = [node for node in stage_ids if indegree[node] == 0]
        leaves = [node for node in stage_ids if outdegree[node] == 0]

        if len(roots) != 1 or len(leaves) != 1:
            raise ValueError("Linear pipeline must have exactly one root and one leaf")

        for node in stage_ids:
            if indegree[node] > 1 or outdegree[node] > 1:
                raise ValueError("Linear pipeline cannot fan-out or fan-in")

        adjacency: dict[str, list[str]] = {stage_id: [] for stage_id in stage_ids}
        for edge in self.edges:
            adjacency[edge.source].append(edge.target)

        topo_order: list[str] = []
        queue: deque[str] = deque(roots)
        local_indegree = indegree.copy()
        while queue:
            node = queue.popleft()
            topo_order.append(node)
            for nxt in adjacency[node]:
                local_indegree[nxt] -= 1
                if local_indegree[nxt] == 0:
                    queue.append(nxt)

        if topo_order != stage_ids:
            raise ValueError("Stage order must match topological order for linear execution")

        return self


class StructuredDiff(BaseModel):
    changed_fields: list[str] = Field(default_factory=list)
    stage_changes: list[dict[str, Any]] = Field(default_factory=list)
