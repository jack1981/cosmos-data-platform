from __future__ import annotations

from app.services.dataset_types import DatasetRef, DatasetRuntimeContext, DatasetStage


class EmitDatasetStage(DatasetStage):
    def run(
        self,
        ctx: DatasetRuntimeContext,
        inputs: dict[str, DatasetRef],
    ) -> DatasetRef:
        del ctx
        if inputs:
            raise ValueError("EmitDatasetStage expects no inputs")
        uri = str(self.params.get("uri", "lance://emit"))
        return DatasetRef(
            uri=uri,
            format=str(self.params.get("format", "lance")),
            metadata={"stage": self.params.get("name", "emit")},
        )


class JoinDatasetStage(DatasetStage):
    def run(
        self,
        ctx: DatasetRuntimeContext,
        inputs: dict[str, DatasetRef],
    ) -> DatasetRef:
        del ctx
        if len(inputs) < 2:
            raise ValueError("JoinDatasetStage expects at least two inputs")

        ordered_inputs = sorted(inputs.items(), key=lambda item: item[0])
        joined_uri = "+".join(ref.uri for _, ref in ordered_inputs)
        return DatasetRef(
            uri=f"lance://joined/{joined_uri}",
            metadata={"inputs": [stage_id for stage_id, _ in ordered_inputs]},
        )
