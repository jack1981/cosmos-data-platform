from __future__ import annotations

import hashlib
import math
from typing import Any

from app.services.dataset_stages import (
    _first_dataset_df,
    _materialize,
    _read_lance,
    _rows_from_df,
    _rows_to_df,
    _single_input,
    _source_uri_from_context,
)
from app.services.dataset_types import DatasetRef, DatasetRuntimeContext, DatasetStage

# ---------------------------------------------------------------------------
# Caption template data (deterministic, no GPU)
# ---------------------------------------------------------------------------

_SCENES = [
    "indoor office",
    "parking lot at night",
    "highway intersection",
    "warehouse interior",
    "rooftop terrace",
    "subway platform",
    "construction site",
    "residential street",
    "airport tarmac",
    "forest trail",
]

_ACTIONS = [
    "a person walking",
    "vehicles passing",
    "camera panning slowly",
    "crowd dispersing",
    "machinery operating",
    "an animal crossing",
    "rain falling steadily",
    "workers assembling equipment",
    "a drone hovering",
    "pedestrians waiting",
]

_SETTINGS = [
    "under daylight",
    "in foggy conditions",
    "during heavy rain",
    "at dusk",
    "under artificial lighting",
    "in bright sunshine",
    "during snowfall",
    "at dawn",
    "under overcast skies",
    "in windy conditions",
]

# ---------------------------------------------------------------------------
# Scoring helpers (matches FastTextScorerStage pattern)
# ---------------------------------------------------------------------------


def _hash_score(value: str, salt: str) -> float:
    digest = hashlib.sha256(f"{value}:{salt}".encode()).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _md5_score(value: str, salt: str) -> float:
    digest = hashlib.md5(f"{value}:{salt}".encode()).hexdigest()  # noqa: S324
    return int(digest[:8], 16) / 0xFFFFFFFF


# ---------------------------------------------------------------------------
# Stage 1: VideoMetadataReaderStage
# ---------------------------------------------------------------------------


class VideoMetadataReaderStage(DatasetStage):
    """Read a video catalog Lance dataset."""

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        del inputs
        source_uri = str(
            self.params.get("uri") or self.params.get("source_uri") or _source_uri_from_context(ctx) or ""
        ).strip()
        if not source_uri:
            raise ValueError("VideoMetadataReaderStage requires a source uri via params or pipeline io.source.uri")

        df = _read_lance(ctx, source_uri)
        return _materialize(
            ctx,
            stage_name="video_metadata_reader",
            params=self.params,
            inputs={},
            df=df,
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": source_uri},
        )


# ---------------------------------------------------------------------------
# Stage 2: VideoClipSplitterStage
# ---------------------------------------------------------------------------


class VideoClipSplitterStage(DatasetStage):
    """Expand each video row into clip rows based on duration and clip_duration."""

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "VideoClipSplitterStage")
        rows = _rows_from_df(df)
        clip_duration = float(self.params.get("clip_duration", 10.0))

        clips: list[dict[str, Any]] = []
        for row in rows:
            duration = float(row.get("duration_seconds") or row.get("duration", 0))
            num_clips = max(1, math.ceil(duration / clip_duration))
            video_id = str(row.get("video_id", ""))
            for i in range(num_clips):
                clip = dict(row)
                clip_start = i * clip_duration
                clip_end = min((i + 1) * clip_duration, duration)
                clip_id = hashlib.sha256(f"{video_id}:clip:{i}".encode()).hexdigest()[:16]
                clip["clip_id"] = clip_id
                clip["clip_index"] = i
                clip["clip_start"] = round(clip_start, 3)
                clip["clip_end"] = round(clip_end, 3)
                clips.append(clip)

        return _materialize(
            ctx,
            stage_name="video_clip_splitter",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(clips, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "clip_duration": clip_duration, "total_clips": len(clips)},
        )


# ---------------------------------------------------------------------------
# Stage 3: VideoMotionScorerStage
# ---------------------------------------------------------------------------


class VideoMotionScorerStage(DatasetStage):
    """Assign deterministic motion_score and motion_category via SHA256."""

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "VideoMotionScorerStage")
        rows = _rows_from_df(df)

        for row in rows:
            clip_id = str(row.get("clip_id", row.get("video_id", "")))
            score = round(_hash_score(clip_id, "motion"), 6)
            row["motion_score"] = score
            if score < 0.15:
                row["motion_category"] = "static"
            elif score < 0.40:
                row["motion_category"] = "low"
            elif score < 0.70:
                row["motion_category"] = "moderate"
            else:
                row["motion_category"] = "high"

        return _materialize(
            ctx,
            stage_name="video_motion_scorer",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri},
        )


# ---------------------------------------------------------------------------
# Stage 4: VideoMotionFilterStage
# ---------------------------------------------------------------------------


class VideoMotionFilterStage(DatasetStage):
    """Filter clips by motion_score >= min_score."""

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "VideoMotionFilterStage")
        rows = _rows_from_df(df)
        min_score = float(self.params.get("min_score", 0.15))

        filtered = [
            row
            for row in rows
            if isinstance(row.get("motion_score"), (int, float)) and float(row["motion_score"]) >= min_score
        ]

        return _materialize(
            ctx,
            stage_name="video_motion_filter",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(filtered, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "min_score": min_score},
        )


# ---------------------------------------------------------------------------
# Stage 5: VideoAestheticScorerStage
# ---------------------------------------------------------------------------


class VideoAestheticScorerStage(DatasetStage):
    """Assign deterministic aesthetic_score and aesthetic_grade via MD5."""

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "VideoAestheticScorerStage")
        rows = _rows_from_df(df)

        for row in rows:
            clip_id = str(row.get("clip_id", row.get("video_id", "")))
            resolution = str(row.get("resolution_width", "1920"))
            score = round(_md5_score(f"{clip_id}:{resolution}", "aesthetic"), 6)
            row["aesthetic_score"] = score
            if score < 0.25:
                row["aesthetic_grade"] = "poor"
            elif score < 0.50:
                row["aesthetic_grade"] = "fair"
            elif score < 0.75:
                row["aesthetic_grade"] = "good"
            else:
                row["aesthetic_grade"] = "excellent"

        return _materialize(
            ctx,
            stage_name="video_aesthetic_scorer",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri},
        )


# ---------------------------------------------------------------------------
# Stage 6: VideoAestheticFilterStage
# ---------------------------------------------------------------------------


class VideoAestheticFilterStage(DatasetStage):
    """Filter clips by aesthetic_score >= min_score."""

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "VideoAestheticFilterStage")
        rows = _rows_from_df(df)
        min_score = float(self.params.get("min_score", 0.3))

        filtered = [
            row
            for row in rows
            if isinstance(row.get("aesthetic_score"), (int, float)) and float(row["aesthetic_score"]) >= min_score
        ]

        return _materialize(
            ctx,
            stage_name="video_aesthetic_filter",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(filtered, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "min_score": min_score},
        )


# ---------------------------------------------------------------------------
# Stage 7: VideoEmbeddingScorerStage
# ---------------------------------------------------------------------------


class VideoEmbeddingScorerStage(DatasetStage):
    """Generate deterministic embedding_norm, embedding_cluster, embedding_dimensions."""

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "VideoEmbeddingScorerStage")
        rows = _rows_from_df(df)

        for row in rows:
            clip_id = str(row.get("clip_id", row.get("video_id", "")))
            norm = round(_hash_score(clip_id, "embedding_norm") * 10.0, 6)
            cluster_digest = hashlib.sha256(f"{clip_id}:cluster".encode()).hexdigest()
            cluster = int(cluster_digest[:8], 16) % 20
            row["embedding_norm"] = norm
            row["embedding_cluster"] = cluster
            row["embedding_dimensions"] = 768

        return _materialize(
            ctx,
            stage_name="video_embedding_scorer",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri},
        )


# ---------------------------------------------------------------------------
# Stage 8: VideoCaptionGeneratorStage
# ---------------------------------------------------------------------------


class VideoCaptionGeneratorStage(DatasetStage):
    """Generate template captions from hash-selected scene/action/setting lists."""

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "VideoCaptionGeneratorStage")
        rows = _rows_from_df(df)
        model_name = str(self.params.get("model_name", "template-vlm-v1"))

        for row in rows:
            clip_id = str(row.get("clip_id", row.get("video_id", "")))
            digest = hashlib.sha256(f"{clip_id}:caption".encode()).hexdigest()
            scene_idx = int(digest[:4], 16) % len(_SCENES)
            action_idx = int(digest[4:8], 16) % len(_ACTIONS)
            setting_idx = int(digest[8:12], 16) % len(_SETTINGS)
            caption = f"{_SCENES[scene_idx]}, {_ACTIONS[action_idx]}, {_SETTINGS[setting_idx]}"
            confidence = round(0.70 + _hash_score(clip_id, "caption_conf") * 0.25, 4)
            row["caption"] = caption
            row["caption_length"] = len(caption)
            row["caption_model"] = model_name
            row["caption_confidence"] = confidence

        return _materialize(
            ctx,
            stage_name="video_caption_generator",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "model_name": model_name},
        )


# ---------------------------------------------------------------------------
# Stage 9: VideoCaptionEmbeddingStage
# ---------------------------------------------------------------------------


class VideoCaptionEmbeddingStage(DatasetStage):
    """Generate deterministic caption embedding norm and dimensions from caption hash."""

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "VideoCaptionEmbeddingStage")
        rows = _rows_from_df(df)

        for row in rows:
            caption = str(row.get("caption", ""))
            norm = round(_hash_score(caption, "caption_emb") * 8.0, 6)
            row["caption_embedding_norm"] = norm
            row["caption_embedding_dimensions"] = 512

        return _materialize(
            ctx,
            stage_name="video_caption_embedding",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri},
        )


# ---------------------------------------------------------------------------
# Stage 10: VideoClipWriterStage
# ---------------------------------------------------------------------------


class VideoClipWriterStage(DatasetStage):
    """Write final Lance output with output_format metadata."""

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        upstream = _single_input(inputs, "VideoClipWriterStage")
        df = _read_lance(ctx, upstream.uri)
        output_format = str(self.params.get("output_format", "lance"))

        return _materialize(
            ctx,
            stage_name="video_clip_writer",
            params=self.params,
            inputs=inputs,
            df=df,
            output_uri=self.params.get("output_uri"),
            metadata={
                "source_uri": upstream.uri,
                "output_format": output_format,
                "writer_stage": True,
            },
        )
