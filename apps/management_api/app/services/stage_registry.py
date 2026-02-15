from __future__ import annotations

import hashlib
import importlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, ClassVar

import httpx

from app.schemas.pipeline_spec import StageDefinition
from app.services.dataset_stages import (
    AddConstantsStage,
    AddRankQuantileStage,
    ColumnAliasStage,
    ColumnDropStage,
    ColumnSelectStage,
    ConcatenateColumnsStage,
    ConcatStage,
    ConversationToParagraphStage,
    DuplicateSampleRatioStage,
    FastTextFilterStage,
    FastTextScorerStage,
    FilterByRatioStage,
    FilterStage,
    FlattenStage,
    GroupFlattenStage,
    InterleavedReorderStage,
    JoinerStage,
    JoinStage,
    LanceReaderStage,
    LanceWriterStage,
    MinHashStage,
    ReorderStage,
    RowNumberStage,
    SamplerStage,
    SchemaStage,
    SelectorStage,
    SeqClassifierScorerStage,
    ShufflerStage,
    SplitterStage,
    StatStage,
    TokenCounterV2Stage,
    UnionByNameStage,
    UnionByPositionStage,
    VisualizerStage,
)
from app.services.video_dataset_stages import (
    VideoAestheticFilterStage,
    VideoAestheticScorerStage,
    VideoCaptionEmbeddingStage,
    VideoCaptionGeneratorStage,
    VideoClipSplitterStage,
    VideoClipWriterStage,
    VideoEmbeddingScorerStage,
    VideoMetadataReaderStage,
    VideoMotionFilterStage,
    VideoMotionScorerStage,
)


@dataclass
class StageExecutor:
    stage_id: str
    name: str
    run: Callable[[list[Any]], list[Any]]


class BuiltinIdentity:
    def __init__(self, **_: Any) -> None:
        pass

    def process_data(self, in_data: list[Any]) -> list[Any]:
        return in_data


class BuiltinUppercase:
    def __init__(self, field: str | None = None, **_: Any) -> None:
        self.field = field

    def process_data(self, in_data: list[Any]) -> list[Any]:
        output: list[Any] = []
        for item in in_data:
            if isinstance(item, str):
                output.append(item.upper())
            elif isinstance(item, dict) and self.field and isinstance(item.get(self.field), str):
                copied = item.copy()
                copied[self.field] = copied[self.field].upper()
                output.append(copied)
            else:
                output.append(item)
        return output


class BuiltinSleep:
    def __init__(self, seconds: float = 0.1, **_: Any) -> None:
        self.seconds = seconds

    def process_data(self, in_data: list[Any]) -> list[Any]:
        time.sleep(self.seconds)
        return in_data


class BuiltinFilterNull:
    def __init__(self, **_: Any) -> None:
        pass

    def process_data(self, in_data: list[Any]) -> list[Any]:
        return [item for item in in_data if item is not None]


def _coerce_record(item: Any, index: int) -> dict[str, Any]:
    if isinstance(item, dict):
        record = item.copy()
    else:
        record = {"payload": item}
    record.setdefault("video_id", f"video-{index + 1:03d}")
    return record


def _stable_video_bytes(seed: str, size: int) -> bytes:
    size = max(1024, size)
    chunks: list[bytes] = []
    written = 0
    counter = 0
    while written < size:
        digest = hashlib.sha256(f"{seed}:{counter}".encode("utf-8")).digest()
        chunks.append(digest)
        written += len(digest)
        counter += 1

    payload = b"".join(chunks)[:size]
    mp4_header = b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2"
    if len(payload) >= len(mp4_header):
        payload = mp4_header + payload[len(mp4_header) :]
    return payload


def _sha16(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()[:16]


def _detect_video_format(payload: bytes) -> str:
    if len(payload) >= 8 and payload[4:8] == b"ftyp":
        return "mp4"
    if payload.startswith(b"\x1a\x45\xdf\xa3"):
        return "webm/mkv"
    if len(payload) >= 12 and payload.startswith(b"RIFF") and payload[8:12] == b"AVI ":
        return "avi"
    if payload.startswith(b"OggS"):
        return "ogg"
    return "unknown"


def _estimate_duration_seconds(size_bytes: int, bitrate_mbps: float = 2.5) -> float:
    if size_bytes <= 0:
        return 0.0
    duration = (size_bytes * 8.0) / (bitrate_mbps * 1_000_000)
    return round(max(duration, 0.2), 2)


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"_type": "bytes", "size_bytes": len(value), "sha16": _sha16(value)}
    if isinstance(value, dict):
        return {str(key): _json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


class BuiltinVideoDownload:
    def __init__(
        self,
        url_field: str = "video_url",
        output_field: str = "video_bytes",
        status_field: str = "download_status",
        max_bytes: int = 2_097_152,
        timeout_seconds: float = 3.0,
        fallback_size_bytes: int = 262_144,
        allow_http: bool = True,
        **_: Any,
    ) -> None:
        self.url_field = url_field
        self.output_field = output_field
        self.status_field = status_field
        self.max_bytes = max(1_024, int(max_bytes))
        self.timeout_seconds = max(0.1, float(timeout_seconds))
        self.fallback_size_bytes = max(1_024, int(fallback_size_bytes))
        self.allow_http = allow_http

    def _download_http(self, url: str) -> bytes:
        response = httpx.get(
            url,
            timeout=self.timeout_seconds,
            follow_redirects=True,
            headers={"User-Agent": "pipelineforge-management-plane/1.0"},
        )
        response.raise_for_status()
        content = response.content or b""
        if not content:
            raise ValueError("empty response body")
        return content[: self.max_bytes]

    def process_data(self, in_data: list[Any]) -> list[Any]:
        out: list[Any] = []
        for index, item in enumerate(in_data):
            record = _coerce_record(item, index)
            url = str(record.get(self.url_field) or "").strip()

            payload: bytes | None = None
            status = "simulated"
            source = url or "synthetic://generated"
            error_message: str | None = None

            if isinstance(record.get(self.output_field), (bytes, bytearray)):
                payload = bytes(record[self.output_field])
                status = "inline_bytes"
                source = "inline://bytes"
            elif url.startswith("s3://"):
                payload = _stable_video_bytes(url, self.fallback_size_bytes)
                status = "simulated_s3"
            elif url.startswith(("http://", "https://")) and self.allow_http:
                try:
                    payload = self._download_http(url)
                    status = "downloaded_http"
                except Exception as exc:  # noqa: BLE001
                    error_message = str(exc)
                    payload = _stable_video_bytes(url, self.fallback_size_bytes)
                    status = "simulated_fallback"
            else:
                seed = url or record["video_id"]
                payload = _stable_video_bytes(seed, self.fallback_size_bytes)
                status = "simulated_fallback"

            record[self.output_field] = payload
            record[self.status_field] = status
            record["video_source"] = source
            record["video_size_bytes"] = len(payload)
            record["video_sha16"] = _sha16(payload)
            record["video_format"] = _detect_video_format(payload)
            if error_message:
                record["download_error"] = error_message[:240]
            out.append(record)
        return out


class BuiltinVideoCaption:
    _SCENES = ("warehouse", "street", "entry_gate", "office", "parking_lot", "loading_dock")
    _MOTIONS = ("low", "moderate", "high", "mixed")

    def __init__(
        self,
        input_bytes_field: str = "video_bytes",
        caption_field: str = "caption",
        model_name: str = "demo-vlm-mini",
        min_bytes: int = 1024,
        **_: Any,
    ) -> None:
        self.input_bytes_field = input_bytes_field
        self.caption_field = caption_field
        self.model_name = model_name
        self.min_bytes = max(1, int(min_bytes))

    def process_data(self, in_data: list[Any]) -> list[Any]:
        out: list[Any] = []
        for index, item in enumerate(in_data):
            record = _coerce_record(item, index)
            raw_payload = record.get(self.input_bytes_field)

            payload: bytes
            if isinstance(raw_payload, (bytes, bytearray)):
                payload = bytes(raw_payload)
            else:
                payload = b""

            checksum = hashlib.sha256(payload or record["video_id"].encode("utf-8")).hexdigest()
            selector = int(checksum[:8], 16)
            confidence = round(0.72 + ((selector % 20) / 100), 2)
            duration = _estimate_duration_seconds(len(payload))
            scene = self._SCENES[selector % len(self._SCENES)]
            motion = self._MOTIONS[(selector // len(self._SCENES)) % len(self._MOTIONS)]
            context_hint = str(record.get("ops_hint") or "").strip()

            if len(payload) < self.min_bytes:
                caption = "video payload too small to caption reliably"
                confidence = min(confidence, 0.55)
            else:
                suffix = f"; hint={context_hint[:60]}" if context_hint else ""
                caption = (
                    f"{scene} scene, {motion} motion, {record.get('video_format', 'unknown')} "
                    f"clip (~{duration}s){suffix}"
                )

            record[self.caption_field] = caption
            record["caption_model"] = self.model_name
            record["caption_confidence"] = confidence
            record["caption_tokens_estimate"] = max(8, min(64, len(caption.split()) * 3))
            out.append(record)
        return out


class BuiltinVideoQualityGate:
    def __init__(
        self,
        input_bytes_field: str = "video_bytes",
        status_field: str = "quality_status",
        min_bytes: int = 65_536,
        min_confidence: float = 0.75,
        allow_simulated_downloads: bool = True,
        drop_failed: bool = False,
        **_: Any,
    ) -> None:
        self.input_bytes_field = input_bytes_field
        self.status_field = status_field
        self.min_bytes = max(1, int(min_bytes))
        self.min_confidence = float(min_confidence)
        self.allow_simulated_downloads = allow_simulated_downloads
        self.drop_failed = drop_failed

    def process_data(self, in_data: list[Any]) -> list[Any]:
        out: list[Any] = []
        for index, item in enumerate(in_data):
            record = _coerce_record(item, index)
            reasons: list[str] = []

            payload = record.get(self.input_bytes_field)
            payload_len = len(payload) if isinstance(payload, (bytes, bytearray)) else 0
            if payload_len < self.min_bytes:
                reasons.append(f"payload_lt_{self.min_bytes}")

            confidence = float(record.get("caption_confidence", 0.0) or 0.0)
            if confidence < self.min_confidence:
                reasons.append(f"confidence_lt_{self.min_confidence}")

            status = str(record.get("download_status", ""))
            if not self.allow_simulated_downloads and status.startswith("simulated"):
                reasons.append("simulated_download_not_allowed")

            record[self.status_field] = "pass" if not reasons else "fail"
            record["quality_reasons"] = reasons

            if not reasons or not self.drop_failed:
                out.append(record)
        return out


class BuiltinVideoIncidentEnricher:
    _PRIORITY: ClassVar[dict[str, int]] = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
    _DEFAULT_RULES: ClassVar[dict[str, str]] = {
        "fire": "CRITICAL",
        "smoke": "HIGH",
        "crash": "HIGH",
        "collision": "HIGH",
        "intrusion": "HIGH",
        "weapon": "CRITICAL",
        "fight": "HIGH",
        "fall": "MEDIUM",
    }

    def __init__(
        self,
        text_fields: list[str] | None = None,
        output_field: str = "incident",
        rules: dict[str, str] | None = None,
        default_severity: str = "LOW",
        **_: Any,
    ) -> None:
        self.text_fields = text_fields or ["caption", "ops_hint", "camera_id", "location"]
        self.output_field = output_field
        self.rules = rules or self._DEFAULT_RULES
        self.default_severity = default_severity if default_severity in self._PRIORITY else "LOW"

    def process_data(self, in_data: list[Any]) -> list[Any]:
        out: list[Any] = []
        for index, item in enumerate(in_data):
            record = _coerce_record(item, index)
            text_blob = " ".join(str(record.get(field, "")) for field in self.text_fields).lower()

            matches = [keyword for keyword in self.rules if keyword in text_blob]
            severity = self.default_severity
            for keyword in matches:
                candidate = self.rules.get(keyword, self.default_severity)
                if self._PRIORITY.get(candidate, 0) > self._PRIORITY.get(severity, 0):
                    severity = candidate

            incident = {
                "severity": severity,
                "keywords": matches,
                "recommended_action": ("page-oncall" if severity in {"HIGH", "CRITICAL"} else "create-ticket"),
            }
            record[self.output_field] = incident
            record["alert_required"] = severity in {"HIGH", "CRITICAL"}
            out.append(record)
        return out


class BuiltinVideoWriter:
    def __init__(
        self,
        output_path: str = "/tmp/pipelineforge_artifacts/video_pipeline_output.jsonl",
        drop_output: bool = True,
        include_payload_bytes: bool = False,
        **_: Any,
    ) -> None:
        self.output_path = output_path
        self.drop_output = drop_output
        self.include_payload_bytes = include_payload_bytes

    def _resolve_path(self) -> Path:
        target = Path(self.output_path)
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            return target
        except OSError:
            fallback = Path("/tmp/pipelineforge_artifacts/video_pipeline_output.jsonl")
            fallback.parent.mkdir(parents=True, exist_ok=True)
            return fallback

    def process_data(self, in_data: list[Any]) -> list[Any]:
        target_path = self._resolve_path()
        transformed: list[Any] = []

        with target_path.open("a", encoding="utf-8") as handle:
            for index, item in enumerate(in_data):
                record = _coerce_record(item, index)
                record["artifact_path"] = str(target_path)
                json_record = _json_safe(record)
                if not self.include_payload_bytes and isinstance(json_record, dict):
                    if isinstance(json_record.get("video_bytes"), dict):
                        json_record["video_bytes"] = {
                            "size_bytes": json_record["video_bytes"].get("size_bytes", 0),
                            "sha16": json_record["video_bytes"].get("sha16", ""),
                        }
                handle.write(json.dumps(json_record, sort_keys=True) + "\n")
                transformed.append(record)

        return [] if self.drop_output else transformed


_TEMPLATE_REGISTRY: dict[str, type] = {
    "builtin.identity": BuiltinIdentity,
    "builtin.uppercase": BuiltinUppercase,
    "builtin.sleep": BuiltinSleep,
    "builtin.filter_null": BuiltinFilterNull,
    "builtin.video_download": BuiltinVideoDownload,
    "builtin.video_caption": BuiltinVideoCaption,
    "builtin.video_quality_gate": BuiltinVideoQualityGate,
    "builtin.video_incident_enrich": BuiltinVideoIncidentEnricher,
    "builtin.video_writer": BuiltinVideoWriter,
    "builtin.dataset_lance_reader": LanceReaderStage,
    "builtin.dataset_filter": FilterStage,
    "builtin.dataset_column_select": ColumnSelectStage,
    "builtin.dataset_shuffle": ShufflerStage,
    "builtin.dataset_union_by_name": UnionByNameStage,
    "builtin.dataset_join": JoinStage,
    "builtin.dataset_lance_writer": LanceWriterStage,
    "builtin.datafiner_lance_reader": LanceReaderStage,
    "builtin.datafiner_lance_writer": LanceWriterStage,
    "builtin.datafiner_splitter": SplitterStage,
    "builtin.datafiner_visualizer": VisualizerStage,
    "builtin.datafiner_schema": SchemaStage,
    "builtin.datafiner_row_number": RowNumberStage,
    "builtin.datafiner_stat": StatStage,
    "builtin.datafiner_column_select": ColumnSelectStage,
    "builtin.datafiner_column_drop": ColumnDropStage,
    "builtin.datafiner_column_alias": ColumnAliasStage,
    "builtin.datafiner_filter": FilterStage,
    "builtin.datafiner_filter_by_ratio": FilterByRatioStage,
    "builtin.datafiner_selector": SelectorStage,
    "builtin.datafiner_reorder": ReorderStage,
    "builtin.datafiner_interleaved_reorder": InterleavedReorderStage,
    "builtin.datafiner_union_by_name": UnionByNameStage,
    "builtin.datafiner_union_by_position": UnionByPositionStage,
    "builtin.datafiner_joiner": JoinerStage,
    "builtin.datafiner_add_constants": AddConstantsStage,
    "builtin.datafiner_conversation_to_paragraph": ConversationToParagraphStage,
    "builtin.datafiner_concatenate_columns": ConcatenateColumnsStage,
    "builtin.datafiner_concat": ConcatStage,
    "builtin.datafiner_duplicate_sample_ratio": DuplicateSampleRatioStage,
    "builtin.datafiner_sampler": SamplerStage,
    "builtin.datafiner_flatten": FlattenStage,
    "builtin.datafiner_group_flatten": GroupFlattenStage,
    "builtin.datafiner_fasttext_scorer": FastTextScorerStage,
    "builtin.datafiner_fasttext_filter": FastTextFilterStage,
    "builtin.datafiner_seq_classifier_scorer": SeqClassifierScorerStage,
    "builtin.datafiner_minhash": MinHashStage,
    "builtin.datafiner_add_rank_quantile": AddRankQuantileStage,
    "builtin.datafiner_token_counter_v2": TokenCounterV2Stage,
    # Video dataset-mode stages (cosmos_curate port)
    "builtin.video_dataset_metadata_reader": VideoMetadataReaderStage,
    "builtin.video_dataset_clip_splitter": VideoClipSplitterStage,
    "builtin.video_dataset_motion_scorer": VideoMotionScorerStage,
    "builtin.video_dataset_motion_filter": VideoMotionFilterStage,
    "builtin.video_dataset_aesthetic_scorer": VideoAestheticScorerStage,
    "builtin.video_dataset_aesthetic_filter": VideoAestheticFilterStage,
    "builtin.video_dataset_embedding_scorer": VideoEmbeddingScorerStage,
    "builtin.video_dataset_caption_generator": VideoCaptionGeneratorStage,
    "builtin.video_dataset_caption_embedding": VideoCaptionEmbeddingStage,
    "builtin.video_dataset_clip_writer": VideoClipWriterStage,
}

_TEMPLATE_METADATA: dict[str, dict[str, str]] = {
    "builtin.identity": {
        "name": "Identity",
        "description": "Pass-through stage for debugging and graph wiring.",
    },
    "builtin.uppercase": {
        "name": "Uppercase",
        "description": "Uppercase text payloads or a configured dictionary field.",
    },
    "builtin.sleep": {
        "name": "Sleep",
        "description": "Deterministic delay stage to simulate slow dependencies.",
    },
    "builtin.filter_null": {
        "name": "Filter Null",
        "description": "Remove null records from a batch.",
    },
    "builtin.video_download": {
        "name": "Video Download",
        "description": "Download video bytes from HTTP/S3 URLs with deterministic fallback bytes.",
    },
    "builtin.video_caption": {
        "name": "Video Caption",
        "description": "Generate deterministic caption text and confidence from video payload bytes.",
    },
    "builtin.video_quality_gate": {
        "name": "Video Quality Gate",
        "description": "Evaluate payload size and caption confidence; optionally drop failed records.",
    },
    "builtin.video_incident_enrich": {
        "name": "Video Incident Enrich",
        "description": "Tag severity and recommendations from caption/hint keyword rules.",
    },
    "builtin.video_writer": {
        "name": "Video Writer",
        "description": "Write pipeline outputs to JSONL artifact files for auditing and replay.",
    },
    "builtin.dataset_lance_reader": {
        "name": "Dataset Lance Reader",
        "description": "Read a Lance dataset URI into the dataset-mode DAG pipeline.",
    },
    "builtin.dataset_filter": {
        "name": "Dataset Filter",
        "description": "Filter rows using a Daft predicate or column/op/value parameters.",
    },
    "builtin.dataset_column_select": {
        "name": "Dataset Column Select",
        "description": "Project a subset of columns from the upstream dataset.",
    },
    "builtin.dataset_shuffle": {
        "name": "Dataset Shuffle",
        "description": "Shuffle/sampler stage for dataset-mode preprocessing flows.",
    },
    "builtin.dataset_union_by_name": {
        "name": "Dataset Union By Name",
        "description": "Union multiple upstream datasets by aligned column names.",
    },
    "builtin.dataset_join": {
        "name": "Dataset Join",
        "description": "Join multiple upstream datasets with Daft join semantics.",
    },
    "builtin.dataset_lance_writer": {
        "name": "Dataset Lance Writer",
        "description": "Materialize dataset output to a Lance URI.",
    },
    "builtin.datafiner_lance_reader": {"name": "LanceReader", "description": "Read source Lance dataset."},
    "builtin.datafiner_lance_writer": {"name": "LanceWriter", "description": "Write output Lance dataset."},
    "builtin.datafiner_splitter": {"name": "Splitter", "description": "Split dataset into deterministic partitions."},
    "builtin.datafiner_visualizer": {"name": "Visualizer", "description": "Write dataset preview rows for inspection."},
    "builtin.datafiner_schema": {"name": "Schema", "description": "Inspect and persist dataset schema metadata."},
    "builtin.datafiner_row_number": {"name": "RowNumber", "description": "Add deterministic row index column."},
    "builtin.datafiner_stat": {"name": "Stat", "description": "Compute numeric column summary statistics."},
    "builtin.datafiner_column_select": {"name": "ColumnSelect", "description": "Project selected columns."},
    "builtin.datafiner_column_drop": {"name": "ColumnDrop", "description": "Drop selected columns."},
    "builtin.datafiner_column_alias": {"name": "ColumnAlias", "description": "Rename columns by alias mapping."},
    "builtin.datafiner_filter": {"name": "Filter", "description": "Filter rows using predicate expression."},
    "builtin.datafiner_filter_by_ratio": {
        "name": "FilterByRatio",
        "description": "Randomly keep rows by configured ratio.",
    },
    "builtin.datafiner_selector": {"name": "Selector", "description": "Select rows by indices or offset/limit."},
    "builtin.datafiner_reorder": {"name": "Reorder", "description": "Sort rows by one or more keys."},
    "builtin.datafiner_interleaved_reorder": {
        "name": "InterleavedReorder",
        "description": "Round-robin rows across groups.",
    },
    "builtin.datafiner_union_by_name": {
        "name": "UnionByName",
        "description": "Union datasets by matching column names.",
    },
    "builtin.datafiner_union_by_position": {
        "name": "UnionByPosition",
        "description": "Union datasets by column positions.",
    },
    "builtin.datafiner_joiner": {"name": "Joiner", "description": "Join datasets using key columns."},
    "builtin.datafiner_add_constants": {
        "name": "AddConstants",
        "description": "Add constant-valued columns.",
    },
    "builtin.datafiner_conversation_to_paragraph": {
        "name": "ConversationToParagraph",
        "description": "Flatten conversation items into paragraph text.",
    },
    "builtin.datafiner_concatenate_columns": {
        "name": "ConcatenateColumns",
        "description": "Concatenate multiple columns into one string column.",
    },
    "builtin.datafiner_concat": {"name": "Concat", "description": "Concatenate dataset inputs into one output."},
    "builtin.datafiner_duplicate_sample_ratio": {
        "name": "DuplicateSampleRatio",
        "description": "Duplicate rows by ratio for oversampling.",
    },
    "builtin.datafiner_sampler": {"name": "Sampler", "description": "Sample by fraction or sample size."},
    "builtin.datafiner_flatten": {"name": "Flatten", "description": "Flatten list-valued column rows."},
    "builtin.datafiner_group_flatten": {
        "name": "GroupFlatten",
        "description": "Group rows and flatten grouped values.",
    },
    "builtin.datafiner_fasttext_scorer": {
        "name": "FastTextScorer",
        "description": "Generate deterministic fasttext-like scores.",
    },
    "builtin.datafiner_fasttext_filter": {
        "name": "FastTextFilter",
        "description": "Filter by fasttext score thresholds.",
    },
    "builtin.datafiner_seq_classifier_scorer": {
        "name": "SeqClassifierScorer",
        "description": "Generate deterministic sequence-classifier labels/scores.",
    },
    "builtin.datafiner_minhash": {"name": "MinHash", "description": "Compute minhash signatures and optional dedup."},
    "builtin.datafiner_add_rank_quantile": {
        "name": "AddRankQuantile",
        "description": "Attach rank and quantile buckets from score column.",
    },
    "builtin.datafiner_token_counter_v2": {
        "name": "TokenCounter_v2",
        "description": "Count text tokens for each row.",
    },
    # Video dataset-mode stages (cosmos_curate port)
    "builtin.video_dataset_metadata_reader": {
        "name": "Video Metadata Reader",
        "description": "Read video catalog Lance dataset into the pipeline.",
    },
    "builtin.video_dataset_clip_splitter": {
        "name": "Video Clip Splitter",
        "description": "Split videos into fixed-duration clips based on duration.",
    },
    "builtin.video_dataset_motion_scorer": {
        "name": "Video Motion Scorer",
        "description": "Assign deterministic motion score and category to each clip.",
    },
    "builtin.video_dataset_motion_filter": {
        "name": "Video Motion Filter",
        "description": "Filter clips below a minimum motion score threshold.",
    },
    "builtin.video_dataset_aesthetic_scorer": {
        "name": "Video Aesthetic Scorer",
        "description": "Assign deterministic aesthetic score and grade to each clip.",
    },
    "builtin.video_dataset_aesthetic_filter": {
        "name": "Video Aesthetic Filter",
        "description": "Filter clips below a minimum aesthetic score threshold.",
    },
    "builtin.video_dataset_embedding_scorer": {
        "name": "Video Embedding Scorer",
        "description": "Generate deterministic embedding norm and cluster assignment.",
    },
    "builtin.video_dataset_caption_generator": {
        "name": "Video Caption Generator",
        "description": "Generate template captions from hash-selected scene/action/setting.",
    },
    "builtin.video_dataset_caption_embedding": {
        "name": "Video Caption Embedding",
        "description": "Generate deterministic caption embedding norm and dimensions.",
    },
    "builtin.video_dataset_clip_writer": {
        "name": "Video Clip Writer",
        "description": "Write final video clip Lance output with format metadata.",
    },
}


def list_templates() -> list[dict[str, Any]]:
    return [
        {
            "id": template_id,
            "name": _TEMPLATE_METADATA.get(template_id, {}).get(
                "name", template_id.split(".")[-1].replace("_", " ").title()
            ),
            "description": _TEMPLATE_METADATA.get(template_id, {}).get("description", "Built-in stage template"),
        }
        for template_id in sorted(_TEMPLATE_REGISTRY.keys())
    ]


def get_template_class(template_id: str) -> type[Any]:
    if template_id not in _TEMPLATE_REGISTRY:
        raise ValueError(f"Unknown stage template: {template_id}")
    return _TEMPLATE_REGISTRY[template_id]


def _load_dynamic_stage(python_import_path: str, params: dict[str, Any]) -> Any:
    if ":" in python_import_path:
        module_path, class_name = python_import_path.split(":", maxsplit=1)
    elif "." in python_import_path:
        parts = python_import_path.rsplit(".", maxsplit=1)
        module_path, class_name = parts[0], parts[1]
    else:
        raise ValueError("python_import_path must be in module:ClassName format")

    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(**params)


def instantiate_stage(stage: StageDefinition) -> Any:
    params = stage.params or {}

    if stage.stage_template:
        if stage.stage_template not in _TEMPLATE_REGISTRY:
            raise ValueError(f"Unknown stage template: {stage.stage_template}")
        return _TEMPLATE_REGISTRY[stage.stage_template](**params)

    assert stage.python_import_path is not None
    return _load_dynamic_stage(stage.python_import_path, params)


def build_stage_executor(stage: StageDefinition) -> StageExecutor:
    instance = instantiate_stage(stage)

    process_data = getattr(instance, "process_data", None)
    if not callable(process_data):
        raise ValueError(f"Stage {stage.name} does not expose process_data(list) method")

    def _runner(data: list[Any]) -> list[Any]:
        result = process_data(data)
        if result is None:
            return []
        if not isinstance(result, list):
            return [result]
        return result

    return StageExecutor(stage_id=stage.stage_id, name=stage.name, run=_runner)
