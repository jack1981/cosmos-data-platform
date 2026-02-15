from __future__ import annotations

import json

from app.schemas.pipeline_spec import StageDefinition
from app.services.stage_registry import build_stage_executor, list_templates


def test_video_templates_are_registered() -> None:
    ids = {item["id"] for item in list_templates()}
    assert "builtin.video_download" in ids
    assert "builtin.video_caption" in ids
    assert "builtin.video_quality_gate" in ids
    assert "builtin.video_incident_enrich" in ids
    assert "builtin.video_writer" in ids


def test_dataset_templates_are_registered() -> None:
    ids = {item["id"] for item in list_templates()}
    assert "builtin.dataset_lance_reader" in ids
    assert "builtin.dataset_filter" in ids
    assert "builtin.dataset_column_select" in ids
    assert "builtin.dataset_shuffle" in ids
    assert "builtin.dataset_union_by_name" in ids
    assert "builtin.dataset_join" in ids
    assert "builtin.dataset_lance_writer" in ids


def test_datafiner_compatibility_templates_are_registered() -> None:
    ids = {item["id"] for item in list_templates()}
    expected = {
        "builtin.datafiner_lance_reader",
        "builtin.datafiner_lance_writer",
        "builtin.datafiner_splitter",
        "builtin.datafiner_visualizer",
        "builtin.datafiner_schema",
        "builtin.datafiner_row_number",
        "builtin.datafiner_stat",
        "builtin.datafiner_column_select",
        "builtin.datafiner_column_drop",
        "builtin.datafiner_column_alias",
        "builtin.datafiner_filter",
        "builtin.datafiner_filter_by_ratio",
        "builtin.datafiner_selector",
        "builtin.datafiner_reorder",
        "builtin.datafiner_interleaved_reorder",
        "builtin.datafiner_union_by_name",
        "builtin.datafiner_union_by_position",
        "builtin.datafiner_joiner",
        "builtin.datafiner_add_constants",
        "builtin.datafiner_conversation_to_paragraph",
        "builtin.datafiner_concatenate_columns",
        "builtin.datafiner_concat",
        "builtin.datafiner_duplicate_sample_ratio",
        "builtin.datafiner_sampler",
        "builtin.datafiner_flatten",
        "builtin.datafiner_group_flatten",
        "builtin.datafiner_fasttext_scorer",
        "builtin.datafiner_fasttext_filter",
        "builtin.datafiner_seq_classifier_scorer",
        "builtin.datafiner_minhash",
        "builtin.datafiner_add_rank_quantile",
        "builtin.datafiner_token_counter_v2",
    }
    assert expected.issubset(ids)


def test_video_template_chain_runs_successfully(tmp_path) -> None:
    output_path = tmp_path / "video_pipeline_output.jsonl"
    stages = [
        StageDefinition(
            stage_id="download",
            name="Download",
            stage_template="builtin.video_download",
            params={"allow_http": False, "timeout_seconds": 0.1},
        ),
        StageDefinition(
            stage_id="caption",
            name="Caption",
            stage_template="builtin.video_caption",
            params={"model_name": "demo-vlm-mini"},
        ),
        StageDefinition(
            stage_id="quality",
            name="Quality",
            stage_template="builtin.video_quality_gate",
            params={"min_confidence": 0.0, "drop_failed": False},
        ),
        StageDefinition(
            stage_id="incident",
            name="Incident",
            stage_template="builtin.video_incident_enrich",
            params={"text_fields": ["caption", "ops_hint"]},
        ),
        StageDefinition(
            stage_id="writer",
            name="Writer",
            stage_template="builtin.video_writer",
            params={"output_path": str(output_path), "drop_output": True},
        ),
    ]

    data = [
        {"video_id": "cam-1", "video_url": "s3://demo/cam-1.mp4", "ops_hint": "smoke near dock"},
        {"video_id": "cam-2", "video_url": "s3://demo/cam-2.mp4", "ops_hint": "normal operations"},
    ]
    for stage in stages:
        data = build_stage_executor(stage).run(data)

    assert data == []
    assert output_path.exists()

    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    first_record = json.loads(lines[0])
    assert first_record["video_id"] == "cam-1"
    assert "caption" in first_record
    assert first_record["incident"]["severity"] in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
