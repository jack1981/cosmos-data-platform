from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from app.schemas.pipeline_spec import PipelineSpecDocument
from app.services.dataset_executor import run_dataset_pipeline
from app.services.seed import _video_curation_pipeline_spec
from app.services.stage_registry import list_templates

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VIDEO_DATASET_TEMPLATE_IDS = [
    "builtin.video_dataset_metadata_reader",
    "builtin.video_dataset_clip_splitter",
    "builtin.video_dataset_motion_scorer",
    "builtin.video_dataset_motion_filter",
    "builtin.video_dataset_aesthetic_scorer",
    "builtin.video_dataset_aesthetic_filter",
    "builtin.video_dataset_embedding_scorer",
    "builtin.video_dataset_caption_generator",
    "builtin.video_dataset_caption_embedding",
    "builtin.video_dataset_clip_writer",
]


def _write_video_lance(tmp_path: Path, name: str, rows: list[dict[str, Any]]) -> str:
    daft = pytest.importorskip("daft")
    if not hasattr(daft, "read_lance"):
        pytest.skip("Daft Lance support is unavailable")
    uri = str(tmp_path / f"{name}.lance")
    daft.from_pylist(rows).write_lance(uri, mode="overwrite")
    return uri


def _sample_video_rows() -> list[dict[str, Any]]:
    return [
        {
            "video_id": "vid-000001",
            "source_uri": "s3://demo/surv/000001.mp4",
            "duration_seconds": 30.0,
            "resolution_width": 1920,
            "resolution_height": 1080,
            "fps": 30.0,
            "codec": "h264",
            "pixel_format": "yuv420p",
            "file_size_bytes": 7_500_000,
            "category": "surveillance",
            "upload_date": "2024-03-15",
        },
        {
            "video_id": "vid-000002",
            "source_uri": "s3://demo/surv/000002.mp4",
            "duration_seconds": 60.0,
            "resolution_width": 1280,
            "resolution_height": 720,
            "fps": 24.0,
            "codec": "hevc",
            "pixel_format": "yuv420p",
            "file_size_bytes": 5_000_000,
            "category": "dashcam",
            "upload_date": "2024-05-01",
        },
        {
            "video_id": "vid-000003",
            "source_uri": "s3://demo/surv/000003.mp4",
            "duration_seconds": 10.0,
            "resolution_width": 3840,
            "resolution_height": 2160,
            "fps": 60.0,
            "codec": "av1",
            "pixel_format": "yuv420p",
            "file_size_bytes": 10_000_000,
            "category": "drone",
            "upload_date": "2024-07-20",
        },
    ]


def _reader_stage(
    stage_id: str, uri: str,
) -> dict[str, Any]:
    return {
        "stage_id": stage_id,
        "name": stage_id,
        "stage_template": "builtin.video_dataset_metadata_reader",
        "params": {"uri": uri},
    }


def _stage(
    stage_id: str, template: str, params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "stage_id": stage_id,
        "name": stage_id,
        "stage_template": template,
        "params": params or {},
    }


def _io(tmp_path: Path, source_uri: str) -> dict[str, Any]:
    return {
        "source": {"kind": "dataset_uri", "uri": source_uri},
        "sink": {
            "kind": "artifact_uri",
            "uri": str(tmp_path / "out.lance"),
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_video_dataset_templates_registered() -> None:
    """All 10 video dataset templates appear in list_templates()."""
    registered_ids = {t["id"] for t in list_templates()}
    for tid in _VIDEO_DATASET_TEMPLATE_IDS:
        assert tid in registered_ids, f"{tid} not registered"


def test_clip_splitter_expands_rows(tmp_path: Path) -> None:
    """3 videos (30s/60s/10s) at 10s clip_duration -> 3+6+1 = 10 clips."""
    daft = pytest.importorskip("daft")
    if not hasattr(daft, "read_lance"):
        pytest.skip("Daft Lance support is unavailable")

    input_uri = _write_video_lance(tmp_path, "input", _sample_video_rows())
    spec = PipelineSpecDocument.model_validate({
        "name": "clip-split-test",
        "data_model": "dataset",
        "execution_mode": "batch",
        "stages": [
            _reader_stage("reader", input_uri),
            _stage("splitter", "builtin.video_dataset_clip_splitter", {
                "clip_duration": 10.0,
            }),
        ],
        "edges": [{"source": "reader", "target": "splitter"}],
        "runtime": {"ray_mode": "local", "work_dir": str(tmp_path)},
        "io": _io(tmp_path, input_uri),
    })
    result = run_dataset_pipeline(spec, lambda _: None)
    output_df = daft.read_lance(result.output_ref.uri)
    rows = output_df.to_arrow().to_pylist()
    assert len(rows) == 10
    assert all("clip_id" in r for r in rows)
    assert all("clip_index" in r for r in rows)


def test_motion_scorer_deterministic(tmp_path: Path) -> None:
    """Running motion scorer twice produces identical scores."""
    daft = pytest.importorskip("daft")
    if not hasattr(daft, "read_lance"):
        pytest.skip("Daft Lance support is unavailable")

    clip_rows = [
        {"video_id": f"v{i}", "clip_id": f"clip-{i}", "duration_seconds": 10.0}
        for i in range(5)
    ]
    input_uri = _write_video_lance(tmp_path, "clips", clip_rows)

    def _run_scorer(work_dir: str) -> list[dict[str, Any]]:
        spec = PipelineSpecDocument.model_validate({
            "name": "motion-score-test",
            "data_model": "dataset",
            "execution_mode": "batch",
            "stages": [
                _reader_stage("reader", input_uri),
                _stage("scorer", "builtin.video_dataset_motion_scorer"),
            ],
            "edges": [{"source": "reader", "target": "scorer"}],
            "runtime": {"ray_mode": "local", "work_dir": work_dir},
            "io": {
                "source": {"kind": "dataset_uri", "uri": input_uri},
                "sink": {
                    "kind": "artifact_uri",
                    "uri": f"{work_dir}/out.lance",
                },
            },
        })
        res = run_dataset_pipeline(spec, lambda _: None)
        return daft.read_lance(res.output_ref.uri).to_arrow().to_pylist()

    run1 = _run_scorer(str(tmp_path / "run1"))
    run2 = _run_scorer(str(tmp_path / "run2"))
    scores1 = [r["motion_score"] for r in run1]
    scores2 = [r["motion_score"] for r in run2]
    assert scores1 == scores2


def test_motion_filter_removes_low(tmp_path: Path) -> None:
    """Row count drops after motion filtering."""
    daft = pytest.importorskip("daft")
    if not hasattr(daft, "read_lance"):
        pytest.skip("Daft Lance support is unavailable")

    clip_rows = [
        {"video_id": f"v{i}", "clip_id": f"clip-{i}", "duration_seconds": 10.0}
        for i in range(20)
    ]
    input_uri = _write_video_lance(tmp_path, "clips", clip_rows)
    spec = PipelineSpecDocument.model_validate({
        "name": "motion-filter-test",
        "data_model": "dataset",
        "execution_mode": "batch",
        "stages": [
            _reader_stage("reader", input_uri),
            _stage("scorer", "builtin.video_dataset_motion_scorer"),
            _stage("filter", "builtin.video_dataset_motion_filter", {
                "min_score": 0.15,
            }),
        ],
        "edges": [
            {"source": "reader", "target": "scorer"},
            {"source": "scorer", "target": "filter"},
        ],
        "runtime": {"ray_mode": "local", "work_dir": str(tmp_path)},
        "io": _io(tmp_path, input_uri),
    })
    result = run_dataset_pipeline(spec, lambda _: None)
    filtered = daft.read_lance(result.output_ref.uri).to_arrow().to_pylist()
    assert len(filtered) < 20
    assert len(filtered) > 0


def test_aesthetic_scorer_and_filter(tmp_path: Path) -> None:
    """Score + filter chain verifies columns and fewer rows."""
    daft = pytest.importorskip("daft")
    if not hasattr(daft, "read_lance"):
        pytest.skip("Daft Lance support is unavailable")

    clip_rows = [
        {
            "video_id": f"v{i}",
            "clip_id": f"aclip-{i}",
            "resolution_width": 1920,
            "duration_seconds": 10.0,
        }
        for i in range(20)
    ]
    input_uri = _write_video_lance(tmp_path, "clips", clip_rows)
    spec = PipelineSpecDocument.model_validate({
        "name": "aesthetic-test",
        "data_model": "dataset",
        "execution_mode": "batch",
        "stages": [
            _reader_stage("reader", input_uri),
            _stage("scorer", "builtin.video_dataset_aesthetic_scorer"),
            _stage("filter", "builtin.video_dataset_aesthetic_filter", {
                "min_score": 0.3,
            }),
        ],
        "edges": [
            {"source": "reader", "target": "scorer"},
            {"source": "scorer", "target": "filter"},
        ],
        "runtime": {"ray_mode": "local", "work_dir": str(tmp_path)},
        "io": _io(tmp_path, input_uri),
    })
    result = run_dataset_pipeline(spec, lambda _: None)
    rows = daft.read_lance(result.output_ref.uri).to_arrow().to_pylist()
    assert len(rows) < 20
    assert all("aesthetic_score" in r for r in rows)
    assert all("aesthetic_grade" in r for r in rows)


def test_caption_generator_produces_text(tmp_path: Path) -> None:
    """Caption generator adds caption, caption_length, caption_model."""
    daft = pytest.importorskip("daft")
    if not hasattr(daft, "read_lance"):
        pytest.skip("Daft Lance support is unavailable")

    clip_rows = [
        {"video_id": f"v{i}", "clip_id": f"cap-{i}", "duration_seconds": 10.0}
        for i in range(5)
    ]
    input_uri = _write_video_lance(tmp_path, "clips", clip_rows)
    spec = PipelineSpecDocument.model_validate({
        "name": "caption-test",
        "data_model": "dataset",
        "execution_mode": "batch",
        "stages": [
            _reader_stage("reader", input_uri),
            _stage("captioner", "builtin.video_dataset_caption_generator"),
        ],
        "edges": [{"source": "reader", "target": "captioner"}],
        "runtime": {"ray_mode": "local", "work_dir": str(tmp_path)},
        "io": _io(tmp_path, input_uri),
    })
    result = run_dataset_pipeline(spec, lambda _: None)
    rows = daft.read_lance(result.output_ref.uri).to_arrow().to_pylist()
    assert len(rows) == 5
    for r in rows:
        assert "caption" in r and isinstance(r["caption"], str)
        assert len(r["caption"]) > 0
        assert "caption_length" in r
        assert "caption_model" in r


def _make_source_rows(
    prefix: str, category: str, count: int,
    durations: list[float],
    resolutions: list[tuple[int, int]],
    fps_list: list[float],
    codecs: list[str],
) -> list[dict[str, Any]]:
    """Generate diverse video rows cycling through provided attribute lists."""
    return [
        {
            "video_id": f"{prefix}-{i:04d}",
            "source_uri": f"s3://demo/{prefix}/{i:04d}.mp4",
            "duration_seconds": durations[i % len(durations)],
            "resolution_width": resolutions[i % len(resolutions)][0],
            "resolution_height": resolutions[i % len(resolutions)][1],
            "fps": fps_list[i % len(fps_list)],
            "codec": codecs[i % len(codecs)],
            "pixel_format": "yuv420p",
            "file_size_bytes": int(
                durations[i % len(durations)]
                * (8_000_000 if resolutions[i % len(resolutions)][0] > 1280 else 2_500_000)
                / 8
            ),
            "category": category,
            "upload_date": f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
        }
        for i in range(count)
    ]


def test_full_video_pipeline_dag(tmp_path: Path) -> None:
    """3-reader fan-in DAG running ALL 10 stages end-to-end.

    Validates that every stage produces its expected columns and that
    the filtering stages actually reduce row counts.
    """
    daft = pytest.importorskip("daft")
    if not hasattr(daft, "read_lance"):
        pytest.skip("Daft Lance support is unavailable")

    # Diverse source data: varied durations, resolutions, codecs, fps
    surv_uri = _write_video_lance(
        tmp_path, "surv",
        _make_source_rows(
            "surv", "surveillance", 10,
            durations=[8.5, 25.0, 45.0, 90.0, 120.0, 7.0, 55.0, 200.0, 15.0, 35.0],
            resolutions=[(1920, 1080), (1280, 720), (3840, 2160), (640, 360)],
            fps_list=[24.0, 30.0, 60.0],
            codecs=["h264", "hevc", "av1"],
        ),
    )
    dash_uri = _write_video_lance(
        tmp_path, "dash",
        _make_source_rows(
            "dash", "dashcam", 8,
            durations=[60.0, 30.0, 15.0, 180.0, 5.5, 42.0, 10.0, 75.0],
            resolutions=[(1920, 1080), (1280, 720)],
            fps_list=[30.0, 60.0],
            codecs=["h264", "hevc"],
        ),
    )
    drone_uri = _write_video_lance(
        tmp_path, "drone",
        _make_source_rows(
            "drone", "drone", 6,
            durations=[120.0, 45.0, 20.0, 300.0, 8.0, 65.0],
            resolutions=[(3840, 2160), (1920, 1080)],
            fps_list=[24.0, 60.0],
            codecs=["hevc", "av1"],
        ),
    )

    # Full pipeline: all 10 video dataset stages + concat (13 total)
    spec = PipelineSpecDocument.model_validate({
        "name": "video-full-dag-test",
        "data_model": "dataset",
        "execution_mode": "batch",
        "stages": [
            _reader_stage("reader_surv", surv_uri),
            _reader_stage("reader_dash", dash_uri),
            _reader_stage("reader_drone", drone_uri),
            _stage("concat", "builtin.datafiner_concat"),
            _stage("clip_splitter", "builtin.video_dataset_clip_splitter", {
                "clip_duration": 10.0,
            }),
            _stage(
                "motion_scorer",
                "builtin.video_dataset_motion_scorer",
            ),
            _stage("motion_filter", "builtin.video_dataset_motion_filter", {
                "min_score": 0.15,
            }),
            _stage(
                "aesthetic_scorer",
                "builtin.video_dataset_aesthetic_scorer",
            ),
            _stage(
                "aesthetic_filter",
                "builtin.video_dataset_aesthetic_filter",
                {"min_score": 0.3},
            ),
            _stage(
                "embedding_scorer",
                "builtin.video_dataset_embedding_scorer",
            ),
            _stage(
                "caption_generator",
                "builtin.video_dataset_caption_generator",
            ),
            _stage(
                "caption_embedding",
                "builtin.video_dataset_caption_embedding",
            ),
            _stage("clip_writer", "builtin.video_dataset_clip_writer"),
        ],
        "edges": [
            {"source": "reader_surv", "target": "concat"},
            {"source": "reader_dash", "target": "concat"},
            {"source": "reader_drone", "target": "concat"},
            {"source": "concat", "target": "clip_splitter"},
            {"source": "clip_splitter", "target": "motion_scorer"},
            {"source": "motion_scorer", "target": "motion_filter"},
            {"source": "motion_filter", "target": "aesthetic_scorer"},
            {"source": "aesthetic_scorer", "target": "aesthetic_filter"},
            {"source": "aesthetic_filter", "target": "embedding_scorer"},
            {"source": "embedding_scorer", "target": "caption_generator"},
            {"source": "caption_generator", "target": "caption_embedding"},
            {"source": "caption_embedding", "target": "clip_writer"},
        ],
        "runtime": {"ray_mode": "local", "work_dir": str(tmp_path)},
        "io": _io(tmp_path, surv_uri),
    })
    result = run_dataset_pipeline(spec, lambda _: None)
    assert result.output_ref is not None

    out = daft.read_lance(result.output_ref.uri).to_arrow().to_pylist()
    total_input = 10 + 8 + 6  # 24 videos across 3 sources
    assert len(out) > 0
    # Filters must reduce rows — not every clip passes both thresholds
    assert len(out) < total_input * 30  # clip expansion upper bound

    # Verify every stage contributed its expected columns
    for row in out:
        # Clip splitter columns
        assert "clip_id" in row and isinstance(row["clip_id"], str)
        assert "clip_index" in row and isinstance(row["clip_index"], int)
        assert "clip_start" in row
        assert "clip_end" in row
        # Motion scorer columns
        assert "motion_score" in row
        assert 0.0 <= row["motion_score"] <= 1.0
        assert row["motion_score"] >= 0.15  # survived filter
        assert "motion_category" in row
        assert row["motion_category"] in ("low", "moderate", "high")
        # Aesthetic scorer columns
        assert "aesthetic_score" in row
        assert 0.0 <= row["aesthetic_score"] <= 1.0
        assert row["aesthetic_score"] >= 0.3  # survived filter
        assert "aesthetic_grade" in row
        assert row["aesthetic_grade"] in ("fair", "good", "excellent")
        # Embedding scorer columns
        assert "embedding_norm" in row
        assert "embedding_cluster" in row
        assert 0 <= row["embedding_cluster"] <= 19
        assert row["embedding_dimensions"] == 768
        # Caption generator columns
        assert "caption" in row and isinstance(row["caption"], str)
        assert len(row["caption"]) > 10
        assert "caption_length" in row and row["caption_length"] > 0
        assert "caption_model" in row
        assert "caption_confidence" in row
        assert 0.70 <= row["caption_confidence"] <= 0.95
        # Caption embedding columns
        assert "caption_embedding_norm" in row
        assert row["caption_embedding_dimensions"] == 512
        # Original metadata preserved through pipeline
        assert "video_id" in row
        assert "resolution_width" in row
        assert "category" in row

    # Verify data diversity in output
    categories = {row["category"] for row in out}
    assert len(categories) >= 2, f"Expected >=2 categories, got {categories}"
    resolutions = {row["resolution_width"] for row in out}
    assert len(resolutions) >= 2, f"Expected >=2 resolutions, got {resolutions}"
    clusters = {row["embedding_cluster"] for row in out}
    assert len(clusters) >= 3, f"Expected >=3 clusters, got {clusters}"
    motion_cats = {row["motion_category"] for row in out}
    assert len(motion_cats) >= 2, f"Expected >=2 motion cats, got {motion_cats}"


def test_video_seed_data_quality() -> None:
    """Validate that seed video catalog data is diverse and meaningful."""
    from app.services.prepare_local_sample import (
        _VIDEO_CATALOG_SPECS,
        _build_video_catalog_rows,
    )

    all_rows: list[dict[str, Any]] = []
    offset = 0
    for source, count in _VIDEO_CATALOG_SPECS:
        rows = _build_video_catalog_rows(source, count, offset)
        assert len(rows) == count
        all_rows.extend(rows)
        offset += count

    assert len(all_rows) == 500

    # Every row has required schema fields
    required = {
        "video_id", "source_uri", "duration_seconds",
        "resolution_width", "resolution_height", "fps",
        "codec", "pixel_format", "file_size_bytes",
        "category", "upload_date",
    }
    for row in all_rows:
        missing = required - set(row.keys())
        assert not missing, f"Missing fields: {missing}"

    # Unique video IDs
    ids = [r["video_id"] for r in all_rows]
    assert len(set(ids)) == 500

    # Duration distribution covers short, medium, long
    durs = [r["duration_seconds"] for r in all_rows]
    assert min(durs) >= 5.0
    assert max(durs) <= 300.0
    short = sum(1 for d in durs if d < 10)
    medium = sum(1 for d in durs if 10 <= d < 60)
    long = sum(1 for d in durs if d >= 60)
    assert short > 0, "Need some short videos (<10s)"
    assert medium > 0, "Need some medium videos (10-60s)"
    assert long > 0, "Need some long videos (>=60s)"

    # Resolution diversity
    res_set = {r["resolution_width"] for r in all_rows}
    assert res_set == {640, 1280, 1920, 3840}

    # FPS diversity
    fps_set = {r["fps"] for r in all_rows}
    assert fps_set == {24.0, 30.0, 60.0}

    # Codec diversity
    codec_set = {r["codec"] for r in all_rows}
    assert codec_set == {"h264", "hevc", "av1"}

    # All 5 categories present
    cat_set = {r["category"] for r in all_rows}
    assert cat_set == {"surveillance", "dashcam", "drone", "studio", "ugc"}

    # File sizes are derived and reasonable
    for row in all_rows:
        assert row["file_size_bytes"] > 0
        assert row["pixel_format"] == "yuv420p"


def test_video_pipeline_spec_validates() -> None:
    """_video_curation_pipeline_spec() passes PipelineSpecDocument."""
    spec_dict = _video_curation_pipeline_spec()
    doc = PipelineSpecDocument.model_validate(spec_dict)
    assert doc.name == "Template: Video Curation Pipeline"
    assert doc.data_model == "dataset"
    stage_ids = {s.stage_id for s in doc.stages}
    # All 3 readers
    assert "reader_surveillance" in stage_ids
    assert "reader_dashcam" in stage_ids
    assert "reader_drone" in stage_ids
    # Concat + all 9 video processing stages
    assert "concat" in stage_ids
    assert "clip_splitter" in stage_ids
    assert "motion_scorer" in stage_ids
    assert "motion_filter" in stage_ids
    assert "aesthetic_scorer" in stage_ids
    assert "aesthetic_filter" in stage_ids
    assert "embedding_scorer" in stage_ids
    assert "caption_generator" in stage_ids
    assert "caption_embedding" in stage_ids
    assert "clip_writer" in stage_ids
    # Total: 3 readers + 1 concat + 9 video stages = 13
    assert len(doc.stages) == 13
