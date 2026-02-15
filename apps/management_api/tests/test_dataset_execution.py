from __future__ import annotations

import sys
import time
import types
from pathlib import Path

import pytest
from app.schemas.pipeline_spec import PipelineSpecDocument
from app.services.dataset_executor import _maybe_init_ray_and_daft, run_dataset_pipeline


def test_dataset_executor_topological_dag() -> None:
    spec = PipelineSpecDocument.model_validate(
        {
            "name": "dataset-topo",
            "data_model": "dataset",
            "execution_mode": "batch",
            "stages": [
                {
                    "stage_id": "root_a",
                    "name": "Root A",
                    "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                    "params": {"uri": "lance://a"},
                },
                {
                    "stage_id": "root_b",
                    "name": "Root B",
                    "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                    "params": {"uri": "lance://b"},
                },
                {
                    "stage_id": "join",
                    "name": "Join",
                    "python_import_path": "app.services.dataset_stage_fixtures:JoinDatasetStage",
                },
            ],
            "edges": [
                {"source": "root_a", "target": "join"},
                {"source": "root_b", "target": "join"},
            ],
            "io": {
                "source": {"kind": "dataset_uri", "uri": "lance://input"},
                "sink": {"kind": "artifact_uri", "uri": "lance://output"},
            },
        }
    )

    result = run_dataset_pipeline(spec, lambda _: None)

    assert result.execution_order == ["root_a", "root_b", "join"]
    assert [item["stage_id"] for item in result.stage_metrics] == ["root_a", "root_b", "join"]
    assert result.output_ref.uri == "lance://joined/lance://a+lance://b"
    assert result.output_ref.metadata["inputs"] == ["root_a", "root_b"]


def test_dataset_executor_supports_stage_template_dataset_stages(tmp_path: Path) -> None:
    daft = pytest.importorskip("daft")
    if not hasattr(daft, "read_lance"):
        pytest.skip("Daft Lance support is unavailable: read_lance missing")

    input_uri = str(tmp_path / "input.lance")
    output_uri = str(tmp_path / "output.lance")
    daft.from_pydict({"id": [1, 2, 3], "score": [0.1, 0.8, 0.5]}).write_lance(input_uri, mode="overwrite")

    spec = PipelineSpecDocument.model_validate(
        {
            "name": "dataset-template-path",
            "data_model": "dataset",
            "execution_mode": "batch",
            "stages": [
                {
                    "stage_id": "reader",
                    "name": "Reader",
                    "stage_template": "builtin.dataset_lance_reader",
                    "params": {"uri": input_uri},
                },
                {
                    "stage_id": "filter",
                    "name": "Filter",
                    "stage_template": "builtin.dataset_filter",
                    "params": {"predicate": "score >= 0.5"},
                },
                {
                    "stage_id": "writer",
                    "name": "Writer",
                    "stage_template": "builtin.dataset_lance_writer",
                    "params": {"output_uri": output_uri},
                },
            ],
            "edges": [
                {"source": "reader", "target": "filter"},
                {"source": "filter", "target": "writer"},
            ],
            "runtime": {"ray_mode": "local", "work_dir": str(tmp_path)},
            "io": {
                "source": {"kind": "dataset_uri", "uri": input_uri},
                "sink": {"kind": "artifact_uri", "uri": output_uri},
            },
        }
    )

    result = run_dataset_pipeline(spec, lambda _: None)
    assert result.output_ref.uri == output_uri

    rows = daft.read_lance(output_uri).sort(["id"]).to_arrow().to_pylist()
    assert rows == [{"id": 2, "score": 0.8}, {"id": 3, "score": 0.5}]


def test_dataset_runtime_falls_back_to_native_runner_when_ray_runner_setup_fails(monkeypatch) -> None:
    logs: list[str] = []
    calls: list[tuple[str, object]] = []

    class FakeRay:
        @staticmethod
        def is_initialized() -> bool:
            return True

    def _set_runner_ray(address: str | None = None) -> None:
        calls.append(("ray", address))
        raise AttributeError("'InProgressSentinel' object has no attribute 'id'")

    def _set_runner_native() -> None:
        calls.append(("native", None))

    fake_daft = types.SimpleNamespace(
        set_runner_ray=_set_runner_ray,
        set_runner_native=_set_runner_native,
    )

    monkeypatch.setitem(sys.modules, "ray", FakeRay)
    monkeypatch.setitem(sys.modules, "daft", fake_daft)

    ctx = types.SimpleNamespace(ray_mode="local", ray_address="auto")
    _maybe_init_ray_and_daft(ctx, logs.append)

    assert ("ray", "auto") in calls
    assert ("native", None) in calls
    assert any("fell back to Daft native runner after Ray-runner failure" in line for line in logs)


def _login(client, email: str, password: str) -> dict[str, str]:
    resp = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert resp.status_code == 200
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def test_dataset_pipeline_run_flow(client) -> None:
    dev_headers = _login(client, "dev@pipelineforge.local", "Dev123!")

    pipeline_resp = client.post(
        "/api/v1/pipelines",
        headers=dev_headers,
        json={
            "external_id": "dataset-e2e",
            "name": "Dataset E2E",
            "description": "",
            "tags": ["dataset"],
            "execution_mode": "batch",
        },
    )
    assert pipeline_resp.status_code == 201
    pipeline_id = pipeline_resp.json()["id"]

    spec = {
        "name": "Dataset E2E",
        "description": "",
        "data_model": "dataset",
        "execution_mode": "batch",
        "tags": ["dataset"],
        "owners": [],
        "team_ids": [],
        "stages": [
            {
                "stage_id": "s1",
                "name": "Left",
                "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                "params": {"uri": "lance://left"},
            },
            {
                "stage_id": "s2",
                "name": "Right",
                "python_import_path": "app.services.dataset_stage_fixtures:EmitDatasetStage",
                "params": {"uri": "lance://right"},
            },
            {
                "stage_id": "s3",
                "name": "Join",
                "python_import_path": "app.services.dataset_stage_fixtures:JoinDatasetStage",
            },
        ],
        "edges": [{"source": "s1", "target": "s3"}, {"source": "s2", "target": "s3"}],
        "io": {
            "source": {"kind": "dataset_uri", "uri": "lance://source"},
            "sink": {"kind": "artifact_uri", "uri": "lance://sink"},
        },
        "runtime": {"ray_mode": "local"},
        "observability": {"log_level": "INFO", "metrics_enabled": True, "tracing_enabled": False},
        "metadata_links": {"datasets": ["dataset://dataset-e2e"], "models": []},
    }

    version_resp = client.post(
        f"/api/v1/pipelines/{pipeline_id}/versions",
        headers=dev_headers,
        json={"spec": spec, "change_summary": "dataset path"},
    )
    assert version_resp.status_code == 201
    version_id = version_resp.json()["id"]

    run_resp = client.post(
        "/api/v1/runs/trigger",
        headers=dev_headers,
        json={"pipeline_id": pipeline_id, "pipeline_version_id": version_id, "trigger_type": "manual"},
    )
    assert run_resp.status_code == 201
    run_id = run_resp.json()["id"]

    terminal = {"SUCCEEDED", "FAILED", "STOPPED"}
    status_value = "QUEUED"
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        current = client.get(f"/api/v1/runs/{run_id}", headers=dev_headers)
        assert current.status_code == 200
        payload = current.json()
        status_value = payload["status"]
        if status_value in terminal:
            break
        time.sleep(0.05)

    assert status_value == "SUCCEEDED"

    run_payload = client.get(f"/api/v1/runs/{run_id}", headers=dev_headers).json()
    assert run_payload["metrics_summary"]["data_model"] == "dataset"
    assert run_payload["metrics_summary"]["output_dataset_uri"] == "lance://joined/lance://left+lance://right"
    assert run_payload["artifact_pointers"]["output_uri"] == "lance://joined/lance://left+lance://right"
