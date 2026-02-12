from __future__ import annotations

import time


def _login(client, email: str, password: str) -> dict[str, str]:
    resp = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert resp.status_code == 200
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def test_end_to_end_pipeline_run_flow(client) -> None:
    dev_headers = _login(client, "dev@xenna.local", "Dev123!")
    admin_headers = _login(client, "admin@xenna.local", "Admin123!")
    aiops_headers = _login(client, "aiops@xenna.local", "Aiops123!")

    pipeline_resp = client.post(
        "/api/v1/pipelines",
        headers=dev_headers,
        json={
            "external_id": "e2e-demo",
            "name": "E2E Demo",
            "description": "",
            "tags": ["e2e"],
            "execution_mode": "batch",
        },
    )
    assert pipeline_resp.status_code == 201
    pipeline_id = pipeline_resp.json()["id"]

    spec = {
        "name": "E2E Demo",
        "description": "",
        "execution_mode": "batch",
        "tags": ["e2e"],
        "owners": [],
        "team_ids": [],
        "stages": [
            {"stage_id": "s1", "name": "Sleep", "stage_template": "builtin.sleep", "params": {"seconds": 0.01}},
            {"stage_id": "s2", "name": "Upper", "stage_template": "builtin.uppercase"},
        ],
        "edges": [{"source": "s1", "target": "s2"}],
        "io": {"source": {"kind": "inline", "static_data": ["alpha", "beta"]}, "sink": {"kind": "none"}},
        "runtime": {},
        "observability": {"log_level": "INFO", "metrics_enabled": True, "tracing_enabled": False},
        "metadata_links": {"datasets": ["dataset://alpha"], "models": []},
    }

    version_resp = client.post(
        f"/api/v1/pipelines/{pipeline_id}/versions",
        headers=dev_headers,
        json={"spec": spec, "change_summary": "initial"},
    )
    assert version_resp.status_code == 201
    version_id = version_resp.json()["id"]

    submit_resp = client.post(
        f"/api/v1/pipelines/{pipeline_id}/versions/{version_id}/submit-review",
        headers=dev_headers,
        json={"comments": "please review"},
    )
    assert submit_resp.status_code == 200

    approve_resp = client.post(
        f"/api/v1/pipelines/{pipeline_id}/versions/{version_id}/approve",
        headers=admin_headers,
        json={"comments": "approved"},
    )
    assert approve_resp.status_code == 200

    publish_resp = client.post(
        f"/api/v1/pipelines/{pipeline_id}/versions/{version_id}/publish",
        headers=admin_headers,
        json={"comments": "publish"},
    )
    assert publish_resp.status_code == 200

    run_resp = client.post(
        "/api/v1/runs/trigger",
        headers=dev_headers,
        json={"pipeline_id": pipeline_id, "pipeline_version_id": version_id, "trigger_type": "manual"},
    )
    assert run_resp.status_code == 201
    run_id = run_resp.json()["id"]

    terminal = {"SUCCEEDED", "FAILED", "STOPPED"}
    status_value = "QUEUED"
    for _ in range(50):
        current = client.get(f"/api/v1/runs/{run_id}", headers=dev_headers)
        assert current.status_code == 200
        status_value = current.json()["status"]
        if status_value in terminal:
            break
        time.sleep(0.05)

    assert status_value == "SUCCEEDED"

    events_resp = client.get(f"/api/v1/runs/{run_id}/events", headers=aiops_headers)
    assert events_resp.status_code == 200
    assert events_resp.json()

    metrics_resp = client.get(f"/api/v1/runs/{run_id}/metrics-summary", headers=aiops_headers)
    assert metrics_resp.status_code == 200
    assert "metrics" in metrics_resp.json()

    rerun_resp = client.post(f"/api/v1/runs/{run_id}/rerun", headers=aiops_headers)
    assert rerun_resp.status_code == 201
