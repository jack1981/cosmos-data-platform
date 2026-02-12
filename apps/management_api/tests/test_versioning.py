from __future__ import annotations


def _login(client, email: str, password: str) -> dict[str, str]:
    resp = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert resp.status_code == 200
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def _spec(name: str, second_template: str) -> dict:
    return {
        "name": name,
        "description": "",
        "execution_mode": "streaming",
        "tags": ["vtest"],
        "owners": [],
        "team_ids": [],
        "stages": [
            {"stage_id": "s1", "name": "Source", "stage_template": "builtin.identity"},
            {"stage_id": "s2", "name": "Transform", "stage_template": second_template},
        ],
        "edges": [{"source": "s1", "target": "s2"}],
        "io": {"source": {"kind": "inline", "static_data": ["a", "b"]}, "sink": {"kind": "none"}},
        "runtime": {},
        "observability": {"log_level": "INFO", "metrics_enabled": True, "tracing_enabled": False},
        "metadata_links": {"datasets": ["dataset://demo"], "models": ["model://demo"]},
    }


def test_version_diff_and_publish_flow(client) -> None:
    dev_headers = _login(client, "dev@xenna.local", "Dev123!")
    admin_headers = _login(client, "admin@xenna.local", "Admin123!")

    pipeline_resp = client.post(
        "/api/v1/pipelines",
        headers=dev_headers,
        json={
            "external_id": "version-demo",
            "name": "Version Demo",
            "description": "",
            "tags": ["vtest"],
            "execution_mode": "streaming",
        },
    )
    assert pipeline_resp.status_code == 201
    pipeline_id = pipeline_resp.json()["id"]

    v1_resp = client.post(
        f"/api/v1/pipelines/{pipeline_id}/versions",
        headers=dev_headers,
        json={"spec": _spec("Version Demo", "builtin.identity"), "change_summary": "init"},
    )
    assert v1_resp.status_code == 201
    v1_id = v1_resp.json()["id"]

    v2_resp = client.post(
        f"/api/v1/pipelines/{pipeline_id}/versions",
        headers=dev_headers,
        json={"spec": _spec("Version Demo", "builtin.uppercase"), "change_summary": "swap transform"},
    )
    assert v2_resp.status_code == 201
    v2_id = v2_resp.json()["id"]

    diff_resp = client.get(
        f"/api/v1/pipelines/{pipeline_id}/diff",
        headers=dev_headers,
        params={"from_version_id": v1_id, "to_version_id": v2_id},
    )
    assert diff_resp.status_code == 200
    assert diff_resp.json()["diff"]["changed_fields"]

    submit_resp = client.post(
        f"/api/v1/pipelines/{pipeline_id}/versions/{v2_id}/submit-review",
        headers=dev_headers,
        json={"comments": "ready"},
    )
    assert submit_resp.status_code == 200

    approve_resp = client.post(
        f"/api/v1/pipelines/{pipeline_id}/versions/{v2_id}/approve",
        headers=admin_headers,
        json={"comments": "looks good"},
    )
    assert approve_resp.status_code == 200

    publish_resp = client.post(
        f"/api/v1/pipelines/{pipeline_id}/versions/{v2_id}/publish",
        headers=admin_headers,
        json={"comments": "publish"},
    )
    assert publish_resp.status_code == 200
    assert publish_resp.json()["status"] == "PUBLISHED"
    assert publish_resp.json()["is_active"] is True
