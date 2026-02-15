from __future__ import annotations


def _login(client, email: str, password: str) -> dict[str, str]:
    resp = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert resp.status_code == 200
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def test_aiops_cannot_edit_pipeline_definition(client) -> None:
    dev_headers = _login(client, "dev@pipelineforge.local", "Dev123!")
    aiops_headers = _login(client, "aiops@pipelineforge.local", "Aiops123!")

    create_resp = client.post(
        "/api/v1/pipelines",
        headers=dev_headers,
        json={
            "external_id": "rbac-demo",
            "name": "RBAC Demo",
            "description": "",
            "tags": ["rbac"],
            "execution_mode": "streaming",
        },
    )
    assert create_resp.status_code == 201
    pipeline_id = create_resp.json()["id"]

    read_resp = client.get(f"/api/v1/pipelines/{pipeline_id}", headers=aiops_headers)
    assert read_resp.status_code == 200

    patch_resp = client.patch(
        f"/api/v1/pipelines/{pipeline_id}",
        headers=aiops_headers,
        json={"name": "Should Fail"},
    )
    assert patch_resp.status_code == 403
