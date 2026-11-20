"""HTTP-level contract tests."""

from __future__ import annotations

from fastapi.testclient import TestClient


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "pool": "10.20.0.0/29"}


def test_acquire_returns_201_then_200_on_reuse(client: TestClient) -> None:
    first = client.post("/v1/leases", json={"client_id": "laptop-1"})
    assert first.status_code == 201
    assert first.headers["location"] == f"/v1/leases/{first.json()['lease_id']}"
    assert first.json()["reused"] is False

    again = client.post("/v1/leases", json={"client_id": "laptop-1"})
    assert again.status_code == 200
    assert again.json()["lease_id"] == first.json()["lease_id"]
    assert again.json()["reused"] is True


def test_acquire_validates_payload(client: TestClient) -> None:
    missing = client.post("/v1/leases", json={})
    assert missing.status_code == 422
    assert missing.json()["error"]["code"] == "invalid_request"
    assert client.post("/v1/leases", json={"client_id": "   "}).status_code == 422
    assert client.post("/v1/leases", json={"client_id": "x", "ttl_seconds": 1}).status_code == 422
    assert (
        client.post("/v1/leases", json={"client_id": "x", "bogus": 1}).status_code == 422
    )


def test_renew_without_body(client: TestClient) -> None:
    lease = client.post("/v1/leases", json={"client_id": "a"}).json()
    response = client.post(f"/v1/leases/{lease['lease_id']}/renew")
    assert response.status_code == 200
    assert response.json()["status"] == "active"


def test_lease_errors_are_structured(client: TestClient) -> None:
    response = client.post("/v1/leases/does-not-exist/renew")
    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "lease_not_found", "message": "no lease with that id"}
    }

    released = client.post("/v1/leases", json={"client_id": "a"}).json()
    client.post(f"/v1/leases/{released['lease_id']}/release")
    response = client.post(f"/v1/leases/{released['lease_id']}/renew")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "lease_released"


def test_pool_switch_and_out_of_pool_report(client: TestClient) -> None:
    lease = client.post("/v1/leases", json={"client_id": "a"}).json()
    switch = client.put("/v1/pool", json={"cidr": "10.21.0.0/24"})
    assert switch.status_code == 200
    assert switch.json()["changed"] is True
    assert switch.json()["previous_cidr"] == "10.20.0.0/29"
    assert switch.json()["occupied_out_of_pool"] == 1

    report = client.get("/v1/admin/out-of-pool")
    assert report.status_code == 200
    body = report.json()
    assert body["count"] == 1
    assert body["leases"][0]["ip"] == lease["ip"]
    assert body["leases"][0]["lease_id"] == lease["lease_id"]
    assert body["leases"][0]["expires_at"] is not None

    renew = client.post(f"/v1/leases/{lease['lease_id']}/renew")
    assert renew.status_code == 409
    assert renew.json()["error"]["code"] == "address_out_of_pool"

    release = client.post(f"/v1/leases/{lease['lease_id']}/release")
    assert release.status_code == 200
    assert client.get("/v1/admin/out-of-pool").json()["count"] == 0


def test_pool_switch_invalid_pool_is_422(client: TestClient) -> None:
    response = client.put("/v1/pool", json={"cidr": "10.0.0.0/99"})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_pool"
    assert client.get("/v1/pool").json()["cidr"] == "10.20.0.0/29"


def test_same_pool_switch_is_noop(client: TestClient) -> None:
    response = client.put("/v1/pool", json={"cidr": "10.20.0.0/29"})
    assert response.status_code == 200
    assert response.json()["changed"] is False


def test_operator_token_enforced(tmp_path) -> None:
    from app.config import Settings
    from app.db import Store
    from app.main import create_app

    settings = Settings(
        db_path=str(tmp_path / "leases.db"),
        default_pool_cidr="10.20.0.0/29",
        operator_token="s3cret",
    )
    with TestClient(create_app(settings, store=Store(settings))) as locked:
        assert locked.get("/v1/admin/out-of-pool").status_code == 401
        assert locked.put("/v1/pool", json={"cidr": "10.0.0.0/24"}).status_code == 401
        assert (
            locked.put(
                "/v1/pool",
                json={"cidr": "10.0.0.0/24"},
                headers={"X-Operator-Token": "wrong"},
            ).status_code
            == 401
        )
        ok = locked.put(
            "/v1/pool",
            json={"cidr": "10.0.0.0/24"},
            headers={"X-Operator-Token": "s3cret"},
        )
        assert ok.status_code == 200
        # Tenant-facing endpoints stay open.
        assert locked.post("/v1/leases", json={"client_id": "a"}).status_code == 201
