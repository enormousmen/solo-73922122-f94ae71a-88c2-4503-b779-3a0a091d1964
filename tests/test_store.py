"""Store-level behaviour: allocation, idempotency, expiry, pool switching."""

from __future__ import annotations

import sqlite3

import pytest

from app.config import Settings
from app.db import LeaseError, Store


def test_acquire_allocates_lowest_free(store: Store) -> None:
    lease, reused = store.acquire("client-a")
    assert reused is False
    assert lease["ip"] == "10.20.0.1"
    second, _ = store.acquire("client-b")
    assert second["ip"] == "10.20.0.2"


def test_acquire_is_idempotent_for_same_client(store: Store) -> None:
    first, reused = store.acquire("client-a")
    second, reused2 = store.acquire("client-a")
    assert reused is False
    assert reused2 is True
    assert first["lease_id"] == second["lease_id"]
    assert first["ip"] == second["ip"]


def test_reallocates_address_after_release(store: Store) -> None:
    first, _ = store.acquire("client-a")
    store.release(first["lease_id"])
    second, _ = store.acquire("client-b")
    assert second["ip"] == first["ip"]  # released address is free again


def test_expired_lease_is_not_reused_and_address_is_returned(
    store: Store, clock
) -> None:
    first, _ = store.acquire("client-a", ttl_seconds=60)
    clock.advance(61)
    fresh, reused = store.acquire("client-a")
    assert reused is False
    assert fresh["lease_id"] != first["lease_id"]
    assert fresh["ip"] == first["ip"]
    assert store.get_lease(first["lease_id"])["status"] == "expired"


def test_pool_exhaustion(store: Store) -> None:
    for i in range(6):  # /29 has 6 usable addresses
        store.acquire(f"client-{i}")
    with pytest.raises(LeaseError) as exc:
        store.acquire("client-overflow")
    assert exc.value.status_code == 409
    assert exc.value.code == "pool_exhausted"


def test_renew_extends_and_validates(store: Store, clock) -> None:
    lease, _ = store.acquire("client-a", ttl_seconds=100)
    original_expiry = lease["expires_at"]
    clock.advance(50)
    renewed = store.renew(lease["lease_id"], ttl_seconds=200)
    assert renewed["expires_at"] > original_expiry
    assert renewed["seconds_remaining"] == 200


def test_renew_rejects_released_and_expired(store: Store, clock) -> None:
    lease, _ = store.acquire("client-a", ttl_seconds=60)
    store.release(lease["lease_id"])
    with pytest.raises(LeaseError) as exc:
        store.renew(lease["lease_id"])
    assert exc.value.code == "lease_released"

    other, _ = store.acquire("client-b", ttl_seconds=60)
    clock.advance(61)
    with pytest.raises(LeaseError) as exc:
        store.renew(other["lease_id"])
    assert exc.value.code == "lease_expired"


def test_renew_unknown_lease(store: Store) -> None:
    with pytest.raises(LeaseError) as exc:
        store.renew("nope")
    assert exc.value.status_code == 404


def test_renew_out_of_pool_address_forbidden(store: Store) -> None:
    lease, _ = store.acquire("client-a")
    store.switch_pool("10.99.0.0/24")
    with pytest.raises(LeaseError) as exc:
        store.renew(lease["lease_id"])
    assert exc.value.code == "address_out_of_pool"


def test_release_out_of_pool_address_allowed(store: Store) -> None:
    lease, _ = store.acquire("client-a")
    store.switch_pool("10.99.0.0/24")
    released, already = store.release(lease["lease_id"])
    assert already is False
    assert released["status"] == "released"
    assert store.out_of_pool_leases() == []


def test_double_release_is_idempotent(store: Store) -> None:
    lease, _ = store.acquire("client-a")
    _, first = store.release(lease["lease_id"])
    _, second = store.release(lease["lease_id"])
    assert first is False
    assert second is True


def test_switch_pool_keeps_out_of_pool_leases_occupied(store: Store) -> None:
    a, _ = store.acquire("client-a")
    b, _ = store.acquire("client-b")
    result = store.switch_pool("10.20.1.0/24")  # adjacent, disjoint pool
    assert result["changed"] is True
    assert result["occupied_out_of_pool"] == 2
    listed = store.out_of_pool_leases()
    assert {item["lease_id"] for item in listed} == {a["lease_id"], b["lease_id"]}
    # New allocations come from the new pool and cannot collide with the old.
    fresh, _ = store.acquire("client-c")
    assert fresh["ip"].startswith("10.20.1.")


def test_switch_pool_out_of_pool_address_not_reissued(store: Store) -> None:
    old, _ = store.acquire("client-a")
    store.switch_pool("10.20.0.0/28")  # superset containing the old address
    # Address now inside the pool again but still held; next client must not
    # receive it until the original lease is released.
    new, _ = store.acquire("client-b")
    assert new["ip"] != old["ip"]


def test_switch_pool_to_same_pool_is_noop(store: Store) -> None:
    store.acquire("client-a")
    result = store.switch_pool("10.20.0.0/29")
    assert result["changed"] is False
    assert result["changed_at"] is None


def test_switch_pool_invalid_keeps_previous(store: Store) -> None:
    with pytest.raises(LeaseError) as exc:
        store.switch_pool("10.0.0.0/33")
    assert exc.value.status_code == 422
    assert store.pool_info()["cidr"] == "10.20.0.0/29"


def test_pool_and_leases_survive_restart(settings: Settings, clock) -> None:
    first = Store(settings, clock=clock)
    first.initialize()
    lease, _ = first.acquire("client-a")
    first.switch_pool("10.30.0.0/24")

    reopened = Store(settings, clock=clock)
    reopened.initialize()
    assert reopened.pool_info()["cidr"] == "10.30.0.0/24"
    lease_after = reopened.get_lease(lease["lease_id"])
    assert lease_after["ip"] == lease["ip"]
    assert lease_after["status"] == "active"
    # And the restored pool still allocates from the new range.
    fresh, _ = reopened.acquire("client-b")
    assert fresh["ip"].startswith("10.30.0.")


def test_restart_ignores_env_default_when_db_has_pool(
    tmp_path, clock
) -> None:
    db = str(tmp_path / "leases.db")
    original = Store(Settings(db_path=db, default_pool_cidr="10.40.0.0/24"), clock=clock)
    original.initialize()
    original.switch_pool("10.50.0.0/24")

    reopened = Store(Settings(db_path=db, default_pool_cidr="10.60.0.0/24"), clock=clock)
    reopened.initialize()
    assert reopened.pool_info()["cidr"] == "10.50.0.0/24"


def test_expired_out_of_pool_lease_disappears(store: Store, clock) -> None:
    lease, _ = store.acquire("client-a", ttl_seconds=60)
    store.switch_pool("10.99.0.0/24")
    assert len(store.out_of_pool_leases()) == 1
    clock.advance(61)
    # A write triggers the sweep; the report then sees the released row.
    store.acquire("client-b")
    assert store.out_of_pool_leases() == []


def test_client_keeps_lease_across_pool_round_trip(store: Store) -> None:
    lease, _ = store.acquire("client-a")
    store.switch_pool("10.99.0.0/24")
    store.switch_pool("10.20.0.0/29")
    # The client never released, so its lease is still the one it gets back.
    same, reused = store.acquire("client-a")
    assert reused is True and same["lease_id"] == lease["lease_id"]
    assert same["ip"] == lease["ip"]


def test_switch_pool_rolls_back_when_write_fails(store: Store) -> None:
    """A failure inside the switch transaction leaves the old pool serving."""
    lease, _ = store.acquire("client-a")
    conn = store._connect()
    try:
        conn.executescript(
            """
            CREATE TRIGGER fail_pool_change
            BEFORE INSERT ON pool_changes
            BEGIN
                SELECT RAISE(ABORT, 'simulated disk failure');
            END;
            """
        )
    finally:
        conn.close()

    with pytest.raises(sqlite3.IntegrityError):
        store.switch_pool("10.99.0.0/24")

    assert store.pool_info()["cidr"] == "10.20.0.0/29"
    # The old lease still renews, i.e. the pool really did not move.
    assert store.renew(lease["lease_id"])["status"] == "active"


def test_reacquire_while_out_of_pool_returns_same_lease(store: Store) -> None:
    lease, _ = store.acquire("client-a")
    store.switch_pool("10.99.0.0/24")
    again, reused = store.acquire("client-a")
    assert reused is True
    assert again["lease_id"] == lease["lease_id"]
    assert again["ip"] == lease["ip"]
    assert again["status"] == "active"
    # A different client must not be able to take the out-of-pool address.
    other, _ = store.acquire("client-b")
    assert other["pool_cidr"] == "10.99.0.0/24"
    assert other["ip"] != lease["ip"]
