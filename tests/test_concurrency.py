"""Serializability tests: pool switches racing with lease operations.

The contract is "outcome equals some sequential order".  Each test races a
pool switch against a lease operation and then checks the final state
against both possible serial orders, plus invariants that no interleaving
may violate (an address is never issued twice, a lease issued from a pool is
always inside that pool, a renewal never resurrects an out-of-pool lease).
"""

from __future__ import annotations

import ipaddress
import itertools
import os
import sqlite3
import tempfile
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from app.config import Settings
from app.db import LeaseError, Store
from app.subnets import usable_bounds

OLD = "10.20.0.0/29"
NEW = "10.40.0.0/29"

_counter = itertools.count()


def fresh_store(base: Settings) -> Store:
    directory = tempfile.mkdtemp(prefix="lease-race-")
    store = Store(
        base.with_(db_path=os.path.join(directory, f"db-{next(_counter)}.db"))
    )
    store.initialize()
    return store


def _in_pool(cidr: str, ip: str) -> bool:
    first, last = usable_bounds(ipaddress.IPv4Network(cidr))
    return first <= int(ipaddress.IPv4Address(ip)) <= last


def _run_pair(
    left: Callable[[], Any], right: Callable[[], Any]
) -> tuple[tuple[str, Any], tuple[str, Any]]:
    barrier = threading.Barrier(2)

    def wrap(fn: Callable[[], Any]) -> Callable[[], tuple[str, Any]]:
        def inner() -> tuple[str, Any]:
            barrier.wait()
            try:
                return "ok", fn()
            except LeaseError as exc:
                return "err", exc.code

        return inner

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(wrap(left)), pool.submit(wrap(right))]
        return futures[0].result(), futures[1].result()


def test_acquire_racing_switch_is_equivalent_to_serial(settings: Settings) -> None:
    for _ in range(25):
        store = fresh_store(settings)
        start_pool = store.pool_info()["cidr"]
        target = NEW if start_pool == OLD else OLD

        (acquire_kind, acquire_val), (switch_kind, switch_val) = _run_pair(
            lambda: store.acquire("client-a"),
            lambda: store.switch_pool(target),
        )

        assert switch_kind == "ok"
        assert switch_val["changed"] is True
        assert store.pool_info()["cidr"] == target

        assert acquire_kind == "ok", acquire_val
        lease, _reused = acquire_val
        # Serial order 1: acquire first -> lease came from the old pool.
        # Serial order 2: switch first  -> lease came from the new pool.
        assert lease["pool_cidr"] in {start_pool, target}
        assert _in_pool(lease["pool_cidr"], lease["ip"])
        if lease["pool_cidr"] == start_pool:
            first, _ = usable_bounds(ipaddress.IPv4Network(start_pool))
            assert int(ipaddress.IPv4Address(lease["ip"])) == first

        current = store.get_lease(lease["lease_id"])
        assert current["status"] == "active"
        out = {item["lease_id"] for item in store.out_of_pool_leases()}
        assert (lease["lease_id"] in out) == (lease["pool_cidr"] != target)


def test_renew_racing_switch_is_equivalent_to_serial(settings: Settings) -> None:
    for _ in range(25):
        store = fresh_store(settings)
        start_pool = store.pool_info()["cidr"]
        lease, _ = store.acquire("client-a", ttl_seconds=3600)
        target = NEW if start_pool == OLD else OLD

        (renew_kind, renew_val), (switch_kind, _) = _run_pair(
            lambda: store.renew(lease["lease_id"], ttl_seconds=7200),
            lambda: store.switch_pool(target),
        )

        assert switch_kind == "ok"
        if renew_kind == "ok":
            # Renew committed before the switch: it saw the old pool.
            assert renew_val["status"] == "active"
            assert renew_val["seconds_remaining"] == 7200
            final = store.get_lease(lease["lease_id"])
            assert final["status"] == "active"
            assert final["ip"] == lease["ip"]
        else:
            assert renew_val == "address_out_of_pool"
            final = store.get_lease(lease["lease_id"])
            assert final["status"] == "active"
            assert final["lease_id"] == lease["lease_id"]
            assert final["expires_at"] == lease["expires_at"]


def test_release_racing_switch_never_breaks_invariants(settings: Settings) -> None:
    for _ in range(25):
        store = fresh_store(settings)
        lease, _ = store.acquire("client-a")
        start_pool = store.pool_info()["cidr"]
        target = NEW if start_pool == OLD else OLD

        (release_kind, _), (switch_kind, _) = _run_pair(
            lambda: store.release(lease["lease_id"]),
            lambda: store.switch_pool(target),
        )
        assert release_kind == "ok"
        assert switch_kind == "ok"
        final = store.get_lease(lease["lease_id"])
        assert final["status"] == "released"
        assert store.out_of_pool_leases() == []
        # The address is free again in the old pool's space.
        store.switch_pool(start_pool)
        fresh, _ = store.acquire("client-b")
        assert fresh["ip"] == lease["ip"]


def test_concurrent_acquires_never_share_an_address(settings: Settings) -> None:
    store = fresh_store(settings)

    def acquire(i: int) -> str | None:
        try:
            return store.acquire(f"client-{i}")[0]["ip"]
        except LeaseError as exc:
            assert exc.code == "pool_exhausted"
            return None

    with ThreadPoolExecutor(max_workers=12) as pool:
        addresses = list(pool.map(acquire, range(12)))

    allocated = [a for a in addresses if a is not None]
    assert len(allocated) == len(set(allocated))
    assert len(allocated) == 6  # /29 capacity


# ---------------------------------------------------------------------------
# A write that queues behind another writer judges time when it can act.
# ---------------------------------------------------------------------------


class _RecordingClock:
    """Clock that records every read and never blocks."""

    def __init__(self, base: float) -> None:
        self.value = base
        self.reads: list[float] = []
        self.read_event = threading.Event()

    def __call__(self) -> float:
        self.reads.append(self.value)
        self.read_event.set()
        return self.value


def _run_queued(
    store: Store,
    clock: _RecordingClock,
    base: float,
    action: Callable[[], Any],
) -> tuple[dict[str, Any], bool]:
    """Run ``action`` while another connection holds the write lock.

    Returns the outcome and whether the clock was read *before* the lock was
    released -- i.e. while the request was still queued.  The fixed code must
    read the clock only after it owns the lock, so that flag is False there
    and True on the buggy code.
    """
    holder = sqlite3.connect(store.db_path, timeout=10.0, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    clock.read_event.clear()
    clock.reads.clear()
    outcome: dict[str, Any] = {}

    def worker() -> None:
        try:
            outcome["value"] = action()
        except LeaseError as exc:
            outcome["error"] = exc.code

    thread = threading.Thread(target=worker)
    thread.start()
    # In the buggy code the clock is read before the lock is even attempted,
    # so this fires while the holder still has the lock.  In the fixed code
    # it cannot fire until we commit below.
    read_while_queued = clock.read_event.wait(0.5)

    clock.value = base + 61
    holder.execute("COMMIT")
    holder.close()
    thread.join(10)
    assert not thread.is_alive()
    return outcome, read_while_queued


def test_queued_renew_cannot_resurrect_a_lease_that_expired_while_waiting(
    settings: Settings,
) -> None:
    base = 1_800_000_000.0
    gate = _RecordingClock(base)
    directory = tempfile.mkdtemp(prefix="lease-race-")
    store = Store(
        settings.with_(db_path=os.path.join(directory, "gated.db")), clock=gate
    )
    store.initialize()
    lease, _ = store.acquire("client-a", ttl_seconds=60)
    original_expiry = lease["expires_at"]

    outcome, read_while_queued = _run_queued(
        store,
        gate,
        base,
        lambda: store.renew(lease["lease_id"], ttl_seconds=3600),
    )

    # The fix: the queued renew judges time only once it holds the lock.
    assert read_while_queued is False
    assert outcome.get("error") == "lease_expired", outcome
    stored = store.get_lease(lease["lease_id"])
    assert stored["status"] == "expired"
    assert stored["expires_at"] == original_expiry
    # A later acquire for the same client starts fresh.
    fresh, reused = store.acquire("client-a")
    assert reused is False
    assert fresh["lease_id"] != lease["lease_id"]


def test_queued_release_reports_expired_after_waiting(
    settings: Settings,
) -> None:
    base = 1_800_000_000.0
    gate = _RecordingClock(base)
    directory = tempfile.mkdtemp(prefix="lease-race-")
    store = Store(
        settings.with_(db_path=os.path.join(directory, "gated-release.db")),
        clock=gate,
    )
    store.initialize()
    lease, _ = store.acquire("client-a", ttl_seconds=60)
    original_expiry = lease["expires_at"]

    outcome, read_while_queued = _run_queued(
        store, gate, base, lambda: store.release(lease["lease_id"])
    )

    assert read_while_queued is False
    assert outcome.get("error") == "lease_expired", outcome
    stored = store.get_lease(lease["lease_id"])
    assert stored["status"] == "expired"
    assert stored["expires_at"] == original_expiry


def test_queued_acquire_does_not_reuse_an_expired_lease(
    settings: Settings,
) -> None:
    base = 1_800_000_000.0
    gate = _RecordingClock(base)
    directory = tempfile.mkdtemp(prefix="lease-race-")
    store = Store(
        settings.with_(db_path=os.path.join(directory, "gated-acquire.db")),
        clock=gate,
    )
    store.initialize()
    first, _ = store.acquire("client-a", ttl_seconds=60)
    assert first["ip"] == "10.20.0.1"

    outcome, read_while_queued = _run_queued(
        store, gate, base, lambda: store.acquire("client-a")
    )

    assert read_while_queued is False
    assert "error" not in outcome, outcome
    fresh, reused = outcome["value"]
    assert reused is False
    assert fresh["lease_id"] != first["lease_id"]
    assert fresh["pool_cidr"] == "10.20.0.0/29"
    # The lowest address is free again, so it is reissued -- from a fresh row.
    assert fresh["ip"] == "10.20.0.1"


def test_query_while_write_lock_is_held_returns_committed_state(
    settings: Settings,
) -> None:
    """A queued writer must not block reads; reads see the last commit."""
    store = fresh_store(settings)
    lease, _ = store.acquire("client-a", ttl_seconds=3600)
    before = store.pool_info()

    holder = sqlite3.connect(store.db_path, timeout=10.0, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        info = store.pool_info()
        got = store.get_lease(lease["lease_id"])
        report = store.out_of_pool_leases()
    finally:
        holder.execute("COMMIT")
        holder.close()

    assert info["cidr"] == before["cidr"]
    assert info["allocated_in_pool"] == 1
    assert info["server_time"] >= before["server_time"]
    assert got["status"] == "active"
    assert got["lease_id"] == lease["lease_id"]
    assert report == []

