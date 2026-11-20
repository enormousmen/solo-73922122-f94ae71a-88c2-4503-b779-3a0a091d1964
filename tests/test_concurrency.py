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
