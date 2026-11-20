"""SQLite persistence and the lease state machine.

Concurrency model
-----------------
Every mutating operation runs inside ``BEGIN IMMEDIATE``, which takes
SQLite's database-wide write lock up front.  Writers therefore execute one
at a time, and because the pool switch is a single transaction, a switch
racing with acquire/renew/release always yields the state of some serial
execution order of the two.

Reads use a short deferred transaction so that multi-statement reports see
one consistent snapshot even while a switch commits.  Reads never take the
write lock, so a request queued behind a writer is still served, and the
snapshot is pinned before the clock is read so the reported ``server_time``
matches the committed state it is returned with.

Time
----
Every command reads the clock *after* it holds the write lock.  A request
that waited behind another writer therefore judges expiry at the instant it
can actually mutate state, not at the instant it arrived: a renewal that
queued while its lease expired fails with the normal expiry error instead of
resurrecting the lease, and a pool switch computes its occupancy counters at
the moment the switch takes effect.

Expiry
------
Leases are never deleted.  An unreleased lease whose ``expires_at`` has
passed is swept to ``released_at = <now>, release_reason = 'expired'`` at
the start of each write transaction.  That keeps the partial unique indexes
("one active lease per address", "one active lease per client") truthful
without needing a clock-dependent index predicate.
"""

from __future__ import annotations

import ipaddress
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeVar

from . import subnets
from .config import Settings
from .subnets import PoolError

_T = TypeVar("_T")

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    lease_id       TEXT PRIMARY KEY,
    client_id      TEXT NOT NULL,
    ip             TEXT NOT NULL,
    pool_cidr      TEXT NOT NULL,
    issued_at      REAL NOT NULL,
    expires_at     REAL NOT NULL,
    released_at    REAL,
    release_reason TEXT,
    CHECK (released_at IS NULL OR released_at >= issued_at),
    CHECK (expires_at > issued_at),
    CHECK (release_reason IS NULL OR release_reason IN ('released', 'expired'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_leases_active_ip
    ON leases(ip) WHERE released_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_leases_active_client
    ON leases(client_id) WHERE released_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_leases_expires
    ON leases(expires_at) WHERE released_at IS NULL;

CREATE TABLE IF NOT EXISTS pool_changes (
    change_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    previous_cidr  TEXT,
    new_cidr       TEXT NOT NULL,
    changed_at     REAL NOT NULL
);
"""


class LeaseError(Exception):
    """Domain error carrying an HTTP status and a stable machine code."""

    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class Lease:
    lease_id: str
    client_id: str
    ip: str
    pool_cidr: str
    issued_at: float
    expires_at: float
    released_at: float | None
    release_reason: str | None

    def is_active(self, now: float) -> bool:
        return self.released_at is None and self.expires_at > now

    def status_at(self, now: float) -> str:
        # A lease past its deadline that has not been swept yet still reads as
        # expired, so a report never claims a dead lease is active.
        if self.released_at is not None:
            return "expired" if self.release_reason == "expired" else "released"
        return "active" if self.expires_at > now else "expired"

    def to_dict(self, now: float) -> dict[str, Any]:
        active = self.is_active(now)
        return {
            "lease_id": self.lease_id,
            "client_id": self.client_id,
            "ip": self.ip,
            "pool_cidr": self.pool_cidr,
            "status": self.status_at(now),
            "issued_at": iso(self.issued_at),
            "expires_at": iso(self.expires_at),
            "released_at": iso(self.released_at) if self.released_at is not None else None,
            "release_reason": self.release_reason,
            "seconds_remaining": max(0, int(self.expires_at - now)) if active else 0,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Lease":
        return cls(
            lease_id=row["lease_id"],
            client_id=row["client_id"],
            ip=row["ip"],
            pool_cidr=row["pool_cidr"],
            issued_at=row["issued_at"],
            expires_at=row["expires_at"],
            released_at=row["released_at"],
            release_reason=row["release_reason"],
        )


class Store:
    """All lease and pool operations, each one a single SQLite transaction."""

    def __init__(
        self,
        settings: Settings,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.settings = settings
        self.clock = clock
        self.db_path = settings.db_path
        self._pool = subnets.parse_pool_cidr(settings.default_pool_cidr)

    # ------------------------------------------------------------------ setup

    def _connect(self) -> sqlite3.Connection:
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            self.db_path,
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 10000")
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    def initialize(self) -> None:
        """Create the schema and restore the persisted pool or seed the default.

        A restart restores whatever pool was last committed; the environment
        default only seeds a fresh database.
        """
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(SCHEMA)
            row = conn.execute(
                "SELECT value FROM settings WHERE key = 'current_pool'"
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO settings(key, value) VALUES ('current_pool', ?)",
                    (str(self._pool),),
                )
            else:
                self._pool = subnets.parse_pool_cidr(row["value"])
        finally:
            conn.close()

    @contextmanager
    def _write(self) -> Iterator[tuple[sqlite3.Connection, float]]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                # Read the clock only after the write lock is held.  A request
                # that waited behind another writer must judge expiry at the
                # instant it can actually mutate state: using its arrival time
                # would let a renewal resurrect a lease that expired while it
                # was queued, and would make switch-time usage counts stale.
                now = self.clock()
                yield conn, now
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        finally:
            if conn.in_transaction:  # pragma: no cover - commit failed
                conn.execute("ROLLBACK")
            conn.close()

    def _read(self, body: Callable[[sqlite3.Connection, float], _T]) -> _T:
        """Run ``body`` against one consistent ``(connection, now)`` snapshot.

        The snapshot is pinned by a real table read *before* the clock is
        taken.  WAL gives the whole read transaction one immutable view, and
        ``server_time`` is therefore never older than the committed state
        (pool, rows) it is returned alongside -- no matter whether a writer
        commits just before, during or after the request.

        The read transaction is deferred, so it takes no write lock and is
        never queued behind an in-flight write.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            try:
                conn.execute("SELECT 1 FROM settings LIMIT 1").fetchone()
                now = self.clock()
                result = body(conn, now)
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            return result
        finally:
            if conn.in_transaction:  # pragma: no cover - commit failed
                conn.execute("ROLLBACK")
            conn.close()

    # ------------------------------------------------------------- primitives

    def _current_pool(self, conn: sqlite3.Connection) -> ipaddress.IPv4Network:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'current_pool'"
        ).fetchone()
        if row is None:  # pragma: no cover - initialize() always seeds it
            raise LeaseError(503, "pool_uninitialized", "current pool is not set")
        return subnets.parse_pool_cidr(row["value"])

    def _sweep_expired(self, conn: sqlite3.Connection, now: float) -> int:
        cur = conn.execute(
            """
            UPDATE leases
               SET released_at = ?, release_reason = 'expired'
             WHERE released_at IS NULL
               AND expires_at <= ?
            """,
            (now, now),
        )
        return cur.rowcount

    def _active_rows(self, conn: sqlite3.Connection, now: float) -> list[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM leases WHERE released_at IS NULL AND expires_at > ?",
            (now,),
        ).fetchall()

    @staticmethod
    def _in_pool(pool: ipaddress.IPv4Network, ip_text: str) -> bool:
        try:
            address = ipaddress.IPv4Address(ip_text)
        except ValueError:
            return False
        first, last = subnets.usable_bounds(pool)
        return first <= int(address) <= last

    def _counts(self, pool: ipaddress.IPv4Network, active: list[sqlite3.Row]) -> dict[str, int]:
        first, last = subnets.usable_bounds(pool)
        in_pool = sum(
            1 for r in active if first <= int(ipaddress.IPv4Address(r["ip"])) <= last
        )
        total = subnets.usable_count(pool)
        return {
            "usable_addresses": total,
            "allocated_in_pool": in_pool,
            "free_in_pool": total - in_pool,
            "occupied_out_of_pool": len(active) - in_pool,
        }

    def _resolve_ttl(self, ttl_seconds: int | None) -> int:
        if ttl_seconds is None:
            return self.settings.default_ttl_seconds
        if not (
            self.settings.min_ttl_seconds
            <= ttl_seconds
            <= self.settings.max_ttl_seconds
        ):
            raise LeaseError(
                422,
                "invalid_ttl",
                f"ttl_seconds must be between {self.settings.min_ttl_seconds} "
                f"and {self.settings.max_ttl_seconds}",
            )
        return ttl_seconds

    # ---------------------------------------------------------------- queries

    def pool_info(self) -> dict[str, Any]:
        def body(conn: sqlite3.Connection, now: float) -> dict[str, Any]:
            pool = self._current_pool(conn)
            counts = self._counts(pool, self._active_rows(conn, now))
            return {
                "cidr": str(pool),
                "network_address": str(pool.network_address),
                "broadcast_address": str(pool.broadcast_address),
                "prefix_length": pool.prefixlen,
                **counts,
                "server_time": iso(now),
            }

        return self._read(body)

    def get_lease(self, lease_id: str) -> dict[str, Any]:
        def body(
            conn: sqlite3.Connection, now: float
        ) -> tuple[sqlite3.Row | None, float]:
            row = conn.execute(
                "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            return row, now

        row, now = self._read(body)
        if row is None:
            raise LeaseError(404, "lease_not_found", "no lease with that id")
        return Lease.from_row(row).to_dict(now)

    def out_of_pool_leases(self) -> list[dict[str, Any]]:
        """Active leases whose address is not inside the current pool.

        Expired-but-not-yet-swept leases are excluded here the same way a
        write transaction would have swept them.
        """

        def body(conn: sqlite3.Connection, now: float) -> list[dict[str, Any]]:
            pool = self._current_pool(conn)
            result = []
            for row in conn.execute(
                "SELECT * FROM leases WHERE released_at IS NULL AND expires_at > ?",
                (now,),
            ).fetchall():
                if self._in_pool(pool, row["ip"]):
                    continue
                lease = Lease.from_row(row)
                item = lease.to_dict(now)
                item["original_pool_cidr"] = lease.pool_cidr
                item["current_pool_cidr"] = str(pool)
                result.append(item)
            result.sort(key=lambda item: (item["expires_at"], item["ip"]))
            return result

        return self._read(body)

    # --------------------------------------------------------------- commands

    def acquire(
        self, client_id: str, ttl_seconds: int | None = None
    ) -> tuple[dict[str, Any], bool]:
        """Allocate an address, or return the client's still-valid lease.

        Returns ``(lease, reused)``.  ``reused`` is true when an existing
        unexpired lease for the client was returned unchanged (even if its
        address has since fallen outside the pool).
        """
        ttl = self._resolve_ttl(ttl_seconds)
        with self._write() as (conn, now):
            self._sweep_expired(conn, now)
            pool = self._current_pool(conn)
            existing = conn.execute(
                "SELECT * FROM leases WHERE client_id = ? AND released_at IS NULL",
                (client_id,),
            ).fetchone()
            # Only a lease that is still live at this instant may be reused.
            # If the client's lease expired while this request queued, the
            # sweep above retired it and we fall through to allocating from
            # the pool and occupancy as they stand now.
            if existing is not None and Lease.from_row(existing).is_active(now):
                return Lease.from_row(existing).to_dict(now), True

            held = {
                int(ipaddress.IPv4Address(row["ip"]))
                for row in conn.execute(
                    "SELECT ip FROM leases WHERE released_at IS NULL"
                ).fetchall()
            }
            address = subnets.first_free_address(pool, held)
            if address is None:
                raise LeaseError(
                    409, "pool_exhausted", f"no free address left in pool {pool}"
                )
            lease_id = uuid.uuid4().hex
            conn.execute(
                """
                INSERT INTO leases(lease_id, client_id, ip, pool_cidr,
                                   issued_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    lease_id,
                    client_id,
                    str(ipaddress.IPv4Address(address)),
                    str(pool),
                    now,
                    now + ttl,
                ),
            )
            row = conn.execute(
                "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
        return Lease.from_row(row).to_dict(now), False

    def renew(self, lease_id: str, ttl_seconds: int | None = None) -> dict[str, Any]:
        ttl = self._resolve_ttl(ttl_seconds)
        with self._write() as (conn, now):
            self._sweep_expired(conn, now)
            pool = self._current_pool(conn)
            row = conn.execute(
                "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            if row is None:
                raise LeaseError(404, "lease_not_found", "no lease with that id")
            lease = Lease.from_row(row)
            if lease.released_at is not None:
                if lease.release_reason == "expired":
                    raise LeaseError(409, "lease_expired", "lease has expired")
                raise LeaseError(409, "lease_released", "lease has already been released")
            # The lease may have run out while this request waited for the
            # write lock even if the sweep saw nothing else to do.  Never
            # extend an expired lease, and never move its original deadline:
            # the expiry error carries the stored expires_at unchanged.
            if lease.expires_at <= now:
                raise LeaseError(409, "lease_expired", "lease has expired")
            if not self._in_pool(pool, lease.ip):
                raise LeaseError(
                    409,
                    "address_out_of_pool",
                    f"{lease.ip} is outside the current pool {pool}; "
                    "it may be released but not renewed",
                )
            conn.execute(
                "UPDATE leases SET expires_at = ? WHERE lease_id = ?",
                (now + ttl, lease_id),
            )
            row = conn.execute(
                "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
        return Lease.from_row(row).to_dict(now)

    def release(self, lease_id: str) -> tuple[dict[str, Any], bool]:
        """Release an active lease.  Releasing an already released lease is a no-op.

        If the lease expired while this request queued behind the write lock,
        the answer is the existing expiry error and the stored deadline is
        left untouched.
        """
        with self._write() as (conn, now):
            self._sweep_expired(conn, now)
            row = conn.execute(
                "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            if row is None:
                raise LeaseError(404, "lease_not_found", "no lease with that id")
            lease = Lease.from_row(row)
            if lease.released_at is None:
                if lease.expires_at <= now:
                    raise LeaseError(409, "lease_expired", "lease has expired")
                conn.execute(
                    """
                    UPDATE leases SET released_at = ?, release_reason = 'released'
                     WHERE lease_id = ?
                    """,
                    (now, lease_id),
                )
                row = conn.execute(
                    "SELECT * FROM leases WHERE lease_id = ?", (lease_id,)
                ).fetchone()
                return Lease.from_row(row).to_dict(now), False
            if lease.release_reason == "expired":
                raise LeaseError(409, "lease_expired", "lease has expired")
            return Lease.from_row(row).to_dict(now), True

    def switch_pool(self, cidr: str) -> dict[str, Any]:
        """Move the server to ``cidr``; the previous pool survives any failure."""
        try:
            target = subnets.parse_pool_cidr(cidr)
        except PoolError as exc:
            raise LeaseError(422, "invalid_pool", str(exc)) from exc
        with self._write() as (conn, now):
            self._sweep_expired(conn, now)
            previous = self._current_pool(conn)
            if previous == target:
                return {
                    "cidr": str(target),
                    "previous_cidr": str(previous),
                    "changed": False,
                    "changed_at": None,
                    **self._counts(target, self._active_rows(conn, now)),
                }
            conn.execute(
                "UPDATE settings SET value = ? WHERE key = 'current_pool'",
                (str(target),),
            )
            conn.execute(
                """
                INSERT INTO pool_changes(previous_cidr, new_cidr, changed_at)
                VALUES (?, ?, ?)
                """,
                (str(previous), str(target), now),
            )
            counts = self._counts(target, self._active_rows(conn, now))
        return {
            "cidr": str(target),
            "previous_cidr": str(previous),
            "changed": True,
            "changed_at": iso(now),
            **counts,
        }
