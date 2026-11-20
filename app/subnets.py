"""Address arithmetic for IPv4 pools (single subnet only)."""

from __future__ import annotations

import ipaddress

MAX_PREFIX = 32


class PoolError(ValueError):
    """Raised when a pool definition or address input is invalid."""


def parse_pool_cidr(cidr: str) -> ipaddress.IPv4Network:
    """Parse an IPv4 CIDR and return the normalized network.

    Host bits in the supplied address are ignored (``10.0.0.7/24`` becomes
    ``10.0.0.0/24``), and the normalized form is what gets persisted.  A /32
    is rejected because it has no assignable address.
    """
    if not isinstance(cidr, str) or not cidr.strip():
        raise PoolError("pool must be an IPv4 CIDR such as 10.20.0.0/24")
    text = cidr.strip()
    if "/" not in text:
        raise PoolError("pool must include a prefix length, e.g. 10.20.0.0/24")
    prefix = text.split("/", 1)[1]
    if not prefix.isdigit() or not 0 <= int(prefix) <= MAX_PREFIX:
        raise PoolError("prefix length must be an integer between 0 and 32")
    try:
        network = ipaddress.IPv4Network(text, strict=False)
    except ValueError as exc:
        raise PoolError(f"invalid IPv4 CIDR: {text!r}") from exc
    if network.prefixlen == 32:
        raise PoolError("pool /32 has no usable addresses")
    return network


def usable_bounds(network: ipaddress.IPv4Network) -> tuple[int, int]:
    """Inclusive integer bounds of assignable addresses in ``network``.

    A normal subnet excludes its network and broadcast addresses; a /31 is a
    point-to-point link whose two addresses are both assignable.
    """
    first = int(network.network_address)
    last = int(network.broadcast_address)
    if network.prefixlen < 31:
        first += 1
        last -= 1
    return first, last


def usable_count(network: ipaddress.IPv4Network) -> int:
    first, last = usable_bounds(network)
    return max(0, last - first + 1)


def first_free_address(network: ipaddress.IPv4Network, held: set[int]) -> int | None:
    """Return the lowest free integer address in ``network``, or ``None``.

    ``held`` may contain addresses outside the pool; they are simply skipped
    by the range check.  Scanning over the sorted held set keeps the cost
    proportional to the number of leases rather than the size of the pool.
    """
    first, last = usable_bounds(network)
    candidate = first
    for used in sorted(held):
        if used < candidate:
            continue
        if used > candidate:
            break
        candidate = used + 1
    return candidate if candidate <= last else None


def normalized_pool(cidr: str) -> str:
    """Return the canonical string form of a validated pool CIDR."""
    return str(parse_pool_cidr(cidr))
