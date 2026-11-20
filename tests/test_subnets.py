"""Tests for pure IPv4 pool arithmetic."""

from __future__ import annotations

import ipaddress

import pytest

from app import subnets
from app.subnets import PoolError


@pytest.mark.parametrize(
    ("cidr", "expected"),
    [
        ("10.0.0.0/24", "10.0.0.0/24"),
        (" 192.168.1.7/24 ", "192.168.1.0/24"),
        ("10.0.0.0/31", "10.0.0.0/31"),
        ("0.0.0.0/0", "0.0.0.0/0"),
    ],
)
def test_parse_pool_normalizes(cidr: str, expected: str) -> None:
    assert str(subnets.parse_pool_cidr(cidr)) == expected


@pytest.mark.parametrize(
    "cidr",
    ["", "10.0.0.0", "10.0.0.0/33", "10.0.0.0/-1", "10.0.0.0/abc", "300.1.1.1/24",
     "10.0.0.1/32", "2001:db8::/64"],
)
def test_parse_pool_rejects_invalid(cidr: str) -> None:
    with pytest.raises(PoolError):
        subnets.parse_pool_cidr(cidr)


def test_usable_bounds_standard_subnet() -> None:
    net = ipaddress.IPv4Network("10.0.0.0/24")
    assert subnets.usable_bounds(net) == (0x0A000001, 0x0A0000FE)
    assert subnets.usable_count(net) == 254


def test_usable_bounds_31_is_point_to_point() -> None:
    net = ipaddress.IPv4Network("10.0.0.0/31")
    assert subnets.usable_count(net) == 2


def test_first_free_picks_lowest_gap() -> None:
    net = ipaddress.IPv4Network("10.0.0.0/29")
    first = int(ipaddress.IPv4Address("10.0.0.1"))
    second = first + 1
    third = first + 2
    assert subnets.first_free_address(net, set()) == first
    assert subnets.first_free_address(net, {first}) == second
    assert subnets.first_free_address(net, {first, third}) == second
    # Out-of-pool entries in ``held`` must not confuse the scan.
    assert subnets.first_free_address(net, {0x0A000001, 0x0AFFFFFF}) == second


def test_first_free_exhausted() -> None:
    net = ipaddress.IPv4Network("10.0.0.0/30")
    usable = int(ipaddress.IPv4Address("10.0.0.1"))
    assert subnets.first_free_address(net, {usable, usable + 1}) is None


def test_first_free_skips_out_of_pool_low_addresses() -> None:
    net = ipaddress.IPv4Network("10.0.0.0/29")
    held = {int(ipaddress.IPv4Address(f"9.0.0.{i}")) for i in range(1, 20)}
    assert subnets.first_free_address(net, held) == int(ipaddress.IPv4Address("10.0.0.1"))
