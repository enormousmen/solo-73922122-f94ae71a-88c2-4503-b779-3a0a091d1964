"""Shared fixtures for the lease API test-suite."""

from __future__ import annotations

import itertools
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import Store
from app.main import create_app


class FakeClock:
    """Monotonic fake clock so expiry tests do not sleep."""

    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self.value = start
        self._ticks = itertools.count()

    def __call__(self) -> float:
        # Advance 1ms per call so successive operations are ordered.
        self.value += 0.001
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        db_path=str(tmp_path / "leases.db"),
        default_pool_cidr="10.20.0.0/29",
        default_ttl_seconds=3600,
        min_ttl_seconds=60,
        max_ttl_seconds=86400,
    )


@pytest.fixture
def store(settings: Settings, clock: FakeClock) -> Store:
    store = Store(settings, clock=clock)
    store.initialize()
    return store


@pytest.fixture
def client(settings: Settings, clock: FakeClock) -> Iterator[TestClient]:
    app = create_app(settings, store=Store(settings, clock=clock))
    with TestClient(app) as test_client:
        yield test_client
