"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace

from .subnets import PoolError, parse_pool_cidr


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    db_path: str = "./data/leases.db"
    default_pool_cidr: str = "10.20.0.0/24"
    default_ttl_seconds: int = 3600
    min_ttl_seconds: int = 60
    max_ttl_seconds: int = 604800
    operator_token: str | None = None

    def __post_init__(self) -> None:
        if not self.min_ttl_seconds <= self.default_ttl_seconds <= self.max_ttl_seconds:
            raise RuntimeError(
                "DEFAULT_TTL_SECONDS must be between MIN_TTL_SECONDS and MAX_TTL_SECONDS"
            )
        if self.min_ttl_seconds < 1 or self.max_ttl_seconds < self.min_ttl_seconds:
            raise RuntimeError("invalid TTL bounds")
        try:
            parse_pool_cidr(self.default_pool_cidr)
        except PoolError as exc:
            raise RuntimeError(f"DEFAULT_POOL_CIDR is invalid: {exc}") from exc

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.environ.get("OPERATOR_TOKEN")
        return cls(
            db_path=os.environ.get("DB_PATH", cls.db_path),
            default_pool_cidr=os.environ.get("DEFAULT_POOL_CIDR", cls.default_pool_cidr),
            default_ttl_seconds=_env_int("DEFAULT_TTL_SECONDS", cls.default_ttl_seconds),
            min_ttl_seconds=_env_int("MIN_TTL_SECONDS", cls.min_ttl_seconds),
            max_ttl_seconds=_env_int("MAX_TTL_SECONDS", cls.max_ttl_seconds),
            operator_token=token.strip() if token and token.strip() else None,
        )

    def with_(self, **changes: object) -> "Settings":
        return replace(self, **changes)  # type: ignore[arg-type]
