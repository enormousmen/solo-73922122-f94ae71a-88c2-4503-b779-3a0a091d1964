"""FastAPI application: single-subnet IPv4 lease broker."""

from __future__ import annotations

import hmac
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Header, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .config import Settings
from .db import LeaseError, Store

DESCRIPTION = """
Single-subnet IPv4 DHCP-style lease API.

* **Acquire** (`POST /v1/leases`) hands out the lowest free address of the
  current pool.  Re-acquiring with the same `client_id` returns the client's
  unexpired lease unchanged (HTTP 200, `reused: true`).
* **Renew / release** (`POST /v1/leases/{lease_id}/renew|release`) require the
  current lease id.  An address that fell outside the current pool can be
  released but not renewed.
* **Pool switch** (`PUT /v1/pool`) is atomic: it either fully commits or the
  previous pool keeps serving.  The pool and all leases survive restarts.
* **Operators** (`GET /v1/admin/out-of-pool`) can list still-occupied
  addresses that lie outside the current pool, with their expiry times.
"""


class LeaseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client_id: str = Field(min_length=1, max_length=128)
    ttl_seconds: int | None = Field(default=None, ge=1)

    @field_validator("client_id")
    @classmethod
    def _strip_client_id(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("client_id must not be blank")
        return value


class RenewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ttl_seconds: int | None = Field(default=None, ge=1)


class PoolRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cidr: str = Field(min_length=1, max_length=32)


class ErrorBody(BaseModel):
    code: str
    message: str


def error_response(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": ErrorBody(code=code, message=message).model_dump()},
    )


def create_app(settings: Settings | None = None, store: Store | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.store.initialize()
        yield

    app = FastAPI(
        title="Branch Gateway Lease API",
        version="1.0.0",
        description=DESCRIPTION,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.store = store or Store(settings)

    def get_store(request: Request) -> Store:
        return request.app.state.store

    def require_operator(
        request: Request,
        x_operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    ) -> None:
        expected = request.app.state.settings.operator_token
        if expected is None:
            return
        if x_operator_token is None or not _constant_time_eq(x_operator_token, expected):
            raise LeaseError(401, "unauthorized", "a valid X-Operator-Token is required")

    @app.exception_handler(LeaseError)
    async def handle_lease_error(_: Request, exc: LeaseError) -> JSONResponse:
        return error_response(exc.status_code, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # Keep the error envelope uniform across domain and schema failures.
        details = "; ".join(
            f"{'.'.join(str(part) for part in err['loc'][1:]) or 'body'}: {err['msg']}"
            for err in exc.errors()
        )
        return error_response(422, "invalid_request", details or "invalid request")

    # ------------------------------------------------------------------ meta

    @app.get("/health", tags=["meta"], summary="Liveness and configuration check")
    def health(request: Request) -> dict[str, Any]:
        return {"status": "ok", "pool": request.app.state.store.pool_info()["cidr"]}

    # ------------------------------------------------------------------ pool

    @app.get("/v1/pool", tags=["pool"], summary="Show the current pool and usage")
    def get_pool(store: Store = Depends(get_store)) -> dict[str, Any]:
        return store.pool_info()

    @app.put(
        "/v1/pool",
        tags=["pool"],
        summary="Switch the pool atomically (operator)",
    )
    def put_pool(
        body: PoolRequest,
        store: Store = Depends(get_store),
        _: None = Depends(require_operator),
    ) -> dict[str, Any]:
        return store.switch_pool(body.cidr)

    # ---------------------------------------------------------------- leases

    @app.post(
        "/v1/leases",
        tags=["leases"],
        summary="Acquire a lease (idempotent per client)",
        status_code=status.HTTP_201_CREATED,
        responses={200: {"description": "Existing unexpired lease returned"}},
    )
    def acquire(
        body: LeaseRequest,
        response: Response,
        store: Store = Depends(get_store),
    ) -> dict[str, Any]:
        lease, reused = store.acquire(body.client_id, body.ttl_seconds)
        if reused:
            response.status_code = status.HTTP_200_OK
            lease["reused"] = True
        else:
            response.status_code = status.HTTP_201_CREATED
            response.headers["Location"] = f"/v1/leases/{lease['lease_id']}"
            lease["reused"] = False
        return lease

    @app.get("/v1/leases/{lease_id}", tags=["leases"], summary="Inspect a lease")
    def get_lease(lease_id: str, store: Store = Depends(get_store)) -> dict[str, Any]:
        return store.get_lease(lease_id)

    @app.post(
        "/v1/leases/{lease_id}/renew",
        tags=["leases"],
        summary="Renew an active lease in the current pool",
    )
    def renew(
        lease_id: str,
        body: RenewRequest | None = None,
        store: Store = Depends(get_store),
    ) -> dict[str, Any]:
        ttl = body.ttl_seconds if body is not None else None
        return store.renew(lease_id, ttl)

    @app.post(
        "/v1/leases/{lease_id}/release",
        tags=["leases"],
        summary="Release an active lease (repeat release is a no-op)",
    )
    def release(lease_id: str, store: Store = Depends(get_store)) -> dict[str, Any]:
        lease, already = store.release(lease_id)
        lease["already_released"] = already
        return lease

    # ----------------------------------------------------------------- admin

    @app.get(
        "/v1/admin/out-of-pool",
        tags=["admin"],
        summary="List still-occupied addresses outside the current pool (operator)",
    )
    def out_of_pool(
        store: Store = Depends(get_store),
        _: None = Depends(require_operator),
    ) -> dict[str, Any]:
        leases = store.out_of_pool_leases()
        return {"count": len(leases), "leases": leases}

    return app


def _constant_time_eq(left: str, right: str) -> bool:
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))


app = create_app()
