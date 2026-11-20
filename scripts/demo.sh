#!/usr/bin/env bash
# End-to-end demo against a running lease API.
#
#   docker compose up -d --build
#   ./scripts/demo.sh                  # defaults to http://127.0.0.1:8000
#   BASE=http://host:8000 ./scripts/demo.sh
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8000}"
TOKEN="${OPERATOR_TOKEN:-}"
AUTH=()
if [[ -n "$TOKEN" ]]; then
  AUTH=(-H "X-Operator-Token: $TOKEN")
fi

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
api() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" "$BASE$path" -H 'Content-Type: application/json' "${AUTH[@]}" -d "$body"
  else
    curl -sS -X "$method" "$BASE$path" "${AUTH[@]}"
  fi
}
pretty() { python3 -m json.tool; }

say "Health"
api GET /health | pretty

say "Current pool"
api GET /v1/pool | pretty

say "Client laptop-01 acquires a lease"
LAPTOP=$(api POST /v1/leases '{"client_id":"laptop-01"}')
echo "$LAPTOP" | pretty
LAPTOP_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["lease_id"])' <<<"$LAPTOP")

say "Client laptop-01 asks again: same unexpired lease is returned"
api POST /v1/leases '{"client_id":"laptop-01"}' | pretty

say "Client tablet-02 acquires a lease"
TABLET=$(api POST /v1/leases '{"client_id":"tablet-02"}')
echo "$TABLET" | pretty
TABLET_IP=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["ip"])' <<<"$TABLET")

say "laptop-01 renews"
api POST "/v1/leases/$LAPTOP_ID/renew" '{"ttl_seconds":7200}' | pretty

say "Operator switches the pool (10.20.0.0/24 -> 10.30.0.0/27)"
api PUT /v1/pool '{"cidr":"10.30.0.0/27"}' | pretty

say "Out-of-pool addresses still occupied (with expiry)"
api GET /v1/admin/out-of-pool | pretty

say "Renewing laptop-01 (address now outside pool) is rejected"
api POST "/v1/leases/$LAPTOP_ID/renew" '{}' | pretty

say "A new client gets a fresh address from the new pool"
api POST /v1/leases '{"client_id":"phone-03"}' | pretty

say "laptop-01 releases its out-of-pool address -> it is no longer occupied"
api POST "/v1/leases/$LAPTOP_ID/release" | pretty
api GET /v1/admin/out-of-pool | pretty

say "Invalid pool is rejected; the old pool keeps serving"
api PUT /v1/pool '{"cidr":"10.30.0.0/40"}' | pretty
api GET /v1/pool | pretty

say "Done. Data persists in the 'lease-data' Docker volume across restarts."
