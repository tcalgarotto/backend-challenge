#!/usr/bin/env bash
# Derrubar uma stack não pode remover os recursos da outra.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=names.bash
source "$(dirname "${BASH_SOURCE[0]}")/names.bash"
cd "$ROOT"

resolve() {
  JWP_REPRO_SUFFIX="${1:-}"
  if [ -n "${2+x}" ]; then COMPOSE_PROJECT_NAME="$2"; else unset COMPOSE_PROJECT_NAME; fi
  jwp_repro_resolve_names
}

resolve ""
test "$JWP_REPRO_PROJECT" = jwp-repro
test "$(jwp_repro_container schema)" = jwp-schema-repro
test "$JWP_REPRO_OBS_PROJECT" = jwp-observability-repro

resolve alpha
A_PROJECT=$JWP_REPRO_PROJECT
A_SCHEMA=$(jwp_repro_container schema)
A_OBS=$JWP_REPRO_OBS_PROJECT
resolve beta
B_PROJECT=$JWP_REPRO_PROJECT
B_SCHEMA=$(jwp_repro_container schema)
B_OBS=$JWP_REPRO_OBS_PROJECT
test "$A_PROJECT" = jwp-repro-alpha
test "$B_PROJECT" = jwp-repro-beta
test "$A_SCHEMA" = jwp-schema-alpha-repro
test "$B_SCHEMA" = jwp-schema-beta-repro
test "$A_OBS" = jwp-observability-repro-alpha
test "$B_OBS" = jwp-observability-repro-beta
test "$A_PROJECT" != "$B_PROJECT"
test "$A_SCHEMA" != "$B_SCHEMA"

resolve "" jwp-repro-gamma
test "$JWP_REPRO_PROJECT" = jwp-repro-gamma
test "$(jwp_repro_container schema)" = jwp-schema-jwp-repro-gamma-repro
test "$JWP_REPRO_OBS_PROJECT" = jwp-observability-repro-jwp-repro-gamma

set +e
resolve "Bad_Name"
code=$?
set -e
test "$code" = 2

docker rm -f "$A_SCHEMA" "$B_SCHEMA" >/dev/null 2>&1 || true
docker create --name "$A_SCHEMA" postgres:17.6-alpine >/dev/null
docker create --name "$B_SCHEMA" postgres:17.6-alpine >/dev/null
docker rm -f "$A_SCHEMA" >/dev/null
docker inspect "$B_SCHEMA" >/dev/null
docker rm -f "$B_SCHEMA" >/dev/null

# Portas distintas permitem executar as duas stacks no mesmo host.
A_PORT=15971
B_PORT=15972
cleanup() {
  JWP_PG_PORT=$A_PORT docker compose -p "$A_PROJECT" -f compose.db.yml down -v >/dev/null 2>&1 || true
  JWP_PG_PORT=$B_PORT docker compose -p "$B_PROJECT" -f compose.db.yml down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
JWP_PG_PORT=$A_PORT docker compose -p "$A_PROJECT" -f compose.db.yml up -d --wait
JWP_PG_PORT=$B_PORT docker compose -p "$B_PROJECT" -f compose.db.yml up -d --wait
docker inspect "${A_PROJECT}-postgres-1" "${B_PROJECT}-postgres-1" >/dev/null
JWP_PG_PORT=$A_PORT docker compose -p "$A_PROJECT" -f compose.db.yml down -v
docker inspect "${B_PROJECT}-postgres-1" >/dev/null
test -z "$(docker ps -aq -f "name=${A_PROJECT}-postgres-1")"
echo "suffix isolation ok: $A_PROJECT and $B_PROJECT coexisted; removing $A_PROJECT left $B_PROJECT"
