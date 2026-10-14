#!/usr/bin/env bash







# Execuções paralelas precisam de nomes e portas distintos para não compartilhar infraestrutura.



# Falha inesperada reprova; falta de PostgreSQL/SQS é controle negativo obrigatório,
# nunca motivo para pular a suíte.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
# shellcheck source=names.bash
source "$(dirname "${BASH_SOURCE[0]}")/names.bash"
jwp_repro_resolve_names || exit 2
RUN_ID="${JWP_REPRO_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${JWP_REPRO_OUT:-$ROOT/test-results/reproduce/runs/$RUN_ID}"
PROJECT="$JWP_REPRO_PROJECT"
OBS_PROJECT="$JWP_REPRO_OBS_PROJECT"
PG_PORT="${JWP_REPRO_PG_PORT:-15916}"
SQS_PORT="${JWP_REPRO_SQS_PORT:-14916}"
OBS_SQS_PORT="${JWP_REPRO_OBS_SQS_PORT:-14926}"
# Evita portas efêmeras do Linux que sockets de saída podem ocupar brevemente.
PORT_BASE="${JWP_REPRO_PORT_BASE:-15930}"
PG_IMAGE=postgres:17.6-alpine
PG_PASSWORD=jwp_repro_local
MATRIX_ONLY=0
KEEP=0
for arg in "$@"; do
  case "$arg" in
    "") ;; # Bun Shell repassa posições ausentes como strings vazias.
    --matrix-only) MATRIX_ONLY=1 ;;
    --keep) KEEP=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

mkdir -p "$OUT"
SUMMARY="$OUT/summary.tsv"
printf 'step\texpect\texit\tverdict\tseconds\tlog\n' > "$SUMMARY"
STEP=0
FAILED=0

# Remove variáveis de infraestrutura nos controles negativos para não herdar dependências externas.
INFRA_VARS=(JWP_DATABASE_URL JWP_MIGRATION_DATABASE_URL JWP_SCHEMA_DATABASE_URL JWP_WALLET_DATABASE_URL
  JWP_WAGER_DATABASE_URL JWP_WAGER_POSTGRES_CONTAINER JWP_QUERY_DATABASE_URL JWP_QUERY_POSTGRES_CONTAINER
  JWP_MESSAGING_DATABASE_URL JWP_MESSAGING_POSTGRES_CONTAINER JWP_OUTBOX_DATABASE_URL JWP_OUTBOX_POSTGRES_CONTAINER
  JWP_SCHEDULER_DATABASE_URL JWP_SCHEDULER_POSTGRES_CONTAINER JWP_OBSERVABILITY_DATABASE_URL JWP_OBSERVABILITY_POSTGRES_CONTAINER
  JWP_OBSERVABILITY_SQS_ENDPOINT JWP_OBSERVABILITY_SQS_CONTAINER JWP_CONCURRENCY_DATABASE_URL JWP_CONCURRENCY_POSTGRES_CONTAINER
  JWP_RECOVERY_DATABASE_URL JWP_RECOVERY_POSTGRES_CONTAINER JWP_MATRIX_DATABASE_URL JWP_MATRIX_SQS_ENDPOINT JWP_SQS_ENDPOINT
  JWP_MESSAGING_SCHEMA_DATABASE_URL JWP_RUNTIME_TEST_DATABASE_URL JWP_CONSUMER_DATABASE_URL JWP_CONSUMER_WORKERS_MODULE)
UNSET=()
for var in "${INFRA_VARS[@]}"; do UNSET+=(-u "$var"); done
# O helper SQS tem endpoint padrão; apontar para porta fechada impede acessar
# LocalStack alheio durante controles negativos.
NO_SQS=http://127.0.0.1:9

slug() { printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_' | cut -c1-80; }


run_step() {
  local name="$1" expect="$2"; shift 2
  STEP=$((STEP + 1))
  local log
  log="$(printf '%02d' "$STEP")-$(slug "$name").txt"
  local started=$SECONDS
  {
    printf '$'; printf ' %q' "$@"; printf '\n'
    "$@"
    printf '\nexit=%s\n' "$?"
  } > "$OUT/$log" 2>&1
  local code
  code="$(tail -n 1 "$OUT/$log" | sed 's/^exit=//')"
  local verdict=PASS
  if [ "$expect" = pass ] && [ "$code" != 0 ]; then verdict=FAIL; fi
  if [ "$expect" = fail ] && [ "$code" = 0 ]; then verdict=FAIL; fi
  # Verifica linhas de erro reais, não trechos de fonte que apenas mencionam erros.
  if [ "$expect" = fail ] && grep -Eq '^[[:space:]]*((error:|Error:)[[:space:]]*(Cannot find (module|package)|dist/main[.]js missing|SyntaxError)|SyntaxError:)' "$OUT/$log"; then verdict=FAIL; fi
  if grep -Eq '^ *[1-9][0-9]* (skip|todo)$' "$OUT/$log"; then verdict=FAIL; fi
  [ "$verdict" = FAIL ] && FAILED=1
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$expect" "$code" "$verdict" "$((SECONDS - started))" "$log" >> "$SUMMARY"
  printf '[%s] %-58s expect=%s exit=%s %ss\n' "$verdict" "$name" "$expect" "$code" "$((SECONDS - started))"
}

compose_main() { JWP_PG_PORT="$PG_PORT" JWP_SQS_PORT="$SQS_PORT" docker compose -p "$PROJECT" -f compose.db.yml -f compose.sqs.yml "$@"; }
compose_obs_sqs() { JWP_SQS_PORT="$OBS_SQS_PORT" docker compose -p "$OBS_PROJECT" -f compose.sqs.yml "$@"; }

# Limpeza atinge somente containers dedicados com sufixo -repro.
fresh_pg() {
  local name="$1" port="$2"
  case "$name" in *-repro) ;; *) echo "refusing container $name" >&2; return 1 ;; esac
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" -e POSTGRES_DB=jwp -e POSTGRES_USER=jwp -e POSTGRES_PASSWORD="$PG_PASSWORD" \
    -p "127.0.0.1:$port:5432" "$PG_IMAGE" >/dev/null || return 1
  for _ in $(seq 1 150); do
    # TCP distingue o servidor definitivo do temporário de inicialização, que usa socket.
    docker exec "$name" pg_isready -h 127.0.0.1 -U jwp -d jwp >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "postgres $name not ready" >&2; return 1
}
pg_url() { printf 'postgresql://jwp:%s@127.0.0.1:%s/jwp' "$PG_PASSWORD" "$1"; }
DEDICATED=()
suite_pg() { DEDICATED+=("$1"); run_step "provision $1" pass fresh_pg "$1" "$2"; }

cleanup() {
  [ "$KEEP" = 1 ] && return
  for name in "${DEDICATED[@]}"; do docker rm -f "$name" >/dev/null 2>&1 || true; done
  compose_obs_sqs down -v >/dev/null 2>&1 || true
  compose_main down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "reproduce run $RUN_ID -> $OUT"


{
  echo "run_id=$RUN_ID"
  echo "date_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "compose_project=$PROJECT"
  echo "observability_project=$OBS_PROJECT"
  echo "repro_suffix=${JWP_REPRO_SUFFIX:-}"
  echo "git_head=$(git rev-parse HEAD)"
  echo "git_dirty_paths=$(git status --porcelain | wc -l)"
  echo "bun=$(bun --version)"
  echo "docker=$(docker version --format '{{.Server.Version}}' 2>/dev/null)"
  echo "compose=$(docker compose version --short 2>/dev/null)"
  echo "kernel=$(uname -sr)"
} > "$OUT/versions.txt"
# Hashes registram exatamente as fontes compiladas e testadas.
find src tests scripts/reproduce -type f -print0 | sort -z | xargs -0 sha256sum > "$OUT/source-sha256.txt"


# Instalar e compilar primeiro evita falsos controles negativos por import ausente
# ou dist inexistente, em vez da falha de infraestrutura exigida.
run_step "bun install --frozen-lockfile" pass bun install --frozen-lockfile
run_step "bun run check (typecheck, unit, build, smoke)" pass env "${UNSET[@]}" JWP_SQS_ENDPOINT="$NO_SQS" bun run check


run_step "static: no .skip/.only/.todo/.if in suites" pass bash -c \
  "! grep -rnE '\\b(test|it|describe)\\.(skip|only|todo|if|skipIf|todoIf)\\b' src tests --include='*.test.ts'"

# Validação dos exemplos JSON Schema não usa infraestrutura; deve passar sem PG/SQS.

run_step "static: messaging JSON Schema contracts (no infra needed)" pass env "${UNSET[@]}" JWP_SQS_ENDPOINT="$NO_SQS" \
  bun test tests/integration/sqs/contracts.test.ts


INFRA_SUITES=(
  tests/matrix/delivery/matrix.test.ts
  tests/integration/schema/financial-schema.test.ts
  tests/integration/schema/messaging-migrations.test.ts
  tests/integration/schema/schema-drift.test.ts
  tests/integration/wallet/open-wallet.test.ts
  tests/integration/wager/process-wager.test.ts
  tests/integration/wager/review-regressions.test.ts
  tests/integration/wager/permanent-failure.test.ts
  tests/integration/query/query-reconcile.test.ts
  tests/integration/query/query-outage.test.ts
  tests/integration/query/compiled-runtime.test.ts
  tests/integration/sqs/sqs.test.ts
  tests/integration/messaging/sqs-inbox.test.ts
  tests/integration/messaging/inbox/audit-regressions.test.ts
  tests/integration/messaging/outbox/outbox-publisher.test.ts
  tests/integration/messaging/outbox/audit-regressions.test.ts
  tests/integration/messaging/pending-reference-scheduler.test.ts
  tests/integration/messaging/scheduler/audit-regressions.test.ts
  tests/integration/messaging/consumer-resilience.test.ts
  tests/integration/runtime/workers-entrypoint.test.ts
  tests/integration/runtime/consumer-backlog.test.ts
  tests/integration/observability/readiness.test.ts
  tests/integration/observability/http-observability.test.ts
  tests/integration/observability/messaging-observability.test.ts
  tests/concurrency/multi-process/multi-process.test.ts
  tests/recovery/crash/recovery.test.ts
)
for suite in "${INFRA_SUITES[@]}"; do
  run_step "no-infra fails: $suite" fail env "${UNSET[@]}" JWP_SQS_ENDPOINT="$NO_SQS" bun test "$suite" --timeout 30000
done
run_step "matrix fails with unreachable PG/SQS" fail env "${UNSET[@]}" JWP_SQS_ENDPOINT="$NO_SQS" \
  JWP_MATRIX_DATABASE_URL="postgresql://jwp:x@127.0.0.1:9/jwp" JWP_MATRIX_SQS_ENDPOINT="$NO_SQS" \
  bun test tests/matrix/delivery --timeout 30000


run_step "compose down -v (previous $PROJECT state)" pass compose_main down -v --remove-orphans
run_step "compose up db+sqs --wait" pass compose_main up -d --wait
run_step "versions: postgres" pass compose_main exec -T postgres psql -U jwp -d jwp -Atc 'SELECT version()'
run_step "versions: localstack" pass curl -sf "http://127.0.0.1:$SQS_PORT/_localstack/health"
run_step "versions: sqs topology from init hook" pass compose_main exec -T sqs awslocal sqs list-queues
{
  echo "postgres=$(compose_main exec -T postgres psql -U jwp -d jwp -Atc 'SHOW server_version' 2>/dev/null)"
  echo "postgres_image=$PG_IMAGE"
  echo "localstack=$(curl -sf "http://127.0.0.1:$SQS_PORT/_localstack/health" | sed -E 's/.*"version": *"([^"]+)".*/\1/')"
  echo "localstack_image=$(docker inspect --format '{{.Config.Image}}' "$PROJECT-sqs-1" 2>/dev/null)"
  echo "postgres_image_id=$(docker inspect --format '{{.Image}}' "$PROJECT-postgres-1" 2>/dev/null)"
  echo "localstack_image_id=$(docker inspect --format '{{.Image}}' "$PROJECT-sqs-1" 2>/dev/null)"
} >> "$OUT/versions.txt"


MATRIX_DB="postgresql://jwp:jwp_local@127.0.0.1:$PG_PORT/jwp"
SQS_URL="http://127.0.0.1:$SQS_PORT"
run_step "matrix: tests/matrix/delivery" pass env "${UNSET[@]}" JWP_SQS_ENDPOINT="$SQS_URL" \
  JWP_MATRIX_DATABASE_URL="$MATRIX_DB" JWP_MATRIX_SQS_ENDPOINT="$SQS_URL" bun test tests/matrix/delivery --timeout 200000
grep -h '^{"proof"' "$OUT"/*-matrix_*.txt > "$OUT/matrix-proofs.jsonl" 2>/dev/null || true


if [ "$MATRIX_ONLY" = 0 ]; then
  port=$PORT_BASE
  regress() {
    local container="$1" suite="$2" timeout="$3"; shift 3
    port=$((port + 1))
    suite_pg "$container" "$port"
    local url; url="$(pg_url "$port")"
    local -a assignments=()
    for pair in "$@"; do assignments+=("${pair//@URL@/$url}"); done
    run_step "regression: $suite" pass env "${UNSET[@]}" JWP_SQS_ENDPOINT="$SQS_URL" "${assignments[@]}" bun test "$suite" --timeout "$timeout"
  }
  regress "$(jwp_repro_container schema)" tests/integration/schema/financial-schema.test.ts 60000 JWP_SCHEMA_DATABASE_URL=@URL@
  # Guarda de drift entre migrations SQL e entidades MikroORM (JWP-ADR-015); exige banco vazio próprio.
  regress "$(jwp_repro_container schemadrift)" tests/integration/schema/schema-drift.test.ts 60000 JWP_SCHEMA_DATABASE_URL=@URL@
  # A role jwp_runtime pertence ao cluster; migrations de mensageria exigem cluster vazio próprio.
  regress "$(jwp_repro_container msgschema)" tests/integration/schema/messaging-migrations.test.ts 60000 JWP_MESSAGING_SCHEMA_DATABASE_URL=@URL@
  regress "$(jwp_repro_container wallet)" tests/integration/wallet 60000 JWP_WALLET_DATABASE_URL=@URL@
  regress "$(jwp_repro_container wager)" tests/integration/wager/process-wager.test.ts 120000 JWP_WAGER_DATABASE_URL=@URL@
  regress "$(jwp_repro_container wagerfix)" tests/integration/wager/review-regressions.test.ts 120000 JWP_WAGER_DATABASE_URL=@URL@ JWP_WAGER_POSTGRES_CONTAINER="$(jwp_repro_container wagerfix)"
  regress "$(jwp_repro_container wagerfix)" tests/integration/wager/permanent-failure.test.ts 120000 JWP_WAGER_DATABASE_URL=@URL@ JWP_WAGER_POSTGRES_CONTAINER="$(jwp_repro_container wagerfix)"
  regress "$(jwp_repro_container query)" tests/integration/query/query-reconcile.test.ts 120000 JWP_QUERY_DATABASE_URL=@URL@ JWP_QUERY_POSTGRES_CONTAINER="$(jwp_repro_container query)"
  regress "$(jwp_repro_container query)" tests/integration/query/query-outage.test.ts 120000 JWP_QUERY_DATABASE_URL=@URL@ JWP_QUERY_POSTGRES_CONTAINER="$(jwp_repro_container query)"
  regress "$(jwp_repro_container query)" tests/integration/query/compiled-runtime.test.ts 120000 JWP_QUERY_DATABASE_URL=@URL@ JWP_QUERY_POSTGRES_CONTAINER="$(jwp_repro_container query)"
  run_step "regression: tests/integration/sqs" pass env "${UNSET[@]}" JWP_SQS_ENDPOINT="$SQS_URL" bun test tests/integration/sqs --timeout 30000
  regress "$(jwp_repro_container inbox)" tests/integration/messaging/sqs-inbox.test.ts 120000 JWP_MESSAGING_DATABASE_URL=@URL@ JWP_MESSAGING_POSTGRES_CONTAINER="$(jwp_repro_container inbox)"
  regress "$(jwp_repro_container inbox)" tests/integration/messaging/inbox 120000 JWP_MESSAGING_DATABASE_URL=@URL@ JWP_MESSAGING_POSTGRES_CONTAINER="$(jwp_repro_container inbox)"

  regress "$(jwp_repro_container resilience)" tests/integration/messaging/consumer-resilience.test.ts 200000 JWP_MESSAGING_DATABASE_URL=@URL@ JWP_MESSAGING_POSTGRES_CONTAINER="$(jwp_repro_container resilience)"
  regress "$(jwp_repro_container outbox)" tests/integration/messaging/outbox/outbox-publisher.test.ts 120000 JWP_OUTBOX_DATABASE_URL=@URL@ JWP_OUTBOX_POSTGRES_CONTAINER="$(jwp_repro_container outbox)"
  regress "$(jwp_repro_container outbox)" tests/integration/messaging/outbox/audit-regressions.test.ts 120000 JWP_OUTBOX_DATABASE_URL=@URL@ JWP_OUTBOX_POSTGRES_CONTAINER="$(jwp_repro_container outbox)"
  regress "$(jwp_repro_container scheduler)" tests/integration/messaging/pending-reference-scheduler.test.ts 120000 JWP_SCHEDULER_DATABASE_URL=@URL@ JWP_SCHEDULER_POSTGRES_CONTAINER="$(jwp_repro_container scheduler)"
  regress "$(jwp_repro_container scheduler)" tests/integration/messaging/scheduler 120000 JWP_SCHEDULER_DATABASE_URL=@URL@ JWP_SCHEDULER_POSTGRES_CONTAINER="$(jwp_repro_container scheduler)"
  run_step "provision observability SQS ($OBS_PROJECT)" pass compose_obs_sqs up -d --wait
  regress "$(jwp_repro_container observability)" tests/integration/observability 120000 JWP_OBSERVABILITY_DATABASE_URL=@URL@ \
    JWP_OBSERVABILITY_POSTGRES_CONTAINER="$(jwp_repro_container observability)" JWP_OBSERVABILITY_SQS_ENDPOINT="http://127.0.0.1:$OBS_SQS_PORT" \
    JWP_OBSERVABILITY_SQS_CONTAINER="${OBS_PROJECT}-sqs-1"
  regress "$(jwp_repro_container concurrency)" tests/concurrency/multi-process 200000 JWP_CONCURRENCY_DATABASE_URL=@URL@ JWP_CONCURRENCY_POSTGRES_CONTAINER="$(jwp_repro_container concurrency)"
  regress "$(jwp_repro_container recovery)" tests/recovery/crash 300000 JWP_RECOVERY_DATABASE_URL=@URL@ JWP_RECOVERY_POSTGRES_CONTAINER="$(jwp_repro_container recovery)"

  regress "$(jwp_repro_container runtime)" tests/integration/runtime/workers-entrypoint.test.ts 200000 JWP_RUNTIME_TEST_DATABASE_URL=@URL@
  # Cluster exclusivo evita herdar migrations da suíte de entrypoint no backlog.
  regress "$(jwp_repro_container consumer)" tests/integration/runtime/consumer-backlog.test.ts 200000 \
    JWP_CONSUMER_DATABASE_URL=@URL@ JWP_CONSUMER_BENCH_ROUNDS=1 JWP_CONSUMER_BENCH_MESSAGES=200
fi


{
  echo "steps=$STEP"
  echo "failed_steps=$(awk -F'\t' 'NR>1 && $4=="FAIL"' "$SUMMARY" | wc -l)"

  for pair in 'tests_pass:pass' 'tests_fail:fail' 'expect_calls:expect\(\) calls'; do
    printf '%s=' "${pair%%:*}"
    awk -F'\t' 'NR>1 && $2=="pass" {print $6}' "$SUMMARY" | sed "s|^|$OUT/|" | xargs grep -hE "^ *[0-9]+ ${pair#*:}\$" 2>/dev/null \
      | awk '{s+=$1} END {print s+0}'
  done
} > "$OUT/totals.txt"
cat "$OUT/totals.txt"
if [ "$FAILED" = 0 ]; then echo "REPRODUCE: ALL STEPS AS EXPECTED"; exit 0; fi
echo "REPRODUCE: UNEXPECTED RESULTS (see $SUMMARY)"; exit 1
