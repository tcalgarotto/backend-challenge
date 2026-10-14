#!/usr/bin/env bash
# Mede a versão candidata final com `bun run test:load` nas duas políticas de reversão.
# Uso: bash scripts/load/final-candidate.sh <diretório de saída>
# Exige árvore de produto limpa (src, tests, scripts, Compose, package.json, bun.lock) para que o commit medido seja o HEAD.
set -euo pipefail
OUT="${1:?informe o diretório de saída, fora de /tmp}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
DIRTY="$(git status --porcelain -- src tests scripts compose.yml compose.db.yml compose.sqs.yml Dockerfile package.json bun.lock)"
if [ -n "$DIRTY" ]; then echo "árvore de produto com mudanças fora do HEAD; faça commit antes de medir" >&2; exit 2; fi
COMMIT="$(git rev-parse HEAD)"
mkdir -p "$OUT"
bun install --frozen-lockfile
for POLICY in per_type per_reference; do
  # Cada política recria a infraestrutura própria do teste de carga (projeto Compose separado).
  JWP_REVERSAL_POLICY="$POLICY" LOAD_OUT_DIR="$OUT/$POLICY" bun run test:load
done
printf '{"commit":"%s","policies":["per_type","per_reference"],"finishedAt":"%s"}\n' "$COMMIT" "$(date -Iseconds)" > "$OUT/run.json"
echo "resultados em $OUT (commit $COMMIT)"
