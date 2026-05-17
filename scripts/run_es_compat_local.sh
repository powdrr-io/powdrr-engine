#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/es_compat/docker-compose.yml"

cleanup() {
  docker compose -f "$COMPOSE_FILE" down -v
}

wait_for_http() {
  local url="$1"
  local label="$2"
  for _ in $(seq 1 60); do
    if curl -sS -o /dev/null "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for $label at $url" >&2
  return 1
}

trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" up -d

wait_for_http "http://localhost:9000/minio/health/live" "MinIO"
wait_for_http "http://localhost:8181" "Iceberg REST catalog"
wait_for_http "http://localhost:4566/_localstack/health" "LocalStack"
wait_for_http "http://localhost:9200" "Elasticsearch"

cd "$ROOT_DIR"
./scripts/cargo-worktree.sh test -p powdrr_lib --test es_compatibility_matrix compatibility_matrix_case_file_parses_and_ids_are_unique -- --nocapture
./scripts/cargo-worktree.sh test -p powdrr_lib --test es_compatibility_matrix compatibility_matrix_local_current_engine -- --nocapture
POWDRR_ES_COMPAT_URL="http://localhost:9200" ./scripts/cargo-worktree.sh test -p powdrr_lib --test es_compatibility_matrix compatibility_matrix_differential_when_external_es_is_configured -- --nocapture
