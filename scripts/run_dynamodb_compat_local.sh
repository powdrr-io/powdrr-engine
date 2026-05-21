#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/es_compat/docker-compose.yml"
REUSE_EXISTING_STACK="${REUSE_EXISTING_STACK:-0}"
REDIS_URL="${POWDRR_TEST_REDIS_URL:-redis://127.0.0.1:6379/}"
REDIS_HOST_PORT="${REDIS_URL#redis://}"
REDIS_HOST_PORT="${REDIS_HOST_PORT%%/*}"
REDIS_HOST="${REDIS_HOST_PORT%%:*}"
REDIS_PORT="${REDIS_HOST_PORT##*:}"

if [[ "$REDIS_HOST" == "$REDIS_HOST_PORT" ]]; then
  REDIS_PORT="6379"
fi

if [[ "$REDIS_HOST_PORT" == "$REDIS_PORT" ]]; then
  REDIS_HOST="127.0.0.1"
fi

cleanup() {
  if [[ "$REUSE_EXISTING_STACK" != "1" ]]; then
    docker compose -f "$COMPOSE_FILE" down -v
  fi
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

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local label="$3"
  for _ in $(seq 1 60); do
    if (echo >"/dev/tcp/$host/$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for $label at $host:$port" >&2
  return 1
}

trap cleanup EXIT

if [[ "$REUSE_EXISTING_STACK" != "1" ]]; then
  if [[ "$REDIS_URL" == "redis://127.0.0.1:6379/" ]]; then
    docker compose -f "$COMPOSE_FILE" up -d redis localstack rest
  else
    docker compose -f "$COMPOSE_FILE" up -d localstack rest
  fi
fi

wait_for_tcp "$REDIS_HOST" "$REDIS_PORT" "Redis"
wait_for_http "http://localhost:9000/minio/health/live" "MinIO"
wait_for_http "http://localhost:8181" "Iceberg REST catalog"
wait_for_http "http://localhost:4566/_localstack/health" "LocalStack"

cd "$ROOT_DIR"
scripts/cargo-worktree.sh test -p powdrr-query-server --test dynamodb_compatibility_matrix -- --nocapture --test-threads=1
