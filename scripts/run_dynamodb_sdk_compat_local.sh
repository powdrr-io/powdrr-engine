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

docker compose -f "$COMPOSE_FILE" up -d

wait_for_tcp "127.0.0.1" "6379" "Redis"
wait_for_http "http://localhost:9000/minio/health/live" "MinIO"
wait_for_http "http://localhost:8181" "Iceberg REST catalog"
wait_for_http "http://localhost:4566/_localstack/health" "LocalStack"

cd "$ROOT_DIR"
scripts/cargo-worktree.sh test -p powdrr_lib --features integration-tests --test dynamodb_sdk_compat -- --nocapture --test-threads=1
