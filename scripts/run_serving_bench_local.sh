#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/serving_bench/docker-compose.yml"

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

wait_for_mongo() {
  for _ in $(seq 1 60); do
    if docker compose -f "$COMPOSE_FILE" exec -T mongodb mongosh --quiet --eval 'db.runCommand({ ping: 1 })' >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for MongoDB" >&2
  return 1
}

trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" up -d

wait_for_http "http://localhost:9200" "Elasticsearch"
wait_for_mongo

cd "$ROOT_DIR"
./scripts/cargo-worktree.sh test -p powdrr_lib --lib serving_protocol::tests -- --nocapture
./scripts/cargo-worktree.sh test -p powdrr_lib --lib lakehouse_serving::tests -- --nocapture
./scripts/cargo-worktree.sh test -p powdrr_lib --lib router::tests::test_serving_config_and_fast_path_query -- --nocapture
./scripts/cargo-worktree.sh run -p powdrr-benchmark
