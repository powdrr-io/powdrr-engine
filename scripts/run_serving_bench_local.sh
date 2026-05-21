#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/serving_bench/docker-compose.yml"
REDIS_COMPOSE_FILE="$ROOT_DIR/tests/redis/docker-compose.yml"
POWDRR_PORT="${POWDRR_SERVE_BENCH_POWDRR_PORT:-19200}"
POWDRR_URL="${POWDRR_SERVE_BENCH_POWDRR_URL:-http://127.0.0.1:${POWDRR_PORT}}"
POWDRR_START_TIMEOUT_SECS="${POWDRR_SERVE_BENCH_ENGINE_START_TIMEOUT_SECS:-1800}"
POWDRR_RELEASE="${POWDRR_SERVE_BENCH_RELEASE:-1}"
ENGINE_LOG="$(mktemp -t powdrr-serving-bench-engine.XXXXXX.log)"
ENGINE_PID=""

ENGINE_RELEASE_FLAG=""
BENCH_RELEASE_FLAG=""
if [[ "$POWDRR_RELEASE" == "1" || "$POWDRR_RELEASE" == "true" || "$POWDRR_RELEASE" == "TRUE" ]]; then
  ENGINE_RELEASE_FLAG="--release"
  BENCH_RELEASE_FLAG="--release"
fi

cleanup() {
  if [[ -n "${ENGINE_PID}" ]] && kill -0 "${ENGINE_PID}" >/dev/null 2>&1; then
    kill "${ENGINE_PID}" >/dev/null 2>&1 || true
    wait "${ENGINE_PID}" >/dev/null 2>&1 || true
  fi
  docker compose -f "$COMPOSE_FILE" down -v
  docker compose -f "$REDIS_COMPOSE_FILE" down -v
  rm -f "$ENGINE_LOG"
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

wait_for_engine() {
  local url="$1"
  local attempts=$((POWDRR_START_TIMEOUT_SECS / 2))
  if [[ "$attempts" -lt 1 ]]; then
    attempts=1
  fi
  for _ in $(seq 1 "$attempts"); do
    if curl -sS -o /dev/null "$url" >/dev/null 2>&1; then
      return 0
    fi
    if [[ -n "${ENGINE_PID}" ]] && ! kill -0 "${ENGINE_PID}" >/dev/null 2>&1; then
      echo "Powdrr engine exited before becoming ready. Log output:" >&2
      cat "$ENGINE_LOG" >&2
      return 1
    fi
    sleep 2
  done
  echo "Timed out waiting for Powdrr engine at $url" >&2
  if [[ -f "$ENGINE_LOG" ]]; then
    tail -n 200 "$ENGINE_LOG" >&2
  fi
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

wait_for_redis() {
  for _ in $(seq 1 60); do
    if docker compose -f "$REDIS_COMPOSE_FILE" exec -T redis redis-cli ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for Redis" >&2
  return 1
}

trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" up -d
docker compose -f "$REDIS_COMPOSE_FILE" up -d redis

wait_for_http "http://localhost:9200" "Elasticsearch"
wait_for_mongo
wait_for_redis

cd "$ROOT_DIR"
echo "Starting powdrr-io-engine for serving benchmark. Logs: $ENGINE_LOG"
MODE=default PORT="$POWDRR_PORT" \
  "$ROOT_DIR/scripts/cargo-worktree.sh" run ${ENGINE_RELEASE_FLAG:+$ENGINE_RELEASE_FLAG} -p powdrr-io-engine \
  >"$ENGINE_LOG" 2>&1 &
ENGINE_PID=$!

wait_for_engine "${POWDRR_URL}/_cluster/health"

"$ROOT_DIR/scripts/cargo-worktree.sh" test -p powdrr-query-runtime --lib serving_protocol::tests -- --nocapture
"$ROOT_DIR/scripts/cargo-worktree.sh" test -p powdrr-query-runtime --lib lakehouse_serving::tests -- --nocapture
"$ROOT_DIR/scripts/cargo-worktree.sh" test -p powdrr-query-server --lib router::tests::test_serving_config_and_fast_path_query -- --nocapture
POWDRR_SERVE_BENCH_POWDRR_URL="$POWDRR_URL" \
  "$ROOT_DIR/scripts/cargo-worktree.sh" run ${BENCH_RELEASE_FLAG:+$BENCH_RELEASE_FLAG} -p powdrr-benchmark
