#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local label="$3"
  local attempts="${4:-1}"

  for _ in $(seq 1 "$attempts"); do
    if (echo > "/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done

  echo "${label} must be reachable at ${host}:${port} for ES mutation regression tests." >&2
  exit 1
}

wait_for_http() {
  local url="$1"
  local label="$2"
  local attempts="${3:-1}"

  for _ in $(seq 1 "$attempts"); do
    if curl -sS -o /dev/null "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done

  echo "${label} must be reachable at ${url} for ES mutation regression tests." >&2
  exit 1
}

require_test_stack() {
  local attempts=1
  if [[ "${POWDRR_WAIT_FOR_STACK:-0}" == "1" ]]; then
    attempts=60
  fi

  wait_for_tcp "127.0.0.1" "6379" "Redis" "$attempts"
  wait_for_http "http://localhost:9000/minio/health/live" "MinIO" "$attempts"
  wait_for_http "http://localhost:8181" "Iceberg REST catalog" "$attempts"
  wait_for_http "http://localhost:4566/_localstack/health" "LocalStack" "$attempts"
}

cd "$ROOT_DIR"
require_test_stack

tests=(
  router::tests::test_es_put_doc_with_id_replaces_existing_doc
  router::tests::test_es_update_single_merges_existing_doc
  router::tests::test_es_bulk_update_merges_existing_doc_after_refresh
)

for test_name in "${tests[@]}"; do
  echo "Running ${test_name}"
  scripts/cargo-worktree.sh test -p powdrr_lib "$test_name" --lib -- --exact --test-threads=1
done
