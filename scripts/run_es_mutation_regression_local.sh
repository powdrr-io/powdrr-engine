#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_redis() {
  if ! (echo > /dev/tcp/127.0.0.1/6379) >/dev/null 2>&1; then
    echo "Redis must be listening on 127.0.0.1:6379 for ES mutation regression tests." >&2
    exit 1
  fi
}

cd "$ROOT_DIR"
require_redis

tests=(
  router::tests::test_es_put_doc_with_id_replaces_existing_doc
  router::tests::test_es_update_single_merges_existing_doc
  router::tests::test_es_bulk_update_merges_existing_doc_after_refresh
)

for test_name in "${tests[@]}"; do
  echo "Running ${test_name}"
  scripts/cargo-worktree.sh test -p powdrr_lib "$test_name" --lib -- --exact --test-threads=1
done
