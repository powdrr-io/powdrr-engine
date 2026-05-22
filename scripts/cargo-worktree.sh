#!/usr/bin/env bash
set -euo pipefail

git_common_dir="$(git rev-parse --git-common-dir)"
repo_root="$(cd "$(dirname "$git_common_dir")" && pwd)"
worktree_root="$(git rev-parse --show-toplevel)"

if [[ -z "${CARGO_TARGET_DIR:-}" ]]; then
  export CARGO_TARGET_DIR="$worktree_root/target"
fi

if [[ -z "${CARGO_BUILD_BUILD_DIR:-}" ]]; then
  export CARGO_BUILD_BUILD_DIR="$repo_root/.cargo-build"
fi

exec cargo "$@"
