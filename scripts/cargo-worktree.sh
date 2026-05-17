#!/usr/bin/env bash
set -euo pipefail

git_common_dir="$(git rev-parse --git-common-dir)"
repo_root="$(cd "$(dirname "$git_common_dir")" && pwd)"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$script_dir/setup-worktree.sh" --quiet

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$repo_root/.cargo-target}"

exec cargo "$@"
