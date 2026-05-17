#!/usr/bin/env bash
set -euo pipefail

quiet=0
if [[ "${1:-}" == "--quiet" ]]; then
  quiet=1
  shift
fi

if [[ $# -ne 0 ]]; then
  echo "usage: $0 [--quiet]" >&2
  exit 1
fi

git_common_dir="$(git rev-parse --git-common-dir)"
repo_root="$(cd "$(dirname "$git_common_dir")" && pwd)"
worktree_root="$(git rev-parse --show-toplevel)"
shared_target="$repo_root/.cargo-target"
worktree_target="$worktree_root/target"

mkdir -p "$shared_target"

if [[ -L "$worktree_target" ]]; then
  existing_target="$(readlink "$worktree_target")"
  if [[ "$existing_target" != "$shared_target" ]]; then
    echo "error: $worktree_target points to $existing_target, expected $shared_target" >&2
    exit 1
  fi
elif [[ -e "$worktree_target" ]]; then
  echo "error: $worktree_target already exists and is not the shared target symlink" >&2
  echo "move it aside or remove it, then rerun $0" >&2
  exit 1
else
  ln -s "$shared_target" "$worktree_target"
fi

if [[ "$quiet" -eq 0 ]]; then
  printf 'shared cargo target: %s\n' "$shared_target"
  printf 'worktree target link: %s -> %s\n' "$worktree_target" "$shared_target"
fi
