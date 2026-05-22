#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/create-worktree.sh [options] <branch>

Create a linked git worktree under .worktrees/ and run a build preset inside it.

Options:
  --base <ref>           Base ref for the new branch. Default: origin/main
  --path <path>          Worktree path. Default: <repo>/.worktrees/<sanitized-branch>
  --build <preset>       Build preset: default, runtime, engine, service, workspace, none
                         Default: default
  --test                 Run the repo-wide test command after the build preset
  --fetch                Run 'git fetch origin' before creating the worktree
  --dry-run              Print planned actions without creating the worktree
  -h, --help             Show this help

Build presets:
  default   check powdrr-io-engine, powdrr-io-service, powdrr-query-runtime, powdrr-query-server
  runtime   check powdrr-query-core, powdrr-query-lib, powdrr-query-runtime, powdrr-query-server
  engine    check powdrr-io-engine
  service   check powdrr-io-service
  workspace check the whole workspace
  none      do not run builds

Examples:
  scripts/create-worktree.sh my-branch
  scripts/create-worktree.sh --fetch --build runtime my-feature
  scripts/create-worktree.sh --path /tmp/powdrr/my-branch --test my-branch
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

sanitize_path_component() {
  local value="$1"
  value="${value//\//-}"
  value="${value//[^[:alnum:]._:-]/-}"
  value="${value##[-.]}"
  value="${value%%[-.]}"
  if [[ -z "$value" ]]; then
    value="workspace"
  fi
  printf '%s\n' "$value"
}

run_or_echo() {
  if (( dry_run )); then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

run_in_worktree() {
  local worktree_path="$1"
  shift

  if (( dry_run )); then
    printf '+ (cd %q &&' "$worktree_path"
    printf ' %q' "$@"
    printf ')\n'
  else
    (
      cd "$worktree_path"
      "$@"
    )
  fi
}

repo_root_from_common_dir() {
  local git_common_dir
  git_common_dir="$(git rev-parse --path-format=absolute --git-common-dir)"
  cd "$(dirname "$git_common_dir")" && pwd
}

build_in_worktree() {
  local worktree_path="$1"

  case "$build_preset" in
    default)
      run_in_worktree "$worktree_path" scripts/cargo-worktree.sh check -p powdrr-io-engine
      run_in_worktree "$worktree_path" scripts/cargo-worktree.sh check -p powdrr-io-service
      run_in_worktree "$worktree_path" scripts/cargo-worktree.sh check -p powdrr-query-runtime
      run_in_worktree "$worktree_path" scripts/cargo-worktree.sh check -p powdrr-query-server
      ;;
    runtime)
      run_in_worktree "$worktree_path" scripts/cargo-worktree.sh check -p powdrr-query-core
      run_in_worktree "$worktree_path" scripts/cargo-worktree.sh check -p powdrr-query-lib
      run_in_worktree "$worktree_path" scripts/cargo-worktree.sh check -p powdrr-query-runtime
      run_in_worktree "$worktree_path" scripts/cargo-worktree.sh check -p powdrr-query-server
      ;;
    engine)
      run_in_worktree "$worktree_path" scripts/cargo-worktree.sh check -p powdrr-io-engine
      ;;
    service)
      run_in_worktree "$worktree_path" scripts/cargo-worktree.sh check -p powdrr-io-service
      ;;
    workspace)
      run_in_worktree "$worktree_path" scripts/cargo-worktree.sh check --workspace
      ;;
    none)
      ;;
    *)
      die "unknown build preset '$build_preset'"
      ;;
  esac

  if (( run_tests )); then
    if (( dry_run )); then
      printf '+ (cd %q && RUST_BACKTRACE=1 %q %q %q %q %q %q)\n' \
        "$worktree_path" \
        "scripts/cargo-worktree.sh" \
        "test" \
        "--" \
        "--nocapture" \
        "--test-threads=1"
    else
      (
        cd "$worktree_path"
        RUST_BACKTRACE=1 scripts/cargo-worktree.sh test -- --nocapture --test-threads=1
      )
    fi
  fi
}

branch=""
base_ref="origin/main"
worktree_path=""
build_preset="default"
run_tests=0
run_fetch=0
dry_run=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base)
      [[ $# -ge 2 ]] || die "--base requires a value"
      base_ref="$2"
      shift 2
      ;;
    --path)
      [[ $# -ge 2 ]] || die "--path requires a value"
      worktree_path="$2"
      shift 2
      ;;
    --build)
      [[ $# -ge 2 ]] || die "--build requires a value"
      build_preset="$2"
      shift 2
      ;;
    --test)
      run_tests=1
      shift
      ;;
    --fetch)
      run_fetch=1
      shift
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      die "unknown option '$1'"
      ;;
    *)
      if [[ -n "$branch" ]]; then
        die "branch already set to '$branch'; unexpected extra argument '$1'"
      fi
      branch="$1"
      shift
      ;;
  esac
done

[[ -n "$branch" ]] || {
  usage
  exit 1
}

repo_root="$(repo_root_from_common_dir)"

if [[ -z "$worktree_path" ]]; then
  worktree_path="$repo_root/.worktrees/$(sanitize_path_component "$branch")"
elif [[ "$worktree_path" != /* ]]; then
  worktree_path="$PWD/$worktree_path"
fi

[[ ! -e "$worktree_path" ]] || die "worktree path already exists: $worktree_path"

if git show-ref --verify --quiet "refs/heads/$branch"; then
  die "branch already exists locally: $branch"
fi

mkdir_parent_cmd=(mkdir -p "$(dirname "$worktree_path")")

echo "Repo root:      $repo_root"
echo "Branch:         $branch"
echo "Base ref:       $base_ref"
echo "Worktree path:  $worktree_path"
echo "Build preset:   $build_preset"
echo "Run tests:      $run_tests"

if (( run_fetch )); then
  run_or_echo git fetch origin
fi

run_or_echo "${mkdir_parent_cmd[@]}"
run_or_echo git worktree add -b "$branch" "$worktree_path" "$base_ref"
build_in_worktree "$worktree_path"

if (( dry_run )); then
  exit 0
fi

echo
echo "Worktree ready:"
echo "  cd $worktree_path"
