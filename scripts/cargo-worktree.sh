#!/usr/bin/env bash
set -euo pipefail

git_common_dir="$(git rev-parse --git-common-dir)"
repo_root="$(cd "$(dirname "$git_common_dir")" && pwd)"

sanitize_shard() {
  local value="$1"
  value="${value//[^[:alnum:]._:-]/-}"
  value="${value##[-.]}"
  value="${value%%[-.]}"
  if [[ -z "$value" ]]; then
    value="workspace"
  fi
  printf '%s\n' "$value"
}

manifest_shard() {
  local manifest_path="$1"
  local abs_path
  if [[ "$manifest_path" = /* ]]; then
    abs_path="$manifest_path"
  else
    abs_path="$PWD/$manifest_path"
  fi
  sanitize_shard "$(basename "$(dirname "$abs_path")")"
}

infer_shard() {
  local package=""
  local manifest=""
  local package_count=0
  local expect_package=0
  local expect_manifest=0
  local arg

  for arg in "$@"; do
    if [[ "$arg" == "--" ]]; then
      break
    fi

    if (( expect_package )); then
      package="$arg"
      package_count=$((package_count + 1))
      expect_package=0
      continue
    fi

    if (( expect_manifest )); then
      manifest="$arg"
      expect_manifest=0
      continue
    fi

    case "$arg" in
      -p|--package)
        expect_package=1
        ;;
      --manifest-path)
        expect_manifest=1
        ;;
      --target-dir)
        printf '%s\n' ""
        return
        ;;
      --package=*)
        package="${arg#--package=}"
        package_count=$((package_count + 1))
        ;;
      --manifest-path=*)
        manifest="${arg#--manifest-path=}"
        ;;
      --target-dir=*)
        printf '%s\n' ""
        return
        ;;
    esac
  done

  if (( package_count == 1 )); then
    sanitize_shard "$package"
    return
  fi

  if [[ -n "$manifest" ]]; then
    manifest_shard "$manifest"
    return
  fi

  printf '%s\n' "workspace"
}

if [[ -z "${CARGO_TARGET_DIR:-}" ]]; then
  shard="${POWDRR_CARGO_SHARD:-$(infer_shard "$@")}"
  if [[ -n "$shard" ]]; then
    export CARGO_TARGET_DIR="$repo_root/.cargo-target/$(sanitize_shard "$shard")"
  fi
fi

exec cargo "$@"
