# Playbook: Serving Engine Change

Use this when you are changing the shared serving path, not just an HTTP
handler.

## Start Here

- [query_runtime/src/serving_protocol.rs](../../query_runtime/src/serving_protocol.rs)
- [query_runtime/src/lakehouse_serving.rs](../../query_runtime/src/lakehouse_serving.rs)
- [query_runtime/src/search_executor.rs](../../query_runtime/src/search_executor.rs)
- [query_lib/src/data_access.rs](../../query_lib/src/data_access.rs)
- [query_core/src/serving_plan.rs](../../query_core/src/serving_plan.rs)

## Typical Steps

1. Identify whether the change belongs in plan typing, runtime orchestration, or
   low-level file access.
2. Keep protocol-specific translation in `query_server`; change the shared
   engine only when all frontends should benefit.
3. Update docs describing fast-path vs slow-path behavior if that contract
   changed.

## Tests To Run

- `scripts/cargo-worktree.sh check -p powdrr-query-runtime`
- `scripts/cargo-worktree.sh check -p powdrr-query-server`
- targeted runtime/server tests for the touched path
- when serving behavior changes materially:
  `scripts/run_serving_bench_local.sh`

## Common Mistakes

- burying plan changes in runtime code without updating `query_core`
- changing a serving fast-path assumption without adding a regression test
- fixing a protocol symptom in `query_server` when the real issue is the shared
  serving path
