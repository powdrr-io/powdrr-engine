# Playbook: Metadata or State-Provider Change

Use this when you are changing:

- checkpoint publication semantics
- committed / target / active frontier behavior
- object-store, DynamoDB, ephemeral, or service metadata interactions
- state-provider write/visibility semantics

## Start Here

- [query_runtime/src/state_provider.rs](../../query_runtime/src/state_provider.rs)
- [query_runtime/src/metadata_store.rs](../../query_runtime/src/metadata_store.rs)
- [service_lib/src/metadata_store.rs](../../service_lib/src/metadata_store.rs)
- provider implementations in `query_runtime/src/*service_impl.rs` and
  `service_lib/src/*service_impl.rs`
- [docs/raft-metadata-coherence-design.md](../raft-metadata-coherence-design.md)

## Typical Steps

1. Write down whether the behavior affects committed, target, or active
   frontiers.
2. Check both the runtime side and the service side before changing semantics.
3. Add or update tests that prove the expected divergence or cutover behavior.

## Tests To Run

- `scripts/cargo-worktree.sh check -p powdrr-query-runtime`
- `scripts/cargo-worktree.sh check -p powdrr-service-lib`
- targeted runtime or service tests for frontier/state-provider behavior
- if protocol visibility behavior changes, run the relevant compatibility suite

## Common Mistakes

- changing runtime semantics without updating service-side metadata behavior
- using “committed” when the caller really means “active”
- reintroducing process-local shortcuts for publication or visibility
