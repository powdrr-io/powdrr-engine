# Playbook: Protocol Change

Use this when you are changing an external protocol surface such as:

- Elasticsearch-compatible HTTP behavior
- DynamoDB-compatible HTTP behavior
- Mongo or Redis protocol bridges
- native serving HTTP routes

## Start Here

- [query_server/src/router.rs](../../query_server/src/router.rs)
- protocol-specific modules in [query_server/src/](../../query_server/src)
- [docs/architecture.md](../architecture.md)

## Typical Steps

1. Find the route or entrypoint in `query_server`.
2. Confirm which shared runtime path the handler should call.
3. Update request/response shaping in `query_server`, not in ad hoc helper
   code in the binaries.
4. Only add new shared DTOs to `query_core` if more than one layer needs them.

## Tests To Run

- targeted crate check:
  `scripts/cargo-worktree.sh check -p powdrr-query-server`
- server/router-focused tests when applicable:
  `scripts/cargo-worktree.sh test -p powdrr-query-server --lib <test-name> -- --nocapture`
- compatibility suites when the protocol contract changed:
  - Elasticsearch: `scripts/run_es_compat_local.sh`
  - DynamoDB: `scripts/run_dynamodb_compat_local.sh`
  - DynamoDB SDK: `scripts/run_dynamodb_sdk_compat_local.sh`

## Common Mistakes

- putting protocol-specific branching into `query_runtime` instead of
  translating at the edge
- adding new fixtures without updating the compatibility manifests
- changing handler behavior without updating the README or matrix docs
