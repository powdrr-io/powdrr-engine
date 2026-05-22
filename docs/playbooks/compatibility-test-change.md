# Playbook: Compatibility Test Change

Use this when you are changing fixtures, manifests, or compatibility matrices.

## Start Here

- [query_server/tests/](../../query_server/tests)
- [testdata/](../../testdata)
- matrix docs:
  - [docs/es-compatibility-matrix.md](../es-compatibility-matrix.md)
  - [docs/dynamodb-compatibility-matrix.md](../dynamodb-compatibility-matrix.md)

## Typical Steps

1. Update the fixture or manifest in `testdata/`.
2. Update the matching test under `query_server/tests/`.
3. If the scope of supported behavior changed, update the matrix doc too.
4. Prefer adding diagnostics to the harness only long enough to learn something;
   do not leave debug churn behind.

## Tests To Run

- targeted syntax/fixture checks:
  `scripts/cargo-worktree.sh check -p powdrr-query-server --test <test-name>`
- full compatibility harness when the contract changed:
  - `scripts/run_es_compat_local.sh`
  - `scripts/run_dynamodb_compat_local.sh`
  - `scripts/run_dynamodb_sdk_compat_local.sh`
  - `scripts/run_es_mutation_regression_local.sh`

## Common Mistakes

- changing fixture JSON without updating the coverage manifest
- landing temporary CI diagnostics
- only running unit tests when the real contract lives in the matrix harness
