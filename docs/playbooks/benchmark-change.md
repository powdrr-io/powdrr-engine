# Playbook: Benchmark Change

Use this when you are changing workload definitions, benchmark harnesses, or
result collection.

## Start Here

- [benchmark/](../../benchmark)
- [scripts/run_serving_bench_local.sh](../../scripts/run_serving_bench_local.sh)
- [README.md](../../README.md) benchmark sections

## Typical Steps

1. Decide whether the change is:
   - query shape coverage
   - harness/runtime setup
   - reporting/output shape
2. Keep the benchmark apples-to-apples across Powdrr and comparison systems.
3. Update benchmark docs when the harness behavior changes.

## Tests To Run

- `scripts/cargo-worktree.sh check -p powdrr-benchmark`
- `scripts/cargo-worktree.sh test -p powdrr-benchmark -- --nocapture`
- `bash -n scripts/run_serving_bench_local.sh`
- when changing real benchmark behavior, run the local benchmark script with the
  smallest settings that still exercise the path

## Common Mistakes

- comparing in-process Powdrr with external Elasticsearch and calling it fair
- changing benchmark defaults without documenting them
- letting benchmark-only support code leak into the shared serving runtime
