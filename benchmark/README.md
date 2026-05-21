## Read-Only Serving Benchmark

This benchmark exercises the new protocol-neutral read-only serving path and
compares equivalent query shapes across:

- Powdrr `POST /{table}/_serve`
- Elasticsearch `_search`
- MongoDB `find`

It is intentionally read-only. The benchmark:

1. Reads a Parquet dataset from local disk.
2. Targets Powdrr over HTTP, either by:
   - starting an in-process fallback server when no external URL is configured
   - or hitting a real external `powdrr-io-engine` when `POWDRR_SERVE_BENCH_POWDRR_URL` is set
3. Registers the dataset as an Iceberg-backed serving table through a checkpoint.
4. Loads the same rows into Elasticsearch and MongoDB.
5. Infers a small workload of equivalent serving queries. Depending on the
   dataset, this can include:
   - `top_n`
   - `top_n_desc`
   - `eq_top_n`
   - `eq_top_n_desc`
   - `in_top_n`
   - `in_top_n_desc`
   - `range_top_n` when a numeric field exists
   - `range_top_n_desc` when a numeric field exists
   - `range_lt_top_n` when a numeric field exists
   - `range_lt_top_n_desc` when a numeric field exists
   - `range_window_top_n` when a numeric field has a usable lower and upper bound
   - `range_window_top_n_desc` when a numeric field has a usable lower and upper bound
   - `eq_range_top_n` when both equality and numeric range fields exist
   - `eq_range_top_n_desc` when both equality and numeric range fields exist
   - `eq_window_top_n` when both equality and bounded numeric range filters exist
   - `eq_window_top_n_desc` when both equality and bounded numeric range filters exist
   - `in_range_top_n` when `IN` and numeric range filters can be combined
   - `in_range_top_n_desc` when `IN` and numeric range filters can be combined
6. Verifies that Powdrr, Elasticsearch, and MongoDB return the same rows.
7. Measures latency for each backend.

### Local Run

```bash
bash scripts/run_serving_bench_local.sh
```

This script now starts:

- local Elasticsearch and MongoDB containers from
  `tests/serving_bench/docker-compose.yml`
- local Redis from `tests/redis/docker-compose.yml`
- a real external `powdrr-io-engine` process on a dedicated local port

It then runs the focused serving tests and launches the benchmark binary
against that external Powdrr HTTP endpoint, so the Powdrr and Elasticsearch
paths both cross a real process boundary.

For a faster local smoke run on a cold machine, you can temporarily drop the
script out of release mode:

```bash
POWDRR_SERVE_BENCH_RELEASE=0 bash scripts/run_serving_bench_local.sh
```

### Environment

- `POWDRR_SERVE_BENCH_DATASET`
  Default: `testdata/flights.parquet`
- `POWDRR_SERVE_BENCH_LIMIT`
  Default: `25`
  Note: the benchmark may reduce the effective per-case limit to avoid
  ambiguous tie cutoffs for ordered comparisons.
- `POWDRR_SERVE_BENCH_ITERATIONS`
  Default: `20`
- `POWDRR_SERVE_BENCH_WARMUP`
  Default: `5`
- `POWDRR_SERVE_BENCH_POWDRR_URL`
  When set, use an external Powdrr HTTP server instead of the in-process
  fallback.
- `POWDRR_SERVE_BENCH_RELEASE`
  Script-only toggle. Default: `1`. Set to `0` for a faster debug-mode smoke
  run.
- `POWDRR_SERVE_BENCH_ES_URL`
  Default: `http://localhost:9200`
- `POWDRR_SERVE_BENCH_MONGO_URI`
  Default: `mongodb://localhost:27017`
- `POWDRR_SERVE_BENCH_SORT_FIELD`
  Force the benchmark to use a specific serving sort field.
- `POWDRR_SERVE_BENCH_SKIP_ES`
- `POWDRR_SERVE_BENCH_SKIP_MONGO`

### What This Measures

This is not a write benchmark and it is not an ES-compatibility benchmark. It
is a serving-path latency benchmark for bounded document-serving workloads over
lakehouse data using protocols clients already know.

## Elasticsearch Workload Benchmark

The compatibility workload benchmark is separate from the serving benchmark. It
replays selected differential `logs-*` fixture cases from
`testdata/es_compat_cases.json` against:

- an in-process Powdrr HTTP server
- a real Elasticsearch node

This is the right benchmark when you want relative latency for the actual
read-only ES workload slice, including:

- filtered `query_string`
- wildcard multi-index `search_after`
- `date_histogram` with empty-bucket options
- `terms` with missing-bucket and `_key` ordering
- nested bucket and metric aggregations

Local run:

```bash
bash scripts/run_es_workload_bench_local.sh
```
