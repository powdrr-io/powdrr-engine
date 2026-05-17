## Read-Only Serving Benchmark

This benchmark exercises the new protocol-neutral read-only serving path and
compares equivalent query shapes across:

- Powdrr `POST /{table}/_serve`
- Elasticsearch `_search`
- MongoDB `find`

It is intentionally read-only. The benchmark:

1. Reads a Parquet dataset from local disk.
2. Starts an in-process Powdrr test server.
3. Registers the dataset as an Iceberg-backed serving table through a checkpoint.
4. Loads the same rows into Elasticsearch and MongoDB.
5. Infers a small workload of equivalent serving queries:
   - `top_n`
   - `eq_top_n`
   - `in_top_n`
   - `range_top_n` when a numeric field exists
6. Verifies that Powdrr, Elasticsearch, and MongoDB return the same rows.
7. Measures latency for each backend.

### Local Run

```bash
bash scripts/run_serving_bench_local.sh
```

This starts local Elasticsearch and MongoDB containers from
`tests/serving_bench/docker-compose.yml`, runs the focused serving tests, and
then launches the benchmark binary.

### Environment

- `POWDRR_SERVE_BENCH_DATASET`
  Default: `main_lib/tests/data/flights.parquet`
- `POWDRR_SERVE_BENCH_LIMIT`
  Default: `25`
- `POWDRR_SERVE_BENCH_ITERATIONS`
  Default: `20`
- `POWDRR_SERVE_BENCH_WARMUP`
  Default: `5`
- `POWDRR_SERVE_BENCH_ES_URL`
  Default: `http://localhost:9200`
- `POWDRR_SERVE_BENCH_MONGO_URI`
  Default: `mongodb://localhost:27017`
- `POWDRR_SERVE_BENCH_SKIP_ES`
- `POWDRR_SERVE_BENCH_SKIP_MONGO`

### What This Measures

This is not a write benchmark and it is not an ES-compatibility benchmark. It
is a serving-path latency benchmark for bounded document-serving workloads over
lakehouse data using protocols clients already know.
