# Exact Lookup Performance

This note records the latest exact-lookup benchmark numbers for the mmap-backed
snapshot lookup path.

The purpose of these measurements is narrow:

- show what the current Redis-style key/value fast path looks like
- show what the current Elasticsearch `_doc` / `_mget` path looks like
- make the remaining gaps visible

These are useful engineering measurements, not a universal product claim. They
were collected on localhost with warmed caches.

## Bottom Line

- Powdrr's warmed mmap-backed exact lookup path is already competitive with
  Redis on very small key/value reads.
- Powdrr stays roughly tied with Redis at `MGET 50`.
- Redis pulls ahead once the batch size reaches `MGET 100`.
- The shared ES exact-id fast path cut Powdrr's local `_doc` and `_mget`
  latency by about `4x`, but real Elasticsearch is still much faster on those
  compatibility endpoints.

## Redis Wire Benchmark

### Setup

- Powdrr release build running from the shared `powdrr-benchmark` shard
- Powdrr Redis wire endpoint on `127.0.0.1:16379`
- Redis container on `127.0.0.1:6379`
- 10k test keys loaded into both systems
- raw RESP over sockets

The small-case run used:

- `100` warmup operations
- `1000` measured operations

The larger batch run used:

- `50` warmup operations
- `500` measured operations

### Results

| Case | Powdrr avg | Powdrr p50 | Powdrr p95 | Powdrr p99 | Redis avg | Redis p50 | Redis p95 | Redis p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `GET key:1` | `0.033 ms` | `0.033` | `0.037` | `0.044` | `0.063 ms` | `0.057` | `0.110` | `0.129` |
| `MGET 5` | `0.065 ms` | `0.064` | `0.074` | `0.080` | `0.081 ms` | `0.080` | `0.089` | `0.101` |
| `MGET 50` | `0.489 ms` | `0.487` | `0.517` | `0.522` | `0.493 ms` | `0.496` | `0.561` | `0.593` |
| `MGET 100` | `0.985 ms` | `0.996` | `1.020` | `1.106` | `0.796 ms` | `0.813` | `0.862` | `0.901` |
| `MGET 100` mixed hit/miss | `1.048 ms` | `1.042` | `1.094` | `1.194` | `0.825 ms` | `0.842` | `0.896` | `0.943` |

### Interpretation

- The current fast path is strong on tiny exact lookups.
- The crossover happens somewhere between `MGET 50` and `MGET 100`.
- The remaining Redis gap is now mostly about larger fanout batch execution,
  not the single-key lookup path.

## Elasticsearch `_doc` / `_mget` Benchmark

### Setup

- `powdrr-benchmark` release binary
- real Elasticsearch on `http://localhost:9200`
- local Powdrr benchmark server
- benchmark cases:
  - `get_existing_doc_returns_source`
  - `table_mget_returns_found_and_missing_docs`
- `3` warmup iterations
- `10` measured iterations

The relevant command was:

```bash
POWDRR_CARGO_SHARD=powdrr-benchmark \
POWDRR_ES_WORKLOAD_BENCH_ES_URL=http://localhost:9200 \
POWDRR_ES_WORKLOAD_BENCH_CASE_IDS=get_existing_doc_returns_source,table_mget_returns_found_and_missing_docs \
rtk scripts/cargo-worktree.sh run --release -p powdrr-benchmark --bin es_workload
```

### Current Results

| Case | Powdrr avg | Powdrr p50 | Powdrr p95 | Powdrr p99 | Elasticsearch avg | Elasticsearch p50 | Elasticsearch p95 | Elasticsearch p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `get_existing_doc_returns_source` | `17.66 ms` | `17.92` | `18.46` | `18.46` | `1.27 ms` | `0.97` | `1.72` | `1.72` |
| `table_mget_returns_found_and_missing_docs` | `17.27 ms` | `16.70` | `18.68` | `18.68` | `0.94 ms` | `0.88` | `0.98` | `0.98` |

### Before / After For Powdrr Local

Routing ES `_doc` and `_mget` through the shared exact-id fast path cut the
local Powdrr numbers substantially in this benchmark:

| Case | Earlier Powdrr avg | Current Powdrr avg |
|---|---:|---:|
| `get_existing_doc_returns_source` | `72.55 ms` | `17.66 ms` |
| `table_mget_returns_found_and_missing_docs` | `65.12 ms` | `17.27 ms` |

### Interpretation

- The server-side routing seam mattered; removing the bespoke ES checkpoint path
  helped a lot.
- Powdrr is still far behind real Elasticsearch on these compatibility
  endpoints.
- The obvious remaining gaps are no longer "wrong path" bugs. They are deeper
  overheads in the ES compatibility surface and its runtime path.

## Caveats

- These are localhost measurements.
- The Redis comparison is not a neutral infrastructure match:
  - Redis is in Docker
  - Powdrr is a host process
- The ES benchmark uses a local Powdrr benchmark server and a real external
  Elasticsearch node.
- Exact numbers will move with CPU, kernel, NVMe, container settings, and
  workload shape.

The numbers are still worth tracking because they show trend direction and tell
us where the next optimization work should go.
