# Powdrr Engine

Powdrr is an Iceberg-first project for building a zero-copy lakehouse server:
a serving system that keeps canonical data in open lakehouse storage while
exposing low-latency APIs that application clients already know.

The long-term goal is not "run arbitrary SQL over Parquet faster." The goal is
to serve bounded, declared application query patterns directly from lakehouse
snapshots without forcing users to load the same data into a second full
search, document, or key-value system.

## Why Powdrr

A common problem in ML and data-heavy products is that the most useful
production data is generated offline in the warehouse or lake: feature tables,
scored entities, recommendations, fraud signals, eligibility outputs, or other
results that come from historical joins and heavyweight batch computation.

The painful part is usually not generating that data. The painful part is
serving it. Teams end up building a second "online" loading system just to copy
the latest output into a search cluster, key-value store, or custom cache layer
without serving mixed snapshots or destroying p99 latency every time a new
batch lands.

Powdrr is meant to remove that whole category of work. You point it at an
Iceberg table, Powdrr tracks the serveable snapshot, keeps bounded
snapshot-aware acceleration state, and exposes low-latency read APIs without
requiring a second full data store. When a new snapshot lands, the system is
built to publish a coherent version and warm what it needs before shifting
traffic.

For the longer product explanation, see `docs/why-powdrr.md`.

## Goal

Powdrr is moving toward a serving database with this contract:

- one canonical copy of base data in the lakehouse
- bounded, managed acceleration state owned by Powdrr
- snapshot-consistent reads
- explicit fast-path vs slow-path query classification
- familiar client protocols on top of one shared serving engine

Today the repo is in transition from a search-first architecture toward that
protocol-neutral serving model. The current main branch already contains the
shared serving path and multiple frontend adapters, but it still carries
Elasticsearch compatibility layers and some search-oriented artifacts while the
lakehouse-serving architecture is being generalized.

## What "Zero-Copy Lakehouse Server" Means

In this repo, zero-copy does **not** mean "no auxiliary state exists." It means:

- the source of truth stays in Iceberg metadata and Parquet files on object
  storage
- Powdrr does **not** require a second full warehouse/search/KV copy of the
  table
- Powdrr is allowed to maintain bounded acceleration state such as:
  - metadata caches
  - file and row-group statistics
  - pruning metadata
  - serving indexes
  - hot data caches
- every acceleration artifact is snapshot-aware and tied to a serveable table
  version

The practical promise is: keep the base table in the lakehouse, then add only
the minimal extra state needed to turn selective read workloads into a
database-like serving surface.

For the current storage-role model of `speedboat` vs `Iceberg`, see
`docs/speedboat-vs-iceberg-architecture.md`.

## High-Level Architecture

At a high level, Powdrr looks like this:

```text
clients
  ├─ Elasticsearch-compatible HTTP
  ├─ DynamoDB-compatible HTTP
  ├─ Mongo-shaped HTTP read API
  └─ native serving API
            |
            v
      powdrr-io-engine
        - protocol adapters
        - query classification
        - shared serving planner
        - snapshot/context loading
        - execution over Parquet + serving artifacts
        - private fanout/merge for clustered work
            |
            +--> Iceberg catalog + object storage + Parquet files
            |
            +--> metadata/checkpoint state
                    ^
                    |
              powdrr-io-service
                - table/org metadata
                - checkpoint publication
                - aliases/templates/pipelines
                - background metadata work
```

The important architectural idea is that the protocol frontends are not
supposed to fork the execution engine. Elasticsearch-style, DynamoDB-style,
Mongo-style, and native serving requests should all compile into one shared
serving plan and run against one shared snapshot-aware execution path.

### Main Runtime Pieces

- `query_lib/`
  The shared query/runtime crate. This is the single source of truth for the
  serving planner, serving executor, Elasticsearch compatibility layer,
  DynamoDB adapter, Mongo bridge, Iceberg access, clustered fanout helpers,
  and the local CLI implementation.

- `control_plane/`
  Shared control-plane types and utilities used across the runtime and service
  layers.

- `engine/`
  The main query and serving server. It now depends directly on the shared
  query/runtime crate.

- `service/` and `service_lib/`
  The control-plane service for table metadata, checkpoints, org setup,
  aliases, templates, and related state transitions.

- `query_runtime/`
  The runtime/orchestration crate. This owns ingest, compaction, state
  providers, peer/runtime fanout, local CLI execution, and the snapshot-aware
  serving runtime.

- `cli/`
  A local CLI for building and querying a local Parquet cache through Powdrr's
  search stack without starting the HTTP service.

- `benchmark/`
  An end-to-end serving benchmark that compares equivalent Powdrr,
  Elasticsearch, and Mongo query shapes.

## Supported Protocols

Powdrr currently exposes several protocol surfaces, but they are not all at the
same maturity level.

| Surface | Current shape | Notes |
|---|---|---|
| Native serving API | `PUT /:table/_serve/config`, `POST /:table/_serve` | This is the long-term protocol-neutral serving path. |
| Elasticsearch-compatible HTTP API | Root `/`, index lifecycle, `_bulk`, `_search`, aliases, templates, selected aggregations | Compatibility is tracked as a subset, not full Elasticsearch parity. See `docs/es-compatibility-matrix.md`. |
| DynamoDB-compatible HTTP API | Root `POST /` with `X-Amz-Target: DynamoDB_20120810.*` plus per-table config | Designed for configured tables on top of the shared serving path. |
| Mongo-shaped read API | `POST /:table/_mongo/find`, `POST /_mongo/:database/_command` | Read-only subset over HTTP. This is **not** full Mongo wire-protocol compatibility yet. |
| Control-plane API | `powdrr-io-service` under `/api/v1` | Used for table creation, checkpoint publication, aliases, templates, pipelines, and org management. |

Two important caveats:

- The Elasticsearch surface is still a compatibility layer, not the product
  identity.
- The Mongo work is intentionally an HTTP bridge today. Off-the-shelf MongoDB
  drivers speaking the Mongo wire protocol are a later step.

## Getting Started

### Prerequisites

- Rust 1.92.0 toolchain
- Docker and Docker Compose for local protocol stacks and benchmarks
- Git worktree support if you plan to develop in this repo

### Contributor Workflow

This repo expects day-to-day work to happen in linked worktrees, and Cargo
commands should go through `scripts/cargo-worktree.sh` so worktrees share the
repo-level build cache.

```bash
git fetch origin
git worktree add -b my-branch .worktrees/my-branch origin/main
cd .worktrees/my-branch
scripts/cargo-worktree.sh check -p powdrr-io-engine
```

### Fastest End-to-End Demo

The easiest way to see the shared serving path in action is the local serving
benchmark:

```bash
bash scripts/run_serving_bench_local.sh
```

That script:

- starts local Elasticsearch and MongoDB containers
- starts local Redis
- starts a real `powdrr-io-engine` process on a dedicated local port
- runs focused serving-path tests
- benchmarks equivalent Powdrr, Elasticsearch, and Mongo query shapes

This is the quickest way to see the protocol-neutral serving layer compared
against familiar systems with Powdrr measured over a real external HTTP server
rather than an in-process test harness.

### Run The Servers

Start the control plane:

```bash
scripts/cargo-worktree.sh run -p powdrr-io-service
```

By default it listens on `http://localhost:7784`.

Start the engine:

```bash
scripts/cargo-worktree.sh run -p powdrr-io-engine
```

By default it listens on `http://localhost:9200`.

The engine also supports:

- `MODE=default` for self-only operation
- `MODE=docker` for Docker-based peer discovery
- `PORT=<port>` to change the listening port

Example:

```bash
MODE=docker PORT=9201 scripts/cargo-worktree.sh run -p powdrr-io-engine
```

### Use The Local CLI

If you want a local query loop without running the HTTP engine, `powdrr-cli`
can mirror Parquet files into a local cache and query them with the existing
Elasticsearch JSON query path.

```bash
scripts/cargo-worktree.sh run -p powdrr-cli -- elastic build \
  --source /path/to/parquet-dir \
  --cache-dir /tmp/powdrr-search \
  --table my_table \
  --doc-id-field my_doc_id \
  --replace
```

Analyze a query before running it:

```bash
scripts/cargo-worktree.sh run -p powdrr-cli -- elastic analyze \
  --body '{"query":{"match":{"message":"failed"}}}'
```

Validate that a source table satisfies the current Elastic sidecar contract:

```bash
scripts/cargo-worktree.sh run -p powdrr-cli -- elastic validate \
  --source /path/to/parquet-dir \
  --doc-id-field _id_seq_no
```

Run the query locally:

```bash
scripts/cargo-worktree.sh run -p powdrr-cli -- elastic query \
  --cache-dir /tmp/powdrr-search \
  --body '{"query":{"match":{"message":"failed"}}}'
```

Current constraints:

- the source data must expose a stable scalar document id column in every file
- the clustered/server-side Elastic path still assumes that field is
  `_id_seq_no`
- if you override the field in the local CLI, `--doc-id-field` must be a simple
  SQL identifier made from ASCII letters, numbers, and underscores
- every file must expose at least one additional top-level string column for
  text indexing
- only top-level string columns are tokenized, using whitespace splitting
- for `s3://...` sources, the build step mirrors the source objects into the
  local cache before query execution, and `elastic validate` downloads them
  into a temporary scratch directory before cleaning it up

See [docs/elastic-table-assumptions.md](docs/elastic-table-assumptions.md) for
the full current contract and optional performance recommendations, including
when Parquet bloom filters and page indexes are likely to help.

## Experimental Mongo Wire Listener

The repo also contains an experimental MongoDB wire listener on top of the
shared serving path.

Start the engine with the Mongo listener enabled:

```bash
PORT=9200 MONGO_PORT=27017 scripts/cargo-worktree.sh run -p powdrr-io-engine --release
```

Current scope:

- read-only
- direct-connection clients
- no auth
- backed by tables with explicit `PUT /:table/_mongo/config`
- intended for `hello`, `ping`, discovery, `find`, `getMore`, and
  `killCursors`

## Useful Validation Commands

Targeted checks during development:

```bash
scripts/cargo-worktree.sh check -p powdrr-io-engine
scripts/cargo-worktree.sh check -p powdrr-io-service
scripts/cargo-worktree.sh check -p powdrr-query-runtime
```

General test guidance:

```bash
RUST_BACKTRACE=1 scripts/cargo-worktree.sh test -- --nocapture --test-threads=1
```

Fast Elasticsearch mutation regression guardrail:

```bash
docker compose -f tests/es_compat/docker-compose.yml up -d redis minio createbuckets rest localstack
bash scripts/run_es_mutation_regression_local.sh
```

That runner expects the same local support stack those router tests use:
Redis on `127.0.0.1:6379`, MinIO on `http://localhost:9000`, the Iceberg REST
catalog on `http://localhost:8181`, and LocalStack on `http://localhost:4566`.
The dedicated CI workflow boots that subset automatically.

Heavy compatibility suites are explicit:

```bash
bash scripts/run_es_compat_local.sh
bash scripts/run_dynamodb_sdk_compat_local.sh
```

`run_es_compat_local.sh` now covers:

- the Rust Elasticsearch fixture matrix
- live Powdrr-vs-Elasticsearch differential checks
- an official JavaScript `@elastic/elasticsearch` smoke suite against the read-only subset

## Acknowledgements

Powdrr is built on top of a large set of open-source projects and protocol
ecosystems. The exact machine-readable dependency graph lives in the workspace
`Cargo.toml` files and `Cargo.lock`, but the major foundations include:

- Rust and the broader Tokio async ecosystem
- Apache Arrow, Apache Parquet, and Apache DataFusion
- Apache Iceberg and the `iceberg-rust` implementation
- Gotham for the HTTP server surface
- Serde and Reqwest for protocol and client plumbing
- Redis for coordination and local runtime behavior
- the AWS Rust SDK and `object_store` for DynamoDB and object-storage access
- `kube` and `k8s-openapi` for Kubernetes-aware runtime behavior
- OpenRaft for the service-side replicated metadata direction
- Liquid Cache for the current Linux-only DataFusion cache integration
- MinIO and LocalStack for local object-store and cloud-service emulation
- Elasticsearch, MongoDB, and DynamoDB as the client ecosystems Powdrr targets
  in compatibility layers, tests, and benchmarks

Powdrr would not exist in its current form without that work upstream.

## Where To Read Next

- `docs/why-powdrr.md`
  The product-level explanation of the offline-to-online serving problem and
  why an Iceberg-first, coherent-snapshot serving layer matters.

- `docs/zero-copy-lakehouse-serving-requirements.md`
  The most direct statement of the product contract and what "zero-copy"
  should mean honestly.

- `docs/speedboat-vs-iceberg-architecture.md`
  The storage-role contract for the mutable frontier, the canonical Iceberg
  snapshot, and the row-to-column promotion boundary.

- `docs/lakehouse-serving-roadmap.md`
  The repo-specific roadmap from the current hybrid stack toward a shared
  serving engine over lakehouse storage.

- `docs/es-compatibility-matrix.md`
  The tracked Elasticsearch-compatible behavior that current tests freeze.

- `benchmark/README.md`
  Details on the protocol-neutral serving benchmark and the compared query
  shapes.
