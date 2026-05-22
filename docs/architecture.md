# Architecture

This document describes the codebase as it exists on the current `main`
branch. It is intentionally narrower than the roadmap docs in this repo.

## Layers

The workspace is split into a small number of explicit layers.

```text
clients / tests / benches
        |
        v
powdrr-io-engine        powdrr-io-service        powdrr-cli        powdrr-benchmark
        |                       |                     |                    |
        +---------+-------------+---------------------+--------------------+
                  |
                  v
          query_server          service_lib
                |                  |
                v                  v
          query_runtime      control_plane
                |
                v
             query_lib
                |
                v
            query_core
```

The rough rule is:

- lower layers should not depend on higher ones
- protocol adapters live in `query_server`
- runtime orchestration and mutable state live in `query_runtime`
- low-level execution and storage helpers live in `query_lib`
- shared pure planning/types live in `query_core`
- control-plane metadata types shared by the runtime and service live in
  `control_plane`

## Crate Ownership

### `powdrr-control-plane`

Directory: [control_plane/](../control_plane)

Owns shared control-plane data structures and schema helpers used by both the
query/runtime side and the service side.

Examples:

- table metadata and checkpoint contracts
- test API contracts
- schema-related shared types

### `powdrr-query-core`

Directory: [query_core/](../query_core)

Owns pure query and serving-plan types that should not know about HTTP,
distributed state providers, or protocol routing.

Examples:

- read/search/serving plan types
- schema massaging
- ES query-shape DTOs used by multiple layers
- query path classification helpers

### `powdrr-query-lib`

Directory: [query_lib/](../query_lib)

Owns low-level execution and storage helpers used by the runtime layer.

Examples:

- object-store and parquet access
- query execution helpers
- speedboat buffer helpers

This crate should stay below state-provider orchestration and protocol logic.

### `powdrr-query-runtime`

Directory: [query_runtime/](../query_runtime)

Owns the shared serving runtime and mutation/runtime orchestration.

Examples:

- search execution and serving runtime
- ingest, index build, and compaction
- peer fanout and prefetch
- state providers and metadata-store integration
- local CLI implementation

This is the main shared runtime layer behind the engine binary.

### `powdrr-query-server`

Directory: [query_server/](../query_server)

Owns protocol adapters and HTTP/wire entrypoints.

Examples:

- router
- Elasticsearch-compatible handlers
- DynamoDB-compatible handlers
- Mongo and Redis protocol shims
- test-only HTTP endpoints

This crate should translate protocol requests into runtime calls; it should not
grow its own competing serving engine.

### `powdrr-service-lib`

Directory: [service_lib/](../service_lib)

Owns the control-plane service implementation.

Examples:

- metadata-store interfaces
- DynamoDB / ephemeral / Raft service-side metadata backends
- service-side peers and read-only coordination

### Binaries

- [engine/](../engine): `powdrr-io-engine`
  Query/serving server.
- [service/](../service): `powdrr-io-service`
  Control-plane service.
- [cli/](../cli): `powdrr-cli`
  Local CLI for indexing/querying without the HTTP server.
- [benchmark/](../benchmark): `powdrr-benchmark`
  Serving benchmark and workload harness.

## Request Flows

### Elasticsearch / DynamoDB / Mongo / Redis Read Request

1. Protocol-specific request enters `powdrr-io-engine`.
2. `query_server` parses the request and maps it onto the shared runtime.
3. `query_runtime` resolves table/checkpoint context and chooses the serving
   path.
4. `query_lib` loads files and executes low-level access/query helpers.
5. `query_core` types define the request/plan shape where relevant.

The important rule is that the protocol frontends should converge on one shared
serving engine rather than forking per protocol.

### Native Serving Request

1. Request enters the `/_serve` endpoints in `query_server`.
2. `query_runtime::serving_protocol` and `query_runtime::lakehouse_serving`
   classify the request as fast-path or slow-path.
3. `query_runtime` loads the published checkpoint context.
4. `query_lib::data_access` and execution helpers read the relevant parquet and
   serving artifacts.

### Mutation / Ingest Request

1. Elasticsearch-compatible mutation handlers in `query_server` accept the
   request.
2. `query_runtime::elastic_search_ingest` normalizes the mutation into
   speedboat rows and delete metadata.
3. `query_runtime::state_provider` / metadata-store-backed services publish
   checkpoints and later compaction work.
4. Background compaction moves visible mutable state into canonical Iceberg
   state.

## Metadata and State Flow

The runtime and the service are separate on purpose:

- `query_runtime` needs checkpoint, publication, and peer/runtime coordination
  to serve reads and writes.
- `service_lib` owns the control-plane service's metadata API and state
  backends.

Today the shared conceptual model is:

- committed frontier: durable metadata state
- target frontier: the next checkpoint intended for activation
- active frontier: the published checkpoint reads can see

When debugging checkpoint issues, the first files to inspect are usually:

- [query_runtime/src/state_provider.rs](../query_runtime/src/state_provider.rs)
- [query_runtime/src/metadata_store.rs](../query_runtime/src/metadata_store.rs)
- [service_lib/src/metadata_store.rs](../service_lib/src/metadata_store.rs)

## Test Ownership

The old `main_lib/tests` surface has been split.

Primary integration ownership now lives in:

- [query_server/tests/](../query_server/tests)
  Protocol compatibility suites and HTTP/wire regressions.
- [query_runtime/tests/](../query_runtime/tests)
  Local CLI and runtime-focused integration coverage.

Shared fixtures live in:

- [testdata/](../testdata)
- [tests/](../tests) for Docker Compose stacks and external harness support

## Support Directories

Not every top-level directory is a Cargo package.

- [clients/](../clients)
  Client-side experiments or helpers.
- [tests/](../tests)
  Docker Compose stacks and external test harness assets.
- [testdata/](../testdata)
  Shared JSON, parquet, and compatibility fixtures.
- `dev_stack/`
  Local support files for development stack bring-up. This directory was
  formerly named `main_lib/` and is not a Rust crate.

## How To Navigate

If you are new to a change, start here:

- protocol/API change: `query_server`
- serving/runtime behavior: `query_runtime`
- file loading or execution primitive: `query_lib`
- plan/type/model change: `query_core`
- checkpoint or service metadata change: `service_lib`

Then use [repo-map.md](./repo-map.md) for the directory/package index and the
playbooks in [docs/playbooks/](./playbooks) for surface-specific validation
guidance.
