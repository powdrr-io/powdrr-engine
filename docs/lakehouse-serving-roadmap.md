# Lakehouse Serving Roadmap

This document translates
[`docs/zero-copy-lakehouse-serving-requirements.md`](./zero-copy-lakehouse-serving-requirements.md)
into a concrete roadmap for this repository.

It is the primary product-direction doc for turning this codebase into a
serving engine over lakehouse storage. It broadens the scope beyond the
search-first direction in [`docs/iceberg-es-roadmap.md`](./iceberg-es-roadmap.md).

## Goal

Turn this repository into a serving database whose canonical storage is an
Iceberg table on object storage.

The product contract should be:

- one canonical copy of base data in Iceberg
- bounded, managed acceleration state owned by this engine
- snapshot-consistent serving reads
- clear fast-path and slow-path boundaries
- explicit rejection or routing for non-serving queries

## Non-Goals

Not the initial goal:

- arbitrary SQL over large lakehouse tables
- joins on the serving path
- full warehouse replacement
- full Elasticsearch compatibility as the product identity
- broad scan support hidden behind low-latency APIs

## Current Codebase Assessment

### What Already Fits

The repo already has useful primitives:

- Iceberg catalog and object-store integration in
  [query_lib/src/data_access.rs](/Users/gregory/code/powdrr-engine/query_lib/src/data_access.rs:1)
- Iceberg snapshot publication during compaction in
  [query_runtime/src/compaction.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/compaction.rs:109)
- checkpointed table metadata in
  [control_plane/src/data_contract.rs](/Users/gregory/code/powdrr-engine/control_plane/src/data_contract.rs:125)
- cluster metadata and checkpoint lookup in
  [query_runtime/src/state_provider.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/state_provider.rs:607)
- a new logical planning boundary in
  [query_core/src/search_plan.rs](/Users/gregory/code/powdrr-engine/query_core/src/search_plan.rs:4)
  and
  [query_runtime/src/search_executor.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/search_executor.rs:34)
- node-local merge plumbing through the private RPC path in
  [query_runtime/src/search_executor.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/search_executor.rs:194)

These pieces are enough to avoid a rewrite from zero.

### What Does Not Fit

The current runtime is still built around a hybrid search stack:

- checkpoints carry both `iceberg_metadata` and `speedboat_metadata` in
  [control_plane/src/data_contract.rs](/Users/gregory/code/powdrr-engine/control_plane/src/data_contract.rs:125)
- private execution still selects concrete file lists from a checkpoint in
  [query_runtime/src/private_api.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/private_api.rs:120)
- the read path still fans out over `iceberg_files` and `speedboat_files` in
  [query_runtime/src/private_api.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/private_api.rs:422)
- text search depends on `_search_index.parquet` sidecars in
  [query_runtime/src/private_api.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/private_api.rs:196)
  and
  [query_runtime/src/elastic_search_index.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/elastic_search_index.rs:162)
- the SQL builder still assumes a join against `{target_table}_search_index` in
  [query_core/src/schema_massager.rs](/Users/gregory/code/powdrr-engine/query_core/src/schema_massager.rs:950)

That is not the target architecture. The requirements call for an Iceberg-first
serving engine with explicit, snapshot-aware acceleration artifacts rather than
paired files and implicit sidecar conventions.

## Product Direction

The engine should evolve from:

- `Iceberg + speedboat + DataFusion + sidecar search index`

to:

- `Iceberg snapshot + serving planner + serving artifacts + caches`

The serving artifacts are allowed to exist, but they must be:

- bounded
- purpose-built
- snapshot-aware
- incrementally maintained
- non-canonical

## Target Architecture

### 1. Canonical Storage

The canonical table is an Iceberg table on object storage.

The engine must plan from:

- catalog metadata
- snapshots
- manifest lists
- manifests
- file-level statistics
- Parquet row-group and page metadata

It should not plan from object-store folder scraping or ad hoc file naming.

### 2. Serving Contract

The user should declare serving patterns per table. A first-cut declaration
should capture:

- primary key
- tenant key if applicable
- range and time keys
- default sort key
- aggregate dimensions and measures
- optional searchable text fields
- latency class / freshness target

The engine should only give serving SLOs for declared patterns.

### 3. Planner And Admission Control

Every request should compile into a serving plan with explicit cost and
eligibility decisions:

- fast path
- slow path
- reject
- route elsewhere

The planner must estimate:

- candidate manifests
- candidate files
- candidate row groups and pages
- projected columns
- bytes to read
- index coverage
- cache coverage
- snapshot freshness

The current `SearchPlan` and `SearchExecutionPlan` are good starting points,
but they need to become storage-agnostic serving IR rather than ES-only query
IR.

### 4. Acceleration State

The first artifact layers should be:

1. Metadata cache
   - current snapshots
   - manifest lists
   - manifests
   - file and column stats

2. Parquet-aware pruning metadata
   - row-group stats
   - page-index awareness
   - bloom-filter awareness where available

3. Serving indexes
   - primary-key index
   - secondary-key index
   - low-cardinality bitmap index
   - optional aggregate index

4. Caches
   - object-range cache
   - decoded-column cache
   - hot-row/object cache
   - result cache for bounded aggregate queries

5. Optional search projection
   - inverted index only for declared text fields
   - not a full duplicate store

### 5. Snapshot Promotion

The engine needs a first-class serveable snapshot lifecycle:

1. detect new Iceberg snapshot
2. diff against the last serveable snapshot
3. build new index and aggregate fragments for added files
4. retire or invalidate fragments for removed files
5. validate coverage and completeness
6. atomically promote the snapshot as serveable

Queries must bind to one serveable snapshot and never mix old and new state.

## Query Contract For The MVP

The first supported serving contract should be narrower than the current
search-heavy surface:

- single-table queries only
- equality predicates on declared keys
- tenant-scoped equality predicates
- range predicates on declared time or sort keys
- small `IN` predicates
- `ORDER BY ... LIMIT N` on declared ordering keys
- `COUNT`, `SUM`, `AVG`
- `GROUP BY` on declared low/medium-cardinality dimensions

Explicit non-goals for the MVP:

- joins
- arbitrary SQL
- broad scans
- high-cardinality global group-bys
- delete-heavy correctness for every lakehouse mutation mode
- full-text search as a required feature

Text search can remain an optional later serving pattern.

## Required Model Changes

### Checkpoint And Metadata Model

`TableMetadataCheckpoint` in
[control_plane/src/data_contract.rs](/Users/gregory/code/powdrr-engine/control_plane/src/data_contract.rs:125)
should stop representing a hybrid of canonical data stores.

It should evolve toward:

- table identity
- current Iceberg snapshot metadata
- current serveable snapshot metadata
- serving pattern definitions
- serving artifact manifests
- freshness and coverage state

`speedboat_metadata` should be treated as a legacy migration concern, not part
of the target model.

### Serving Artifact Metadata

Add explicit metadata types for:

- serveable snapshot descriptor
- serving pattern
- artifact kind
- artifact coverage
- artifact freshness
- artifact location and size
- artifact build status

This belongs near
[control_plane/src/data_contract.rs](/Users/gregory/code/powdrr-engine/control_plane/src/data_contract.rs:1)
and
[query_runtime/src/state_provider.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/state_provider.rs:1).

### Execution IR

Generalize the current search-centric executor into a serving executor:

- `SearchPlan` becomes a broader logical request plan
- `SearchExecutionPlan` becomes a serving execution plan
- private RPC payloads stop shipping only SQL and search sort instructions
- `private_api.rs` executes serving operators, not just file-backed SQL batches

The existing executor split in
[query_runtime/src/search_executor.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/search_executor.rs:34)
is the right seam for this.

## Phased Implementation Plan

### Phase 0: Freeze Behavior And Baselines

Goal:

- preserve current supported behavior while architecture changes

Work:

- keep the ES compatibility suite as a regression gate
- add serving-oriented benchmark cases:
  - key lookup
  - tenant + time range
  - top-N by time
  - low-cardinality aggregate
- measure single-node cold and warm paths separately

### Phase 1: Introduce Serving Metadata

Goal:

- make serving patterns and serveable snapshots first-class state

Work:

- extend `CreateTable` and table metadata with serving-pattern declarations
- add serveable snapshot records to state-provider metadata
- represent artifact manifests independently of the legacy extension model

Likely files:

- [control_plane/src/data_contract.rs](/Users/gregory/code/powdrr-engine/control_plane/src/data_contract.rs:1)
- [query_runtime/src/state_provider.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/state_provider.rs:1)
- [query_server/src/router.rs](/Users/gregory/code/powdrr-engine/query_server/src/router.rs:1)

### Phase 2: Build A Serving Planner

Goal:

- compile requests into bounded serving plans with explicit admission outcomes

Work:

- generalize `SearchPlan` into serving request IR
- add query classification and cost estimates
- add `EXPLAIN SERVING`
- move planning from checkpoint file enumeration to snapshot/manifest/file
  pruning

Likely files:

- [query_core/src/search_plan.rs](/Users/gregory/code/powdrr-engine/query_core/src/search_plan.rs:1)
- [query_runtime/src/search_executor.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/search_executor.rs:1)
- [query_runtime/src/private_api.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/private_api.rs:1)
- [query_lib/src/data_access.rs](/Users/gregory/code/powdrr-engine/query_lib/src/data_access.rs:1)

### Phase 3: Key Lookup And Range Serving

Goal:

- serve the highest-value database-like access patterns

Work:

- add primary-key index artifacts
- add secondary-key index artifacts for declared patterns
- add row-group/page-aware row materialization
- add row/object cache for hot keys

Success criteria:

- `WHERE id = ?`
- `WHERE tenant_id = ? AND ts BETWEEN ? AND ?`
- `WHERE tenant_id = ? ORDER BY ts DESC LIMIT N`

### Phase 4: Aggregate Serving

Goal:

- make narrow aggregates fast without broad scans

Work:

- add bitmap indexes for low-cardinality dimensions
- add aggregate fragments keyed by snapshot and dimension set
- add aggregate result caching
- add explicit rejection for aggregates outside the serving contract

### Phase 5: Optional Search Projection

Goal:

- support text search only as a declared serving pattern

Work:

- replace `_search_index.parquet` assumptions with explicit text-index artifacts
- store only declared searchable fields
- keep canonical row materialization in Iceberg or cache
- support facets only where the artifact model can answer them efficiently

This is where the work in
[docs/iceberg-es-roadmap.md](./iceberg-es-roadmap.md)
still matters, but it should be treated as one serving-artifact family rather
than the product core.

### Phase 6: Multi-Node Scale-Out

Goal:

- distribute compute and cache while preserving snapshot correctness

Work:

- node-local planning over local artifact caches
- node-local merge before controller merge
- shard or partition assignment for serving artifacts
- request coalescing
- tenant budgets and admission control

## Repo Work Map

### Storage And Snapshot Layer

- [query_lib/src/data_access.rs](/Users/gregory/code/powdrr-engine/query_lib/src/data_access.rs:1)
  should become the home for manifest caching, snapshot diffing, file-stat
  extraction, and Parquet metadata access.
- [query_runtime/src/compaction.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/compaction.rs:109)
  should publish serveable-snapshot inputs and trigger artifact maintenance,
  not just move data from speedboat into Iceberg.

### Metadata And Control Plane

- [control_plane/src/data_contract.rs](/Users/gregory/code/powdrr-engine/control_plane/src/data_contract.rs:125)
  should define serving patterns, serveable snapshots, and artifact manifests.
- [query_runtime/src/state_provider.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/state_provider.rs:607)
  should track lifecycle state for those records.

### Planner And Executor

- [query_core/src/search_plan.rs](/Users/gregory/code/powdrr-engine/query_core/src/search_plan.rs:4)
  should become a general serving IR.
- [query_runtime/src/search_executor.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/search_executor.rs:34)
  should become the serving planner and merge coordinator.
- [query_runtime/src/private_api.rs](/Users/gregory/code/powdrr-engine/query_runtime/src/private_api.rs:422)
  should stop centering execution around DataFusion batches over checkpoint file
  lists.

### Compatibility Layer

- `elastic_search_*` modules should become one API compatibility surface, not
  the engine core.
- a later SQL or gRPC serving API can sit beside them once the serving planner
  exists.

## Immediate Next Steps

1. Import the requirements doc into the repo and keep it as the product-level
   source reference.
2. Add a small serving-pattern schema to the state model.
3. Rename and generalize the current search IR into a serving IR.
4. Build a first query-classification path that can say fast-path, slow-path,
   or reject before execution.
5. Implement one real serving path end to end:
   primary-key lookup over Iceberg snapshot plus a bounded index artifact.

That first serving path is the real architectural checkpoint. Once it works,
the rest of the roadmap becomes extension work rather than speculation.
