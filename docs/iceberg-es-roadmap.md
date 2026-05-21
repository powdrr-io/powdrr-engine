# Iceberg-First Elasticsearch Replacement Roadmap

This roadmap supersedes `docs/slatedb-es-search-plan.md`.

## Goal

Make this repository a more compliant, more fully functional Elasticsearch
replacement while keeping Iceberg as the primary immutable storage and
publication layer.

The priority is:

- preserve and expand the ES-compatible API surface that already works
- replace the current SQL/DataFusion-sidecar shortcut with a real search engine
- keep Iceberg-managed immutable segment storage
- support efficient single-node execution first
- support multi-node shard execution with node-local merge before controller
  merge

## Non-Goals

Not the immediate goal:

- full Lucene feature parity
- full xpack and monitoring compatibility
- a metrics/OLAP engine
- replacing all analytical uses of DataFusion

## What The Current Code Does

Today the implementation is a specific search stack, not a storage-agnostic ES
engine.

The hot path is:

- HTTP handling in `query_server/src/elastic_search_endpoints.rs`
- ES-like DSL parsing in `query_runtime/src/elastic_search_parser.rs`
- SQL assembly in `query_core/src/schema_massager.rs`
- file loading and query execution in `query_runtime/src/private_api.rs`
- sidecar `_search_index.parquet` generation in
  `query_runtime/src/elastic_search_index.rs`

The important current assumptions are:

1. `match` and `simple_query_string` depend on a sidecar search index table and
   SQL joins on `si.doc_id = t._id_seq_no`.
2. Bulk ingest writes object-store files first and search indexing is treated as
   derived coverage work.
3. Query fan-out is based on file-path hashing rather than explicit shard
   ownership.
4. Existing coverage is scattered through parser tests and router end-to-end
   tests, but not organized as a compatibility contract.

Useful existing starting points:

- end-to-end router tests in `query_server/src/router.rs`
- benchmark harness in `benchmark/src/main.rs`

## Why Stay Iceberg

If immutable object-store-backed Iceberg tables are a hard requirement, the
engine should be redesigned around Iceberg-managed search segments rather than
around a KV store.

That means:

- treat Iceberg snapshots as the durable publication boundary
- treat Parquet data files as Iceberg-managed storage, not as free-floating
  primary artifacts
- treat search execution as a custom engine over segment metadata and postings
- keep DataFusion out of the search hot path except where it is genuinely useful

The main mistake to avoid is keeping the current "SQL over loosely paired
Parquet files" shape. The replacement should be "real shard and segment search
execution over Iceberg-backed search tables".

## Target Architecture

### High-Level Shape

Keep:

- HTTP routing and response shaping
- alias and template compatibility where already present
- some state-provider concepts for cluster metadata

Replace:

- SQL-oriented search planning
- sidecar join assumptions
- file-hash distribution
- search scoring and result collation in the DataFusion path

Add:

- `SearchPlan` as a storage-agnostic logical query model
- explicit index shards and shard placement
- Iceberg-backed immutable search segments per shard
- custom postings/doc-values/stored-fields execution
- hierarchical merge:
  segment -> shard -> node -> controller

## Iceberg Search Storage Model

### Shards

Each index is split into a fixed number of logical shards for the life of that
index generation.

Routing:

- if an explicit routing key exists, hash that
- otherwise hash `_id`
- `shard_id = hash % shard_count`

Each shard is owned by exactly one writer and zero or more readers.

### Iceberg Tables And Segments

Each shard publishes immutable search segments through Iceberg snapshots.

The storage should stay Iceberg-first, which means:

- the durable search data lives in Iceberg tables
- the underlying files are still Parquet, but they are managed through Iceberg
  metadata and snapshots
- query visibility should follow Iceberg snapshot publication, not ad hoc file
  discovery

Recommended first-cut logical tables:

1. `docs`
   Canonical document metadata and stored source fields.

2. `postings`
   Inverted index rows sorted for fast term access.

3. `terms`
   Term dictionary and term-level stats.

4. `doc_values`
   Column-oriented values used for sort, range, aggregations, and existence.

5. `deletes`
   Tombstone rows or delete-state records.

6. `segment_summaries`
   Segment metadata, row counts, bounds, field stats, and planner hints.

There are two viable physical organizations:

1. one Iceberg table per logical structure, partitioned by `shard_id` and
   `segment_id`
2. one Iceberg table set per shard

The better default is the first option unless operational testing shows table
metadata scaling problems. It keeps the storage model Iceberg-native without
creating an explosion of tiny tables.

### Recommended Physical Layout

For the first cut:

- `postings` rows sorted by `(shard_id, field, term, doc_id)`
- `terms` rows sorted by `(shard_id, field, term)`
- `docs` rows sorted by `(shard_id, internal_doc_id)`
- `doc_values` rows sorted per field according to that field's encoding

Each visible search segment should carry:

- segment ID
- shard ID
- generation ID
- created-at
- analyzer version
- mapping version
- doc count
- live doc count
- field existence counts
- min/max for range-prunable fields such as `@timestamp`
- term stats pointers or row-group boundaries if precomputed

Where possible, this metadata should be represented either in Iceberg table
metadata, snapshot summaries, or dedicated Iceberg summary tables rather than
as opaque sidecar JSON.

## Write Path

### Ingest

Replace the current "write speedboat file, then derive search sidecar" path with
explicit shard write logic.

For each bulk item:

1. resolve target shard
2. normalize document according to mappings
3. assign internal doc ID, `_seq_no`, `_version`, `_primary_term`
4. tokenize indexed text fields
5. write into shard-local mutable state
6. on refresh/flush, commit a new immutable Iceberg snapshot for the shard's
   search data

### Updates and Deletes

Use immutable segments plus tombstones.

For update:

- mark old doc version deleted
- write new doc version to a fresh mutable buffer
- publish on refresh

For delete:

- record tombstone against the existing doc version

This is much closer to Elasticsearch semantics and makes reasoning about search
visibility simpler than mutating paired files in place.

### Refresh

A shard refresh should:

- flush mutable in-memory docs and postings into new Parquet data files
- commit those files through Iceberg
- publish a new visible shard snapshot or shard-visible snapshot mapping
- make that visibility boundary available to readers

That Iceberg publication boundary should become the unit of search visibility.

## Query Execution Model

### Replace SQL With `SearchPlan`

The parser should stop emitting SQL/DataFusion-shaped logic and instead emit a
logical plan:

- target indices
- resolved shards
- query tree
- requested fields
- pagination
- sort
- scoring mode

Supported first-class query nodes:

- `Term`
- `Match`
- `SimpleQueryString`
- `Range`
- `Bool`
- `Exists`

### Execution Layers

Use four layers of execution:

1. Segment executor
   Reads postings/doc-values within one segment.

2. Shard executor
   Merges all visible segments for one shard and applies tombstones.

3. Node worker
   Merges the results of all local shards assigned to that node.

4. Controller
   Merges one partial result set per node and shapes the final ES response.

## Result Collation Plan

### Segment To Shard

Within one shard:

- execute term/range/doc-values lookups per segment
- apply segment-local scoring
- merge segment hits into shard top `K`
- apply tombstones before the shard returns candidates

### Shard To Node

Within one node:

- run the `SearchPlan` across each local shard
- collect shard-local top `K`
- merge to one node-local top `K`
- return only lightweight hit metadata:
  - internal doc ID or stable doc key
  - `_id`
  - score
  - sort values
  - shard and segment location

Do not ship full `_source` for every candidate.

### Node To Controller

At the controller:

- merge one partial result set per node
- resolve the final top `K`
- fetch full `_source` only for the winning docs
- build the ES-compatible response

### When Overfetch Is Needed

If shard-local execution is exact, top `K` per shard is sufficient.

Overfetch is needed only when ranking or eligibility is deferred, for example:

- final reranking
- collapse or dedup across shards
- partial local filtering
- pagination for `from + size`

The first implementation should avoid deferred ranking where possible so the
merge pipeline stays simple and exact.

## Planner Metadata And Shard Pruning

The query planner should not hit every shard blindly.

Maintain planner-visible metadata:

- shard placement
- shard health
- latest visible manifest
- live doc count
- exact min/max for range-prunable fields
- exact partition or tenant keys when used for routing
- field existence flags or counts
- approximate term-presence summaries where safe

Use it in two places:

1. controller decides which shards to send to which nodes
2. node worker prunes again within its local shard set before opening executors

Exact metadata may be used for hard elimination.
Approximate metadata may be used only if it has no false negatives.

## Compliance Roadmap

### Phase 0: Freeze Existing Behavior

Before changing architecture, build a compatibility contract around what already
works.

Add a test matrix that captures:

- `PUT /:index`
- aliases
- templates
- `_bulk`
- `_create`
- `_doc` read
- delete
- `_search`
- `_update_by_query` for the currently supported subset

### Phase 1: Introduce `SearchPlan`

Refactor the parser to emit a storage-agnostic logical plan while temporarily
keeping the old SQL backend behind an adapter.

This creates the seam needed to swap the engine without changing the HTTP layer.

### Phase 2: Add Iceberg Search Tables, Segment Metadata, and Shard Manifests

Define:

- Iceberg table layout for docs, postings, terms, doc values, and deletes
- shard manifest or shard snapshot mapping format
- segment summary representation
- postings/doc-values layout
- refresh publication rules

### Phase 3: Replace Ingest

Move `_bulk`, `_create`, update, and delete onto:

- real shard routing
- mutable shard buffers
- immutable Iceberg snapshot publication
- tombstone-based refresh semantics

### Phase 4: Replace Search Execution

Implement:

- term queries
- bool composition
- range execution
- match and `simple_query_string`
- exact top-k merge
- `_source` fetch after final merge

### Phase 5: Expand Compatibility

Add:

- more sort modes
- better mapping enforcement
- better analyzers
- `track_total_hits`
- more complete error and edge-case behavior

## Test Plan

### 1. Differential Compatibility Tests

This should become the main migration gate.

Run the same request corpus against:

- real Elasticsearch
- current engine
- new Iceberg engine

Normalize responses where necessary:

- ignore shard IDs and timing
- compare hit IDs, hit counts, score ordering where deterministic
- compare error status and error type for unsupported/bad requests

### 2. End-To-End API Matrix

Turn the existing inline router tests into a deliberate matrix grouped by
feature.

Minimum matrix:

- create index with settings
- alias create/remove
- template create/read
- bulk ingest success and partial failure cases
- create existing doc conflict
- get existing doc
- get nonexistent doc
- delete existing doc
- delete nonexistent doc
- update-by-query subset
- search on nonexistent index

### 3. Query Semantics Matrix

For each supported query type:

- `term`
- `match`
- `simple_query_string`
- `bool.must`
- `bool.should`
- `bool.filter`
- `bool.must_not`
- `range`
- `exists`

Test:

- hit IDs
- total hits
- score ordering where defined
- behavior with nulls, arrays, and missing fields
- behavior across refresh boundaries

### 4. Visibility and Mutation Tests

Critical migration invariants:

- docs are not visible before refresh if that is the chosen contract
- docs become visible after refresh
- updates replace old search terms
- deletes remove hits
- tombstones survive segment merge
- refresh publication is monotonic

### 5. Topology Tests

Need explicit topology coverage:

- single shard, single node
- multi-shard, single node
- multi-node, one node owning multiple shards
- node-local merge plus controller merge
- writer handoff for a shard
- shard pruning using planner metadata

### 6. Single-Node Performance Baselines

Use the existing benchmark harness as the seed and make it produce stable
baseline output.

Single-node measurements to track:

- bulk ingest throughput
- refresh latency
- term query p50/p95/p99
- bool query p50/p95/p99
- match query p50/p95/p99
- range query p50/p95/p99
- `_source` fetch latency
- cold-cache versus warm-cache behavior

The important comparison is relative:

- current engine single-node baseline
- new Iceberg engine single-node baseline

### 7. Workload Types For Performance

At minimum, benchmark these workloads:

- high-selectivity `term`
- low-selectivity `term`
- `bool` of `term + term + range`
- `match` on short text
- `simple_query_string` across multiple fields
- update and refresh loop

### 8. Failure and Regression Tests

Add explicit tests for:

- malformed bulk bodies
- malformed search requests
- unsupported query constructs
- stale shard manifest reads
- missing segment files
- partial node failure during distributed search

## How To Organize The Test Suite

### Compatibility Suites

Add a top-level compatibility test area with:

- request fixtures
- expected normalized responses
- a runner that can target real ES and local implementations

### Engine Invariant Suites

Add engine-level tests around:

- segment publication
- tombstones
- manifest visibility
- postings encoding
- doc-values encoding
- node-local merge correctness

### Benchmark Harness

Refactor `benchmark/src/main.rs` so it can:

- load a fixed corpus
- run a fixed request mix
- emit machine-readable summaries
- compare baseline versus candidate runs

## Immediate Next Steps

1. Create a formal compatibility matrix from the existing router tests.
2. Add a differential test runner against real Elasticsearch.
3. Add `SearchPlan` types without changing HTTP behavior.
4. Design shard manifest and segment manifest schemas.
5. Prototype one shard-local Iceberg-backed segment executor for:
   - `term`
   - `bool`
   - `range`
   - `_source` fetch
6. Extend the benchmark harness to report single-node ingest and query latency
   for fixed corpora and fixed request mixes.

## Recommended First Milestone

The first meaningful milestone is:

"One-node, multi-shard Iceberg-backed engine with exact `term`, `bool`,
`range`, and `match` execution, plus a differential compatibility suite that
proves it preserves the currently working ES-compatible behavior."

That gives a stable platform for the rest of the migration. Without that test
contract, architecture work will be hard to evaluate and easy to regress.
