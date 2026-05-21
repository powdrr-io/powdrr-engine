# Cross-Protocol Serving Optimization Plan

This document describes how to make query planning, pruning, and acceleration
protocol-neutral so Elasticsearch-style, Mongo-style, and Dynamo-style
frontends all benefit from the same optimization layer.

It builds on:

- [`docs/lakehouse-serving-roadmap.md`](./lakehouse-serving-roadmap.md)
- [`docs/zero-copy-lakehouse-serving-requirements.md`](./zero-copy-lakehouse-serving-requirements.md)
- the current serving MVP in
  [query_runtime/src/lakehouse_serving.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_runtime/src/lakehouse_serving.rs:1)
- the current protocol adapters in
  [query_runtime/src/serving_protocol.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_runtime/src/serving_protocol.rs:1)

## Goal

Use one optimization stack for all frontend protocols:

- one canonical request IR
- one admission and planning pipeline
- one typed optimization metadata model
- one physical execution model
- multiple protocol renderers on top

The important inversion is:

- frontend protocols should describe requested semantics
- the serving planner should choose the cheapest safe execution path
- protocol-specific code should not own pruning, caching, or index selection

## Current State

The codebase already has the right starting seam, but the optimization logic is
still missing.

What already exists:

- a protocol-neutral request shape in
  [query_core/src/serving_plan.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_core/src/serving_plan.rs:1)
- outbound protocol renderers for Elasticsearch and MongoDB in
  [query_runtime/src/serving_protocol.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_runtime/src/serving_protocol.rs:1)
- a read-only serving endpoint in
  [query_runtime/src/lakehouse_serving.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_runtime/src/lakehouse_serving.rs:178)
- placeholder checkpoint metadata for file stats in
  [control_plane/src/data_contract.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/control_plane/src/data_contract.rs:56)

What does not exist yet:

- typed optimization metadata
- snapshot/file/row-group pruning in the serving path
- access-path selection shared across protocols
- top-k optimization shared across protocols
- projection-aware or covering execution
- a Dynamo frontend on the same serving IR

The current MVP still treats "fast path" as "allowed query shape" rather than
"query shape plus optimized read set." In
[query_runtime/src/lakehouse_serving.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_runtime/src/lakehouse_serving.rs:383),
matching a `ServingPattern` still selects all files and sums all file sizes.

## Design Principles

1. Separate semantics from execution.
   ES, Mongo, and Dynamo should compile into the same logical query model.

2. Separate safe pruning metadata from advisory ranking metadata.
   Exact min/max, partition values, null counts, and exact key indexes are safe
   pruning inputs. Bloom/sketch/text summaries may guide candidate selection but
   must not create false negatives unless explicitly declared exact.

3. Make optimization artifacts snapshot-aware.
   Every artifact must be tied to a base snapshot or checkpoint and must be
   invalidated or rebuilt deterministically.

4. Keep the optimizer below the protocol layer.
   A new protocol should mainly require a parser and a response renderer, not a
   new pruning engine.

5. Model slow-path and reject decisions explicitly.
   Protocol neutrality should not hide unsupported or expensive queries.

## Target Architecture

### 1. Frontend Adapters

Each frontend should translate requests into a canonical logical request.

Examples:

- Elasticsearch `_search`
- MongoDB `find`
- DynamoDB-style `GetItem` and `Query`

The canonical model should be broader than the current
`ServingRequestPlan`. It should represent:

- target table
- selected fields
- equality predicates
- `IN` predicates
- range predicates
- sort requirements
- limit
- consistency / snapshot requirements
- optional text clauses
- optional aggregation clauses
- frontend-specific constraints that affect correctness

Suggested new types:

- `ServingLogicalRequest`
- `ServingFilterExpr`
- `ServingSortExpr`
- `ServingProjection`
- `ServingFrontend`
- `ServingFrontendConstraints`

This should live near the current
[query_core/src/serving_plan.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_core/src/serving_plan.rs:1).

The current `ServingRequestPlan` can remain the MVP wire shape for `POST
/{table}/_serve`, but it should become one frontend into the same logical
planner rather than the planner's final input format.

### 2. Typed Optimization Metadata

Replace the loose `column_stats: Vec<(String, String)>` placeholder in
[control_plane/src/data_contract.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/control_plane/src/data_contract.rs:63)
and
[query_lib/src/data_access.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_lib/src/data_access.rs:207)
with typed metadata.

Suggested structure:

- `ServingOptimizationSnapshot`
  - base snapshot/checkpoint id
  - schema id
  - partition spec/version
  - artifact versions

- `ServingFileStats`
  - file path
  - row count
  - byte size
  - partition values
  - per-column null count
  - per-column min/max
  - optional exact distinct count

- `ServingRowGroupStats`
  - file path
  - row-group id
  - row count
  - byte range
  - per-column min/max/null count
  - page index presence
  - bloom filter presence

- `ServingAccessArtifact`
  - artifact kind
  - snapshot binding
  - covered fields
  - exact vs advisory
  - storage location

Artifact kinds should include:

- partition/file pruning metadata
- row-group pruning metadata
- exact key lookup index
- secondary equality/range index
- text search artifact
- covering projection artifact
- aggregate rollup artifact
- hot object / hot row cache descriptor

This is the foundation that lets all protocols share the same optimizer.

### 3. Shared Optimizer

Introduce an optimizer that converts a logical request into a physical plan.

Suggested pipeline:

1. Normalize request.
   Flatten frontend-specific syntax into canonical predicates and projections.

2. Bind schema and serving config.
   Resolve field names, types, declared serving patterns, and allowed shapes.

3. Choose an access path.
   Select among:
   - exact key lookup
   - secondary equality/range index
   - text artifact
   - ordered top-k scan
   - aggregate rollup
   - bounded lake scan

4. Prune by metadata.
   Use snapshot, manifest, file, row-group, and optional page metadata to
   shrink the read set before opening Parquet readers.

5. Choose projection strategy.
   Prefer covering artifacts or narrow column reads over broad row materialization.

6. Choose sort/limit strategy.
   Prefer order-preserving access paths and bounded top-k merges over full sort.

7. Emit admission outcome.
   Return:
   - `fast_path`
   - `slow_path`
   - `rejected`

8. Emit `ServingPhysicalPlan`.

Suggested plan enums:

- `ServingAccessPath`
  - `PrimaryKeyLookup`
  - `SecondaryPredicateIndex`
  - `TextArtifactSearch`
  - `OrderedMetadataScan`
  - `AggregateRollup`
  - `LakeScan`

- `ServingPhysicalOperator`
  - `PruneSnapshots`
  - `PruneFiles`
  - `PruneRowGroups`
  - `ReadProjection`
  - `MergeTopK`
  - `FetchRows`
  - `ApplyResidualFilter`
  - `RenderAggregates`

The optimizer should own the currently missing behavior in
[query_runtime/src/lakehouse_serving.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_runtime/src/lakehouse_serving.rs:383),
where file selection is still all-or-nothing.

### 4. Shared Execution

Execution should consume a protocol-neutral physical plan and produce a
canonical result set plus execution stats.

Suggested result model:

- `ServingExecutionResult`
  - rows
  - optional aggregate payload
  - physical plan summary
  - pruning stats
  - bytes opened
  - bytes read
  - files considered
  - files selected
  - row groups considered
  - row groups selected
  - cache hits/misses

That result can then be rendered as:

- ES search response
- Mongo result set
- Dynamo response shape
- internal `_serve` explain output

## How All Frontends Benefit

### Shared Equality And Range Optimization

These are the same logical problem across frontends:

- ES `term` or `terms`
- Mongo equality or `$in`
- Dynamo partition key equality and sort-key range

All should compile into the same canonical predicate class and share:

- manifest/file pruning
- exact key lookup artifacts
- row-group pruning
- narrow projection reads

### Shared Top-K Optimization

These are also the same logical problem:

- ES `sort` + `size`
- Mongo `sort` + `limit`
- Dynamo `Query` with key order plus limit

All should share:

- order-aware access-path selection
- bounded heap or merge top-k operators
- early stop when enough qualifying rows are found

### Shared Projection Optimization

These differ in syntax but not in planning value:

- ES `_source`
- Mongo projection
- Dynamo `ProjectionExpression`

All should share:

- column pruning
- covering projection artifacts
- hot row/object projection caches

### Shared Text Optimization

Text search is protocol-specific at the API layer but should still fit the same
artifact model.

- ES text queries need text indexes and possibly term statistics
- Mongo text-style compatibility could map to the same artifact later
- Dynamo itself does not provide text semantics, but a Dynamo-shaped frontend on
  top of Powdrr could still call the same text-serving engine if exposed

The important rule is:

- text-specific artifacts are one `ServingAccessArtifactKind`
- not a separate planning stack

### Shared Aggregation Optimization

Likewise:

- ES aggregations
- Mongo aggregate-like bounded patterns
- Dynamo precomputed counters / rollups

All should eventually share:

- aggregate rollup metadata
- snapshot-aware precomputed summaries
- bounded fallback scans when rollups are missing

## Protocol Capability Model

Not every frontend exposes the same query surface. That should be modeled
explicitly instead of letting protocol-specific code fork the optimizer.

Suggested `ServingFrontendCapabilityProfile` fields:

- supports_text
- supports_aggregations
- supports_multi_field_sort
- supports_projection_exclusion
- supports_cursor/pagination mode
- requires_exact_key_shape
- requires_partition_key
- requires_ordered_result_by_key

The optimizer should use this profile to:

- reject impossible translations early
- preserve correctness
- still reuse the same physical planning core

For example:

- a Dynamo `Query`-shaped request may require declared partition-key equality
- the same underlying logical request could still be accepted as ES or Mongo if
  the serving table allows a slower access path

## Concrete Repo Changes

### Near-Term Module Boundaries

1. Expand
   [query_core/src/serving_plan.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_core/src/serving_plan.rs:1)
   from an MVP request struct into the canonical logical planning layer.

2. Keep
   [query_runtime/src/serving_protocol.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_runtime/src/serving_protocol.rs:1)
   as protocol adapter code only.
   Add Dynamo request/response translations there or in a sibling frontend module.

3. Split
   [query_runtime/src/lakehouse_serving.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_runtime/src/lakehouse_serving.rs:1)
   into:
   - request admission
   - logical planning
   - optimization / pruning
   - physical execution
   - explain/result rendering

4. Replace the checkpoint metadata placeholder in
   [control_plane/src/data_contract.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/control_plane/src/data_contract.rs:56)
   with typed optimization metadata.

5. Teach
   [query_lib/src/data_access.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/query_lib/src/data_access.rs:1179)
   to materialize file and row-group stats into those typed structures.

6. Extend benchmarks in
   [benchmark/src/main.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-cross-protocol-optimization-plan/benchmark/src/main.rs:1)
   so they measure:
   - protocol parity
   - planner stats
   - files/row-groups pruned
   - bytes opened vs bytes read
   Add Dynamo once its frontend adapter exists.

### Suggested New Modules

- `query_runtime/src/serving_optimizer.rs`
- `query_runtime/src/serving_metadata.rs`
- `query_runtime/src/serving_physical_plan.rs`
- `query_server/src/serving_frontends.rs` or protocol-specific siblings
- `query_runtime/src/serving_explain.rs`

## Rollout Phases

### Phase 1: Make Metadata Typed

- replace `column_stats: Vec<(String, String)>`
- add typed file stats
- expose pruning counters in explain output
- keep execution behavior mostly unchanged

Success condition:

- explain can show exact candidate file counts and estimated bytes from typed
  metadata instead of full-snapshot totals

### Phase 2: Add Shared File Pruning

- compile canonical predicates into metadata filters
- prune manifests/files before execution
- feed `files_selected` from actual pruning output

Success condition:

- ES, Mongo, and `_serve` queries with the same equality/range shape all prune
  to the same candidate file set

### Phase 3: Add Row-Group And Top-K Optimization

- row-group metadata pruning
- bounded merge top-k
- projection pushdown improvements

Success condition:

- ordered limited queries stop behaving like whole-file scans

### Phase 4: Add Access Artifacts

- exact key index
- secondary equality/range index
- text artifact
- covering projection artifact

Success condition:

- access-path selection is explicit and visible in explain output

### Phase 5: Add More Frontends

- Dynamo-shaped frontend adapter
- richer ES search and aggregation shapes
- Mongo aggregate-like bounded patterns where appropriate

Success condition:

- new frontends mostly add translation logic, not new optimizer logic

## Recommended First Implementation Slice

The first slice should not be "add more protocol endpoints." It should be:

1. typed optimization metadata
2. file-pruning planner
3. explain output showing why files were kept or pruned
4. reuse that same planner from:
   - `_serve`
   - ES-compatible filter queries
   - Mongo-compatible filter queries

That gives immediate cross-protocol leverage and creates the substrate needed
for Dynamo later.

## Bottom Line

The right abstraction is:

- `frontend protocol -> canonical request -> shared optimizer -> physical plan -> protocol renderer`

not:

- `ES optimizer`
- `Mongo optimizer`
- `Dynamo optimizer`

If we keep pruning metadata, access artifacts, and top-k/projection logic below
the frontend boundary, every protocol can benefit from the same stats and
acceleration work, and adding Dynamo becomes mostly a request-shape and
response-shape problem instead of a new storage-engine problem.
