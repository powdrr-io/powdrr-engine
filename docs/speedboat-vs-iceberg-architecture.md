# Speedboat Vs Iceberg Architecture

This document defines the intended architectural relationship between
`speedboat` and `Iceberg` in Powdrr.

The short version is:

- `speedboat` is the mutable write frontier
- `Iceberg` is the canonical columnar snapshot
- compaction and snapshot promotion are the row-to-column translation boundary

That distinction is about system semantics, not just file format.

## Why This Needs To Be Explicit

The current codebase still exposes a hybrid checkpoint model in
`TableMetadataCheckpoint` with both `iceberg_metadata` and
`speedboat_metadata`.

That hybrid state is useful for the current migration period, but it makes the
storage model easy to misread:

- `speedboat` sometimes writes Arrow files, which look columnar
- the read path can still fan out over both `speedboat` and `Iceberg` files
- some compatibility behavior still depends on the mutable frontier

Without a clear design statement, it is easy to treat `speedboat` and
`Iceberg` as two peer primary stores. That is not the target architecture.

## Storage Roles

### Speedboat

`speedboat` is the mutable write frontier.

Its job is to absorb document and row mutations with low write latency and
correct update semantics:

- inserts
- replacements
- updates
- deletes
- `_id`, `_seq_no`, and `_version` tracking
- short-term freshness before promotion into a canonical snapshot

In the current implementation, this is visible in:

- `query_runtime/src/elastic_search_ingest.rs`
- `query_runtime/src/elastic_search_storage_schema.rs`
- `control_plane/src/data_contract.rs` via `SpeedboatCommit`

The important point is that `speedboat` is logically row-oriented even when it
serializes batches as Arrow.

Its unit of meaning is:

- a document mutation
- a delete tombstone
- a small mutable segment
- a freshness overlay

Its unit of meaning is not:

- a canonical table snapshot
- a durable serving version
- a planner-facing columnar layout

### Iceberg

`Iceberg` is the canonical storage layer.

Its job is to represent the durable table state as immutable, snapshot-addressed
columnar data on object storage:

- Parquet data files
- manifest and snapshot metadata
- file-level and row-group-level stats
- partition and sort metadata
- stable snapshot identity for readers

In the current implementation, this is visible in:

- `query_runtime/src/compaction.rs`
- `query_lib/src/data_access.rs`
- `control_plane/src/data_contract.rs` via `IcebergMetadata` and `IcebergCommit`

The unit of meaning here is a table snapshot, not a stream of mutations.

### Serving Artifacts

Serving artifacts are optional derived state.

Examples:

- search sidecars
- primary-key indexes
- secondary-key indexes
- aggregate fragments
- caches

These artifacts are allowed, but they must be:

- non-canonical
- bounded
- snapshot-aware
- replaceable

They should attach to a serveable Iceberg snapshot, not become an independent
source of truth.

## Arrow Does Not Make Speedboat Canonical

One confusing part of the current system is that the mutable frontier can write
Arrow files.

That does not make `speedboat` the columnar lakehouse layer.

Arrow here is only a batch serialization format. The semantics are still those
of a mutable row/document frontier because:

1. the write path is driven by document mutations, not by snapshot planning
2. the frontier carries update and delete semantics directly
3. the compactor still has to reconcile replacements and tombstones
4. the final partitioning, sorting, and file layout are chosen later
5. the planner should not treat frontier files as the long-term canonical read
   surface

So the row-to-column transition is not "JSON becomes Arrow" or "Arrow becomes
Parquet."

It is:

- mutable mutation batches
- reconciled into stable rows
- reordered and packed for columnar storage
- published as an immutable snapshot with pruning metadata

## Row-To-Column Promotion

This is the intended conceptual pipeline:

```text
client write
  -> normalize document / row mutation
  -> append to mutable frontier (speedboat)
  -> checkpoint freshness-visible frontier state
  -> background compaction reads frontier + deletes
  -> resolve latest live row version per key
  -> cluster / sort / partition for target layout
  -> write Parquet data files
  -> collect file stats and row-group stats
  -> publish Iceberg snapshot metadata
  -> validate / promote serveable snapshot
```

The row-oriented world ends at the point where the system stops reasoning in
terms of mutation segments and starts reasoning in terms of immutable files,
row groups, columns, and snapshots.

### Step 1: Normalize Mutable Input

Incoming writes are normalized into records with identity and version metadata:

- `_id`
- `_seq_no`
- `_id_seq_no`
- `_version`
- `_source`
- denormalized searchable/projected fields

This is the write-friendly shape used by the current compatibility surfaces.

### Step 2: Buffer Freshness In The Frontier

The normalized rows are written into the mutable frontier.

This frontier exists so the system can acknowledge writes and preserve update
semantics before a full columnar rewrite happens.

The frontier should be thought of as:

- freshness state
- mutation state
- pre-compaction state

not as the table's final storage layout.

### Step 3: Reconcile Rows During Compaction

Compaction is where the mutable row world gets reconciled into the canonical
table view.

This stage is responsible for:

- removing superseded row versions
- applying delete files / tombstones
- choosing the surviving row version per logical key
- coalescing many small frontier files into fewer larger files

This is the true semantic boundary between "document mutation log" and
"serveable table state."

### Step 4: Materialize Columnar Files

Once the live rows are known, the system can materialize them into Parquet in a
layout that serves analytical and serving reads better:

- rows grouped into row groups
- columns encoded independently
- partitioning applied
- sort order applied
- stats captured for pruning
- bloom filters and page indexes added where useful

This is where row-oriented logical data becomes physically column-oriented.

### Step 5: Publish A Snapshot

The Parquet files do not become canonical just because they were written.

They become canonical when the system publishes and adopts an Iceberg snapshot
that points to them, with the right metadata:

- file lists
- schema
- partition spec
- sort order
- file sizes
- file stats
- compaction lineage

Only after snapshot publication should the planner treat the data as durable
canonical table state.

### Step 6: Promote A Serveable Snapshot

A snapshot can exist before it is fully ready for the serving engine.

The serving layer may still need:

- derived artifact coverage
- validation of artifact completeness
- cache warmup
- planner-visible readiness state

So there are really two promotions:

1. row mutations become an Iceberg snapshot
2. an Iceberg snapshot becomes a serveable snapshot

That separation matters for correctness.

## Read Path Rules

### Current Transitional Rule

Today the read path can still combine:

- `iceberg_files`
- `speedboat_files`
- delete files
- sidecar search files

That is acceptable as a migration-state implementation detail.

### Target Steady-State Rule

The target steady-state rule should be:

- queries bind to one serveable Iceberg snapshot
- all planner decisions derive from that snapshot and its serving artifacts
- mutable frontier state is either already promoted or exposed only through an
  explicit bounded overlay mechanism

In other words, the planner should not permanently depend on a co-equal union
of `speedboat` and `Iceberg`.

## Design Invariants

These should stay true even if the implementation changes:

1. Canonical truth lives in Iceberg snapshots, not in the mutable frontier.
2. Mutable freshness state is allowed, but it is temporary and bounded.
3. Snapshot identity, not file naming convention, defines readable table state.
4. Serving artifacts are derived from snapshots and are never canonical.
5. The planner should evolve toward snapshot-first execution, not frontier-file
   enumeration.

## Implication For Current Metadata Types

The current `speedboat_metadata` field in `TableMetadataCheckpoint` is useful
for the current implementation, but it should not survive as a co-equal
canonical storage concept in the long-term design.

The long-term model should instead talk about:

- current Iceberg snapshot
- current serveable snapshot
- freshness state, if any
- serving artifact coverage
- snapshot promotion state

That is why the lakehouse roadmap says `speedboat_metadata` is a legacy
migration concern even though `speedboat` still has a real operational role
today.

The role survives. The co-equal storage-model status should not.

## Open Design Questions

This document intentionally leaves some implementation choices open:

- whether the mutable frontier should continue to use Arrow segments
- whether the mutable frontier should later become a different write-optimized
  structure
- how much read visibility should come from frontier overlay versus synchronous
  promotion
- which serving artifacts should be required before a snapshot is considered
  serveable

But those choices should be made within the storage-role contract above, not by
blurring the line between `speedboat` and `Iceberg`.
