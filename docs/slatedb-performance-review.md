# SlateDB Performance Review

This note reviews the performance techniques SlateDB uses and maps them onto
Powdrr's lakehouse serving path.

## Bottom Line

The most transferable ideas from SlateDB are:

- separate decoded-cache and byte-cache responsibilities
- treat metadata and stats as first-class query-planning inputs
- expose targeted cache warming and eviction rather than hoping reads warm the
  right things
- keep pruning/filter artifacts pluggable so new access patterns do not require
  planner rewrites
- move expensive maintenance off the foreground read/write path
- align maintenance and retention work with natural data segments rather than
  rewriting unrelated cold data

For Powdrr specifically, the highest-value next steps are:

1. add a shared two-tier serving cache
2. add typed row-group and page-level metadata to checkpoints
3. add targeted warming/eviction APIs for hot files and hot ranges
4. make advisory pruning artifacts pluggable under the shared serving optimizer

## What SlateDB Does Well

### 1. Two cache layers with different jobs

SlateDB keeps two distinct caches for reads:

- a block cache for decoded data blocks plus metadata blocks
- an object-store cache for raw fetched bytes

That separation matters. The decoded cache saves both remote I/O and decode
work. The raw-byte cache only saves remote I/O, but it is easier to share
across short-lived readers and across processes on the same machine.

The useful Powdrr translation is:

- keep a process-local decoded cache for hot Parquet metadata and decoded column
  chunks
- keep a node-local object-range cache for raw Parquet byte ranges
- do not collapse those into one generic "cache" setting

### 2. Protect metadata from scan pollution

SlateDB's default `SplitCache` keeps data blocks separate from index/filter
metadata so large reads do not evict the structures needed for selective
queries.

Powdrr should copy that idea directly. If we add serving caches, we should not
let broad scans evict:

- manifest and snapshot metadata
- file/row-group stats
- Parquet footer and page-index metadata
- small hot projection blocks

This is more important than adding a single larger cache.

### 3. Warm and evict caches intentionally

SlateDB added explicit APIs to:

- warm selected filter/index/stats/data blocks for one SST
- evict dead cache entries for one SST after compaction

The main idea is not "more cache." The main idea is "cache management is an API,
not an accident."

Powdrr should add the same concept at the serving layer:

- warm manifests, file stats, Parquet footers, and hot byte ranges for a
  promoted snapshot
- optionally warm hot-order files for declared `ORDER BY ... LIMIT` patterns
- evict cache entries for removed files after snapshot promotion or compaction

Those APIs should work on physical files and ranges so ES, Mongo, and Dynamo
frontends all benefit without special-case code.

### 4. Spend metadata to avoid remote reads

SlateDB keeps adding richer metadata:

- bloom and prefix-bloom filters
- per-SST stats
- per-block stats
- public manifest/index/stats inspection APIs

The recurring pattern is clear: spend bounded metadata once so the query path
can avoid expensive object-store reads later.

Powdrr has started this with file-level Iceberg stats in checkpoints, but it is
still only the first layer. The next layers should be:

- row-group stats
- page-index awareness
- bloom-filter awareness where Parquet/datafusion support is good enough
- planner-visible read-set estimates and cache-coverage estimates

### 5. Keep filter/pruning artifacts pluggable

SlateDB did not hardcode a single bloom implementation forever. It moved to a
pluggable filter-policy model so point-lookup, prefix, and future filters can
coexist without changing the engine core.

Powdrr should do the analogous thing for serving artifacts. The shared optimizer
should understand a generic artifact surface:

- exact vs advisory
- covered fields
- supported predicate classes
- snapshot binding
- storage location

Then we can add:

- file/row-group zone maps
- Parquet bloom/page-index adapters
- exact key lookup artifacts
- text-search artifacts
- low-cardinality bitmap artifacts

without teaching each frontend protocol about each artifact type.

### 6. Move maintenance off the foreground path

SlateDB treats compaction as background work and supports running the compactor
as a separate service. That is important because maintenance competes with the
same object-store and local-disk budgets as reads.

Powdrr should follow the same operating model for:

- snapshot-diff processing
- cache warming
- serving-index maintenance
- file retirement / cache eviction
- layout maintenance and compaction recommendations

The foreground request path should consume prepared artifacts, not discover and
build them on demand.

### 7. Align maintenance to natural segments

SlateDB's segment-oriented compaction is specific to LSMs, but the principle is
useful for us: keep hot and cold ranges from interfering with each other.

For Powdrr, the analogous idea is:

- layout data so hot query ranges stay physically clustered
- maintain serving artifacts by segment or partition window
- drop or retire whole old segments when possible
- avoid rewriting cold files because hot files changed

For time-scoped or append-heavy tables, this matters as much as caching.

## What Powdrr Already Has

Powdrr already has a few pieces in the same direction:

- file-level Iceberg stats are loaded into checkpoints in
  [query_lib/src/data_access.rs](../query_lib/src/data_access.rs)
- the shared serving path uses those stats for file pruning and bounded top-k in
  [query_runtime/src/lakehouse_serving.rs](../query_runtime/src/lakehouse_serving.rs)
- the serving optimization docs already call for row-group metadata, page-index
  awareness, shared caches, and protocol-neutral planning in
  [docs/cross-protocol-serving-optimization-plan.md](./cross-protocol-serving-optimization-plan.md)
  and
  [docs/zero-copy-lakehouse-serving-requirements.md](./zero-copy-lakehouse-serving-requirements.md)
- DataFusion Parquet pruning is enabled in
  [query_lib/src/data_access.rs](../query_lib/src/data_access.rs)

So the gap is not architectural direction. The gap is that these concerns are
not yet first-class enough in code or operations.

## What We Should Do Next

### 1. Add a shared two-tier serving cache

Build:

- a node-local object-range cache keyed by snapshot/file/range
- a process-local decoded metadata and column cache
- a protected metadata pool separate from bulk data reads

This should live below the protocol adapters so ES, Mongo, and Dynamo get the
same cache behavior.

### 2. Add typed row-group and page metadata to checkpoints

Extend the current `IcebergFileStats` checkpoint model with:

- row-group row counts and byte ranges
- per-column row-group bounds and null counts
- page-index presence
- bloom-filter presence

That gives the planner an explicit `PruneRowGroups` stage instead of relying on
opaque downstream pruning.

### 3. Add targeted warming and eviction hooks

Add shared maintenance operations to:

- warm a promoted snapshot's manifests, stats, and selected hot files/ranges
- warm declared serving-pattern order keys
- evict removed-file cache entries after promotion or compaction

These should be best-effort and snapshot-aware.

### 4. Add a pluggable artifact model

Codify the exact/advisory split already described in the optimizer docs. The
planner should be able to ask for artifacts by capability instead of by storage
implementation.

That is what will let us add:

- Parquet bloom/page-index usage
- exact key indexes
- bitmap indexes
- text artifacts

without forking the serving engine by protocol.

### 5. Add layout-aware maintenance

For append-heavy tables, add a layout advisor and maintenance logic that tries
to keep hot windows and cold windows physically distinct. That is the lakehouse
equivalent of SlateDB's segment-oriented compaction.

### 6. Expose planner and cache metrics

SlateDB turns these features into tunable operational surfaces. Powdrr should do
the same. At minimum, explain output and metrics should expose:

- files considered/selected
- row groups considered/selected
- estimated bytes vs actual bytes read
- object-range cache hits/misses
- decoded-cache hits/misses
- warmup coverage for the current serveable snapshot

## What Not To Copy Literally

Some SlateDB optimizations are tied to being an object-store-backed LSM and are
not directly applicable to Powdrr:

- WAL flush intervals
- memtable sizing and writer backpressure details
- synchronous-commit semantics
- SST compaction scheduling specifics

We should copy the principles behind them:

- keep foreground latency separate from maintenance
- bound amplification
- make hot-path metadata cheap to reuse

## Recommendation

If we want one next implementation slice, it should be:

1. shared object-range cache
2. typed row-group metadata
3. planner-visible row-group pruning and cache stats

That is the cleanest place where SlateDB's playbook overlaps our current
lakehouse serving architecture and where ES, Mongo, and Dynamo frontends all
benefit immediately.

## Sources

- SlateDB caching design:
  https://slatedb.io/docs/design/caching/
- SlateDB tuning guide:
  https://slatedb.io/docs/operations/tuning/
- SlateDB reads design:
  https://slatedb.io/docs/design/reads/
- SlateDB standalone compactor:
  https://slatedb.io/docs/tutorials/standalone-compactor/
- SlateDB RFC 0002 compaction:
  https://slatedb.io/rfcs/0002-compaction/
- SlateDB RFC 0020 range metadata and size estimation:
  https://slatedb.io/rfcs/0020-range-metadata/
- SlateDB RFC 0022 prefix bloom filters via pluggable filter policies:
  https://slatedb.io/rfcs/0022-pluggable-filter/
- SlateDB RFC 0023 targeted cache warming and best-effort block cache eviction:
  https://slatedb.io/rfcs/0023-cache-manager/
- SlateDB RFC 0024 segment-oriented compaction:
  https://slatedb.io/rfcs/0024-segment-oriented-compaction/
