# Object-Store Read-Only State Provider Design

This document specifies a read-only Powdrr state provider that loads table
metadata, checkpoint metadata, and publication pointers directly from the
object store.

The goal is to support serving from lakehouse storage without requiring
DynamoDB or the remote leaderless service for metadata reads.

This is intentionally not a full replacement for the mutable control plane.

## Goal

Add a runtime mode that:

- reads table and checkpoint metadata from the object store
- serves queries from published active checkpoints
- plans local prefetch from published target checkpoints
- performs no metadata writes
- requires no DynamoDB-backed state authority

This mode should be suitable for:

- single-node serving
- local development against exported metadata
- simple deployments where metadata publication happens elsewhere
- future staged migration away from tightly coupled metadata services

## Non-Goals

This design does not attempt to replace the full mutable metadata plane.

Out of scope for v1:

- `create_table`, `speedboat_commit`, `iceberg_commit`, or any other metadata
  mutation
- extension, compaction, or cleanup work claiming
- serving-node leases or activation acknowledgements
- cutover planning
- org creation or access-key lookup
- background metadata publication
- multi-writer coordination

Those behaviors are part of the current control-plane contract in
[service_lib/src/metadata_store.rs](../service_lib/src/metadata_store.rs) and
the in-memory service snapshot in
[service_lib/src/ephemeral_service_impl.rs](../service_lib/src/ephemeral_service_impl.rs).

## Why Read-Only First

The repo already stores data files in the object store. The gap is metadata
authority, not file access.

Today:

- runtime provider selection happens through `StateMode` in
  [control_plane/src/test_api.rs](../control_plane/src/test_api.rs)
- the runtime dispatches providers in
  [query_runtime/src/state_provider.rs](../query_runtime/src/state_provider.rs)
- query execution already reads Iceberg, Parquet, JSON, and Arrow data from
  object storage in
  [query_lib/src/data_access.rs](../query_lib/src/data_access.rs)

The current metadata authority is larger than checkpoint lookup:

- published active and target checkpoint pointers
- aliases, templates, pipelines, and lifetime policies
- checkpoint publication queues
- extension, compaction, and cleanup work items
- serving-node leases and activations

That state is visible in
[service_lib/src/ephemeral_service_impl.rs](../service_lib/src/ephemeral_service_impl.rs).

Replacing all of that with object-store-only coordination is a separate
project. A read-only provider avoids that problem and gives the serving layer a
useful new operating mode now.

## Existing Runtime Semantics To Preserve

The provider must fit the current runtime expectations.

### Active vs Target Checkpoints

Serving queries resolve the current active serveable checkpoint through
[query_runtime/src/lakehouse_serving.rs](../query_runtime/src/lakehouse_serving.rs).

The handle-level API in
[query_runtime/src/state_provider.rs](../query_runtime/src/state_provider.rs)
distinguishes:

- published active checkpoint
- published target checkpoint
- local prefetched target checkpoint

The naming is not perfect, but the behaviors matter:

- active checkpoint: the one queries should use now
- target checkpoint: the next checkpoint the node should prefetch toward
- local target checkpoint: the latest checkpoint this node has already warmed

### Prefetch Flow

Prefetch today is coordinated locally with
[query_runtime/src/ephemeral_fetch_tracker.rs](../query_runtime/src/ephemeral_fetch_tracker.rs)
and executed through
[query_runtime/src/prefetch.rs](../query_runtime/src/prefetch.rs).

The read-only provider should reuse the same basic model:

- desired target comes from published metadata in the object store
- local warmed target stays in process memory
- `get_next_prefetch_checkpoints(...)` compares desired target with local warm
  state
- `set_target_checkpoints(...)` records local warm completion only

### Native Cache Mode

If this mode is supposed to avoid external state dependencies, it cannot keep
requiring Redis just to start.

Today `CacheMode::Native` still panics in
[query_runtime/src/state_provider.rs](../query_runtime/src/state_provider.rs).

The read-only object-store mode therefore depends on adding a supported
in-process cache path.

## Proposed Mode

Add a new `StateMode` variant in
[control_plane/src/test_api.rs](../control_plane/src/test_api.rs):

```rust
ObjectStoreReadOnly {
    metadata_root: String,
    org_id: String,
    refresh_interval_ms: Option<u64>,
}
```

Field semantics:

- `metadata_root`
  The object-store URI prefix containing Powdrr metadata, for example
  `s3://warehouse/powdrr-state`.
- `org_id`
  The single org namespace this process serves.
- `refresh_interval_ms`
  Poll interval for metadata refresh. Default `5000`.

This should be wired into engine startup in
[engine/src/configuration.rs](../engine/src/configuration.rs) under a new mode
such as `MODE=object-store-readonly`.

Expected runtime defaults:

- `storage_mode = StorageMode::S3 { ... }`
- `cache_mode = CacheMode::Native`
- `peer_mode = PeerMode::SelfOnly`
- `indexing_mode = IndexingMode::Disabled`
- `compaction_mode = CompactionMode::Disabled`
- `prefetch_mode` left configurable

## Metadata Storage Layout

The read-only provider should not attempt to deserialize the full mutable
service snapshot. That format includes transient coordination state that the
serving node should not own.

Instead, use a purpose-built manifest format with immutable manifests and a
small mutable pointer.

### Object Keys

```text
<metadata_root>/orgs/<org_id>/manifest-pointer.json
<metadata_root>/orgs/<org_id>/manifests/<generation>.json
<metadata_root>/orgs/<org_id>/checkpoints/<table>/<escaped-full-checkpoint-id>.json
```

### Manifest Pointer

`manifest-pointer.json`:

```json
{
  "format_version": 1,
  "generation": 42,
  "manifest_key": "orgs/default/manifests/00000042.json",
  "written_at_ms": 1747850000000
}
```

Semantics:

- this is the only mutable object on the hot path
- updating the pointer publishes a new manifest generation
- the provider should treat the pointer file as the atomic publication source

### Immutable Manifest

`manifests/<generation>.json`:

```json
{
  "format_version": 1,
  "org_id": "default",
  "generation": 42,
  "written_at_ms": 1747850000000,
  "tables": {
    "logs": {
      "...": "serialized TableDescription"
    }
  },
  "aliases": {
    "logs_alias": "logs"
  },
  "table_templates": {},
  "pipelines": {},
  "lifetime_policies": {},
  "published": {
    "base": {
      "logs": {
        "active": "cp-100",
        "target": "cp-101"
      }
    },
    "es": {
      "logs": {
        "active": "cp-100",
        "target": "cp-101"
      }
    }
  }
}
```

Rules:

- `tables` values are serialized
  [`TableDescription`](../control_plane/src/data_contract.rs)
- `aliases` maps alias name to canonical table name
- `published.base` holds no-extension publication pointers
- `published.<extension>` holds extension-scoped publication pointers

### Checkpoint Objects

Checkpoint bodies should be serialized
[`TableMetadataCheckpoint`](../control_plane/src/data_contract.rs).

Key naming should use the logical full checkpoint identifier from
[control_plane/src/checkpoint_descriptor.rs](../control_plane/src/checkpoint_descriptor.rs):

- plain checkpoint: `<checkpoint_id>`
- replacement checkpoint: `<original_checkpoint_id>:<checkpoint_id>`

The key segment should then be URL-escaped before writing it into the object
path.

This preserves compatibility with replacement checkpoints that retain an
`original_checkpoint_id`.

## Provider Structure

Add a new runtime implementation file:

- `query_runtime/src/object_store_state_provider.rs`

Primary types:

- `ObjectStoreReadOnlyStateProvider`
- `ObjectStoreReadOnlyConfig`
- `ObjectStoreManifestPointer`
- `ObjectStoreManifest`
- `PublishedCheckpointTargets`
- `ManifestCache`
- `CheckpointCacheKey`

Suggested provider fields:

- `config: ObjectStoreReadOnlyConfig`
- `store: Arc<dyn object_store::ObjectStore>`
- `manifest_cache: Option<ManifestCache>`
- `checkpoint_cache: HashMap<CheckpointCacheKey, TableMetadataCheckpoint>`
- `fetch_tracker: EphemeralFetchTracker`
- `last_refresh_ms: i64`

Important implementation note:

The provider should depend on `Arc<dyn ObjectStore>`, not on
`AmazonS3` directly. The runtime already has one hardcoded S3-shaped path in
[query_lib/src/data_access.rs](../query_lib/src/data_access.rs), but this new
provider should not deepen that coupling.

The current object-store setup logic in
[query_runtime/src/local_cli.rs](../query_runtime/src/local_cli.rs) should be
extracted into a shared helper instead of duplicated.

## Read Semantics

### Startup

Provider initialization should:

1. build the object-store client
2. load `manifest-pointer.json`
3. load the referenced manifest
4. seed the local desired prefetch targets from published target or active
   pointers
5. fail startup if pointer or manifest loading fails

Initial startup failure should be hard, not soft. A serving process without any
metadata authority cannot answer correctly.

### Refresh

The provider should refresh the manifest in two ways:

- lazily on read when `refresh_interval_ms` has elapsed
- proactively when `update_all_checkpoints()` is called by the existing runtime
  loop in [query_runtime/src/test_api.rs](../query_runtime/src/test_api.rs)

Refresh algorithm:

1. read `manifest-pointer.json`
2. if generation is unchanged, return `false`
3. otherwise load the new immutable manifest
4. swap the in-memory manifest cache
5. update desired prefetch targets
6. return `true`

Checkpoint cache entries should survive manifest refresh, because checkpoint
objects are immutable.

### Table Description

`describe_table(name)` should:

1. refresh if stale
2. resolve `name` through the alias map if present
3. return the matching canonical `TableDescription`

Alias semantics must match the current in-memory implementation in
[query_runtime/src/ephemeral_service_impl.rs](../query_runtime/src/ephemeral_service_impl.rs).

### Table Enumeration

`get_all_iceberg_tables()` should return canonical table names only.

It should not include aliases, because callers like
[query_server/src/elastic_search_endpoints.rs](../query_server/src/elastic_search_endpoints.rs)
already derive alias behavior separately from table descriptions.

### Checkpoint Resolution

`get_published_active_checkpoint(table, extension)`:

- read from manifest publication pointers
- extension lookup order is exact extension only
- the handle-level fallback to base checkpoints should remain in
  `StateProviderHandle`, not in the provider implementation

`get_latest_committed_checkpoint(table, extension)`:

- return the published target pointer for that scope
- if target is absent, return the active pointer

This matches how the runtime currently treats published target vs published
active in the handle methods in
[query_runtime/src/state_provider.rs](../query_runtime/src/state_provider.rs).

`get_checkpoint(descriptor)`:

1. resolve canonical table name
2. build key from `descriptor.full_checkpoint_id()`
3. read and deserialize checkpoint JSON
4. cache the result

If a manifest points at a checkpoint object that does not exist, the provider
should return a normal not-found error to the caller rather than silently
falling back.

### Templates, Pipelines, and Lifetime Policies

The read-only provider should support these read methods from the manifest:

- `describe_table_template(...)`
- `describe_pipeline(...)`
- `describe_lifetime_policy(...)`

Those reads are part of normal frontend behavior in
[query_server/src/elastic_search_endpoints.rs](../query_server/src/elastic_search_endpoints.rs)
and should not break simply because the runtime is read-only.

## Prefetch Semantics

The provider should use the existing in-memory fetch tracker model from
[query_runtime/src/ephemeral_fetch_tracker.rs](../query_runtime/src/ephemeral_fetch_tracker.rs).

There are two distinct kinds of target:

- desired target from manifest publication pointers
- local warmed target for this process

Behavior:

- on startup or manifest refresh, seed desired targets into
  `fetch_tracker.next_target`
- `get_next_prefetch_checkpoints(...)` compares desired target with local warmed
  target
- `set_target_checkpoints(...)` updates only the local warmed target

Desired target selection rule:

- use published `target` when present
- otherwise use published `active`

That ensures the prefetch loop can still warm the currently active checkpoint
after startup even when no cutover is pending.

The provider must not attempt to write activation acknowledgements. That
behavior is leaderless-service-specific and belongs to mutable coordination, not
this mode.

## Unsupported Operations

These methods should return explicit read-only errors:

- `create_table`
- `upsert_table_metadata`
- `add_alias`
- `remove_alias`
- `create_table_template`
- `create_pipeline`
- `create_lifetime_policy`
- `speedboat_commit`
- `iceberg_commit`
- `extension_commit`
- `compaction_commit`
- `cleanup_commit`

Suggested error message form:

`Object-store read-only mode does not support <operation>`

These methods should quietly return empty results:

- `get_extension_work_items()`
- `get_compaction_work_items()`
- `get_cleanup_work_items()`

This keeps the runtime background loops idle instead of generating repeated
errors in a mode that intentionally does not own background work.

`lookup_secret_access_key(...)` should return `Ok(None)`.

## Consistency And Publication Rules

The provider should assume:

- checkpoint objects are immutable
- manifest generations are immutable
- only `manifest-pointer.json` is mutable

Publication order from whatever writer/exporter owns this metadata must be:

1. write checkpoint object(s)
2. write immutable manifest
3. update pointer file

If that order is violated, readers may observe a manifest that references
missing checkpoint objects.

Read-side consistency rules:

- if refresh fails after startup, continue serving from the last known good
  manifest and log the error
- if a requested checkpoint object cannot be loaded, fail the specific request
- never derive state by listing arbitrary prefixes on the hot path

## Implementation Plan

### Files To Touch

- [control_plane/src/test_api.rs](../control_plane/src/test_api.rs)
  Add `StateMode::ObjectStoreReadOnly`.
- [engine/src/configuration.rs](../engine/src/configuration.rs)
  Add env/config parsing for the new runtime mode.
- [query_runtime/src/state_provider.rs](../query_runtime/src/state_provider.rs)
  Add the new provider variant and stop panicking on `CacheMode::Native`.
- `query_runtime/src/object_store_state_provider.rs`
  New provider implementation.
- object-store helper extraction from
  [query_runtime/src/local_cli.rs](../query_runtime/src/local_cli.rs)
  so runtime and CLI can share setup logic.

### Phase 1

- new mode wiring
- object-store client factory
- manifest pointer + manifest loading
- `describe_table`
- `get_all_iceberg_tables`
- `get_published_active_checkpoint`
- `get_latest_committed_checkpoint`
- `get_checkpoint`
- explicit read-only errors for unsupported writes

### Phase 2

- template / pipeline / lifetime-policy reads
- manifest refresh polling
- prefetch seeding and local warmed-target tracking
- startup and refresh tests

### Phase 3

- exporter / publisher for manifest generation from the current metadata
  authority
- operational docs for how metadata gets written

The provider itself is not enough. A separate metadata publisher must exist or
be added.

## Testing Plan

Add focused tests around the new provider:

- pointer refresh loads a new manifest generation
- alias resolution returns the canonical table description
- active and target checkpoint lookup work for base and extension scopes
- checkpoint lookup correctly handles replacement checkpoints with
  `original_checkpoint_id`
- unsupported mutation methods return read-only errors
- `get_extension_work_items`, `get_compaction_work_items`, and
  `get_cleanup_work_items` stay empty
- `update_all_checkpoints()` returns `true` only when manifest generation
  changes
- prefetch scheduling emits work for target checkpoints not yet warmed locally

Add one integration-style runtime test that:

- loads manifest + checkpoint metadata from a test object store
- resolves the active serveable checkpoint
- successfully executes a serving query

Using an in-memory or local filesystem object-store backend for tests is
preferable to booting a full MinIO stack just for provider logic.

## Open Questions

- Should the manifest include org settings at all, or should read-only mode
  ignore them completely?
- Should table templates, pipelines, and lifetime policies be mandatory in the
  manifest, or optional sections?
- Should the provider support multiple orgs in one process later, or stay
  explicitly single-org?
- Should checkpoint metadata stay as individual JSON objects forever, or should
  larger packed manifests be introduced once checkpoint counts grow?

## Follow-On Work

After this mode exists, the next logical steps are:

1. add a metadata exporter from the current mutable authority
2. make local serving and offline environments use the read-only provider
3. decide whether a future writable object-store metadata authority is still
   worth pursuing

That later writable project should be treated separately. It has different
coordination and correctness risks than this read-only serving mode.
