# Redis Dependency Removal Plan

This document describes how Powdrr should remove its Redis dependency, what
Redis is currently doing in the codebase, the replacement options, and the
recommended migration path.

The short version is:

- the current Redis footprint is small
- one part is correctness-sensitive
- one part is best-effort scoring metadata
- the easiest short-term replacement is a single in-memory owner
- the easiest long-term architecture to maintain is a service-owned replicated
  state path, ideally backed by Raft

## Goal

Remove Redis from Powdrr's runtime and compatibility flows without regressing:

- monotonic `_seq_no` assignment for mutations
- document `_version` behavior
- search result ordering semantics that depend on `_seq_no`
- approximate row-count inputs used in query scoring
- clustered serving correctness

This should also remove the operational burden of:

- requiring Redis locally for common flows
- requiring Redis in compatibility and regression test stacks
- keeping Redis in the write-critical mutation path

## Current Redis Footprint

Redis is not acting as a general cache in Powdrr today. The current runtime
surface in
[query_runtime/src/distributed_cache.rs](../query_runtime/src/distributed_cache.rs)
stores only two pieces of shared state per table:

- the next table `_seq_no`
- the approximate row count

### Current Runtime Call Sites

#### 1. Sequence Number Allocation

The mutation path allocates `_seq_no` ranges through
`distributed_cache::report_table_changes(...)` in
[query_runtime/src/elastic_search_storage_schema.rs](../query_runtime/src/elastic_search_storage_schema.rs).

That allocation feeds:

- `_seq_no`
- `_id_seq_no`
- mutation result envelopes
- delete tombstone identity

This is correctness-sensitive shared state.

#### 2. Approximate Row Count

Search scoring reads the approximate record count through
`distributed_cache::get_approx_num_records(...)` in
[query_runtime/src/elastic_search_commands.rs](../query_runtime/src/elastic_search_commands.rs).

That value is used to compute BM25-style score constants. This is not a core
correctness boundary in the same way as `_seq_no`.

#### 3. Table Initialization

Table creation currently seeds Redis keys from
[query_runtime/src/state_provider.rs](../query_runtime/src/state_provider.rs)
by calling `distributed_cache::create_table(...)` before table metadata writes.

#### 4. Startup And Testing Assumptions

The current mode wiring and local flows still assume Redis:

- `CacheMode::Redis` remains the default in
  [control_plane/src/test_api.rs](../control_plane/src/test_api.rs)
- `CacheMode::Native` still panics in
  [query_runtime/src/state_provider.rs](../query_runtime/src/state_provider.rs)
- engine leaderless mode still sets `cache_mode: CacheMode::Redis(None)` in
  [engine/src/configuration.rs](../engine/src/configuration.rs)
- local scripts and compatibility suites still start or require Redis, as noted
  in [README.md](../README.md)

## What Must Remain Correct

Any replacement must preserve these invariants.

### `_seq_no` Must Be Monotonic

Within the chosen allocation scope, `_seq_no` assignment must remain monotonic.

Today that scope is effectively per table. Longer term, the repo already points
toward per-shard or per-writer ownership in
[docs/slatedb-es-search-plan.md](./slatedb-es-search-plan.md).

### `_version` Must Stay Coherent

`_version` is carried in mutation logic and response rendering in:

- [query_runtime/src/elastic_search_storage_schema.rs](../query_runtime/src/elastic_search_storage_schema.rs)
- [query_runtime/src/elastic_search_ingest.rs](../query_runtime/src/elastic_search_ingest.rs)
- [query_runtime/src/private_api.rs](../query_runtime/src/private_api.rs)

Removing Redis should not quietly change the existing version contract.

### Approximate Counts Must Stop Being Write-Critical

Approximate row count is currently used in scoring, but it should not force a
cluster-wide consensus write in the hot path if the value is only a heuristic.

The replacement should treat:

- `_seq_no` as correctness-sensitive
- `approx_num_records` as best-effort

### Cluster Shape Must Not Leak Into Query Code

The data-plane mutation path should not own ad hoc discovery of a special host
for sequence allocation. That would be easy to prototype and expensive to
maintain.

The routing boundary should be explicit and reusable.

## Options

There are three realistic directions.

### Option A: Single Host In-Memory Ownership

One process holds the shared counters in memory. Other nodes discover it and ask
it for `_seq_no` ranges and count updates.

#### Pros

- smallest implementation
- no external Redis dependency
- easiest first milestone

#### Cons

- introduces a hidden singleton
- requires discovery, failover, and retry logic in the data plane
- loses state on owner restart unless rebuilt carefully
- creates a new special-purpose coordination path outside the service metadata
  layer
- duplicates future Raft-like leadership concepts instead of reusing them

#### Verdict

Easier to ship first, worse to own long-term.

### Option B: Service-Owned In-Memory Coordination

Move the counter logic behind the existing service boundary first, but keep the
initial backend in memory.

That means engines stop calling Redis directly and instead call a service-level
counter API.

#### Pros

- removes Redis from the runtime surface quickly
- avoids pushing discovery logic into the engines
- creates the right abstraction boundary for later backends
- lets ephemeral, DynamoDB, and Raft backends all implement the same contract

#### Cons

- still not durable if the chosen backend is process-local memory
- requires adding a new service API shape before Redis is fully gone

#### Verdict

Best transitional shape.

### Option C: Replicated Metadata-Owned Coordination Via Raft

Move the shared counter state into the service metadata plane and back it with
Raft.

The repo is already heading toward a replicated metadata direction in
[docs/raft-metadata-coherence-design.md](./raft-metadata-coherence-design.md)
and [service_lib/src/raft_service_impl.rs](../service_lib/src/raft_service_impl.rs).

#### Pros

- one authoritative coordination layer
- no special singleton outside the metadata plane
- consistent with the repo's existing "remove external coordination
  dependencies" direction
- cleanest long-term answer for clustered correctness

#### Cons

- more work than a single-host in-memory owner
- current `RaftServiceImpl` uses `openraft_memstore`, so replicated state is not
  yet durable across full process restart
- requires a persistent Raft storage story before it is a full production-grade
  Redis replacement

#### Verdict

Best long-term architecture.

## Recommendation

Use a two-stage recommendation:

1. **Transitional architecture**
   Move Redis responsibilities behind a service-owned coordination abstraction.
   The first implementation can be in-memory.
2. **Long-term architecture**
   Back that same abstraction with replicated service metadata, preferably Raft,
   once the Raft storage layer is persistent.

So the answer to "what is easier to maintain long-term?" is:

- **Raft-backed service-owned coordination**

The answer to "what is easiest to implement immediately?" is:

- **single owner in memory**

The right plan is to avoid baking the short-term answer into the data-plane API.

## Target Architecture

### Core Principle

The query runtime should stop knowing about Redis directly.

Instead, it should depend on a narrow coordination interface that can be backed
by:

- ephemeral in-memory state
- DynamoDB-backed metadata
- Raft-backed metadata

### New Abstraction

Add a small coordination contract, for example:

```rust
pub trait MutationCoordination {
    async fn init_table(&mut self, table: &str) -> Result<(), CoordinationError>;

    async fn allocate_seq_no_range(
        &mut self,
        table: &str,
        num_ops: u64,
    ) -> Result<std::ops::RangeInclusive<u64>, CoordinationError>;

    async fn apply_count_delta(
        &mut self,
        table: &str,
        inserts: i64,
        deletes: i64,
    ) -> Result<Option<u64>, CoordinationError>;

    async fn get_approx_num_records(
        &mut self,
        table: &str,
    ) -> Result<Option<u64>, CoordinationError>;
}
```

Key design decisions:

- `_seq_no` allocation is required
- count updates are allowed to be approximate
- count lookups may return `None`
- the mutation path should not panic if the approximate count is unavailable

### Where It Should Live

Do not put the long-term abstraction in another runtime-local singleton like
[query_runtime/src/distributed_cache.rs](../query_runtime/src/distributed_cache.rs).

Preferred ownership:

- service metadata layer
- exposed to runtimes through explicit methods

That keeps:

- one authority for correctness-sensitive shared mutation state
- one place to change backends later
- one compatibility story across ephemeral, DynamoDB, and Raft

## Data Model

The current Redis state can be modeled as a tiny record per allocation scope.

Suggested shape:

```rust
struct MutationCounterRecord {
    scope: String,
    next_seq_no: u64,
    approx_num_records: i64,
    updated_at_ms: i64,
}
```

Initial scope:

- per table

Likely future scope:

- per shard or per logical write owner

That future is already consistent with the direction in
[docs/slatedb-es-search-plan.md](./slatedb-es-search-plan.md).

## Backend Strategy

### Ephemeral Backend

Use a process-local `HashMap<String, MutationCounterRecord>`.

Use this for:

- single-process local development
- pure ephemeral runtime mode
- tests that do not need distributed coordination

### DynamoDB Backend

Use atomic counter updates in the service metadata plane.

This keeps parity for current durable deployments and removes Redis even before
Raft is ready.

### Raft Backend

Store `MutationCounterRecord` in the replicated state machine.

Important caveat:

Current `RaftServiceImpl` is built on `openraft_memstore` in
[service_lib/src/raft_service_impl.rs](../service_lib/src/raft_service_impl.rs).

That means the current Raft backend is replicated but not durably persisted
across full process restart. Before Raft can fully replace Redis for clustered
correctness, it needs:

- persistent Raft storage
- or a safe replay/rebuild strategy for counters from durable history

## Approximate Count Policy

Approximate row count should stop being a hard dependency in query execution.

Today scoring turns a count lookup failure into a command error in
[query_runtime/src/elastic_search_commands.rs](../query_runtime/src/elastic_search_commands.rs).

That should change.

Recommended policy:

- if approximate count exists, use it
- otherwise fall back to checkpoint/file stats when available
- otherwise use a stable default or score expression variant that does not
  require the total count

This makes the scoring stat:

- useful
- not correctness-critical
- not a reason to force consensus updates in the write path

## Detailed Migration Plan

### Phase 0: Document And Freeze The Current Surface

Track the current Redis call sites and required invariants.

Outcome:

- no new Redis call sites added
- everyone agrees Redis is only replacing counter state, not a generic cache

### Phase 1: Introduce Native Cache Support

Current `CacheMode::Native` still panics in
[query_runtime/src/state_provider.rs](../query_runtime/src/state_provider.rs).

Replace that panic with a supported local path.

Outcome:

- runtime can start without Redis in single-node/local modes
- tests can begin migrating off Redis incrementally

### Phase 2: Add A Coordination Abstraction

Replace direct imports of `distributed_cache` in:

- [query_runtime/src/elastic_search_storage_schema.rs](../query_runtime/src/elastic_search_storage_schema.rs)
- [query_runtime/src/elastic_search_commands.rs](../query_runtime/src/elastic_search_commands.rs)
- [query_runtime/src/state_provider.rs](../query_runtime/src/state_provider.rs)

with calls to a narrow coordination interface.

Outcome:

- Redis is no longer hard-coded in the mutation path
- backend selection becomes an implementation detail

### Phase 3: Put The Abstraction Behind The Service Boundary

Add service-level methods for:

- initialize mutation counters for a table
- allocate `_seq_no` ranges
- apply approximate count deltas
- read approximate count

That lets engines talk to "the metadata authority" instead of "Redis".

Outcome:

- no dedicated discovery path for a special in-memory owner in engines
- one reusable coordination boundary for all service modes

### Phase 4: Implement Ephemeral And DynamoDB Backends

Ephemeral:

- in-memory `HashMap`

DynamoDB:

- atomic counter row per scope

Outcome:

- Redis can be removed even before Raft is production-ready
- current durable deployments have a non-Redis path

### Phase 5: Make Approximate Count Best-Effort

Change search scoring so missing approximate count does not fail the request.

Possible fallback sources:

- checkpoint file stats
- row-group stats
- constant/default estimate

Outcome:

- only `_seq_no` remains correctness-sensitive
- count maintenance no longer dominates architecture

### Phase 6: Add Raft Counter Backing

Implement the coordination contract through the service's Raft state machine.

Do not stop here if Raft remains in-memory only.

Outcome:

- replicated shared mutation state
- no external cache/service dependency for coordination

### Phase 7: Make Raft Durable

Replace `openraft_memstore` or add durable persistence for the required state.

Until this happens, Raft is not a complete durable replacement for Redis in a
real clustered correctness story.

Outcome:

- long-term production-capable Redis replacement

### Phase 8: Remove Redis From Tooling And CI

After code no longer needs it:

- remove Redis startup from local scripts
- remove Redis from compatibility stacks where it is no longer needed
- update README and docs
- remove the `redis` crate from `query_runtime/Cargo.toml`

## File-By-File Change Surface

The first implementation pass would likely touch:

- [query_runtime/src/distributed_cache.rs](../query_runtime/src/distributed_cache.rs)
  Replace or delete the Redis-specific implementation.
- [query_runtime/src/elastic_search_storage_schema.rs](../query_runtime/src/elastic_search_storage_schema.rs)
  Swap direct `_seq_no` allocation to the new coordination path.
- [query_runtime/src/elastic_search_commands.rs](../query_runtime/src/elastic_search_commands.rs)
  Make approximate count lookup optional/best-effort.
- [query_runtime/src/state_provider.rs](../query_runtime/src/state_provider.rs)
  Remove Redis initialization assumptions and support `CacheMode::Native`.
- [control_plane/src/test_api.rs](../control_plane/src/test_api.rs)
  Revisit `CacheMode` defaults.
- [engine/src/configuration.rs](../engine/src/configuration.rs)
  Stop defaulting leaderless runtime mode to `CacheMode::Redis(None)`.
- [service_lib/src/raft_service_impl.rs](../service_lib/src/raft_service_impl.rs)
  Add the replicated counter implementation.
- [service/src/service_impl_provider.rs](../service/src/service_impl_provider.rs)
  Expose the new coordination operations through the service abstraction.
- [README.md](../README.md)
  Remove Redis as a required coordination/runtime dependency once the code no
  longer needs it.

## Risks

### Scope Leakage

If the project treats approximate count and `_seq_no` as the same class of
state, the replacement will be over-designed.

### Temporary Singleton Becoming Permanent

If the project adds "discover the special owner host" directly to the query
runtime, that shortcut may become permanent.

### Raft Without Durability

Replicated in-memory state is better than process-local state for some cases,
but it is still not durable enough to be the final long-term answer for
shared correctness-sensitive mutation counters.

### Hidden Test Coupling

Many existing tests and scripts currently assume Redis in local stacks. The
code migration and the tooling migration need to be tracked separately.

## Success Criteria

The Redis dependency is meaningfully removed when all of the following are
true:

- runtime mutation paths do not import or call Redis directly
- common local and CI flows do not require a Redis container
- `_seq_no` allocation still preserves the current mutation semantics
- approximate row count no longer causes request failure when unavailable
- service modes have a clear non-Redis coordination path
- the repo no longer describes Redis as required coordination infrastructure in
  normal operation

## Recommended End State

The end state should be:

- no Redis in the mutation path
- `_seq_no` allocation owned by the service metadata plane
- best-effort approximate scoring stats
- ephemeral and DynamoDB backends for transition
- persistent Raft-backed coordination as the long-term clustered target

That is easier to maintain than a special in-memory owner discovered by peers,
because it keeps one coherent metadata boundary instead of introducing another
cluster singleton with bespoke behavior.
