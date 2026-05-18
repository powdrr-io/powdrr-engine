# Raft Metadata And Coherence Design

## Goal

Replace DynamoDB-backed metadata coordination with an embedded consensus layer
that keeps Iceberg snapshot publication, derived work, and serving visibility
coherent across many Powdrr servers.

The design target is:

- no dependency on DynamoDB for correctness
- no process-local coordination state that can be lost on restart
- one durable published serving frontier per table
- automatic recovery after node loss
- support for many readers and workers across many servers

## What Exists Today

Today there are three different kinds of cluster-related behavior in the code:

1. peer discovery and fanout
2. metadata coordination
3. node-local cache/prefetch tracking

Only the first item has a non-DynamoDB path today.

### Peer Discovery

The repo already has a basic `PeerProvider` that can:

- run self-only
- use a static remote peer list
- discover Docker/Kubernetes peers

This is enough to find other servers, but it is not consensus and it is not a
durable metadata store.

### Metadata Coordination

The current durable metadata model is DynamoDB-specific. It persists:

- latest checkpoint pointers
- checkpoints
- speedboat commits
- iceberg commits
- extension commits
- extension work items
- compaction work items
- cleanup work items
- tracker rows and leases

This is the part that must be replaced.

### Node-Local Tracking

Some important state is still only in memory today:

- the `update_cache` that drives `update_all_checkpoints`
- prefetch target tracking
- local loaded/activated cache state

That is acceptable for cache warming, but not for correctness.

## Why Raft

If the goal is "no external coordination service", the clean replacement is an
embedded replicated metadata log.

Raft gives:

- one leader at a time for metadata mutation
- replicated durable state across Powdrr nodes
- linearizable metadata updates when needed
- deterministic failover

That makes it a better fit than the current leaderless or peer-fanout pieces
for:

- latest published checkpoint
- work queues
- worker ownership
- cleanup safety

## Architecture

Every Powdrr server runs two roles:

1. data plane
2. metadata plane

### Data Plane

The data plane continues to:

- serve reads
- prefetch files
- build extension outputs
- run compaction jobs
- host node-local caches

### Metadata Plane

The metadata plane is a Raft replica. It owns:

- table metadata
- checkpoint graph
- work queues
- claims/leases
- published serving frontier
- cleanup eligibility

One server is the current Raft leader. All metadata mutations go through that
leader. Reads that require strict coherence should also come from the leader or
use a linearizable read path.

## State Machine Model

The easiest path is to model the existing DynamoDB concepts directly first,
then simplify later.

### Core Objects

Use the following replicated state-machine records.

#### TableRecord

Per table:

- table name
- tags
- serving config
- aliases
- templates/pipeline references as needed

#### CheckpointRecord

Per checkpoint:

- `table_name`
- `checkpoint_id`
- `original_checkpoint_id`
- full `TableMetadataCheckpoint`
- creation term/index
- status

Checkpoint status should be explicit:

- `Draft`
- `WaitingForExtension { extension: "es" }`
- `Published`
- `Superseded`
- `PendingCleanup`

#### PublishedFrontier

Per table and query class:

- `standard_checkpoint`
- `search_checkpoint`

Longer term, prefer one published serving frontier where possible. If query
classes must differ, make that split explicit in metadata instead of implicit
in code.

#### CommitLogRecord

Persist incoming logical commits:

- `SpeedboatCommit`
- `IcebergCommit`
- `ExtensionCommit`
- `CompactionCommit`
- `CleanupCommit`

This is primarily for auditability, replay, and deterministic reconciliation.

#### WorkItemRecord

A unified work-item model is simpler than several separate pseudo-queues.

Fields:

- `work_item_id`
- `table_name`
- `kind`
- `payload`
- `state`
- `claim`
- `created_at`
- `updated_at`

Kinds:

- `AdvanceStandardCheckpoint`
- `AdvanceExtensionCheckpoint`
- `BuildExtension`
- `Compaction`
- `Cleanup`

States:

- `Pending`
- `Claimed`
- `Completed`
- `Cancelled`

Claim:

- `node_id`
- `claim_epoch`
- `lease_expires_at`

#### ActivationRecord

If strict multi-node cutover matters, track server activation:

- `node_id`
- `checkpoint_id`
- `extension`
- `activated_at`

This is optional for first cut, but required before you can safely say "all
servers are now serving checkpoint X".

## Mapping From The Current DynamoDB Model

Map the current entities to Raft state like this.

### Direct Mappings

- `powdrr_table` -> `TableRecord`
- `checkpoint` -> `CheckpointRecord`
- `latest checkpoint` -> `PublishedFrontier`
- `speedboat_commit`, `iceberg_commit`, `extension_commit`, `compaction` ->
  `CommitLogRecord`
- `extension_work_item`, `compaction_work_item`, `cleanup_work_item` ->
  `WorkItemRecord`

### Tracker And Lease Mappings

The current tracker rows become explicit queue or claim state.

- `speedboat_commit_checkpointed` ->
  `WorkItemRecord { kind: AdvanceStandardCheckpoint }`
- `extension_commit_checkpointed` ->
  `WorkItemRecord { kind: AdvanceExtensionCheckpoint }`
- `checkpoint_waiting_for_extension` ->
  `CheckpointRecord.status = WaitingForExtension`
- `extension_work_item_lease`, `compaction_work_item_lease`,
  `cleanup_work_item_lease` -> `WorkItemRecord.claim`

### Things To Delete In The New Model

The new system should not preserve:

- process-local `update_cache`
- tracker rows that only exist to emulate a queue
- split "latest entity id" indirection for work items

Raft state should directly hold queue items and published frontiers.

## Write Path

All metadata writes go through the leader.

### Speedboat Commit

1. Client sends `SpeedboatCommit` to any node.
2. Non-leader forwards to leader.
3. Leader appends:
   - `CommitLogRecord`
   - `WorkItemRecord { kind: AdvanceStandardCheckpoint }`
   - `WorkItemRecord { kind: BuildExtension }` if needed
4. Entry commits once replicated.
5. Workers may now claim the generated work.

### Iceberg Commit

1. Client sends `IcebergCommit`.
2. Leader appends:
   - `CommitLogRecord`
   - new draft checkpoint derived from previous checkpoint plus Iceberg commit
   - extension build work for the new files
   - compaction replacement / cleanup work if referenced
3. If full extension coverage is not yet present:
   - checkpoint status becomes `WaitingForExtension`
4. Published frontier does not advance until rules are satisfied.

### Extension Commit

1. Extension worker builds derived files.
2. Worker sends `ExtensionCommit` to leader.
3. Leader appends:
   - `CommitLogRecord`
   - checkpoint coverage update
   - `AdvanceExtensionCheckpoint` work item or direct publish if the checkpoint
     is now fully covered
4. Once full coverage exists, leader advances the published search frontier.

## Read Path

This is where coherence must become strict.

### Rule

Queries must only read from published checkpoint metadata.

That means:

- choose the checkpoint from `PublishedFrontier`
- load exact file sets from the checkpoint
- load exact extension files from `checkpoint.extension_metadata`
- never guess sidecar filenames from base file paths

### Query Classes

There are two acceptable models.

#### Model A: One Frontier

Every query type uses the same published checkpoint.

Pros:

- simplest correctness story
- no query-class skew

Cons:

- publication waits for all required derived data

#### Model B: Multiple Explicit Frontiers

Maintain:

- `standard_checkpoint`
- `search_checkpoint`

Then every query path must declare which frontier it needs.

Pros:

- more flexible

Cons:

- more complex user-visible semantics

If this model is kept, the split must be explicit in metadata and tests.

## Work Claiming

Workers may run on any server.

### Claim Protocol

1. Worker asks leader for next claimable work item of a supported kind.
2. Leader marks it `Claimed` with:
   - `node_id`
   - `claim_epoch`
   - `lease_expires_at`
3. Worker executes.
4. Worker sends completion command.
5. Leader transitions work item to `Completed`.

### Recovery

If worker dies:

- leader notices expired lease
- work returns to `Pending`
- another node may reclaim it

This is the Raft replacement for the current DynamoDB lease rows.

## Peer Discovery And Membership

Raft still needs bootstrap and membership handling.

### Discovery

Reuse the current peer-discovery sources for initial contact:

- static remote address list
- Kubernetes pod discovery
- Docker env list

### Membership

Do not treat dynamic peer discovery as the Raft membership list.

Instead:

- discover seed nodes
- join cluster through leader
- leader commits membership changes through Raft

This avoids accidental split-brain caused by a pod list changing underneath the
consensus set.

## Cleanup Safety

Cleanup must not be based only on "new checkpoint exists".

Use one of these policies.

### Minimum Safe Policy

- retain old files for a fixed safety window
- only delete when replacement checkpoint is published

### Strong Policy

- replacement checkpoint is published
- all nodes have activated the new checkpoint
- no in-flight request is pinned to the old checkpoint

The strong policy requires `ActivationRecord` plus request pinning or epoch
tracking.

## Server Activation

For many servers, "published" and "actively served everywhere" are not the
same.

Recommended model:

1. leader publishes checkpoint X
2. nodes observe new frontier
3. nodes prefetch and activate checkpoint X
4. nodes ack activation
5. cleanup of checkpoint X-1 only happens after quorum or all-node policy

For correctness, requests should be pinned to the checkpoint chosen at request
start.

## Failure Scenarios

### Writer Node Dies After Commit

Safe if:

- commit and resulting work item were in Raft before ack

Not safe if:

- work trigger lives only in local memory

This is why `update_cache` must disappear.

### Extension Worker Dies Mid-Build

Safe if:

- claim lease expires
- work returns to `Pending`

### Leader Dies During Publication

Safe if:

- publish intent is in Raft log
- new leader replays committed state and resumes scheduling

### Network Partition

Safe if:

- only quorum side can elect leader and mutate metadata

Minority side may continue serving already activated checkpoints in read-only
mode if desired, but it must not publish new metadata.

## Recommended Implementation Plan

### Phase 1: Define Metadata Interface

Create a storage-agnostic metadata trait for:

- table CRUD
- checkpoint CRUD
- published frontier reads/writes
- work queue operations
- work claim/complete/retry
- activation tracking

Keep current behavior behind the trait.

### Phase 2: Remove Process-Local Correctness State

Move these behind the metadata trait first:

- `update_cache`
- checkpoint advancement triggers
- work item claims

This improves the DynamoDB path immediately and shrinks the Raft delta.

### Phase 3: Unify Read Visibility

Change read paths to:

- resolve only through published frontier metadata
- load derived files from checkpoint metadata

Do this before or alongside Raft.

### Phase 4: Add Embedded Raft Backend

Implement:

- replicated log
- snapshots
- leader RPC
- metadata state machine
- worker claim/lease flow

Run one replica per Powdrr server.

### Phase 5: Activation And Cleanup Safety

Add:

- node activation ack
- checkpoint pinning
- safe cleanup barrier

### Phase 6: Remove DynamoDB Dependency

When Raft backend reaches parity:

- make it default
- keep DynamoDB backend optionally for migration
- eventually remove DynamoDB-specific queue logic

## Practical Recommendation

If the target is "many servers and always coherent", the right sequence is:

1. stop relying on local memory for checkpoint advancement
2. unify reads onto published checkpoint metadata
3. store exact extension artifacts in the published checkpoint path
4. then replace DynamoDB with a Raft-backed metadata state machine

That sequence reduces correctness risk while making the Raft migration much
smaller.
