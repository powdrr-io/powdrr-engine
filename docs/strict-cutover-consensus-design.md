# Strict Cutover Consensus Design

## Goal

Serve only cluster-committed, coherent checkpoints during Iceberg snapshot advancement.

The cutover model separates:

- `active checkpoint`: the checkpoint every server is allowed to serve
- `target checkpoint`: the next checkpoint the cluster is preparing to activate

## State Machine

The metadata boundary now exposes the core strict-cutover records:

- `PublishedCheckpointRole`
  - `Active`
  - `Target`
- `PublishedCheckpointSelector`
  - `{ table_name, extension, role }`
- `PublishedCheckpointRecord`
  - `{ selector, checkpoint_id }`
- `CutoverEpoch`
  - monotonic epoch used to reject stale activation acks
- `CheckpointCutoverState`
  - `{ selector, epoch, active_checkpoint_id, target_checkpoint_id }`
- `ServingNodeLease`
  - `{ node_id, membership_epoch, observed_at_ms }`
- `ServingNodeActivationAck`
  - `{ selector, node_id, epoch, checkpoint_id, activated_at_ms }`
- `CheckpointCutoverRequest`
  - `{ org_id, selector, target_checkpoint_id }`

## Intended Flow

1. Leader observes a coherent next checkpoint.
2. Leader writes a `CheckpointCutoverRequest` for the `Target` role.
3. The cluster prefetches and activates the target checkpoint locally.
4. Each live serving node records a `ServingNodeActivationAck`.
5. Once the required membership has acked the target epoch, the leader promotes:
   - `active_checkpoint_id = target_checkpoint_id`
6. Reads continue to use only the `Active` role.

## Current Compatibility Behavior

Current DynamoDB and ephemeral backends still expose one published checkpoint frontier.

For this compatibility slice:

- `Active` resolves to the current published checkpoint
- `Target` falls back to the same published checkpoint unless a backend starts tracking a distinct target frontier
- node leases and activation acks are no-op hooks in the current backends

This lets the codebase speak in strict-cutover terms without requiring the Raft backend to land in the same change.

## Codebase Mapping

- Read paths now explicitly use `get_active_servable_checkpoint(...)`
- Target/preparation paths can use `get_target_servable_checkpoint(...)`
- The metadata boundary in `main_lib/src/metadata_store.rs` and `service_lib/src/metadata_store.rs`
  now defines the strict-cutover records needed by a future consensus backend

## Remaining Work

1. Add a durable backend that persists `Target`, `Active`, `CutoverEpoch`, membership leases, and activation acks.
2. Define node identity and live-serving membership.
3. Make prefetch/activation paths emit durable `ServingNodeActivationAck` records.
4. Promote `Target` to `Active` only after quorum or required live membership ack.
5. Gate cleanup on the minimum active or acked checkpoint still needed by live members.
6. Replace test-only or process-local startup mode switching with explicit cluster-backed runtime configuration.
