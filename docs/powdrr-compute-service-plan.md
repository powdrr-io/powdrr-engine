# Powdrr Compute Service Plan

This document lays out a concrete plan for a new `Powdrr Compute` repository.

The target is a credible streaming compute service that:

- consumes Kafka or Kinesis
- lands canonical raw events into Iceberg
- computes derived feature tables continuously
- computes derived feature tables in offline bulk runs
- supports one-off offline experiment and backfill runs
- lands those derived tables into Iceberg
- handles late-arriving data in a bounded and auditable way
- integrates with a separate `Powdrr Metadata` service where feature programs
  and deployment intent live

This plan assumes we start from Arroyo and reuse as much of its runtime model
as is practical without inheriting its entire control-plane product shape.

## Executive Summary

The best product split is:

- `Powdrr Metadata`
  - owns feature programs
  - owns program revisions
  - owns deployment intent
  - owns table and policy metadata
  - owns rollout and publication coordination

- `Powdrr Compute`
  - consumes source streams
  - maintains execution state
  - checkpoints recovery state
  - writes raw Iceberg tables
  - writes derived Iceberg tables
  - runs historical replay and bulk generation jobs
  - runs one-off offline experiment jobs
  - emits and processes late-data repair work

The right implementation strategy is:

- start from Arroyo runtime and connector concepts
- do not deploy stock Arroyo as the final architecture
- keep `Powdrr Compute` as a narrow execution service rather than a generic
  streaming SQL product
- depend on an Iceberg REST catalog rather than trying to make compute own
  Iceberg catalog semantics

## Goals

- Build a dedicated `Powdrr Compute` service in a new repository.
- Use Arroyo as the starting point for the streaming runtime.
- Support Kafka and Kinesis as input sources.
- Land all canonical raw events into append-only Iceberg tables.
- Land all derived feature outputs into append-only Iceberg tables.
- Integrate with a separate `Powdrr Metadata` service for feature-program
  definitions and deployment state.
- Make `Powdrr Compute` the execution engine for both continuous streaming
  generation and offline bulk generation.
- Support one-off offline experimentation runs that materialize results into
  Iceberg without inventing a separate compute stack.
- Handle late-arriving data in a way that is bounded, explicit, and auditable.
- Keep the compute runtime operationally simple enough to run as a normal
  service rather than a large streaming platform.

## Non-Goals

- A full Arroyo-compatible multi-tenant product.
- A SQL editor, generic query UI, or user-managed pipeline cluster in v1.
- A second online KV store in the compute write path.
- Arbitrary unbounded correction of extremely late data in the hot path.
- A compute service that owns its own registry and metadata plane.

## Why Start From Arroyo

Arroyo already has several of the hard runtime ideas we want:

- event-time processing
- watermark propagation
- stateful operators
- checkpointing and recovery
- Kafka connectors
- Kinesis connectors
- Iceberg sink patterns

That makes it a good base.

But stock Arroyo also brings assumptions we should not keep as the finished
product shape:

- a controller/API/config-database cluster model
- a generic streaming SQL surface
- broader multi-tenant control-plane concerns

So the right stance is:

- reuse Arroyo runtime pieces
- trim away the generic product surface
- make `Powdrr Compute` a focused execution service under Powdrr’s own control
  plane

## Service Split

### Powdrr Metadata Owns

- feature program definitions
- feature program revision history
- deployment specs
- input-source bindings
- raw and derived table contracts
- event-time and lateness policies
- output revision/finality policy
- rollout state
- per-program desired vs actual state
- repair policy configuration

This is the authoritative source of intent.

### Powdrr Compute Owns

- source consumption
- offset or sequence tracking
- event normalization
- raw Iceberg landing
- feature program execution
- bulk historical replay
- offline backfill execution
- one-off experiment execution
- operator state
- checkpoints
- derived Iceberg writes
- late-data detection
- repair-manifest generation
- repair execution in a slower mode

This is the authoritative source of execution.

### Why Offline Bulk Generation Belongs In Compute

`Powdrr Compute` should own offline bulk generation too.

That includes:

- initial historical generation of derived tables
- replay-based backfills after program changes
- one-off experiment runs for evaluation and training
- targeted re-materialization for selected key or time ranges

This matters because the core product promise is not just "we can stream."
The promise is:

- the same raw events
- the same feature program revision
- the same time/lateness policy
- the same execution semantics

drive both continuous and offline generation.

If offline experimentation uses a different engine, skew returns immediately.

## External Dependencies

Assuming we accept an Iceberg REST catalog, the required external dependencies
are:

- Kafka or Kinesis
- object storage
- Iceberg REST catalog
- Powdrr Metadata service

That is a credible small system.

`Powdrr Compute` should not require:

- Redis
- Postgres
- Spark
- Flink
- a separate online serving store
- a separate workflow scheduler

The metadata service may have its own persistence choices, but compute itself
should not depend on a general-purpose database for its runtime behavior.

## High-Level Architecture

```text
                  +---------------------------+
                  |     Powdrr Metadata       |
                  |---------------------------|
                  | program registry          |
                  | program revisions         |
                  | deployment specs          |
                  | lateness/finality policy  |
                  | source/table bindings     |
                  +-------------+-------------+
                                |
                                | desired deployments
                                v
                  +---------------------------+
                  |      Powdrr Compute       |
                  |---------------------------|
                  | source reader             |
                  | event normalizer          |
                  | raw landing writer        |
                  | feature runtime           |
                  | checkpoint manager        |
                  | repair planner            |
                  +------+------+-------------+
                         |      |
                         |      +--------------------------+
                         |                                 |
                         v                                 v
              +-----------------------+       +-------------------------+
              |  Iceberg REST Catalog |       |     Object Storage      |
              +-----------------------+       |-------------------------|
                                              | raw data files          |
                                              | feature data files      |
                                              | checkpoints             |
                                              | repair manifests        |
                                              | repair snapshots        |
                                              +-------------------------+
```

## Core Product Contract

The product contract should be:

- every consumed event is durably represented in a canonical raw Iceberg table
- every derived feature table is produced from declared feature programs stored
  in `Powdrr Metadata`
- every offline experiment or backfill run is also executed by `Powdrr Compute`
  against the same raw-event contract
- every output row is attributable to:
  - a feature program revision
  - a source range or checkpoint
  - a late/finality policy
- late-arriving data is not ignored silently; it is either:
  - processed in the bounded hot path
  - scheduled for slower repair
  - rejected according to explicit policy

That is what makes the system credible.

## Online And Offline Compute Modes

`Powdrr Compute` should support at least three execution modes.

### 1. Continuous Mode

- consume from Kafka or Kinesis
- update raw and derived tables incrementally
- maintain checkpoints and watermark progress

### 2. Bulk Replay Mode

- read bounded historical source ranges
- regenerate derived tables in bulk
- support first-time population and backfills

### 3. Experiment Mode

- execute one-off offline feature computations
- write outputs into experiment-scoped Iceberg tables or namespaces
- preserve program revision and source-range provenance
- avoid interfering with the primary production derived tables

The main architectural rule is that these should be different modes of one
compute system, not separate engines.

## Raw Event Landing

Every source stream should be normalized into a canonical raw event envelope
before feature computation.

Required fields:

- `event_id`
- `source_name`
- `source_partition` or `source_shard`
- `source_offset` or `source_sequence`
- `event_time`
- `ingest_time`
- `schema_revision`
- `payload`

Recommended fields:

- `event_type`
- `trace_id`
- `correlation_id`
- `dedupe_key`
- `producer_timestamp`

Why this matters:

- replay is uniform
- repair is driven from one canonical source
- feature programs do not need bespoke ingestion logic

The same raw landing contract is also what makes bulk replay and experimentation
credible. Offline jobs should read from these canonical raw tables rather than
reconstructing source-specific logic.

## Derived Table Contract

Derived feature outputs should be append-only.

Required fields:

- entity key columns
- feature timestamp
- feature values
- program revision
- emission revision
- provenance pointer to source range or checkpoint
- output status: `provisional`, `final`, or `correction`

Recommended fields:

- supersedes emission revision
- emitted watermark
- correction reason
- deployment revision

The derived table is not just "feature values." It is a revisioned, auditable
output log.

## Append-Only Output Model

The compute service should not mutate feature rows in place.

Instead:

- every output is an immutable emission
- every correction is another immutable emission
- consumers resolve the correct value using key, timestamp, revision, and
  status rules

This keeps the sink path simple and makes replay/correction much safer.

If later we need read-optimized current-state projections, those can be derived
outside the hot write path.

For experiment mode, the same append-only rule should hold. The difference is
just the destination:

- production derived tables for continuous and official backfill runs
- experiment-scoped derived tables for one-off offline runs

## Late-Arriving Data Strategy

This is the most important design decision in the entire service.

The recommended model is explicitly two-tier.

### Tier 1: Bounded Hot Path

Within the configured allowed lateness window:

- process by event time
- use watermarks
- keep keyed state live
- checkpoint state to object storage
- emit provisional or final outputs according to program policy

This is where Arroyo gives us strong primitives.

### Tier 2: Slow Repair Path

After the allowed lateness window:

- do not keep reopening arbitrarily old state in the hot path
- record a repair manifest in object storage
- later run a slower repair flow
- recompute only the affected keys/time ranges
- append correction rows to the derived table

This makes late handling credible without forcing unbounded online state.

### Required Policy Fields Per Program

Every deployed program should declare:

- `event_time`
- `allowed_lateness`
- `correction_horizon`
- `state_ttl`
- `finalization_rule`
- `late_data_policy`

These are not optional tuning knobs. They are part of the product contract.

### Recommended Semantics

- Before watermark + allowed lateness:
  outputs may be `provisional`.
- After that threshold:
  outputs may become `final`.
- Events arriving after allowed lateness but before correction horizon:
  generate repair work.
- Events arriving after correction horizon:
  are either quarantined, ignored, or force an explicit replay path depending
  on policy.

That is the right bounded-correctness story.

## State And Checkpoint Strategy

### V1 State Model

- in-memory keyed operator state
- periodic checkpoints to object storage
- no Redis
- no always-on external state store

This keeps operations simple.

### Tradeoffs

The cost of this simplicity is:

- state size is bounded by memory more directly
- restore time depends on checkpoint size
- long retention windows are expensive

That is acceptable for v1 if we keep feature-program scope disciplined.

### When To Add A Heavier State Backend

Only add local-disk or embedded persistent state if:

- state size becomes too large for memory
- checkpoint restore time is too slow
- retention windows require too much resident state

That should be a later optimization, not the default architecture.

## Integration With Powdrr Metadata

This is the new core control-plane seam.

The canonical metadata proposal now lives in:

- [feast-metadata-service-extension-proposal.md](./feast-metadata-service-extension-proposal.md)
- [feature-metadata-control-plane-contract.md](./feature-metadata-control-plane-contract.md)

The sections below are the compute-specific summary of that contract.

### Metadata API Responsibilities

`Powdrr Metadata` should provide:

- program definitions
- immutable program revisions
- deployment specs
- experiment specs or experiment-run definitions
- program-to-source bindings
- raw-table contracts
- derived-table contracts
- lateness/correction policy
- desired activation state
- rollout intent

### Detailed Metadata Objects

The first version of `Powdrr Metadata` should define explicit objects with
stable IDs and immutable revisions where appropriate.

#### `SourceBinding`

Purpose:

- declare where a stream comes from
- define how compute should read it
- define the canonical raw landing target

Required fields:

- `source_binding_id`
- `source_type`: `kafka` or `kinesis`
- `connection_ref`
- `stream_name` or `topic_name`
- `source_revision`
- `raw_table_contract_id`
- `ordering_key`: partition or shard semantics
- `startup_mode`: latest, committed, or explicit offset/sequence

Recommended fields:

- authentication reference
- default parallelism
- schema registry or schema decoding config

#### `RawTableContract`

Purpose:

- declare the canonical raw Iceberg destination and schema contract

Required fields:

- `raw_table_contract_id`
- `catalog_namespace`
- `table_name`
- `schema_revision`
- `event_time_column`
- `id_column`
- `source_position_columns`
- `partition_spec`

Recommended fields:

- clustering hints
- retention policy
- compaction policy reference

#### `FeatureProgram`

Purpose:

- define the stable logical program identity independent of implementation
  revision

Required fields:

- `feature_program_id`
- `program_name`
- `owner`
- `source_binding_ids`
- `derived_table_contract_ids`
- `status`

#### `FeatureProgramRevision`

Purpose:

- define an immutable executable revision of a program

Required fields:

- `feature_program_revision_id`
- `feature_program_id`
- `revision_number` or content hash
- `program_spec`
- `input_schema_revisions`
- `output_schema_revision`
- `event_time_policy`
- `late_data_policy_id`
- `state_policy`

Recommended fields:

- compatibility notes
- migration hints
- rollout notes

#### `LateDataPolicy`

Purpose:

- define exactly how the program handles delayed data

Required fields:

- `late_data_policy_id`
- `allowed_lateness`
- `correction_horizon`
- `finalization_rule`
- `late_arrival_action`

`late_arrival_action` should be one of:

- `repair`
- `quarantine`
- `drop`
- `manual_replay_required`

#### `DerivedTableContract`

Purpose:

- define the canonical destination and semantics of derived outputs

Required fields:

- `derived_table_contract_id`
- `catalog_namespace`
- `table_name`
- `entity_key_columns`
- `feature_time_column`
- `output_schema_revision`
- `append_only = true`
- `partition_spec`
- `status_resolution_rule`

Recommended fields:

- clustering hints
- compaction policy reference
- serving-eligibility flag

#### `ComputeDeployment`

Purpose:

- tell compute to run a continuous production deployment

Required fields:

- `compute_deployment_id`
- `feature_program_revision_id`
- `source_binding_id`
- `derived_table_contract_id`
- `desired_state`: active, draining, or stopped
- `parallelism`
- `runtime_profile`

Recommended fields:

- placement hints
- rollout wave
- drain behavior

#### `ComputeBatchRun`

Purpose:

- request a bounded offline replay or backfill into a production target

Required fields:

- `compute_batch_run_id`
- `feature_program_revision_id`
- `source_binding_id`
- `source_range`
- `target_table_contract_id`
- `run_reason`
- `target_write_mode`

`target_write_mode` should start with:

- `append_production`
- `append_shadow`

#### `ExperimentRun`

Purpose:

- request an isolated one-off offline run for evaluation or training

Required fields:

- `experiment_run_id`
- `feature_program_revision_id`
- `source_binding_id`
- `source_range`
- `experiment_namespace`
- `experiment_table_name`
- `retention_policy`

Recommended fields:

- linked model or experiment ticket
- note or hypothesis
- cleanup deadline

### Compute Control Loop

`Powdrr Compute` should periodically reconcile:

- desired deployments from metadata
- local active deployments
- checkpointed deployment state

For each deployment, compute should:

1. fetch the current deployment spec
2. load or restore state
3. start or update the program
4. report health and progress
5. report checkpoint state and lag
6. report repair backlog if any

This is a much better fit than making compute read local YAML forever. YAML is
still useful for bootstrapping the service itself, but not as the long-term
home of feature-program definitions.

For offline jobs, metadata should also be able to express:

- a bounded source range
- the target output namespace or table
- the feature program revision to run
- whether the run is a production backfill or an isolated experiment
- any override output retention policy

### Metadata And Compute Execution Contract

The cleanest contract is:

- metadata owns desired work
- compute owns actual work
- compute reports progress and durable checkpoints back

The first version does not need a complicated scheduler protocol. It does need
clear object transitions and durable run IDs.

#### Required Metadata -> Compute Reads

- list active `ComputeDeployment`s
- get one `FeatureProgramRevision`
- get referenced `SourceBinding`
- get referenced `RawTableContract`
- get referenced `DerivedTableContract`
- list queued `ComputeBatchRun`s
- list queued `ExperimentRun`s
- get referenced `LateDataPolicy`

#### Required Compute -> Metadata Writes

- heartbeat and capacity
- deployment health
- deployment lag and watermark
- active checkpoint reference
- last committed raw-table source position
- last committed derived-table source position
- batch run status
- experiment run status
- repair backlog summary

#### Suggested API Shape

The exact transport can vary, but the objects should support operations like:

- `GET /v1/compute/deployments`
- `GET /v1/compute/deployments/{id}`
- `POST /v1/compute/deployments/{id}/status`
- `GET /v1/compute/batch-runs?status=queued`
- `POST /v1/compute/batch-runs/{id}/claim`
- `POST /v1/compute/batch-runs/{id}/status`
- `GET /v1/compute/experiment-runs?status=queued`
- `POST /v1/compute/experiment-runs/{id}/claim`
- `POST /v1/compute/experiment-runs/{id}/status`
- `POST /v1/compute/nodes/{node_id}/heartbeat`

The important thing is not the verb set. It is:

- deployments are long-lived
- batch runs are finite
- experiment runs are finite
- every execution has a durable ID and reported state

#### Execution Status Model

`ComputeDeployment` should report:

- `Pending`
- `Starting`
- `Restoring`
- `Running`
- `Draining`
- `Stopped`
- `Failed`
- `Degraded`

`ComputeBatchRun` and `ExperimentRun` should report:

- `Queued`
- `Claimed`
- `Planning`
- `Running`
- `Succeeded`
- `Failed`
- `Cancelled`

#### Checkpoint Contract

For every running deployment, metadata should be able to store:

- `checkpoint_ref`
- `source_position_ref`
- `raw_output_commit_ref`
- `derived_output_commit_ref`
- `watermark_at_checkpoint`
- `feature_program_revision_id`

That is the minimum needed to make restart, audit, and repair credible.

## Deployment Model

### Bootstrap Config

The compute service itself still needs a local bootstrap config for:

- node identity
- metadata service endpoint
- object-store credentials/config
- Iceberg REST catalog endpoint
- source credentials or auth references
- checkpoint root
- repair root
- worker capacity settings

Example shape:

```yaml
service:
  node_id: compute-a
  metadata_url: https://metadata.internal
  checkpoint_root: s3://warehouse/compute-checkpoints
  repair_root: s3://warehouse/compute-repairs
  experiment_root: s3://warehouse/compute-experiments

iceberg:
  rest_catalog_url: https://iceberg-catalog.internal
  warehouse: prod-warehouse

runtime:
  checkpoint_interval: 30s
  max_parallelism: 16
  worker_slots: 8
```

This config boots the service. It should not describe feature programs
themselves.

### Program Lifecycle

For each deployment:

- `Created`
- `Starting`
- `Restoring`
- `Running`
- `Draining`
- `Stopped`
- `Failed`
- `Repairing`

Metadata owns desired state. Compute owns actual execution state.

For batch and experiment runs, compute should also track:

- `Queued`
- `Planning`
- `Running`
- `Succeeded`
- `Failed`
- `Cancelled`

## Output Naming And Layout Conventions

The table naming rules should be boring and explicit.

### Raw Tables

Recommended namespace pattern:

- `raw.<source_binding_name>`

Examples:

- `raw.web_events`
- `raw.payments_cdc`
- `raw.mobile_sessions`

Reason:

- stable logical table names
- source identity stays obvious
- bulk replay and repair have one canonical raw source

### Production Derived Tables

Recommended namespace pattern:

- `features.<feature_program_name>`

Examples:

- `features.user_features`
- `features.account_risk`
- `features.session_scores`

Important rule:

- keep the production table name stable
- store `feature_program_revision_id` in rows
- do not encode every revision into the table name

That keeps the logical serving target stable while preserving lineage in data.

### Backfill Or Shadow Tables

Recommended namespace pattern:

- `features_shadow.<feature_program_name>__<batch_run_id>`

Use these when:

- validating a large replay before promotion
- comparing new output against production
- testing a major revision

### Experiment Tables

Recommended namespace pattern:

- `experiments.<experiment_run_id>.<feature_program_name>`

Examples:

- `experiments.exp_2026_05_23_ctr.user_features`
- `experiments.ablation_42.session_scores`

Important rules:

- experiments must never write directly into production tables by default
- experiment outputs must include the same provenance and revision columns
- experiment tables should carry an explicit retention or cleanup policy

### Object Store Layout

Recommended high-level shape:

```text
s3://warehouse/
  raw/<source_binding_name>/...
  features/<feature_program_name>/...
  features_shadow/<feature_program_name>/<batch_run_id>/...
  experiments/<experiment_run_id>/<feature_program_name>/...
  checkpoints/<compute_deployment_id>/...
  repairs/<derived_table>/<repair_id>/...
```

The exact path scheme can vary, but production, shadow, and experiment outputs
must be kept distinct.

## Source-Specific Notes

### Kafka

Kafka should be the first production source.

Why:

- simpler partition model
- consumer group coordination already exists
- easier replay story
- lower custom coordination burden

### Kinesis

Kinesis should be supported, but likely after Kafka-first hardening.

Why it is harder:

- shard lifecycle and reshard handling
- enhanced fan-out choices
- checkpoint and assignment behavior are less turnkey if we avoid KCL-style
  infrastructure assumptions

That does not mean "no Kinesis." It means Kafka is the better first source.

## Iceberg Write Strategy

### Raw Tables

Raw tables should:

- be append only
- partition by event date or hour
- preserve source position metadata
- write reasonably sized files

### Derived Tables

Derived tables should:

- be append only
- include revision/finality metadata
- partition by feature time
- optionally cluster by entity key where helpful

### Maintenance

The hot compute path should not try to be a complete Iceberg maintenance
system.

We still need a plan for:

- file sizing
- commit frequency
- snapshot cleanup
- compaction

Recommendation:

- keep hot-path writes simple
- add a separate maintenance mode or later maintenance service for compaction
  and cleanup

## Offline Bulk Generation

This is a first-class responsibility of `Powdrr Compute`, not an afterthought.

### Required Offline Workloads

- initial historical generation of a new derived table
- replay after a feature-program revision changes
- replay after a bug fix
- bounded backfill for a missing source interval
- one-off experiment runs for evaluation or training data

### Execution Principle

Offline generation should use:

- the same program revision model
- the same raw-event schema
- the same event-time semantics
- the same lateness/finality policy interpretation

as continuous execution.

The runtime implementation can differ internally for performance, but the
observable semantics should not.

### Output Strategy

Offline runs should write to Iceberg in one of two ways:

- production target tables for approved backfills
- isolated experiment tables or namespaces for one-off runs

That distinction should be explicit in metadata and in the output contract.

### Why This Matters

If we use one engine for streaming and another for offline experimentation, we
recreate the exact offline/online skew problem this system is supposed to
eliminate.

## Concrete Arroyo Change Plan

This is the key implementation section.

Arroyo already has useful runtime pieces, but its current product shape is not
the one we want. The changes below are what turn it into `Powdrr Compute`.

### 1. Split Runtime From Arroyo’s Product Control Plane

Current Arroyo shape:

- REST API exposes pipelines and jobs in
  `/private/tmp/arroyo-compute-plan/crates/arroyo-api/src/rest.rs`
- controller state is database-backed and pipeline/job oriented in
  `/private/tmp/arroyo-compute-plan/crates/arroyo-controller/src/lib.rs`

Needed change:

- extract a reusable execution core
- make the core runnable without Arroyo’s pipeline CRUD API
- replace pipeline/job identity with Powdrr deployment/run identity

Concrete work:

- remove the requirement that pipeline definitions originate in Arroyo API
- add a `Powdrr Metadata` client instead of pipeline CRUD endpoints
- treat REST/UI/database crates as optional or removable

### 2. Make Embedded Single-Service Execution The First-Class Mode

Arroyo already has an embedded/process scheduler path and local engine startup.
That should become the default for `Powdrr Compute`, not a side mode.

Concrete work:

- make single-process or tightly scoped multi-process execution the default
- remove assumptions that a separate controller cluster is always present
- keep Kubernetes-style orchestration as optional later work, not the core
  architecture

### 3. Replace Pipeline/Job Objects With Deployment And Run Objects

Current Arroyo concepts are pipeline/job centric.

Needed Powdrr concepts are:

- `ComputeDeployment` for continuous mode
- `ComputeBatchRun` for bulk replay
- `ExperimentRun` for offline one-offs

Concrete work:

- add new runtime object model
- propagate deployment/run IDs through logs, checkpoints, and sink metadata
- stop assuming every execution is just a pipeline job

### 4. Add A Metadata Reconcile Loop

Arroyo expects its own API/controller to create and manage jobs.
`Powdrr Compute` instead needs a reconcile loop against `Powdrr Metadata`.

Concrete work:

- add metadata polling or watch client
- claim queued batch and experiment runs
- reconcile desired deployments
- report status, lag, checkpoint refs, and failures back to metadata

### 5. Support Three First-Class Execution Modes Efficiently

This is the biggest product change.

#### Continuous Mode

Arroyo already aligns reasonably well here.

Needed work:

- map `ComputeDeployment` to long-lived streaming execution
- make raw landing and derived landing both standard sinks
- persist source progress and checkpoint refs under Powdrr deployment IDs

#### Bulk Replay Mode

This is not just "run the stream job again."

Needed work:

- support bounded input ranges
- support reading canonical raw Iceberg tables as replay input
- optimize for throughput rather than low-latency watermark churn
- support output to production or shadow targets

Efficiency requirement:

- bulk replay should avoid unnecessary per-record online coordination
- checkpointing cadence should be tuned separately from continuous mode

#### Experiment Mode

This is similar to bulk replay but with stronger isolation.

Needed work:

- support experiment-scoped output namespaces
- isolate outputs from production tables
- preserve provenance and revision metadata
- support automatic cleanup/retention

Efficiency requirement:

- experiments should reuse bulk replay machinery, not invent a third engine

### 6. Add Raw-Iceberg Replay As A First-Class Source

Today Arroyo starts from external connectors. `Powdrr Compute` also needs to
start from canonical raw Iceberg tables for backfills and experiments.

Concrete work:

- add a first-class bounded Iceberg source mode for replay
- preserve source-range and raw-row provenance
- allow replay by:
  - time range
  - partition range
  - selected key range where feasible

This is the key enabler for keeping one engine across online and offline modes.

### 7. Add Powdrr-Specific Sink Metadata

Current Iceberg sinking is not enough by itself.

We need every derived emission to carry:

- `feature_program_revision_id`
- deployment or run ID
- output status
- watermark or finalization context
- source-range or checkpoint provenance

Concrete work:

- extend sink row shaping
- extend commit metadata
- ensure raw and derived sinks can stamp Powdrr lineage columns efficiently

### 8. Add Late-Repair Manifest Emission And Repair Execution

Arroyo has watermarks and stateful processing, but `Powdrr Compute` needs a
more explicit split between hot-path lateness and slow repair.

Concrete work:

- detect events beyond allowed lateness
- emit repair manifests to object storage
- add repair-run planning and execution mode
- append correction rows instead of mutating prior outputs

This is one of the main Powdrr-specific semantics that must be added.

### 9. Decouple Checkpoint Identity From Controller Database Semantics

Arroyo’s worker code still has explicit controller-vs-leader checkpoint modes in
`/private/tmp/arroyo-compute-plan/crates/arroyo-worker/src/job_controller/committing_state.rs`.

Needed change:

- checkpoint identity must key off Powdrr deployment or run objects
- checkpoint metadata must be consumable without Arroyo’s controller database

Concrete work:

- define Powdrr checkpoint manifests
- store checkpoint refs under object storage plus metadata-service status
- make restore logic independent of Arroyo pipeline/job DB rows

### 10. Separate Runtime Tuning By Mode

Efficiency depends on not treating all modes the same.

Needed tuning surface:

- continuous mode:
  - shorter checkpoints
  - lower latency
  - steady watermark progression
- bulk replay mode:
  - larger batches
  - fewer checkpoints
  - higher throughput
- experiment mode:
  - isolated outputs
  - bounded resource caps
  - optional lower scheduling priority

Concrete work:

- mode-aware runtime profiles
- mode-aware checkpoint intervals
- mode-aware sink commit cadence

### 11. Remove Or Defer Unneeded Arroyo Product Surface

Not everything should come forward into `Powdrr Compute`.

Good candidates to defer or drop:

- generic pipeline CRUD UI
- generic SQL validation endpoints
- UDF management APIs as a user-facing product
- broad connection-table abstractions that are not needed immediately

Keep the first repository as narrow as possible.

### 12. Suggested Arroyo Refactor Order

1. Prove embedded execution with raw Iceberg landing.
2. Remove dependence on Arroyo pipeline CRUD for starting a job.
3. Add Powdrr deployment/run identity through runtime and checkpoints.
4. Add metadata reconcile loop.
5. Add raw-Iceberg replay source.
6. Add production batch-run mode.
7. Add experiment-run mode.
8. Add late-repair manifest flow.
9. Add mode-specific runtime tuning.
10. Trim remaining unneeded Arroyo API/controller surface.

## Concrete Arroyo Fork Bootstrap Plan

This is the recommended repository bootstrap plan if we start from the Arroyo
codebase today.

### Crates To Carry Forward First

These are the most directly useful runtime pieces:

- `arroyo-worker`
  - core execution runtime
  - operator scheduling
  - checkpoint and recovery machinery
- `arroyo-operator`
  - operator abstractions and execution helpers
- `arroyo-connectors`
  - Kafka and Kinesis source connectors
  - Iceberg sink patterns
- `arroyo-state`
  - runtime state handling
- `arroyo-state-protocol`
  - checkpoint and state protocol objects
- `arroyo-storage`
  - object-store access helpers
- `arroyo-types`
  - shared runtime ids and utility types
- `arroyo-rpc`
  - only the minimal shared wire and config types still needed by the runtime

These are the core of a viable `Powdrr Compute` runtime.

### Crates To Carry Forward Early If We Keep SQL Authoring In-Repo

- `arroyo-planner`
  - needed if `Powdrr Metadata` hands compute SQL or a planner-facing IR
- `arroyo-formats`
  - useful if we need its format handling directly

Important nuance:

`arroyo-worker` currently depends on `arroyo-planner`, so the first fork may
need to carry `arroyo-planner` even if the long-term goal is to narrow that
dependency.

### Crates To Mine Selectively, Not Carry Whole

- `arroyo-controller`
  - embedded/process scheduler ideas
  - checkpoint orchestration ideas
  - but not the full DB/job-controller product shape
- `arroyo`
  - top-level app wiring ideas
  - but not the full product binary shape
- `arroyo-server-common`
  - only utility pieces that are still genuinely useful

### Crates To Defer Or Exclude At First

- `arroyo-api`
  - heavy pipeline CRUD and SQL product surface
  - brings SQLite-oriented API baggage
- `arroyo-openapi`
  - not needed for initial runtime fork
- `arroyo-compiler-service`
  - not needed until we decide if compilation runs inside metadata or compute
- `arroyo-node`
  - defer unless a separate node service becomes necessary
- `arroyo-sql-testing`
  - useful reference, not first-class runtime dependency
- `arroyo-udf-python`
  - defer unless Python UDFs are explicitly in v1 scope
- broad `arroyo-udf` surface
  - defer unless user-defined code is actually required in v1

### Why This Split

The runtime-heavy crates are where the useful streaming engine pieces live.
The API/controller crates are where the generic product surface, job CRUD, and
SQLite or broader control-plane baggage live.

That matches the product goal:

- keep the execution engine
- drop the generic Arroyo control-plane product

### Recommended New Repo Shape

A good first target is:

```text
powdrr-compute/
  crates/
    powdrr-compute-app/
    powdrr-compute-runtime/
    powdrr-compute-connectors/
    powdrr-compute-state/
    powdrr-compute-storage/
    powdrr-compute-types/
    powdrr-compute-rpc/
    powdrr-compute-planner/        # if needed initially
  docs/
  deploy/
```

Recommended mapping:

- `powdrr-compute-runtime`
  - derived mainly from `arroyo-worker` + selected `arroyo-operator`
- `powdrr-compute-connectors`
  - derived from `arroyo-connectors`
- `powdrr-compute-state`
  - derived from `arroyo-state` + `arroyo-state-protocol`
- `powdrr-compute-storage`
  - derived from `arroyo-storage`
- `powdrr-compute-types`
  - derived from `arroyo-types`
- `powdrr-compute-rpc`
  - derived from the minimal needed subset of `arroyo-rpc`
- `powdrr-compute-planner`
  - derived from `arroyo-planner` if compute still needs to own planning
- `powdrr-compute-app`
  - new thin binary crate replacing the generic `arroyo` app crate

### Bootstrap Sequence

#### Step 1: Create The New Workspace

- create the new repo
- copy the minimal workspace structure
- bring over the runtime-heavy crates only
- make the app crate start one embedded runtime process

Goal:

- compile a single-binary `Powdrr Compute` skeleton

#### Step 2: Prove Raw Landing

- wire Kafka source
- wire Iceberg REST catalog sink
- land canonical raw rows
- checkpoint source progress and runtime state

Goal:

- one process can ingest and recover

#### Step 3: Add Metadata Client

- remove startup dependence on Arroyo pipeline CRUD
- add `Powdrr Metadata` client
- resolve `ComputeDeployment`s and run them

Goal:

- runtime is metadata-driven, not Arroyo-API-driven

#### Step 4: Add Batch And Experiment Modes

- add raw-Iceberg replay source
- add `ComputeBatchRun`
- add `ExperimentRun`
- add experiment output namespaces

Goal:

- the same engine supports all three modes

#### Step 5: Strip Residual Product Surface

- remove unused API/controller code
- narrow planner/rpc dependencies where possible
- reduce runtime config to Powdrr-specific bootstrap config

Goal:

- a focused compute service, not a renamed Arroyo distro

### Concrete Efficiency Guidance By Mode

The runtime should tune differently by mode from the start.

#### Continuous Mode

- shorter checkpoint intervals
- lower sink latency
- stable watermark progression
- long-lived deployment identity

#### Batch Replay Mode

- larger batch sizes
- fewer checkpoints
- more aggressive throughput settings
- optional shadow-output mode

#### Experiment Mode

- reuse batch replay engine
- isolated table namespaces
- explicit retention and cleanup
- optional lower scheduling priority

### First Crates To Fork In Practice

If we want the smallest realistic first fork set, it is:

1. `arroyo-worker`
2. `arroyo-operator`
3. `arroyo-connectors`
4. `arroyo-state`
5. `arroyo-state-protocol`
6. `arroyo-storage`
7. `arroyo-types`
8. `arroyo-rpc`
9. `arroyo-planner`

The main reason `arroyo-planner` is included in that first set is practical:
the current worker crate already depends on it. We can shrink that later, but
trying to cut it out before the runtime boots is likely wasted effort.

## Program Model

The repository should not start as "generic user SQL over streams."

It should start as:

- versioned Powdrr feature programs
- compiled to a runtime plan
- executed by `Powdrr Compute`

Possible implementation shapes:

- constrained SQL/IR
- Rust DSL
- compiled Wasm/plugin operators

The most important thing is not syntax. It is that every program has:

- explicit inputs
- explicit schema
- explicit keying
- explicit event-time semantics
- explicit state and lateness policy

## Observability And Operations

The service needs first-class visibility into:

- source lag
- watermark progress
- checkpoint duration and size
- restore duration
- per-program error rate
- repair backlog
- derived-table commit latency
- late-drop or late-quarantine counts

Without that, the late-data story is not credible in production.

## Failure And Recovery Model

The initial correctness goal should be:

- deterministic replay
- recoverable processing
- idempotent raw landing
- idempotent derived emissions

That is more important than making broad exactly-once claims in v1.

The service should recover by:

- restoring from the latest checkpoint
- resuming from checkpointed source positions
- safely re-emitting rows when needed via stable emission identity

## What Else We Need To Think About

### 1. Schema Evolution

- raw event schema changes
- feature-program input/output revisioning
- backward compatibility during replay

### 2. Replay And Backfill

- full replay from raw tables
- targeted replay for key/time ranges
- isolating replay outputs before cutover
- experiment-table lifecycle and cleanup

### 3. Program Rollouts

- can old and new revisions overlap?
- how do we cut traffic or publication over?
- how do we roll back?

### 4. Output Identity

- what uniquely identifies a derived emission?
- how do we dedupe after retries or replay?

### 5. Resource Isolation

- multiple programs per compute node
- CPU and memory control
- noisy-neighbor effects

### 6. Repair Governance

- when do repairs run?
- who approves large repairs?
- how do we expose repair backlog and impact?

## Recommended Phased Plan

### Phase 0: Runtime Feasibility

- clone Arroyo
- identify reusable crates
- prove one process can consume Kafka and land raw Iceberg
- prove object-store checkpoints and recovery

Exit:

- raw landing works credibly with checkpoint restore

### Phase 1: Metadata Integration

- define `Powdrr Metadata` objects for feature programs and deployments
- define metadata objects for batch runs and experiment runs
- make compute poll or watch metadata
- load one deployed program from metadata
- run one derived-table pipeline

Exit:

- compute is driven by metadata, not hard-coded local programs

### Phase 2: Offline Bulk And Experiment Runs

- add bounded raw-table replay mode
- add production backfill runs
- add experiment-scoped offline runs
- write results to Iceberg with revision provenance

Exit:

- one compute system handles streaming, bulk generation, and experiments

### Phase 3: Late Data Contract

- implement allowed lateness and finalization rules
- emit repair manifests
- add repair mode that appends correction rows

Exit:

- late-data handling is explicit and testable

### Phase 4: Multi-Program Runtime

- run multiple deployments
- improve scheduling and isolation
- add richer health reporting back to metadata

Exit:

- one compute service can host a useful production slice

### Phase 5: Hardening

- compaction/cleanup plan
- replay tooling
- Kinesis hardening
- larger-state handling

Exit:

- the system is operationally boring enough to trust

## Recommendation

The right concrete plan is:

- build a new `Powdrr Compute` repository
- start from Arroyo runtime pieces
- keep `Powdrr Metadata` as the control-plane authority
- depend on Kafka or Kinesis, object storage, and an Iceberg REST catalog
- land both raw and derived data into Iceberg
- make `Powdrr Compute` the shared engine for streaming, bulk generation, and
  offline experimentation
- make late data a bounded hot-path concern plus a slower repair path
- keep outputs append only and revisioned

That gives Powdrr a credible compute service instead of a generic streaming
platform glued awkwardly onto the side.

## External References

- Existing Powdrr feature-compute direction:
  [guaranteed-feature-computation-platform.md](./guaranteed-feature-computation-platform.md)
- Arroyo architecture:
  <https://doc.arroyo.dev/architecture/>
- Arroyo getting started:
  <https://doc.arroyo.dev/getting-started/>
- Arroyo deployment on VMs:
  <https://doc.arroyo.dev/deployment/vm/>
- Arroyo concepts:
  <https://doc.arroyo.dev/concepts/>
- Arroyo Iceberg sink:
  <https://doc.arroyo.dev/connectors/iceberg/>
- Arroyo Kafka connector:
  <https://doc.arroyo.dev/connectors/kafka/>
- Arroyo Kinesis connector:
  <https://doc.arroyo.dev/connectors/kinesis/>
