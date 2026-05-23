# Feast Metadata Service Extension Proposal

This document supersedes the narrower Feast metadata proposal by folding in
the metadata needed for `Powdrr Compute`, `Powdrr Engine`, and offline
training or experimentation.

It is the product-level proposal for how Powdrr should bridge from Feast's
registry model to the full metadata needed for:

- raw source contracts
- guaranteed feature definitions
- compiled plan revisions
- continuous compute deployments
- offline batch and experiment runs
- publication and serving frontiers
- revision-pinned training datasets
- validation evidence

For the compute-side execution plan, see
[powdrr-compute-service-plan.md](./powdrr-compute-service-plan.md).

For the implementation-facing service and DTO contract, see
[feature-metadata-control-plane-contract.md](./feature-metadata-control-plane-contract.md).

## Summary

The right shape is still not:

- "replace Feast"
- "stuff Powdrr runtime state into Feast tags"
- "let compute invent a second metadata plane on the side"

The right shape is:

- keep Feast for registry, discovery, and governance
- add a Powdrr metadata extension layer for runtime semantics
- expose both through one logical `Powdrr Metadata` service

That service becomes the control plane for:

- feature authoring
- compilation
- continuous compute deployments
- offline backfills
- one-off experiments
- publication and rollout
- model and dataset provenance
- online/offline validation evidence

## Design Goals

- keep one logical metadata plane for operators
- keep Feast-native authoring and discovery where Feast is strong
- make Powdrr-specific runtime semantics first-class instead of implicit
- let `Powdrr Compute` use the same metadata for continuous, bulk, and
  experiment modes
- let `Powdrr Engine` serve from explicit published revisions
- make training and experimentation revision-aware

## Non-Goals

Not in the first version:

- a deep Feast fork
- arbitrary user code in metadata
- generic DAG authoring for the guaranteed class
- forcing Feast's standard online store to become the canonical serving path

## Service Shape

`Powdrr Metadata` should expose two logical layers through one service.

### 1. Feast Registry Layer

This layer owns:

- `Entity`
- `FeatureView`
- `FeatureService`
- `DataSource`
- tags
- permissions
- discovery and catalog search

This remains the user-facing registry and governance surface.

### 2. Powdrr Extension Layer

This layer owns:

- raw source and table contracts
- guaranteed feature specs
- plan revisions
- compute deployments
- batch runs
- experiment runs
- late-data policy
- publications
- serving frontiers
- training dataset records
- model bindings
- validation runs

This is the runtime-facing control plane.

## Ownership Boundary

### Feast Owns

- object catalog and discovery
- feature-service grouping
- governance and permissions
- base object search and browse workflows
- basic lineage of registry objects

### Powdrr Extension Owns

- canonical SQL or IR feature definitions
- replayable source contracts
- event-time, lateness, correction, and finalization semantics
- compiled plan revisions
- compute deployment state
- batch and experiment run state
- publication and rollback state
- serving frontier state
- model-to-revision bindings
- training and experiment provenance
- validation evidence

## Core Metadata Objects

The cleanest model is to group the Powdrr extension objects into four families.

### A. Catalog Bridge Objects

These bridge Feast objects into runtime semantics.

#### `PowdrrFeatureSpec`

One Powdrr-managed companion spec for one Feast `FeatureView` that belongs to
the guaranteed class.

Required concepts:

- Feast `project`
- Feast `feature_view_name`
- canonical SQL or program spec
- entity keys
- event-time column
- source contracts
- output schema
- supported compute modes
- guaranteed feature class
- owner

Policy fields:

- watermark policy
- allowed lateness
- correction policy
- correction horizon
- retention horizon
- finalization rule

Serving fields:

- online enabled
- whether provisional results may be served
- output request and response schema
- serving key columns

#### `PowdrrSourceContract`

Describes the canonical replayable source behind a guaranteed feature spec.

Required concepts:

- project
- source name
- raw table reference
- source type
- event id column
- entity key columns
- event-time column
- ingest-time column
- payload columns
- offset or sequence columns
- dedupe columns where relevant

This is what both continuous compute and offline replay rely on.

#### `RawTableContract`

Describes the canonical raw Iceberg destination.

Required concepts:

- namespace
- table name
- schema revision
- event-time column
- source position columns
- partition spec

`PowdrrSourceContract` should point at a `RawTableContract`, not just a loose
table name string.

#### `DerivedTableContract`

Describes the canonical destination and semantics of derived outputs.

Required concepts:

- namespace
- table name
- entity key columns
- feature-time column
- output schema revision
- append-only requirement
- partition spec
- status resolution rule

### B. Revision And Compute Objects

These describe what should run and what did run.

#### `FeatureDefinitionRevision`

Immutable version of the user or author intent for a guaranteed feature spec.

Required concepts:

- feature revision id
- feature spec hash
- Feast registry version
- created at/by
- status

#### `FeaturePlanRevision`

Immutable version of the compiled runtime plan.

Required concepts:

- plan revision id
- feature revision id
- compiler version
- runtime compatibility
- IR artifact reference
- compile diagnostics
- support for bounded vs live execution

#### `LateDataPolicy`

Canonical policy object for late-data handling.

Required concepts:

- allowed lateness
- correction horizon
- finalization rule
- late-arrival action

Recommended `late_arrival_action` values:

- repair
- quarantine
- drop
- manual replay required

#### `ComputeDeployment`

Represents a long-lived continuous production deployment.

Required concepts:

- deployment id
- feature program revision
- source binding
- target derived table contract
- desired state
- parallelism
- runtime profile

#### `ComputeBatchRun`

Represents a bounded backfill or replay into a production or shadow target.

Required concepts:

- batch run id
- feature program revision
- source range
- target table contract
- run reason
- target write mode

Recommended `target_write_mode` values:

- append production
- append shadow

#### `ExperimentRun`

Represents an isolated one-off offline run.

Required concepts:

- experiment run id
- feature program revision
- source range
- experiment namespace
- experiment table name
- retention policy

### C. Publication And Serving Objects

These connect compute outputs to serving.

#### `FeaturePublication`

One published output of a plan revision.

Required concepts:

- publication id
- feature view reference
- feature revision id
- plan revision id
- output table reference
- source coverage
- compute checkpoint
- Powdrr checkpoint
- Iceberg snapshot id
- finality
- publication status

#### `ServingFrontier`

Tracks active vs target serving state.

Required concepts:

- frontier id
- scope type: feature view or feature service
- scope name
- target publication id
- active publication id
- active finality
- activation policy

### D. Training, Experiment, And Validation Objects

These make offline correctness auditable.

#### `TrainingDatasetRecord`

Describes a reproducible dataset used for training or evaluation.

Required concepts:

- training dataset id
- feature service reference
- entity source reference
- label source reference where applicable
- revision set
- finality policy
- dataset URI

#### `ModelBinding`

Binds a trained or deployed model version to exact feature revisions.

Required concepts:

- model binding id
- model name
- model version
- feature service
- training dataset id
- revision refs
- default frontier
- status

#### `ValidationRun`

Records evidence that the guarantee still holds.

Recommended validation types:

- compile
- golden replay
- stream/batch diff
- online shadow
- backfill reconciliation

## Compute And Metadata Relationship

This proposal extends the earlier Feast/Powdrr split by making `Powdrr
Compute` a first-class metadata client.

### Metadata Tells Compute

- what feature program revision should run
- which source binding to consume or replay
- which raw and derived table contracts apply
- which late-data policy to enforce
- whether the requested work is:
  - continuous deployment
  - bounded backfill
  - isolated experiment

### Compute Tells Metadata

- deployment health
- watermark and lag
- active checkpoint references
- output commit references
- batch and experiment run status
- repair backlog
- failure details

That is the bridge from Feast's registry world into a full Powdrr execution
system.

## Read And Write Flows

### Authoring Flow

1. User defines Feast objects:
   - entities
   - feature views
   - feature services
2. User defines Powdrr companion specs:
   - `PowdrrFeatureSpec`
   - `PowdrrSourceContract`
   - table contracts
   - late-data policy
3. Powdrr compiles the spec into:
   - `FeatureDefinitionRevision`
   - `FeaturePlanRevision`

### Continuous Compute Flow

1. `Powdrr Compute` resolves active `ComputeDeployment`s.
2. It loads the referenced plan revision and source contract.
3. It tails the source and writes:
   - canonical raw rows
   - derived append-only outputs
4. It records `FeaturePublication`s.
5. `Powdrr Engine` serves through `ServingFrontier`.

### Batch Backfill Flow

1. Metadata records a `ComputeBatchRun`.
2. Compute claims the run.
3. Compute replays bounded raw source coverage.
4. Compute writes to production or shadow targets.
5. Compute records publications and completion state.

### Experiment Flow

1. Metadata records an `ExperimentRun`.
2. Compute claims it.
3. Compute replays the requested source range.
4. Compute writes to experiment-scoped tables.
5. Metadata records resulting dataset or artifact references.

### Training And Validation Flow

1. User requests a training dataset.
2. Metadata resolves the revision set and finality policy.
3. Offline retrieval pins to those revisions.
4. Metadata records a `TrainingDatasetRecord`.
5. Model training produces a `ModelBinding`.
6. Validation evidence is captured in `ValidationRun`.

## Feast Integration Points

The extension should integrate with Feast in three places.

### 1. Registry Server

Use Feast for:

- catalog browsing
- feature-service grouping
- governance
- permissions

### 2. Powdrr Metadata Extension APIs

Use Powdrr-owned APIs for:

- runtime semantics
- revisions
- compute deployments and runs
- publications
- frontiers
- model and validation metadata

### 3. Feast-Compatible Serving And Retrieval Adapters

After the extension model is stable, Powdrr can provide:

- a Feast-compatible online adapter backed by Powdrr Engine
- a Feast-compatible offline adapter backed by Powdrr retrieval

These are compatibility surfaces, not the canonical runtime state.

## Where Feast Must Not Stay In Charge

Feast should not be the system of record for:

- publication cutover
- provisional vs final state
- continuous deployment state
- batch and experiment runs
- checkpoint references
- repair backlog
- online/offline equivalence evidence

Those are Powdrr runtime contracts.

## Storage Proposal

The first version should still look like one metadata service.

Recommended shape:

- one `Powdrr Metadata` service
- one logical metadata database
- Feast registry tables for Feast-native objects
- Powdrr extension tables for runtime metadata

That gives:

- one auth boundary
- one backup story
- one control-plane service

without forcing a deep Feast fork.

## Recommendation

Build `Powdrr Metadata` as a unified metadata service that:

- fronts or embeds Feast for registry objects
- owns Powdrr extension metadata for runtime semantics
- adds first-class compute deployment, batch run, and experiment objects
- is the single bridge from Feast catalog concepts to the full needs of
  Powdrr Compute and Powdrr Engine

That is the coherent metadata proposal needed to move from Feast-compatible
feature authoring to a full Powdrr feature platform.
