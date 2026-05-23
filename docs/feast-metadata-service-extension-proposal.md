# Feast Metadata Service Extension Proposal

This document turns the high-level feature-service plan in
[`guaranteed-feature-computation-platform.md`](./guaranteed-feature-computation-platform.md)
into a concrete metadata-service proposal.

For the implementation-facing Rust DTO and service contract that follows from
this proposal, see
[`feature-metadata-control-plane-contract.md`](./feature-metadata-control-plane-contract.md).

The assumption here is:

- Feast is used as a service for feature registry, discovery, and governance
- Powdrr Engine serves canonical Iceberg feature tables at low latency
- Powdrr Compute executes SQL feature specifications in bounded replay and
  live incremental modes, writing canonical outputs to Iceberg

The goal is to define what metadata must stay in Feast, what metadata must be
added by Powdrr, and how the two surfaces fit into one control plane.

## Summary

The right shape is not "replace Feast" and not "stuff every Powdrr concern
into Feast tags."

The right shape is:

- keep Feast for base registry concepts:
  - entities
  - feature views
  - feature services
  - tags
  - permissions
  - basic lineage and discovery
- add a Powdrr metadata extension layer for:
  - SQL feature definitions
  - compilation and guaranteed-subset validation
  - time, lateness, correction, and finalization policy
  - revision lineage
  - publication, serveability, and rollback
  - model bindings
  - experiment and training dataset provenance
  - online/offline validation runs

Operationally, this should still look like one metadata service.
The cleanest path is one Powdrr control-plane service that:

- embeds or fronts Feast registry APIs for base objects
- owns Powdrr extension APIs for runtime metadata
- stores both in one logical metadata plane

## Design Goals

- keep one feature definition workflow for users
- keep one logical metadata plane for operators
- avoid a hard Feast fork in the first version
- make revision-aware serving a first-class contract
- make skew validation a first-class product surface
- keep online and offline retrieval pinned to the same published revisions

## Non-Goals

Not in the first version:

- arbitrary user code execution inside Feast metadata objects
- broad support for non-deterministic UDFs
- arbitrary multi-stage DAG authoring in the guaranteed class
- forcing Feast's standard online serving path to become the product serving
  path

## Service Shape

The metadata plane should have two logical layers exposed by one service.

### 1. Feast Registry Layer

This layer owns user-facing registry concepts:

- `Entity`
- `FeatureView`
- `FeatureService`
- `DataSource`
- tags
- permissions
- saved dataset metadata where useful

This layer is what users browse, search, and reason about when managing
feature definitions at a catalog level.

### 2. Powdrr Extension Layer

This layer owns runtime-facing metadata:

- SQL feature specs
- source contracts
- compile outputs
- feature revisions
- publication records
- serving frontiers
- model bindings
- training and experiment metadata
- validation runs

This layer is what compute, publication, serving, and audit flows use to
enforce the feature guarantee.

## Ownership Boundary

### Feast Owns

- object catalog and discovery
- grouping features into feature services
- tags and ownership metadata
- permission policies
- basic registry APIs and UI/search surfaces
- basic lineage of registry objects

### Powdrr Extension Owns

- the canonical SQL definition for guaranteed features
- compilation into canonical feature IR
- event-time semantics
- correction and finalization semantics
- revision ids and lineage
- publication state
- active and target serving frontiers
- rollback state
- online/offline equivalence evidence
- model-to-revision bindings

## Authoring Model

Users should keep using Feast-native objects for the catalog layer, but every
guaranteed feature view needs a Powdrr companion spec.

### Feast Objects

Use Feast definitions for:

- entities
- feature views
- feature services
- governance metadata

### Powdrr Companion Objects

Add Powdrr-managed companion specs keyed to Feast objects.

The first required companion object is `PowdrrFeatureSpec`.

One `PowdrrFeatureSpec` should correspond to one Feast `FeatureView` that is
part of the guaranteed class.

## Core Extension Objects

### `PowdrrFeatureSpec`

Purpose:
The canonical SQL and semantic contract for a guaranteed feature view.

Required fields:

- `project`
- `feature_view_name`
- `sql_text`
- `sql_dialect`
- `entity_keys`
- `event_time_column`
- `source_contracts`
- `output_schema`
- `compute_modes`
  - `bounded`
  - `live`
- `guaranteed_class`
- `owner`

Semantic policy fields:

- `watermark_policy`
- `allowed_lateness`
- `correction_policy`
- `correction_horizon`
- `retention_horizon`
- `finalization_rule`

Serving policy fields:

- `online_enabled`
- `serve_provisional`
- `default_finality`
- `serving_key_columns`
- `request_schema`
- `response_schema`

Validation policy fields:

- `golden_test_suite`
- `equivalence_policy`
- `shadow_validation_policy`

Notes:

- this object is Powdrr-owned, not a Feast tag blob
- the schema must validate against the referenced Feast `FeatureView`
- only `PowdrrFeatureSpec` objects in the guaranteed class are compiled into
  revision-aware serving contracts

### `PowdrrSourceContract`

Purpose:
Describe the canonical replayable source behind a guaranteed feature spec.

Required fields:

- `project`
- `source_name`
- `raw_table_ref`
- `source_type`
  - `append_log`
  - `cdc_log`
- `event_id_column`
- `entity_key_columns`
- `event_time_column`
- `ingest_time_column`
- `payload_columns`

Ordering and replay fields:

- `offset_columns`
- `dedupe_key_columns`
- `correction_event_type_column`
- `schema_revision_column`
- `watermark_origin`

Notes:

- this is the contract Powdrr Compute replays in bounded and live modes
- Feast data sources alone are not enough because they do not encode the
  replay and correction guarantees needed here

### `FeatureDefinitionRevision`

Purpose:
Version the author intent for one guaranteed feature spec.

Required fields:

- `feature_revision_id`
- `project`
- `feature_view_name`
- `feature_spec_hash`
- `feast_registry_version`
- `created_at`
- `created_by`
- `status`
  - `draft`
  - `compiled`
  - `rejected`
  - `retired`

Notes:

- this revision changes when the logical feature definition changes
- it should not change for a pure runtime publication event

### `FeaturePlanRevision`

Purpose:
Version the compiled Powdrr execution plan.

Required fields:

- `plan_revision_id`
- `feature_revision_id`
- `compiler_version`
- `engine_compatibility`
- `ir_uri` or inline IR payload
- `compile_diagnostics`
- `supports_bounded`
- `supports_live`

Notes:

- one feature definition revision may have multiple plan revisions during
  compiler evolution, but only one should be active for new publication

### `FeaturePublication`

Purpose:
Record one published output of a plan revision.

Required fields:

- `publication_id`
- `project`
- `feature_view_name`
- `feature_revision_id`
- `plan_revision_id`
- `output_table_ref`
- `source_coverage`
- `compute_checkpoint`
- `powdrr_checkpoint`
- `iceberg_snapshot_id`
- `published_at`
- `published_by`
- `finality`
  - `provisional`
  - `final`
- `status`
  - `pending`
  - `serveable`
  - `active`
  - `superseded`
  - `rolled_back`

Recommended fields:

- `row_count`
- `data_interval_start`
- `data_interval_end`
- `artifact_uris`
- `validation_summary`

Notes:

- this is the core bridge between Powdrr Compute output and Powdrr Engine
  serving state
- online serving and offline retrieval both resolve through these records

### `ServingFrontier`

Purpose:
Track the published serving state for a feature view or feature service.

Required fields:

- `frontier_id`
- `project`
- `scope_type`
  - `feature_view`
  - `feature_service`
- `scope_name`
- `target_publication_id`
- `active_publication_id`
- `active_finality`
- `activation_policy`
- `updated_at`

Notes:

- this maps directly to the committed / target / active frontier concepts
  already used by Powdrr
- target and active state must be explicit, not inferred from latest write

### `ModelBinding`

Purpose:
Bind a deployed or trained model version to an exact feature contract.

Required fields:

- `model_binding_id`
- `project`
- `model_name`
- `model_version`
- `feature_service_name`
- `training_dataset_id`
- `training_revision_set`
- `default_online_frontier_id`
- `created_at`
- `status`
  - `candidate`
  - `active`
  - `retired`

Notes:

- Feast feature services are a useful grouping layer
- they are not sufficient by themselves to prove what revision a model trained
  or served against

### `TrainingDatasetRecord`

Purpose:
Describe a reproducible dataset used for training, evaluation, or experiments.

Required fields:

- `training_dataset_id`
- `project`
- `feature_service_name`
- `entity_source_ref`
- `label_source_ref`
- `revision_set`
- `finality_policy`
- `dataset_uri`
- `created_at`
- `created_by`

Recommended fields:

- `point_in_time_query`
- `entity_snapshot_ref`
- `row_count`
- `validation_run_ids`

### `ValidationRun`

Purpose:
Record evidence that the guarantee still holds.

Required fields:

- `validation_run_id`
- `project`
- `validation_type`
  - `compile`
  - `golden_replay`
  - `stream_batch_diff`
  - `online_shadow`
  - `backfill_reconciliation`
- `scope_name`
- `scope_type`
- `revision_refs`
- `started_at`
- `finished_at`
- `status`
  - `passed`
  - `failed`
  - `warning`
- `metrics`
- `artifact_uri`

## Metadata Keys and Identity

Every Powdrr extension object should be keyed by:

- `project`
- Feast object name
- immutable revision id where relevant

Avoid using mutable names alone as serving identifiers.

The minimum stable ids are:

- `feature_revision_id`
- `plan_revision_id`
- `publication_id`
- `frontier_id`
- `training_dataset_id`
- `validation_run_id`

## API Proposal

Expose one control-plane service with two API families.

### 1. Registry APIs

Use Feast-compatible registry APIs for:

- list and get entities
- list and get feature views
- list and get feature services
- tags and permissions
- registry search

### 2. Powdrr Extension APIs

Add Powdrr APIs for runtime metadata.

#### Spec and Compile APIs

- `PUT /v1/powdrr/projects/{project}/feature-specs/{feature_view}`
- `GET /v1/powdrr/projects/{project}/feature-specs/{feature_view}`
- `POST /v1/powdrr/projects/{project}/feature-specs/{feature_view}:compile`
- `GET /v1/powdrr/projects/{project}/feature-revisions/{feature_revision_id}`
- `GET /v1/powdrr/projects/{project}/plan-revisions/{plan_revision_id}`

#### Source and Ingest APIs

- `PUT /v1/powdrr/projects/{project}/source-contracts/{source_name}`
- `GET /v1/powdrr/projects/{project}/source-contracts/{source_name}`
- `POST /v1/powdrr/projects/{project}/ingest-contracts:validate`

#### Publication and Frontier APIs

- `POST /v1/powdrr/projects/{project}/publications`
- `GET /v1/powdrr/projects/{project}/publications/{publication_id}`
- `POST /v1/powdrr/projects/{project}/frontiers/{scope_type}/{scope_name}:promote`
- `POST /v1/powdrr/projects/{project}/frontiers/{scope_type}/{scope_name}:rollback`
- `GET /v1/powdrr/projects/{project}/frontiers/{scope_type}/{scope_name}`

#### Offline Retrieval and Dataset APIs

- `POST /v1/powdrr/projects/{project}/training-datasets`
- `GET /v1/powdrr/projects/{project}/training-datasets/{training_dataset_id}`
- `POST /v1/powdrr/projects/{project}/feature-services/{name}:resolve-revisions`

#### Model Binding APIs

- `POST /v1/powdrr/projects/{project}/model-bindings`
- `GET /v1/powdrr/projects/{project}/model-bindings/{model_name}/{model_version}`
- `POST /v1/powdrr/projects/{project}/model-bindings/{model_name}/{model_version}:activate`

#### Validation APIs

- `POST /v1/powdrr/projects/{project}/validation-runs`
- `GET /v1/powdrr/projects/{project}/validation-runs/{validation_run_id}`
- `GET /v1/powdrr/projects/{project}/feature-services/{name}/validation-summary`

## Read and Write Flows

### Authoring Flow

1. User defines Feast objects:
   - entities
   - feature views
   - feature services
2. User defines `PowdrrFeatureSpec` and `PowdrrSourceContract`.
3. User applies the Feast objects to the registry.
4. User applies the Powdrr companion specs.
5. Powdrr compiles the guaranteed feature specs into canonical IR.
6. Powdrr records `FeatureDefinitionRevision` and `FeaturePlanRevision`.

### Live Compute Flow

1. Powdrr Compute resolves the active `FeaturePlanRevision`.
2. Powdrr Compute tails the canonical raw event source defined by
   `PowdrrSourceContract`.
3. Powdrr Compute writes provisional results to canonical Iceberg feature
   tables.
4. Powdrr publishes a `FeaturePublication`.
5. Powdrr Engine promotes the matching `ServingFrontier` when the publication
   is serveable.

### Finalization Flow

1. Allowed lateness or correction horizon closes.
2. Powdrr Compute produces final output for the covered range.
3. Powdrr publishes a final `FeaturePublication`.
4. The `ServingFrontier` is updated according to finality policy.
5. Training retrieval defaults to the final publication unless overridden.

### Online Serving Flow

1. Client asks for a Feast feature service or native Powdrr feature vector.
2. Powdrr resolves the `ServingFrontier`.
3. Powdrr Engine serves from the active publication's Iceberg snapshot.
4. Response carries:
   - `feature_revision_id`
   - `plan_revision_id`
   - `publication_id`
   - `powdrr_checkpoint`
   - `iceberg_snapshot_id`
   - `finality`

### Training / Experiment Flow

1. User requests a dataset for a Feast feature service.
2. Powdrr resolves the revision set and finality policy.
3. Offline retrieval runs point-in-time joins pinned to those publications.
4. Powdrr writes a `TrainingDatasetRecord`.
5. Model training binds the model version back to that dataset and revision set.

## Feast Integration Points

The extension should integrate with Feast in three places.

### 1. Registry Server

Use Feast registry APIs and object model for:

- authoring
- discovery
- permissions
- search

This should remain the user-facing catalog.

### 2. Custom Online Store

Add a Powdrr-backed Feast online-store adapter, but treat it as a compatibility
surface rather than the canonical serving implementation.

Its responsibilities:

- map Feast online reads onto Powdrr active frontiers
- return low-latency feature values from Powdrr Engine
- include revision metadata where possible

Its non-goals:

- becoming the source of truth for online state
- reintroducing a separate mutable KV copy as canonical serving data

### 3. Custom Offline Store

Add a Powdrr-backed Feast offline-store implementation, likely through the
remote offline-store path, so historical retrieval is resolved by Powdrr and
executed against canonical Iceberg snapshots.

Its responsibilities:

- revision-pinned historical retrieval
- point-in-time joins against canonical feature tables
- saved dataset export

## Where Feast Must Not Stay In Charge

Feast should not be the system of record for:

- latest live feature values
- publication cutover
- provisional vs final state
- rollback
- source coverage and replay boundaries
- compute checkpoints
- online/offline equivalence evidence

Those are Powdrr runtime contracts, not registry concerns.

## Storage Proposal

Back both metadata layers with one SQL database in the first version.

Suggested split:

- Feast registry tables for native Feast objects
- Powdrr extension tables for runtime metadata

This keeps:

- one service
- one operational database
- one auth boundary
- one backup and restore story

without forcing a deep Feast fork.

## Repo Integration Proposal

### `control_plane`

Add shared types for:

- `PowdrrFeatureSpec`
- `PowdrrSourceContract`
- `FeatureDefinitionRevision`
- `FeaturePlanRevision`
- `FeaturePublication`
- `ServingFrontier`
- `ModelBinding`
- `TrainingDatasetRecord`
- `ValidationRun`

### `service_lib`

Add metadata-store interfaces and service handlers for:

- CRUD for Powdrr extension objects
- compile orchestration
- promotion and rollback
- dataset and model binding metadata
- validation metadata

### `query_core`

Add canonical feature IR types and validation diagnostics.

### `query_runtime`

Add:

- revision-aware plan resolution
- frontier reads for serving
- publication writes from compute
- validation hooks for shadow and reconciliation flows

### `query_server`

Add or extend:

- native feature-vector APIs
- revision-aware response metadata
- compatibility adapters that resolve through frontiers

## Recommended First Implementation

Keep the first cut narrow.

### Guaranteed Class V1

Support only:

- single replayable raw event source per feature view
- deterministic SQL
- explicit entity keys
- explicit event timestamps
- projections
- filters
- simple aggregations
- well-defined window semantics

Reject:

- arbitrary UDFs
- non-deterministic functions
- features without explicit event-time semantics
- side writes outside Powdrr publication

### Metadata Scope V1

Implement first:

- `PowdrrFeatureSpec`
- `PowdrrSourceContract`
- `FeatureDefinitionRevision`
- `FeaturePlanRevision`
- `FeaturePublication`
- `ServingFrontier`
- `TrainingDatasetRecord`
- `ModelBinding`

Add later:

- richer experiment metadata
- broader lineage export
- more complex guaranteed classes

## Migration Strategy

### Phase 1

Use Feast only for:

- feature catalog
- feature service grouping
- governance

Keep Powdrr extension objects in the Powdrr control plane.

### Phase 2

Add Powdrr-backed custom Feast online and offline stores.

This gives Feast-compatible clients access to:

- low-latency online retrieval
- revision-pinned offline retrieval

without changing the canonical storage model.

### Phase 3

Present both layers through one operator-facing control plane and, if useful,
one shared UI.

## Open Questions

- whether the Powdrr extension objects should live in a separate API namespace
  or be partially exposed through Feast registry extensions
- whether the first custom offline path should be a direct Feast offline-store
  plugin or a remote offline service backed by Powdrr
- whether model bindings belong in the Powdrr control plane only or also need a
  mirrored Feast-visible object for UI purposes

## Recommendation

Build the first version as a companion metadata layer, not a deep Feast fork.

Concretely:

- use Feast Registry Server for catalog and governance
- build Powdrr Metadata Extension APIs in the existing control-plane service
- keep one logical metadata plane backed by one SQL database
- add Powdrr custom Feast online/offline adapters after the extension object
  model is stable

That keeps Feast in the role it is good at, while giving Powdrr ownership over
the contracts that actually determine whether online and offline feature values
are provably the same system.
