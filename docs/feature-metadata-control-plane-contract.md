# Feature Metadata Control-Plane Contract

This document is the implementation-facing follow-on to
[feast-metadata-service-extension-proposal.md](./feast-metadata-service-extension-proposal.md).

Its job is to define:

- the Rust module layout to add in `control_plane`
- the service-trait additions for `service_lib`
- the endpoint families `Powdrr Metadata` should expose
- the concrete control-plane objects needed by `Powdrr Compute`

The key update from the narrower prior proposal is that this contract now
includes not just feature specs and publications, but also:

- continuous compute deployments
- batch backfill runs
- experiment runs
- raw and derived table contracts

## Scope

This contract covers metadata for:

- guaranteed feature specs
- replay source contracts
- raw table contracts
- derived table contracts
- feature definition revisions
- feature plan revisions
- late-data policies
- compute deployments
- compute batch runs
- experiment runs
- feature publications
- serving frontiers
- training dataset records
- model bindings
- validation runs

It does not define:

- the feature IR itself
- the compute runtime internals
- the serving implementation
- the offline retrieval executor

Those systems should consume this metadata contract rather than redefine it.

## Recommended File Layout

### `control_plane`

Add:

- `control_plane/src/feature_metadata.rs`

Keep the object families together there rather than scattering them across
checkpoint metadata files.

Recommended top-level groups in that module:

- references and selectors
- registry bridge types
- compute deployment and run types
- publication and frontier types
- training and validation types

### `service_lib`

Add:

- `service_lib/src/feature_metadata_store.rs`

This should be a separate trait from checkpoint-oriented metadata storage.

### `service`

Add:

- feature-metadata handlers in `service/src/v1_handlers.rs`
- routes in `service/src/router.rs`
- forwarding variants in `service/src/service_impl_provider.rs`

The first cut can stay aligned with the current action-style handler pattern.

## Object Families

The easiest implementation shape is to treat the metadata as six groups.

### 1. Registry Bridge

- `FeatureViewRef`
- `FeatureServiceRef`
- `PowdrrFeatureSpec`
- `PowdrrSourceContract`
- `RawTableContract`
- `DerivedTableContract`
- `LateDataPolicy`

These bridge Feast objects into explicit runtime contracts.

### 2. Revision Objects

- `FeatureDefinitionRevision`
- `FeaturePlanRevision`

These make author intent and compiled plan state immutable and auditable.

### 3. Continuous Compute Objects

- `ComputeDeployment`
- `ComputeDeploymentStatus`
- `DeploymentCheckpointRef`

These describe long-lived streaming execution.

Suggested fields for `ComputeDeployment`:

- `compute_deployment_id`
- `project`
- `feature_program_revision_id`
- `source_binding_id`
- `raw_table_contract_id`
- `derived_table_contract_id`
- `late_data_policy_id`
- `desired_state`
- `parallelism`
- `runtime_profile`

Suggested fields for `ComputeDeploymentStatus`:

- `compute_deployment_id`
- `reported_state`
- `node_id`
- `watermark_ms`
- `source_lag`
- `checkpoint_ref`
- `raw_commit_ref`
- `derived_commit_ref`
- `repair_backlog`
- `updated_at_ms`

### 4. Finite Offline Run Objects

- `ComputeBatchRun`
- `ExperimentRun`
- `ComputeRunStatus`

Suggested fields for `ComputeBatchRun`:

- `compute_batch_run_id`
- `project`
- `feature_program_revision_id`
- `source_binding_id`
- `source_range`
- `target_table_contract_id`
- `target_write_mode`
- `run_reason`
- `status`

Suggested fields for `ExperimentRun`:

- `experiment_run_id`
- `project`
- `feature_program_revision_id`
- `source_binding_id`
- `source_range`
- `experiment_namespace`
- `experiment_table_name`
- `retention_policy`
- `status`

Suggested fields for `ComputeRunStatus`:

- run id
- run kind
- planning state
- claimed node
- started and finished timestamps
- checkpoint or progress ref
- output table refs
- failure reason

### 5. Publication And Serving Objects

- `FeaturePublication`
- `ServingFrontier`

These are the bridge from compute output to serving.

### 6. Training And Validation Objects

- `TrainingDatasetRecord`
- `ModelBinding`
- `ValidationRun`

These make offline reproducibility explicit.

## Selector And Request Types

The service boundary should define explicit selectors and requests rather than
exposing only raw metadata rows.

Recommended selectors:

- `FeatureSpecSelector`
- `SourceContractSelector`
- `FeatureRevisionSelector`
- `PlanRevisionSelector`
- `ComputeDeploymentSelector`
- `ComputeBatchRunSelector`
- `ExperimentRunSelector`
- `FeaturePublicationSelector`
- `ServingFrontierSelector`
- `TrainingDatasetSelector`
- `ModelBindingSelector`
- `ValidationRunSelector`

Recommended write requests:

- `UpsertFeatureSpecRequest`
- `CompileFeatureSpecRequest`
- `UpsertSourceContractRequest`
- `UpsertRawTableContractRequest`
- `UpsertDerivedTableContractRequest`
- `UpsertLateDataPolicyRequest`
- `UpsertComputeDeploymentRequest`
- `CreateComputeBatchRunRequest`
- `CreateExperimentRunRequest`
- `RecordDeploymentStatusRequest`
- `RecordComputeRunStatusRequest`
- `RecordFeaturePublicationRequest`
- `PromoteServingFrontierRequest`
- `RollbackServingFrontierRequest`
- `CreateTrainingDatasetRecordRequest`
- `CreateModelBindingRequest`
- `CreateValidationRunRequest`

## Service Trait Contract

Add a new trait:

- `FeatureMetadataStore`

Recommended method groups:

### Registry Bridge Methods

- `upsert_feature_spec`
- `get_feature_spec`
- `compile_feature_spec`
- `upsert_source_contract`
- `get_source_contract`
- `upsert_raw_table_contract`
- `get_raw_table_contract`
- `upsert_derived_table_contract`
- `get_derived_table_contract`
- `upsert_late_data_policy`
- `get_late_data_policy`

### Revision Methods

- `get_feature_revision`
- `get_plan_revision`

### Continuous Deployment Methods

- `upsert_compute_deployment`
- `get_compute_deployment`
- `list_active_compute_deployments`
- `record_compute_deployment_status`

### Batch And Experiment Methods

- `create_compute_batch_run`
- `claim_next_compute_batch_run`
- `get_compute_batch_run`
- `record_compute_batch_run_status`
- `create_experiment_run`
- `claim_next_experiment_run`
- `get_experiment_run`
- `record_experiment_run_status`

### Publication And Frontier Methods

- `record_feature_publication`
- `get_feature_publication`
- `promote_serving_frontier`
- `rollback_serving_frontier`
- `get_serving_frontier`

### Training And Validation Methods

- `create_training_dataset_record`
- `get_training_dataset_record`
- `create_model_binding`
- `get_model_binding`
- `create_validation_run`
- `get_validation_run`
- `resolve_feature_service_revisions`

## Endpoint Contract

All new endpoints should live under `/api/v1`.

For the first version, keep the repo’s current style:

- JSON body requests
- `POST` for all reads and writes
- explicit handler names

### Feature Spec And Contract Endpoints

- `POST /api/v1/upsert_feature_spec`
- `POST /api/v1/get_feature_spec`
- `POST /api/v1/compile_feature_spec`
- `POST /api/v1/upsert_source_contract`
- `POST /api/v1/get_source_contract`
- `POST /api/v1/upsert_raw_table_contract`
- `POST /api/v1/get_raw_table_contract`
- `POST /api/v1/upsert_derived_table_contract`
- `POST /api/v1/get_derived_table_contract`
- `POST /api/v1/upsert_late_data_policy`
- `POST /api/v1/get_late_data_policy`

### Revision Endpoints

- `POST /api/v1/get_feature_revision`
- `POST /api/v1/get_plan_revision`

### Continuous Deployment Endpoints

- `POST /api/v1/upsert_compute_deployment`
- `POST /api/v1/get_compute_deployment`
- `POST /api/v1/list_active_compute_deployments`
- `POST /api/v1/record_compute_deployment_status`

### Batch And Experiment Endpoints

- `POST /api/v1/create_compute_batch_run`
- `POST /api/v1/claim_next_compute_batch_run`
- `POST /api/v1/get_compute_batch_run`
- `POST /api/v1/record_compute_batch_run_status`
- `POST /api/v1/create_experiment_run`
- `POST /api/v1/claim_next_experiment_run`
- `POST /api/v1/get_experiment_run`
- `POST /api/v1/record_experiment_run_status`

### Publication And Frontier Endpoints

- `POST /api/v1/record_feature_publication`
- `POST /api/v1/get_feature_publication`
- `POST /api/v1/promote_serving_frontier`
- `POST /api/v1/rollback_serving_frontier`
- `POST /api/v1/get_serving_frontier`

### Training And Validation Endpoints

- `POST /api/v1/create_training_dataset_record`
- `POST /api/v1/get_training_dataset_record`
- `POST /api/v1/create_model_binding`
- `POST /api/v1/get_model_binding`
- `POST /api/v1/create_validation_run`
- `POST /api/v1/get_validation_run`
- `POST /api/v1/resolve_feature_service_revisions`

## First-Pass API Schema

These are not intended to be the final OpenAPI definitions. They are the
concrete JSON shapes the first implementation should target so the service,
compute runtime, and future UI all converge on the same contract.

### `POST /api/v1/upsert_feature_spec`

```json
{
  "spec": {
    "feature_view": {
      "project": "prod",
      "feature_view_name": "user_features"
    },
    "feature_service_names": ["ranking_v1"],
    "source_names": ["web_events"],
    "sql_text": "SELECT user_id, event_time, ...",
    "sql_dialect": "powdrr_sql_v1",
    "entity_key_columns": ["user_id"],
    "event_time_column": "event_time",
    "output_schema": {},
    "guaranteed_class": "SqlV1",
    "compute_modes": ["Bounded", "Live"],
    "owner": "ml-platform",
    "tags": {
      "team": "ranking"
    },
    "watermark_policy": {
      "kind": "Source",
      "max_out_of_orderness_ms": 300000
    },
    "allowed_lateness_ms": 600000,
    "correction_policy": {
      "kind": "ReplayAffectedRange",
      "dedupe_key_columns": ["event_id"]
    },
    "correction_horizon_ms": 604800000,
    "retention_horizon_ms": 2592000000,
    "finalization_rule": {
      "kind": "AfterLatenessWindow",
      "finalize_after_ms": 600000
    },
    "serving": {
      "online_enabled": true,
      "serve_provisional": false,
      "output_table_name": "features.user_features",
      "key_columns": ["user_id"],
      "request_entity_columns": ["user_id"],
      "response_feature_columns": ["feature_a", "feature_b"]
    },
    "validation": {
      "golden_test_suite": "user_features_smoke",
      "require_stream_batch_equivalence": true,
      "require_online_shadow_validation": true
    }
  },
  "if_match_feature_revision_id": null,
  "dry_run": false
}
```

### `POST /api/v1/upsert_compute_deployment`

```json
{
  "deployment": {
    "compute_deployment_id": "deploy_user_features_live",
    "project": "prod",
    "feature_program_revision_id": "fpr_2026_05_23_001",
    "source_binding_id": "src_web_events",
    "raw_table_contract_id": "raw_web_events_v1",
    "derived_table_contract_id": "drv_user_features_v1",
    "late_data_policy_id": "late_default_v1",
    "desired_state": "Running",
    "parallelism": 8,
    "runtime_profile": "continuous_default"
  }
}
```

### `POST /api/v1/create_compute_batch_run`

```json
{
  "batch_run": {
    "compute_batch_run_id": "backfill_user_features_2026_05_23",
    "project": "prod",
    "feature_program_revision_id": "fpr_2026_05_23_001",
    "source_binding_id": "src_web_events",
    "source_range": {
      "event_time_start_ms": 1746057600000,
      "event_time_end_ms": 1746144000000
    },
    "target_table_contract_id": "drv_user_features_v1",
    "target_write_mode": "append_shadow",
    "run_reason": "backfill_after_bugfix"
  }
}
```

### `POST /api/v1/create_experiment_run`

```json
{
  "experiment_run": {
    "experiment_run_id": "exp_ctr_ablation_42",
    "project": "prod",
    "feature_program_revision_id": "fpr_2026_05_23_001",
    "source_binding_id": "src_web_events",
    "source_range": {
      "event_time_start_ms": 1746057600000,
      "event_time_end_ms": 1746662400000
    },
    "experiment_namespace": "experiments.exp_ctr_ablation_42",
    "experiment_table_name": "user_features",
    "retention_policy": {
      "ttl_days": 30
    }
  }
}
```

### `POST /api/v1/record_compute_deployment_status`

```json
{
  "deployment_status": {
    "compute_deployment_id": "deploy_user_features_live",
    "reported_state": "Running",
    "node_id": "compute-a",
    "watermark_ms": 1748055600000,
    "source_lag": {
      "records": 1234
    },
    "checkpoint_ref": "s3://warehouse/checkpoints/deploy_user_features_live/cp-42.json",
    "raw_commit_ref": "iceberg:raw.web_events:snapshot:12345",
    "derived_commit_ref": "iceberg:features.user_features:snapshot:98765",
    "repair_backlog": {
      "pending_manifests": 3
    },
    "updated_at_ms": 1748055660000
  }
}
```

### `POST /api/v1/record_feature_publication`

```json
{
  "publication": {
    "publication_id": "pub_user_features_2026_05_23_001",
    "feature_view": {
      "project": "prod",
      "feature_view_name": "user_features"
    },
    "feature_revision_id": "fr_2026_05_23_001",
    "plan_revision_id": "fpr_2026_05_23_001",
    "output_table_name": "features.user_features",
    "source_coverage": {
      "event_time_start_ms": 1748052000000,
      "event_time_end_ms": 1748055600000
    },
    "compute_checkpoint_id": "cp-42",
    "powdrr_checkpoint": {},
    "iceberg_snapshot_id": "98765",
    "finality": "Provisional",
    "status": "Serveable",
    "published_at_ms": 1748055660000,
    "published_by": "compute-a",
    "row_count": 1200345
  },
  "promote_when_ready": false
}
```

## Metadata And Compute Execution Contract

`Powdrr Compute` should interact with this service through explicit object
transitions, not implicit side effects.

### Reads Required By Compute

- active `ComputeDeployment`s
- referenced `FeaturePlanRevision`s
- referenced source and table contracts
- referenced late-data policies
- queued `ComputeBatchRun`s
- queued `ExperimentRun`s

### Writes Required From Compute

- deployment status heartbeats
- deployment watermark and lag
- checkpoint references
- raw and derived commit refs
- batch run status
- experiment run status
- repair backlog summary
- resulting `FeaturePublication`s

### Required State Machines

`ComputeDeployment` should support states like:

- `Pending`
- `Starting`
- `Restoring`
- `Running`
- `Draining`
- `Stopped`
- `Failed`
- `Degraded`

`ComputeBatchRun` and `ExperimentRun` should support:

- `Queued`
- `Claimed`
- `Planning`
- `Running`
- `Succeeded`
- `Failed`
- `Cancelled`

## Implementation Slice Order

This is the recommended implementation order across `control_plane`,
`service_lib`, and `service`.

### Slice 0: Module Scaffolding

`control_plane`

- add `feature_metadata.rs`
- add shared refs and enums

`service_lib`

- add `feature_metadata_store.rs`
- add trait skeleton only

`service`

- add forwarding stubs in `service_impl_provider.rs`

Goal:

- establish file and trait boundaries before adding dozens of objects

### Slice 1: Registry Bridge Contract

`control_plane`

- `PowdrrFeatureSpec`
- `PowdrrSourceContract`
- `RawTableContract`
- `DerivedTableContract`
- `LateDataPolicy`
- selectors and upsert requests for those types

`service_lib`

- CRUD methods for the above

`service`

- handlers and routes for the above
- in-memory implementation in `EphemeralServiceImpl`

Goal:

- make feature authoring and replay contracts real

### Slice 2: Revision Contract

`control_plane`

- `FeatureDefinitionRevision`
- `FeaturePlanRevision`
- compile request/response types

`service_lib`

- get and compile methods

`service`

- compile endpoint and read endpoints

Goal:

- make immutable revision identity explicit before compute state is added

### Slice 3: Continuous Compute Contract

`control_plane`

- `ComputeDeployment`
- `ComputeDeploymentStatus`
- request and selector types

`service_lib`

- deployment CRUD
- list active deployments
- record deployment status

`service`

- deployment routes and handlers

Goal:

- allow `Powdrr Compute` to reconcile long-lived streaming deployments

### Slice 4: Finite Offline Run Contract

`control_plane`

- `ComputeBatchRun`
- `ExperimentRun`
- `ComputeRunStatus`

`service_lib`

- create, claim, get, and status methods for batch and experiment runs

`service`

- run-queue and run-status routes

Goal:

- make backfills and experiments first-class control-plane objects

### Slice 5: Publication And Frontier Contract

`control_plane`

- `FeaturePublication`
- `ServingFrontier`

`service_lib`

- record publication
- get publication
- promote and rollback frontier
- get frontier

`service`

- publication and frontier endpoints

Goal:

- connect compute output to serving promotion explicitly

### Slice 6: Training And Validation Contract

`control_plane`

- `TrainingDatasetRecord`
- `ModelBinding`
- `ValidationRun`

`service_lib`

- dataset, model, and validation methods

`service`

- training/model/validation endpoints

Goal:

- close the loop from authoring to compute to serving to model provenance

## Validation Rules

The control-plane service should reject invalid metadata writes early.

Examples:

- feature specs without entity keys
- feature specs without source contracts
- invalid lateness vs correction horizon
- compute deployments that reference missing plan revisions
- batch runs that target unknown derived table contracts
- experiment runs without explicit retention policy
- publications that reference missing revisions
- frontiers that reference publications in the wrong scope

## Storage Expectations

Each backend implementation should persist the objects by stable identity.

Minimum stable ids:

- `feature_revision_id`
- `plan_revision_id`
- `compute_deployment_id`
- `compute_batch_run_id`
- `experiment_run_id`
- `publication_id`
- `frontier_id`
- `training_dataset_id`
- `validation_run_id`

Backends must not infer these objects from Iceberg alone.

## Recommended First Implementation Slice

The smallest coherent first slice is:

1. registry bridge objects:
   - feature spec
   - source contract
   - raw table contract
   - derived table contract
   - late data policy
2. revision objects:
   - feature revision
   - plan revision
3. continuous compute object:
   - compute deployment
   - deployment status
4. in-memory implementation in `EphemeralServiceImpl`
5. matching `/api/v1/...` routes

After that, add:

- batch runs
- experiment runs
- publications
- frontiers
- dataset/model/validation objects

If the team wants a PR ladder instead of one long branch, the clean sequence is:

1. scaffolding
2. registry bridge objects
3. revisions
4. continuous deployments
5. batch and experiment runs
6. publications and frontiers
7. training/model/validation

## Recommendation

Implement this as a dedicated `feature_metadata` module and
`FeatureMetadataStore` trait.

Do not overload checkpoint metadata or serving metadata with these concerns.

The whole point of this contract is to give Powdrr one coherent metadata plane
that can bridge:

- Feast authoring and governance
- Powdrr Compute execution
- Powdrr Engine publication and serving
- training, experimentation, and validation
