# Feature Metadata Control-Plane Contract

This document is the implementation-facing follow-on to
[`feast-metadata-service-extension-proposal.md`](./feast-metadata-service-extension-proposal.md).

Its job is to define:

- the exact Rust object model to add in `powdrr-control-plane`
- the exact service-trait additions for the metadata plane
- the exact `/api/v1/...` endpoints to expose from `powdrr-io-service`

The intent is to keep the first implementation aligned with the codebase's
current patterns:

- serde DTOs in `control_plane`
- service traits in `service_lib`
- action-style handlers in `service`
- one actor-backed `SERVICE_IMPL` dispatch surface

## Scope

This contract covers the metadata plane for:

- guaranteed feature specifications
- replay source contracts
- compiled feature revisions
- plan revisions
- publication records
- serving frontiers
- training dataset records
- model bindings
- validation runs

It does not define:

- the feature IR itself
- the SQL compiler internals
- the runtime serving implementation
- the offline retrieval executor

Those should use this metadata contract, not redefine it.

## Recommended File Layout

### `control_plane`

Add:

- `control_plane/src/feature_metadata.rs`
- `pub mod feature_metadata;` in
  [control_plane/src/lib.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-feast-metadata-service-proposal/control_plane/src/lib.rs:1)

Reason:

- `data_contract.rs` already owns checkpoint and table metadata and is large
- feature metadata is a separate concern and should stay readable
- the new module can still reuse shared types such as
  [`CheckpointDescriptor`](/Users/gregory/code/powdrr-engine/.worktrees/codex-feast-metadata-service-proposal/control_plane/src/checkpoint_descriptor.rs:1)
  and `PowdrrSchema`

### `service_lib`

Add:

- `service_lib/src/feature_metadata_store.rs`
- `pub mod feature_metadata_store;` in
  [service_lib/src/lib.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-feast-metadata-service-proposal/service_lib/src/lib.rs:1)

Reason:

- the existing
  [service_lib/src/metadata_store.rs](/Users/gregory/code/powdrr-engine/.worktrees/codex-feast-metadata-service-proposal/service_lib/src/metadata_store.rs:1)
  trait is checkpoint- and work-item-specific
- feature metadata should have its own trait instead of overloading checkpoint
  semantics with unrelated methods

### `service`

Add:

- new handler functions in `service/src/v1_handlers.rs` for the first cut
- new routes in `service/src/router.rs`
- `ServiceImplProviderActorMessage` variants and forwarding methods in
  `service/src/service_impl_provider.rs`

Reason:

- this is the least disruptive first step
- the route and handler shape stays consistent with the current service

## API Style Decision

The current service uses two patterns:

- `GET /describe_* /:name` for simple single-name reads
- JSON body handlers for more structured selectors

The feature metadata surface needs selectors such as:

- `project`
- `feature_view_name`
- `feature_revision_id`
- `publication_id`
- `feature_service_name`

That is more than one stable path segment in most calls.

For the first version, use JSON request bodies and `POST` for all new feature
metadata endpoints, including reads. This matches the repo's existing handler
macros and avoids inventing new multi-parameter path extractors.

## Exact Rust Types

The following code blocks are the proposed contract for
`control_plane/src/feature_metadata.rs`.

### Imports

```rust
use crate::checkpoint_descriptor::CheckpointDescriptor;
use crate::schema_massager::PowdrrSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
```

### Base References

```rust
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct FeatureViewRef {
    pub project: String,
    pub feature_view_name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct FeatureServiceRef {
    pub project: String,
    pub feature_service_name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServingScopeRef {
    pub project: String,
    pub scope_type: ServingScopeType,
    pub scope_name: String,
}
```

### Enums

```rust
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ServingScopeType {
    FeatureView,
    FeatureService,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum GuaranteedFeatureClass {
    SqlV1,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ComputeMode {
    Bounded,
    Live,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SourceType {
    AppendLog,
    CdcLog,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum WatermarkPolicyKind {
    Source,
    EventTime,
    IngestTime,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum CorrectionPolicyKind {
    IgnoreLate,
    UpsertWithinHorizon,
    ReplayAffectedRange,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum FinalizationRuleKind {
    AfterLatenessWindow,
    Manual,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum FeatureRevisionStatus {
    Draft,
    Compiled,
    Rejected,
    Retired,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum FeatureFinality {
    Provisional,
    Final,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum FeaturePublicationStatus {
    Pending,
    Serveable,
    Active,
    Superseded,
    RolledBack,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ServingActivationPolicy {
    Manual,
    PromoteWhenReady,
    PromoteFinalOnly,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ModelBindingStatus {
    Candidate,
    Active,
    Retired,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TrainingFinalityPolicy {
    FinalOnly,
    AllowProvisional,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ValidationType {
    Compile,
    GoldenReplay,
    StreamBatchDiff,
    OnlineShadow,
    BackfillReconciliation,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ValidationStatus {
    Running,
    Passed,
    Failed,
    Warning,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DiagnosticLevel {
    Error,
    Warning,
    Info,
}
```

### Policy and Helper Types

```rust
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct WatermarkPolicy {
    pub kind: WatermarkPolicyKind,
    #[serde(default)]
    pub max_out_of_orderness_ms: Option<u64>,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CorrectionPolicy {
    pub kind: CorrectionPolicyKind,
    #[serde(default)]
    pub dedupe_key_columns: Vec<String>,
    #[serde(default)]
    pub version_column: Option<String>,
    #[serde(default)]
    pub sequence_column: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FinalizationRule {
    pub kind: FinalizationRuleKind,
    #[serde(default)]
    pub finalize_after_ms: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FeatureServingConfig {
    pub online_enabled: bool,
    #[serde(default)]
    pub serve_provisional: bool,
    pub output_table_name: String,
    #[serde(default)]
    pub key_columns: Vec<String>,
    #[serde(default)]
    pub request_entity_columns: Vec<String>,
    #[serde(default)]
    pub response_feature_columns: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FeatureValidationConfig {
    #[serde(default)]
    pub golden_test_suite: Option<String>,
    #[serde(default)]
    pub require_stream_batch_equivalence: bool,
    #[serde(default)]
    pub require_online_shadow_validation: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FeatureValidationDiagnostic {
    pub level: DiagnosticLevel,
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub field: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct FeatureSourceCoverage {
    #[serde(default)]
    pub source_offset_start: HashMap<String, String>,
    #[serde(default)]
    pub source_offset_end: HashMap<String, String>,
    #[serde(default)]
    pub event_time_start_ms: Option<i64>,
    #[serde(default)]
    pub event_time_end_ms: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct RevisionReferenceSet {
    #[serde(default)]
    pub feature_revision_ids: Vec<String>,
    #[serde(default)]
    pub plan_revision_ids: Vec<String>,
    #[serde(default)]
    pub publication_ids: Vec<String>,
    #[serde(default)]
    pub frontier_ids: Vec<String>,
    #[serde(default)]
    pub training_dataset_ids: Vec<String>,
}
```

### Core Metadata Types

```rust
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PowdrrSourceContract {
    pub project: String,
    pub source_name: String,
    pub raw_table_name: String,
    pub source_type: SourceType,
    pub event_id_column: String,
    #[serde(default)]
    pub entity_key_columns: Vec<String>,
    pub event_time_column: String,
    pub ingest_time_column: String,
    #[serde(default)]
    pub payload_columns: Vec<String>,
    #[serde(default)]
    pub offset_columns: Vec<String>,
    #[serde(default)]
    pub dedupe_key_columns: Vec<String>,
    #[serde(default)]
    pub correction_event_type_column: Option<String>,
    #[serde(default)]
    pub schema_revision_column: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PowdrrFeatureSpec {
    pub feature_view: FeatureViewRef,
    #[serde(default)]
    pub feature_service_names: Vec<String>,
    #[serde(default)]
    pub source_names: Vec<String>,
    pub sql_text: String,
    pub sql_dialect: String,
    #[serde(default)]
    pub entity_key_columns: Vec<String>,
    pub event_time_column: String,
    pub output_schema: PowdrrSchema,
    pub guaranteed_class: GuaranteedFeatureClass,
    #[serde(default)]
    pub compute_modes: Vec<ComputeMode>,
    pub owner: String,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    pub watermark_policy: WatermarkPolicy,
    pub allowed_lateness_ms: u64,
    pub correction_policy: CorrectionPolicy,
    pub correction_horizon_ms: u64,
    #[serde(default)]
    pub retention_horizon_ms: Option<u64>,
    pub finalization_rule: FinalizationRule,
    pub serving: FeatureServingConfig,
    pub validation: FeatureValidationConfig,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FeatureDefinitionRevision {
    pub feature_view: FeatureViewRef,
    pub feature_revision_id: String,
    pub feature_spec_hash: String,
    pub feast_registry_version: String,
    pub created_at_ms: i64,
    pub created_by: String,
    pub status: FeatureRevisionStatus,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FeaturePlanRevision {
    pub feature_view: FeatureViewRef,
    pub feature_revision_id: String,
    pub plan_revision_id: String,
    pub compiler_version: String,
    pub engine_compatibility: String,
    #[serde(default)]
    pub ir_artifact_uri: Option<String>,
    #[serde(default)]
    pub compile_diagnostics: Vec<FeatureValidationDiagnostic>,
    pub supports_bounded: bool,
    pub supports_live: bool,
    pub created_at_ms: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FeaturePublication {
    pub publication_id: String,
    pub feature_view: FeatureViewRef,
    pub feature_revision_id: String,
    pub plan_revision_id: String,
    pub output_table_name: String,
    pub source_coverage: FeatureSourceCoverage,
    pub compute_checkpoint_id: String,
    pub powdrr_checkpoint: CheckpointDescriptor,
    #[serde(default)]
    pub iceberg_snapshot_id: Option<String>,
    pub finality: FeatureFinality,
    pub status: FeaturePublicationStatus,
    pub published_at_ms: i64,
    pub published_by: String,
    #[serde(default)]
    pub row_count: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ServingFrontier {
    pub frontier_id: String,
    pub scope: ServingScopeRef,
    #[serde(default)]
    pub target_publication_id: Option<String>,
    #[serde(default)]
    pub active_publication_id: Option<String>,
    #[serde(default)]
    pub active_finality: Option<FeatureFinality>,
    pub activation_policy: ServingActivationPolicy,
    pub updated_at_ms: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TrainingDatasetRecord {
    pub training_dataset_id: String,
    pub feature_service: FeatureServiceRef,
    pub entity_source_ref: String,
    #[serde(default)]
    pub label_source_ref: Option<String>,
    pub revision_refs: RevisionReferenceSet,
    pub finality_policy: TrainingFinalityPolicy,
    pub dataset_uri: String,
    pub created_at_ms: i64,
    pub created_by: String,
    #[serde(default)]
    pub row_count: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ModelBinding {
    pub model_binding_id: String,
    pub project: String,
    pub model_name: String,
    pub model_version: String,
    pub feature_service_name: String,
    pub training_dataset_id: String,
    pub revision_refs: RevisionReferenceSet,
    #[serde(default)]
    pub default_frontier_id: Option<String>,
    pub created_at_ms: i64,
    pub status: ModelBindingStatus,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ValidationRun {
    pub validation_run_id: String,
    pub project: String,
    pub validation_type: ValidationType,
    pub scope: ServingScopeRef,
    pub revision_refs: RevisionReferenceSet,
    pub started_at_ms: i64,
    #[serde(default)]
    pub finished_at_ms: Option<i64>,
    pub status: ValidationStatus,
    #[serde(default)]
    pub metrics: HashMap<String, Value>,
    #[serde(default)]
    pub artifact_uri: Option<String>,
}
```

## Request and Selector DTOs

The service should not expose raw metadata records alone. It should also expose
request and selector DTOs so the service boundary remains stable when write
semantics become richer.

```rust
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FeatureSpecSelector {
    pub feature_view: FeatureViewRef,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SourceContractSelector {
    pub project: String,
    pub source_name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FeatureRevisionSelector {
    pub project: String,
    pub feature_revision_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PlanRevisionSelector {
    pub project: String,
    pub plan_revision_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FeaturePublicationSelector {
    pub project: String,
    pub publication_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ServingFrontierSelector {
    pub scope: ServingScopeRef,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TrainingDatasetSelector {
    pub project: String,
    pub training_dataset_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ModelBindingSelector {
    pub project: String,
    pub model_name: String,
    pub model_version: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ValidationRunSelector {
    pub project: String,
    pub validation_run_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpsertFeatureSpecRequest {
    pub spec: PowdrrFeatureSpec,
    #[serde(default)]
    pub if_match_feature_revision_id: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpsertFeatureSpecResponse {
    pub feature_revision: FeatureDefinitionRevision,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CompileFeatureSpecRequest {
    pub feature_view: FeatureViewRef,
    #[serde(default)]
    pub feature_revision_id: Option<String>,
    pub compiler_version: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CompileFeatureSpecResponse {
    pub feature_revision: FeatureDefinitionRevision,
    pub plan_revision: FeaturePlanRevision,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RecordFeaturePublicationRequest {
    pub publication: FeaturePublication,
    #[serde(default)]
    pub promote_when_ready: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PromoteServingFrontierRequest {
    pub scope: ServingScopeRef,
    pub target_publication_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RollbackServingFrontierRequest {
    pub scope: ServingScopeRef,
    #[serde(default)]
    pub rollback_to_publication_id: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ResolveFeatureServiceRevisionsRequest {
    pub feature_service: FeatureServiceRef,
    #[serde(default)]
    pub finality: Option<FeatureFinality>,
    #[serde(default)]
    pub point_in_time_ms: Option<i64>,
    #[serde(default)]
    pub prefer_active_frontier: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ResolveFeatureServiceRevisionsResponse {
    pub revision_refs: RevisionReferenceSet,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateTrainingDatasetRecordRequest {
    pub record: TrainingDatasetRecord,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateModelBindingRequest {
    pub binding: ModelBinding,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CreateValidationRunRequest {
    pub run: ValidationRun,
}
```

## Validation Rules on DTOs

The control-plane service should enforce these invariants before any metadata
write is accepted:

- `PowdrrFeatureSpec.feature_view.project` must be non-empty
- `PowdrrFeatureSpec.feature_view.feature_view_name` must be non-empty
- `PowdrrFeatureSpec.sql_text` must be non-empty
- `PowdrrFeatureSpec.entity_key_columns` must be non-empty
- `PowdrrFeatureSpec.compute_modes` must contain at least one mode
- `PowdrrFeatureSpec.source_names` must be non-empty
- `PowdrrFeatureSpec.allowed_lateness_ms <= correction_horizon_ms`
- `FeaturePublication.publication_id` must be immutable once written
- `FeaturePublication.feature_revision_id` and `plan_revision_id` must already
  exist
- `ServingFrontier.active_publication_id` must refer to a publication in the
  same project and scope
- `TrainingDatasetRecord.revision_refs.publication_ids` must be non-empty
- `ModelBinding.training_dataset_id` must exist before activation

## Service Trait Contract

Add a new trait in `service_lib/src/feature_metadata_store.rs`.

```rust
use crate::state_provider::ServiceApiError;
use async_trait::async_trait;
use powdrr_control_plane::feature_metadata::{
    CompileFeatureSpecRequest, CompileFeatureSpecResponse, CreateModelBindingRequest,
    CreateTrainingDatasetRecordRequest, CreateValidationRunRequest, FeatureDefinitionRevision,
    FeaturePlanRevision, FeaturePublication, FeaturePublicationSelector, FeatureRevisionSelector,
    FeatureSpecSelector, ModelBinding, ModelBindingSelector, PlanRevisionSelector,
    PowdrrFeatureSpec, PowdrrSourceContract, PromoteServingFrontierRequest,
    RecordFeaturePublicationRequest, ResolveFeatureServiceRevisionsRequest,
    ResolveFeatureServiceRevisionsResponse, RollbackServingFrontierRequest, ServingFrontier,
    ServingFrontierSelector, SourceContractSelector, TrainingDatasetRecord,
    TrainingDatasetSelector, UpsertFeatureSpecRequest, UpsertFeatureSpecResponse,
    ValidationRun, ValidationRunSelector,
};

#[async_trait]
pub trait FeatureMetadataStore {
    async fn upsert_feature_spec(
        &mut self,
        request: &UpsertFeatureSpecRequest,
    ) -> Result<UpsertFeatureSpecResponse, ServiceApiError>;

    async fn get_feature_spec(
        &mut self,
        selector: &FeatureSpecSelector,
    ) -> Result<Option<PowdrrFeatureSpec>, ServiceApiError>;

    async fn compile_feature_spec(
        &mut self,
        request: &CompileFeatureSpecRequest,
    ) -> Result<CompileFeatureSpecResponse, ServiceApiError>;

    async fn upsert_source_contract(
        &mut self,
        contract: &PowdrrSourceContract,
    ) -> Result<bool, ServiceApiError>;

    async fn get_source_contract(
        &mut self,
        selector: &SourceContractSelector,
    ) -> Result<Option<PowdrrSourceContract>, ServiceApiError>;

    async fn get_feature_revision(
        &mut self,
        selector: &FeatureRevisionSelector,
    ) -> Result<Option<FeatureDefinitionRevision>, ServiceApiError>;

    async fn get_plan_revision(
        &mut self,
        selector: &PlanRevisionSelector,
    ) -> Result<Option<FeaturePlanRevision>, ServiceApiError>;

    async fn record_feature_publication(
        &mut self,
        request: &RecordFeaturePublicationRequest,
    ) -> Result<FeaturePublication, ServiceApiError>;

    async fn get_feature_publication(
        &mut self,
        selector: &FeaturePublicationSelector,
    ) -> Result<Option<FeaturePublication>, ServiceApiError>;

    async fn promote_serving_frontier(
        &mut self,
        request: &PromoteServingFrontierRequest,
    ) -> Result<ServingFrontier, ServiceApiError>;

    async fn rollback_serving_frontier(
        &mut self,
        request: &RollbackServingFrontierRequest,
    ) -> Result<ServingFrontier, ServiceApiError>;

    async fn get_serving_frontier(
        &mut self,
        selector: &ServingFrontierSelector,
    ) -> Result<Option<ServingFrontier>, ServiceApiError>;

    async fn create_training_dataset_record(
        &mut self,
        request: &CreateTrainingDatasetRecordRequest,
    ) -> Result<TrainingDatasetRecord, ServiceApiError>;

    async fn get_training_dataset_record(
        &mut self,
        selector: &TrainingDatasetSelector,
    ) -> Result<Option<TrainingDatasetRecord>, ServiceApiError>;

    async fn create_model_binding(
        &mut self,
        request: &CreateModelBindingRequest,
    ) -> Result<ModelBinding, ServiceApiError>;

    async fn get_model_binding(
        &mut self,
        selector: &ModelBindingSelector,
    ) -> Result<Option<ModelBinding>, ServiceApiError>;

    async fn create_validation_run(
        &mut self,
        request: &CreateValidationRunRequest,
    ) -> Result<ValidationRun, ServiceApiError>;

    async fn get_validation_run(
        &mut self,
        selector: &ValidationRunSelector,
    ) -> Result<Option<ValidationRun>, ServiceApiError>;

    async fn resolve_feature_service_revisions(
        &mut self,
        request: &ResolveFeatureServiceRevisionsRequest,
    ) -> Result<ResolveFeatureServiceRevisionsResponse, ServiceApiError>;
}
```

## Why This Is a Separate Trait

Do not extend the existing
[`MetadataStore`](/Users/gregory/code/powdrr-engine/.worktrees/codex-feast-metadata-service-proposal/service_lib/src/metadata_store.rs:1)
trait for this surface.

Reasons:

- checkpoint metadata and feature metadata evolve at different rates
- checkpoint methods are runtime-internal and publication-oriented
- feature metadata is a control-plane authoring and audit surface
- a dedicated trait keeps storage implementations clearer

The same concrete service implementations should implement both traits:

- `EphemeralServiceImpl`
- `DynamoDBServiceImpl`
- `RaftServiceImpl`

## `SERVICE_IMPL` Contract Changes

Add new `ServiceImplProviderActorMessage` variants in
[`service/src/service_impl_provider.rs`](/Users/gregory/code/powdrr-engine/.worktrees/codex-feast-metadata-service-proposal/service/src/service_impl_provider.rs:1)
for each new feature metadata operation.

Suggested first-cut variants:

- `UpsertFeatureSpec`
- `GetFeatureSpec`
- `CompileFeatureSpec`
- `UpsertSourceContract`
- `GetSourceContract`
- `GetFeatureRevision`
- `GetPlanRevision`
- `RecordFeaturePublication`
- `GetFeaturePublication`
- `PromoteServingFrontier`
- `RollbackServingFrontier`
- `GetServingFrontier`
- `CreateTrainingDatasetRecord`
- `GetTrainingDatasetRecord`
- `CreateModelBinding`
- `GetModelBinding`
- `CreateValidationRun`
- `GetValidationRun`
- `ResolveFeatureServiceRevisions`

Add a `feature_metadata_store_func_impl!` macro parallel to the existing
`metadata_store_func_impl!` macro so forwarding stays explicit.

## Endpoint Contract

All new endpoints should live under `/api/v1`.

### Feature Spec Endpoints

- `POST /api/v1/upsert_feature_spec`
  - body: `UpsertFeatureSpecRequest`
  - response: `UpsertFeatureSpecResponse`
- `POST /api/v1/get_feature_spec`
  - body: `FeatureSpecSelector`
  - response: `PowdrrFeatureSpec`
- `POST /api/v1/compile_feature_spec`
  - body: `CompileFeatureSpecRequest`
  - response: `CompileFeatureSpecResponse`

### Source Contract Endpoints

- `POST /api/v1/upsert_source_contract`
  - body: `PowdrrSourceContract`
  - response: `bool`
- `POST /api/v1/get_source_contract`
  - body: `SourceContractSelector`
  - response: `PowdrrSourceContract`

### Revision Endpoints

- `POST /api/v1/get_feature_revision`
  - body: `FeatureRevisionSelector`
  - response: `FeatureDefinitionRevision`
- `POST /api/v1/get_plan_revision`
  - body: `PlanRevisionSelector`
  - response: `FeaturePlanRevision`

### Publication and Frontier Endpoints

- `POST /api/v1/record_feature_publication`
  - body: `RecordFeaturePublicationRequest`
  - response: `FeaturePublication`
- `POST /api/v1/get_feature_publication`
  - body: `FeaturePublicationSelector`
  - response: `FeaturePublication`
- `POST /api/v1/promote_serving_frontier`
  - body: `PromoteServingFrontierRequest`
  - response: `ServingFrontier`
- `POST /api/v1/rollback_serving_frontier`
  - body: `RollbackServingFrontierRequest`
  - response: `ServingFrontier`
- `POST /api/v1/get_serving_frontier`
  - body: `ServingFrontierSelector`
  - response: `ServingFrontier`

### Dataset and Model Endpoints

- `POST /api/v1/create_training_dataset_record`
  - body: `CreateTrainingDatasetRecordRequest`
  - response: `TrainingDatasetRecord`
- `POST /api/v1/get_training_dataset_record`
  - body: `TrainingDatasetSelector`
  - response: `TrainingDatasetRecord`
- `POST /api/v1/create_model_binding`
  - body: `CreateModelBindingRequest`
  - response: `ModelBinding`
- `POST /api/v1/get_model_binding`
  - body: `ModelBindingSelector`
  - response: `ModelBinding`

### Validation and Resolution Endpoints

- `POST /api/v1/create_validation_run`
  - body: `CreateValidationRunRequest`
  - response: `ValidationRun`
- `POST /api/v1/get_validation_run`
  - body: `ValidationRunSelector`
  - response: `ValidationRun`
- `POST /api/v1/resolve_feature_service_revisions`
  - body: `ResolveFeatureServiceRevisionsRequest`
  - response: `ResolveFeatureServiceRevisionsResponse`

## Handler Contract

The new handlers should use the existing `body_handler_json!` macro in
[`service/src/v1_handlers.rs`](/Users/gregory/code/powdrr-engine/.worktrees/codex-feast-metadata-service-proposal/service/src/v1_handlers.rs:1).

Representative examples:

```rust
body_handler_json! { upsert_feature_spec(input: UpsertFeatureSpecRequest) -> GenericResponse {
    handle_result(SERVICE_IMPL.upsert_feature_spec(&input).await)
}}

body_handler_json! { get_feature_spec(input: FeatureSpecSelector) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.get_feature_spec(&input).await)
}}

body_handler_json! { compile_feature_spec(input: CompileFeatureSpecRequest) -> GenericResponse {
    handle_result(SERVICE_IMPL.compile_feature_spec(&input).await)
}}

body_handler_json! { record_feature_publication(input: RecordFeaturePublicationRequest) -> GenericResponse {
    handle_result(SERVICE_IMPL.record_feature_publication(&input).await)
}}

body_handler_json! { get_serving_frontier(input: ServingFrontierSelector) -> GenericResponse {
    handle_result_option(SERVICE_IMPL.get_serving_frontier(&input).await)
}}
```

## Router Contract

Add the following routes to
[`service/src/router.rs`](/Users/gregory/code/powdrr-engine/.worktrees/codex-feast-metadata-service-proposal/service/src/router.rs:1)
inside the existing `/api/v1` scope:

```rust
route.post("/upsert_feature_spec").to(v1_handlers::upsert_feature_spec);
route.post("/get_feature_spec").to(v1_handlers::get_feature_spec);
route.post("/compile_feature_spec").to(v1_handlers::compile_feature_spec);
route.post("/upsert_source_contract").to(v1_handlers::upsert_source_contract);
route.post("/get_source_contract").to(v1_handlers::get_source_contract);
route.post("/get_feature_revision").to(v1_handlers::get_feature_revision);
route.post("/get_plan_revision").to(v1_handlers::get_plan_revision);
route.post("/record_feature_publication").to(v1_handlers::record_feature_publication);
route.post("/get_feature_publication").to(v1_handlers::get_feature_publication);
route.post("/promote_serving_frontier").to(v1_handlers::promote_serving_frontier);
route.post("/rollback_serving_frontier").to(v1_handlers::rollback_serving_frontier);
route.post("/get_serving_frontier").to(v1_handlers::get_serving_frontier);
route.post("/create_training_dataset_record").to(v1_handlers::create_training_dataset_record);
route.post("/get_training_dataset_record").to(v1_handlers::get_training_dataset_record);
route.post("/create_model_binding").to(v1_handlers::create_model_binding);
route.post("/get_model_binding").to(v1_handlers::get_model_binding);
route.post("/create_validation_run").to(v1_handlers::create_validation_run);
route.post("/get_validation_run").to(v1_handlers::get_validation_run);
route.post("/resolve_feature_service_revisions").to(v1_handlers::resolve_feature_service_revisions);
```

## Return Semantics

Use the same response conventions as the existing service:

- `handle_result(...)` for successful writes and successful lookup results
- `handle_result_option(...)` for selectors that may miss
- `200 OK` with JSON for successful hits
- `404 Not Found` for selector misses
- `503 Service Unavailable` for storage or leadership failures

Do not introduce a second response envelope for this first cut.

## Storage Expectations for Implementors

Each backend implementation should provide durable storage keyed by stable ids.

Minimum keys:

- feature spec: `project + feature_view_name`
- source contract: `project + source_name`
- feature revision: `project + feature_revision_id`
- plan revision: `project + plan_revision_id`
- publication: `project + publication_id`
- frontier: `project + scope_type + scope_name`
- training dataset: `project + training_dataset_id`
- model binding: `project + model_name + model_version`
- validation run: `project + validation_run_id`

Backends must not infer these objects from Iceberg alone.

## First PR Slice

The smallest reviewable implementation slice for this contract is:

1. Add `control_plane/src/feature_metadata.rs` with:
   - `FeatureViewRef`
   - `PowdrrSourceContract`
   - `PowdrrFeatureSpec`
   - `FeatureDefinitionRevision`
   - `FeatureSpecSelector`
   - `SourceContractSelector`
   - `UpsertFeatureSpecRequest`
   - `UpsertFeatureSpecResponse`
   - `CompileFeatureSpecRequest`
   - `CompileFeatureSpecResponse`
2. Add `service_lib/src/feature_metadata_store.rs` with:
   - `upsert_feature_spec`
   - `get_feature_spec`
   - `compile_feature_spec`
   - `upsert_source_contract`
   - `get_source_contract`
3. Add the matching `/api/v1/...` routes and handlers.
4. Add in-memory support in `EphemeralServiceImpl`.

Only after that should the implementation grow:

- publications
- frontiers
- datasets
- model bindings
- validation runs

## Recommendation

Implement this as a new `feature_metadata` module and a new
`FeatureMetadataStore` trait, not as ad hoc additions to the checkpoint
metadata path.

That keeps:

- the `control_plane` contract readable
- the service interface explicit
- the runtime checkpoint model separate from the feature authoring model
- the eventual Feast integration layered cleanly on top of Powdrr metadata
