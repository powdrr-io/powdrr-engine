use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::pin::Pin;
use std::sync::{LazyLock, Mutex};

use futures::FutureExt;
use futures::stream::{self, StreamExt};
use gotham::handler::HandlerFuture;
use gotham::helpers::http::response::create_response;
use gotham::hyper::{Body, body};
use gotham::mime;
use gotham::state::{FromState, State};
use http::StatusCode;
use idgenerator::IdInstance;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::data_access::{self, execute_sql_async};
use crate::data_contract::{
    CreateTable, ExtensionFile, FileDescriptor, IcebergAccessArtifact, IcebergFileStats,
    IcebergPartitionField, IcebergRowGroupStats, IcebergSortField, ServingAggregateMeasure,
    ServingAggregateSpec, ServingPattern, ServingTableConfig, TableDescription,
    TableMetadataCheckpoint,
};
use crate::elastic_search_endpoints::NamePathExtractor;
use crate::peers::CheckpointDescriptor;
use crate::prefetch::warm_iceberg_checkpoints;
use crate::query_execution::{
    QueryExecutionPlan, QueryInputFile, QuerySqlTemplate, QueryStorageKind,
    execute_query_plan_batches, group_query_input_files_by_schema,
};
use crate::query_path::{
    QueryPredicate, column_is_all_null, compare_scalar_values, file_may_match_predicates,
    group_files_by_schema, row_group_may_match_predicates,
};
use crate::schema_massager::{PowdrrDataType, PowdrrSchema};
use crate::search_runtime::batches_to_serde_value;
use crate::serving_plan::{ServingPredicate, ServingQueryClassification, ServingRequestPlan};
use crate::state_provider::STATE_PROVIDER;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const MAX_IN_VALUES: usize = 32;
const MAX_WARMUP_FILE_GROUPS_PER_PATTERN: usize = 2;
const MAX_WARMUP_FILES: usize = 8;
const ACCESS_ARTIFACT_KIND_BLOOM_FILTER: &str = "bloom-filter";
const ACCESS_ARTIFACT_KIND_EXACT_INDEX: &str = "exact-index";
const ACCESS_ARTIFACT_KIND_EXACT_PRUNING: &str = "exact-pruning";
const ACCESS_ARTIFACT_KIND_FILE_STATS: &str = "file-stats";
const ACCESS_ARTIFACT_KIND_PAGE_INDEX: &str = "page-index";
const ACCESS_ARTIFACT_KIND_PARTITION_SPEC: &str = "partition-spec";
const ACCESS_ARTIFACT_KIND_ROW_GROUP_STATS: &str = "row-group-stats";
const ACCESS_ARTIFACT_KIND_SECONDARY_PATTERN: &str = "secondary-pattern";
const ACCESS_ARTIFACT_KIND_SORT_ORDER: &str = "sort-order";
const DEFAULT_FAST_PATH_MAX_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_FAST_PATH_MAX_FILES: usize = 32;
const DEFAULT_FAST_PATH_MAX_ROW_GROUPS: usize = 128;
const DEFAULT_FAST_PATH_MAX_DELETE_FILES: usize = 8;
const DEFAULT_SLOW_PATH_MAX_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_SLOW_PATH_MAX_FILES: usize = 128;
const DEFAULT_SLOW_PATH_MAX_ROW_GROUPS: usize = 512;
const DEFAULT_SLOW_PATH_MAX_DELETE_FILES: usize = 64;

static EXACT_PRUNING_SUMMARY_CACHE: LazyLock<Mutex<HashMap<String, ExactPruningSummary>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServingQueryResponse {
    pub table: String,
    pub classification: ServingQueryClassification,
    #[serde(default)]
    pub matched_pattern: Option<String>,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    pub files_considered: usize,
    pub files_selected: usize,
    #[serde(default)]
    pub row_groups_considered: usize,
    #[serde(default)]
    pub row_groups_selected: usize,
    #[serde(default)]
    pub delete_files_considered: usize,
    pub estimated_bytes: u64,
    #[serde(default)]
    pub partition_fields_available: usize,
    #[serde(default)]
    pub sort_fields_available: usize,
    #[serde(default)]
    pub declared_sort_order_match: bool,
    #[serde(default)]
    pub metadata_snapshot_cached: bool,
    #[serde(default)]
    pub metadata_files_cached: usize,
    #[serde(default)]
    pub metadata_row_groups_cached: usize,
    #[serde(default)]
    pub page_index_row_groups_selected: usize,
    #[serde(default)]
    pub bloom_filter_row_groups_selected: usize,
    #[serde(default)]
    pub artifacts_considered: Vec<String>,
    #[serde(default)]
    pub artifacts_used: Vec<String>,
    #[serde(default)]
    pub bulk_cache: data_access::ServingBulkCacheStats,
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub rows: Vec<Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServingCacheManagerRequestBody {
    #[serde(default)]
    pub warm_targets: Vec<ServingCacheWarmTargetBody>,
    #[serde(default)]
    pub evict_targets: Vec<ServingCacheEvictTargetBody>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServingCacheWarmTargetBody {
    Metadata,
    Pattern {
        pattern: String,
    },
    Files {
        files: Vec<String>,
    },
    Range {
        field: String,
        #[serde(default)]
        gt: Option<Value>,
        #[serde(default)]
        gte: Option<Value>,
        #[serde(default)]
        lt: Option<Value>,
        #[serde(default)]
        lte: Option<Value>,
        #[serde(default)]
        order_descending: bool,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        projection: Option<Vec<String>>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServingCacheEvictTargetBody {
    Files { files: Vec<String> },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServingCacheManagerResponse {
    pub acknowledged: bool,
    pub table: String,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub matched_patterns: Vec<String>,
    #[serde(default)]
    pub matched_artifacts: Vec<String>,
    pub warmed_files: usize,
    pub evicted_files: usize,
    pub estimated_warm_bytes: u64,
    #[serde(default)]
    pub targeted_ranges: usize,
    #[serde(default)]
    pub metadata_refreshed: bool,
    #[serde(default)]
    pub bulk_cache_flushed: bool,
    #[serde(default)]
    pub bulk_cache_reset: bool,
    #[serde(default)]
    pub bulk_cache: data_access::ServingBulkCacheStats,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServingLayoutPatternAdvice {
    pub pattern: String,
    #[serde(default)]
    pub missing_identity_partition_eq_fields: Vec<String>,
    #[serde(default)]
    pub declared_sort_order_match: bool,
    #[serde(default)]
    pub exact_artifact_fields_missing: Vec<String>,
    #[serde(default)]
    pub recommendation: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServingLayoutAdviceResponse {
    pub table: String,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub identity_partition_fields: Vec<String>,
    #[serde(default)]
    pub declared_sort_order_fields: Vec<String>,
    #[serde(default)]
    pub exact_artifact_fields: Vec<String>,
    #[serde(default)]
    pub issues: Vec<String>,
    #[serde(default)]
    pub recommendations: Vec<String>,
    #[serde(default)]
    pub patterns: Vec<ServingLayoutPatternAdvice>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ServingConfigResponse {
    pub acknowledged: bool,
    pub table: String,
    pub serving: ServingTableConfig,
}

struct ServingExecutionContext {
    description: TableDescription,
    checkpoint: CheckpointDescriptor,
    schema: PowdrrSchema,
    files: Vec<FileDescriptor>,
    speedboat_files: Vec<FileDescriptor>,
    delete_files: Vec<String>,
    file_stats: HashMap<String, IcebergFileStats>,
    partition_spec: Vec<IcebergPartitionField>,
    sort_order: Vec<IcebergSortField>,
    access_artifacts: Vec<IcebergAccessArtifact>,
    extension_files: HashMap<String, Vec<ExtensionFile>>,
    exact_artifact_fields: Vec<String>,
    snapshot_id: Option<String>,
    metadata_snapshot_cached: bool,
}

struct ServingPlan {
    classification: ServingQueryClassification,
    matched_pattern: Option<String>,
    reason: Option<String>,
    limit: usize,
    sql: String,
    selected_files: Vec<FileDescriptor>,
    files_considered: usize,
    files_selected: usize,
    row_groups_considered: usize,
    row_groups_selected: usize,
    delete_files_considered: usize,
    estimated_bytes: u64,
    partition_fields_available: usize,
    sort_fields_available: usize,
    declared_sort_order_match: bool,
    metadata_snapshot_cached: bool,
    metadata_files_cached: usize,
    metadata_row_groups_cached: usize,
    page_index_row_groups_selected: usize,
    bloom_filter_row_groups_selected: usize,
    artifacts_considered: Vec<String>,
    artifacts_used: Vec<String>,
}

#[derive(Clone, Debug)]
struct AggregateMeasurePlan {
    function: AggregateFunction,
    field: Option<String>,
    alias: String,
    partial_value_alias: String,
    partial_count_alias: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Debug, Default)]
struct ServingExecutionResult {
    rows: Vec<Value>,
    files_selected: usize,
    estimated_bytes: u64,
    artifacts_used: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ServingWarmupStep {
    pub pattern_name: String,
    pub request: ServingRequestPlan,
    pub selected_files: Vec<FileDescriptor>,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ServingWarmupPlan {
    pub matched_patterns: Vec<String>,
    pub selected_files: Vec<FileDescriptor>,
    pub estimated_bytes: u64,
    pub steps: Vec<ServingWarmupStep>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ServingCacheRangeTarget {
    pub field: String,
    pub gt: Option<Value>,
    pub gte: Option<Value>,
    pub lt: Option<Value>,
    pub lte: Option<Value>,
    pub order_descending: bool,
    pub limit: Option<usize>,
    pub projection: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ServingCacheWarmTarget {
    Metadata,
    Pattern(String),
    Files(Vec<String>),
    Range(ServingCacheRangeTarget),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ServingCacheEvictTarget {
    Files(Vec<String>),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ServingCacheManagerRequest {
    pub warm_targets: Vec<ServingCacheWarmTarget>,
    pub evict_targets: Vec<ServingCacheEvictTarget>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ServingCacheManagerPlan {
    pub matched_patterns: Vec<String>,
    pub matched_artifacts: Vec<String>,
    pub warm_files: Vec<FileDescriptor>,
    pub estimated_warm_bytes: u64,
    pub warmup_steps: Vec<ServingWarmupStep>,
    pub files_to_evict: Vec<String>,
    pub targeted_ranges: usize,
    pub metadata_refreshed: bool,
    pub bulk_cache_reset: bool,
}

#[derive(Clone, Debug)]
struct OrderedFileGroup {
    files: Vec<FileDescriptor>,
    best_case_sort_value: Value,
}

#[derive(Clone, Debug, Default)]
struct PrunedFileSelection {
    selected_files: Vec<FileDescriptor>,
    files_selected: usize,
    row_groups_considered: usize,
    row_groups_selected: usize,
    estimated_bytes: u64,
    artifacts_used: Vec<String>,
    metadata_files_cached: usize,
    metadata_row_groups_cached: usize,
    page_index_row_groups_selected: usize,
    bloom_filter_row_groups_selected: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ExactPruningFieldSummary {
    complete: bool,
    values: HashSet<String>,
}

type ExactPruningSummary = HashMap<String, ExactPruningFieldSummary>;

pub fn get_serving_config(state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state);
        match STATE_PROVIDER.describe_table(&path.name).await {
            Ok(Some(description)) => match description.serving {
                Some(serving) => {
                    let response = json_response(
                        &state,
                        StatusCode::OK,
                        &ServingConfigResponse {
                            acknowledged: true,
                            table: description.name,
                            serving,
                        },
                    );
                    Ok((state, response))
                }
                None => {
                    let response = json_response(
                        &state,
                        StatusCode::NOT_FOUND,
                        &json_error("No serving config declared for table"),
                    );
                    Ok((state, response))
                }
            },
            Ok(None) => {
                let response = json_response(
                    &state,
                    StatusCode::NOT_FOUND,
                    &json_error("Table not found"),
                );
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn put_serving_config(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let body = match parse_json_body::<ServingTableConfig>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response =
                    json_response(&state, StatusCode::BAD_REQUEST, &json_error(&message));
                return Ok((state, response));
            }
        };

        let (tags, dynamodb, mongodb) = match STATE_PROVIDER.describe_table(&path).await {
            Ok(Some(description)) => (description.tags, description.dynamodb, description.mongodb),
            Ok(None) => (HashMap::new(), None, None),
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                return Ok((state, response));
            }
        };

        let request = CreateTable {
            name: path.clone(),
            tags,
            serving: Some(body.clone()),
            dynamodb,
            mongodb,
        };

        match STATE_PROVIDER.upsert_table_metadata(&request).await {
            Ok(_) => {
                let response = json_response(
                    &state,
                    StatusCode::OK,
                    &ServingConfigResponse {
                        acknowledged: true,
                        table: path,
                        serving: body,
                    },
                );
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(
                    &state,
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json_error(&error.to_string()),
                );
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn serve_query(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let request = match parse_json_body::<ServingRequestPlan>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response =
                    json_response(&state, StatusCode::BAD_REQUEST, &json_error(&message));
                return Ok((state, response));
            }
        };

        match execute_serving_query(&path, request).await {
            Ok(response) => {
                let status = match response.classification {
                    ServingQueryClassification::FastPath => StatusCode::OK,
                    ServingQueryClassification::SlowPath => StatusCode::OK,
                    ServingQueryClassification::Rejected => StatusCode::UNPROCESSABLE_ENTITY,
                };
                let response = json_response(&state, status, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(&state, error.status, &json_error(&error.message));
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn manage_serving_cache(mut state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        let request = match parse_json_body::<ServingCacheManagerRequestBody>(&mut state).await {
            Ok(body) => body,
            Err(message) => {
                let response =
                    json_response(&state, StatusCode::BAD_REQUEST, &json_error(&message));
                return Ok((state, response));
            }
        };

        match execute_serving_cache_manager_request(&path, request).await {
            Ok(response) => {
                let response = json_response(&state, StatusCode::OK, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(&state, error.status, &json_error(&error.message));
                Ok((state, response))
            }
        }
    }
    .boxed()
}

pub fn get_serving_layout_advice(state: State) -> Pin<Box<HandlerFuture>> {
    async move {
        let path = NamePathExtractor::borrow_from(&state).name.clone();
        match execute_serving_layout_advice(&path).await {
            Ok(response) => {
                let response = json_response(&state, StatusCode::OK, &response);
                Ok((state, response))
            }
            Err(error) => {
                let response = json_response(&state, error.status, &json_error(&error.message));
                Ok((state, response))
            }
        }
    }
    .boxed()
}

async fn execute_serving_cache_manager_request(
    table_name: &str,
    request: ServingCacheManagerRequestBody,
) -> Result<ServingCacheManagerResponse, ServingQueryError> {
    let context = load_serving_context(table_name).await?;
    let internal_request = into_serving_cache_manager_request(request);
    let plan = build_serving_cache_manager_plan(
        &internal_request,
        &context.description.serving.clone().unwrap_or_default(),
        &context.files,
        &context.file_stats,
        &context.sort_order,
        &context.access_artifacts,
    );
    if plan.metadata_refreshed {
        warm_iceberg_checkpoints(&vec![context.checkpoint.clone()])
            .await
            .map_err(|error| {
                ServingQueryError::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    &format!("Unable to warm iceberg metadata: {error}"),
                )
            })?;
    }
    execute_serving_cache_manager_plan(&plan, &context.delete_files).await?;
    data_access::flush_serving_bulk_cache()
        .await
        .map_err(|message| ServingQueryError::new(StatusCode::SERVICE_UNAVAILABLE, &message))?;
    data_access::record_serving_cache_manager_operation(
        data_access::ServingCacheManagerOperationStats {
            table: context.description.name.clone(),
            snapshot_id: context.snapshot_id.clone(),
            warmed_files: plan.warm_files.len(),
            evicted_files: plan.files_to_evict.len(),
            targeted_ranges: plan.targeted_ranges,
            matched_patterns: plan.matched_patterns.clone(),
            matched_artifacts: plan.matched_artifacts.clone(),
            metadata_refreshed: plan.metadata_refreshed,
            bulk_cache_flushed: true,
            bulk_cache_reset: plan.bulk_cache_reset,
        },
    );

    Ok(ServingCacheManagerResponse {
        acknowledged: true,
        table: context.description.name,
        snapshot_id: context.snapshot_id,
        matched_patterns: plan.matched_patterns,
        matched_artifacts: plan.matched_artifacts,
        warmed_files: plan.warm_files.len(),
        evicted_files: plan.files_to_evict.len(),
        estimated_warm_bytes: plan.estimated_warm_bytes,
        targeted_ranges: plan.targeted_ranges,
        metadata_refreshed: plan.metadata_refreshed,
        bulk_cache_flushed: true,
        bulk_cache_reset: plan.bulk_cache_reset,
        bulk_cache: data_access::serving_bulk_cache_stats(),
    })
}

async fn execute_serving_layout_advice(
    table_name: &str,
) -> Result<ServingLayoutAdviceResponse, ServingQueryError> {
    let context = load_serving_context(table_name).await?;
    Ok(build_serving_layout_advice(&context))
}

fn into_serving_cache_manager_request(
    request: ServingCacheManagerRequestBody,
) -> ServingCacheManagerRequest {
    ServingCacheManagerRequest {
        warm_targets: request
            .warm_targets
            .into_iter()
            .map(|target| match target {
                ServingCacheWarmTargetBody::Metadata => ServingCacheWarmTarget::Metadata,
                ServingCacheWarmTargetBody::Pattern { pattern } => {
                    ServingCacheWarmTarget::Pattern(pattern)
                }
                ServingCacheWarmTargetBody::Files { files } => ServingCacheWarmTarget::Files(files),
                ServingCacheWarmTargetBody::Range {
                    field,
                    gt,
                    gte,
                    lt,
                    lte,
                    order_descending,
                    limit,
                    projection,
                } => ServingCacheWarmTarget::Range(ServingCacheRangeTarget {
                    field,
                    gt,
                    gte,
                    lt,
                    lte,
                    order_descending,
                    limit,
                    projection,
                }),
            })
            .collect(),
        evict_targets: request
            .evict_targets
            .into_iter()
            .map(|target| match target {
                ServingCacheEvictTargetBody::Files { files } => {
                    ServingCacheEvictTarget::Files(files)
                }
            })
            .collect(),
    }
}

fn build_serving_layout_advice(context: &ServingExecutionContext) -> ServingLayoutAdviceResponse {
    let serving = context.description.serving.clone().unwrap_or_default();
    let mut identity_partition_fields = identity_partition_fields(&context.partition_spec)
        .into_iter()
        .collect::<Vec<_>>();
    identity_partition_fields.sort();
    let mut declared_sort_order_fields = context
        .sort_order
        .iter()
        .map(|field| field.source_field_name.clone())
        .collect::<Vec<_>>();
    declared_sort_order_fields.sort();
    let exact_artifact_fields = if context
        .access_artifacts
        .iter()
        .any(|artifact| artifact.name == ACCESS_ARTIFACT_KIND_EXACT_PRUNING)
    {
        context.exact_artifact_fields.clone()
    } else {
        vec![]
    };
    let mut issues = vec![];
    let mut recommendations = vec![];
    let identity_partition_field_set = identity_partition_fields
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let mut patterns = vec![];

    for pattern in serving.patterns.iter() {
        let missing_identity_partition_eq_fields = pattern
            .eq_fields
            .iter()
            .filter(|field| !identity_partition_field_set.contains(*field))
            .cloned()
            .collect::<Vec<_>>();
        let declared_sort_order_match = pattern
            .order_field
            .as_ref()
            .map(|field| sort_order_supports_field(&context.sort_order, field))
            .unwrap_or(true);
        let exact_artifact_fields_missing = if exact_artifact_fields.is_empty() {
            pattern.eq_fields.clone()
        } else {
            pattern
                .eq_fields
                .iter()
                .filter(|field| !exact_artifact_fields.contains(*field))
                .cloned()
                .collect::<Vec<_>>()
        };
        let secondary_artifact_available = context
            .access_artifacts
            .iter()
            .any(|artifact| artifact.name == secondary_pattern_artifact_name(&pattern.name));
        let recommendation = if !missing_identity_partition_eq_fields.is_empty() {
            Some(format!(
                "Cluster or partition files on {} for pattern {}",
                missing_identity_partition_eq_fields.join(", "),
                pattern.name
            ))
        } else if !declared_sort_order_match {
            pattern.order_field.as_ref().map(|field| {
                format!(
                    "Rewrite files with Iceberg sort order on {} for pattern {}",
                    field, pattern.name
                )
            })
        } else if pattern.aggregate.is_some() {
            Some(format!(
                "Build aggregate fragments or bitmap rollups for pattern {}",
                pattern.name
            ))
        } else if pattern_is_secondary(pattern) && !secondary_artifact_available {
            Some(format!(
                "Build a declared secondary serving artifact for pattern {}",
                pattern.name
            ))
        } else if !exact_artifact_fields_missing.is_empty() {
            Some(format!(
                "Publish exact_pruning/exact_index sidecars for {}",
                exact_artifact_fields_missing.join(", ")
            ))
        } else {
            None
        };
        if let Some(recommendation) = recommendation.clone() {
            issues.push(format!(
                "Pattern {} is not fully layout-aligned",
                pattern.name
            ));
            recommendations.push(recommendation.clone());
        }
        patterns.push(ServingLayoutPatternAdvice {
            pattern: pattern.name.clone(),
            missing_identity_partition_eq_fields,
            declared_sort_order_match,
            exact_artifact_fields_missing,
            recommendation,
        });
    }

    issues.sort();
    recommendations.sort();
    recommendations.dedup();

    ServingLayoutAdviceResponse {
        table: context.description.name.clone(),
        snapshot_id: context.snapshot_id.clone(),
        identity_partition_fields,
        declared_sort_order_fields,
        exact_artifact_fields,
        issues,
        recommendations,
        patterns,
    }
}

pub async fn execute_serving_query(
    table_name: &str,
    request: ServingRequestPlan,
) -> Result<ServingQueryResponse, ServingQueryError> {
    let context = load_serving_context(table_name).await?;
    validate_request(&request, &context.schema)?;
    let bulk_cache = data_access::serving_bulk_cache_stats();

    let plan = plan_request(&context, &request)?;
    if request.explain {
        return Ok(ServingQueryResponse {
            table: context.description.name,
            classification: plan.classification,
            matched_pattern: plan.matched_pattern,
            snapshot_id: context.snapshot_id,
            reason: plan.reason,
            files_considered: plan.files_considered,
            files_selected: plan.files_selected,
            row_groups_considered: plan.row_groups_considered,
            row_groups_selected: plan.row_groups_selected,
            delete_files_considered: plan.delete_files_considered,
            estimated_bytes: plan.estimated_bytes,
            partition_fields_available: plan.partition_fields_available,
            sort_fields_available: plan.sort_fields_available,
            declared_sort_order_match: plan.declared_sort_order_match,
            metadata_snapshot_cached: plan.metadata_snapshot_cached,
            metadata_files_cached: plan.metadata_files_cached,
            metadata_row_groups_cached: plan.metadata_row_groups_cached,
            page_index_row_groups_selected: plan.page_index_row_groups_selected,
            bloom_filter_row_groups_selected: plan.bloom_filter_row_groups_selected,
            artifacts_considered: plan.artifacts_considered,
            artifacts_used: plan.artifacts_used,
            bulk_cache,
            sql: Some(plan.sql),
            rows: vec![],
        });
    }

    if plan.classification == ServingQueryClassification::Rejected {
        return Ok(ServingQueryResponse {
            table: context.description.name,
            classification: plan.classification,
            matched_pattern: plan.matched_pattern,
            snapshot_id: context.snapshot_id,
            reason: plan.reason,
            files_considered: plan.files_considered,
            files_selected: plan.files_selected,
            row_groups_considered: plan.row_groups_considered,
            row_groups_selected: plan.row_groups_selected,
            delete_files_considered: plan.delete_files_considered,
            estimated_bytes: plan.estimated_bytes,
            partition_fields_available: plan.partition_fields_available,
            sort_fields_available: plan.sort_fields_available,
            declared_sort_order_match: plan.declared_sort_order_match,
            metadata_snapshot_cached: plan.metadata_snapshot_cached,
            metadata_files_cached: plan.metadata_files_cached,
            metadata_row_groups_cached: plan.metadata_row_groups_cached,
            page_index_row_groups_selected: plan.page_index_row_groups_selected,
            bloom_filter_row_groups_selected: plan.bloom_filter_row_groups_selected,
            artifacts_considered: plan.artifacts_considered,
            artifacts_used: plan.artifacts_used,
            bulk_cache,
            sql: Some(plan.sql),
            rows: vec![],
        });
    }

    if plan.classification == ServingQueryClassification::SlowPath && !request.allow_slow_path {
        return Ok(ServingQueryResponse {
            table: context.description.name,
            classification: plan.classification,
            matched_pattern: plan.matched_pattern,
            snapshot_id: context.snapshot_id,
            reason: Some(
                "Query requires slow path. Set allow_slow_path=true to execute it".to_string(),
            ),
            files_considered: plan.files_considered,
            files_selected: plan.files_selected,
            row_groups_considered: plan.row_groups_considered,
            row_groups_selected: plan.row_groups_selected,
            delete_files_considered: plan.delete_files_considered,
            estimated_bytes: plan.estimated_bytes,
            partition_fields_available: plan.partition_fields_available,
            sort_fields_available: plan.sort_fields_available,
            declared_sort_order_match: plan.declared_sort_order_match,
            metadata_snapshot_cached: plan.metadata_snapshot_cached,
            metadata_files_cached: plan.metadata_files_cached,
            metadata_row_groups_cached: plan.metadata_row_groups_cached,
            page_index_row_groups_selected: plan.page_index_row_groups_selected,
            bloom_filter_row_groups_selected: plan.bloom_filter_row_groups_selected,
            artifacts_considered: plan.artifacts_considered,
            artifacts_used: plan.artifacts_used,
            bulk_cache,
            sql: Some(plan.sql),
            rows: vec![],
        });
    }

    let mut execution = execute_plan(
        &plan.selected_files,
        &context.speedboat_files,
        &context.delete_files,
        &context.file_stats,
        &context.extension_files,
        &context.sort_order,
        &request,
        &plan.sql,
        plan.limit,
    )
    .await?;
    if request.order_by.is_empty() {
        execution.rows.truncate(plan.limit);
    }
    let bulk_cache = data_access::serving_bulk_cache_stats();
    let mut artifacts_used = plan.artifacts_used.clone();
    for artifact in execution.artifacts_used.iter().cloned() {
        push_unique_string(&mut artifacts_used, artifact);
    }

    Ok(ServingQueryResponse {
        table: context.description.name,
        classification: plan.classification,
        matched_pattern: plan.matched_pattern,
        snapshot_id: context.snapshot_id,
        reason: plan.reason,
        files_considered: plan.files_considered,
        files_selected: execution.files_selected,
        row_groups_considered: plan.row_groups_considered,
        row_groups_selected: plan.row_groups_selected,
        delete_files_considered: plan.delete_files_considered,
        estimated_bytes: execution.estimated_bytes,
        partition_fields_available: plan.partition_fields_available,
        sort_fields_available: plan.sort_fields_available,
        declared_sort_order_match: plan.declared_sort_order_match,
        metadata_snapshot_cached: plan.metadata_snapshot_cached,
        metadata_files_cached: plan.metadata_files_cached,
        metadata_row_groups_cached: plan.metadata_row_groups_cached,
        page_index_row_groups_selected: plan.page_index_row_groups_selected,
        bloom_filter_row_groups_selected: plan.bloom_filter_row_groups_selected,
        artifacts_considered: plan.artifacts_considered,
        artifacts_used,
        bulk_cache,
        sql: Some(plan.sql),
        rows: execution.rows,
    })
}

async fn load_serving_context(
    table_name: &str,
) -> Result<ServingExecutionContext, ServingQueryError> {
    let description = match STATE_PROVIDER.describe_table(&table_name.to_string()).await {
        Ok(Some(description)) => description,
        Ok(None) => {
            return Err(ServingQueryError::new(
                StatusCode::NOT_FOUND,
                "Table not found",
            ));
        }
        Err(error) => {
            return Err(ServingQueryError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                &error.to_string(),
            ));
        }
    };

    let checkpoint_id = match STATE_PROVIDER
        .get_published_active_servable_checkpoint(&description.name)
        .await
    {
        Ok(Some(checkpoint_id)) => checkpoint_id,
        Ok(None) => {
            return Err(ServingQueryError::new(
                StatusCode::NOT_FOUND,
                "No checkpoint available for table",
            ));
        }
        Err(error) => {
            return Err(ServingQueryError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                &error.to_string(),
            ));
        }
    };

    let checkpoint = match STATE_PROVIDER
        .get_checkpoint(CheckpointDescriptor::new(
            description.name.clone(),
            checkpoint_id.clone(),
        ))
        .await
    {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => {
            return Err(ServingQueryError::new(
                StatusCode::NOT_FOUND,
                "Checkpoint metadata was not found",
            ));
        }
        Err(error) => {
            return Err(ServingQueryError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                &error.to_string(),
            ));
        }
    };

    let iceberg_metadata = match checkpoint.iceberg_metadata.clone() {
        Some(metadata) => metadata,
        None => {
            return Err(ServingQueryError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Serving queries currently require Iceberg-backed storage",
            ));
        }
    };

    let files = iceberg_metadata.files.as_file_tuples();
    let speedboat_files = checkpoint
        .speedboat_metadata
        .as_ref()
        .map(|metadata| metadata.files.as_file_tuples())
        .unwrap_or_default();
    let delete_files = checkpoint
        .deletes_metadata
        .as_ref()
        .map(|metadata| metadata.files.clone())
        .unwrap_or_default();
    let file_stats = iceberg_metadata
        .file_stats
        .iter()
        .cloned()
        .map(|stats| (stats.file_path.clone(), stats))
        .collect();
    let extension_files = flatten_extension_files(&checkpoint);
    let exact_artifact_fields = exact_artifact_fields(&iceberg_metadata.table_schema);
    let mut access_artifacts = iceberg_metadata.access_artifacts.clone();
    let sort_order = iceberg_metadata.sort_order.clone();
    let serving = description.serving.clone().unwrap_or_default();
    let mut files_for_artifacts = files.clone();
    files_for_artifacts.extend(speedboat_files.clone());
    append_exact_sidecar_artifacts(
        &mut access_artifacts,
        &files_for_artifacts,
        &extension_files,
        &exact_artifact_fields,
    );
    append_secondary_pattern_artifacts(&mut access_artifacts, &serving, &sort_order);
    let metadata_snapshot_cached = iceberg_metadata
        .snapshot_id
        .as_ref()
        .and_then(|snapshot_id| snapshot_id.parse::<i64>().ok())
        .map(|snapshot_id| {
            data_access::iceberg_table_metadata_cache_contains("default", table_name, snapshot_id)
        })
        .unwrap_or(false);
    Ok(ServingExecutionContext {
        description,
        checkpoint: CheckpointDescriptor::new(table_name.to_string(), checkpoint_id),
        schema: iceberg_metadata.table_schema.clone(),
        snapshot_id: iceberg_metadata.snapshot_id.clone(),
        files,
        speedboat_files,
        delete_files,
        file_stats,
        partition_spec: iceberg_metadata.partition_spec.clone(),
        sort_order,
        access_artifacts,
        extension_files,
        exact_artifact_fields,
        metadata_snapshot_cached,
    })
}

fn plan_request(
    context: &ServingExecutionContext,
    request: &ServingRequestPlan,
) -> Result<ServingPlan, ServingQueryError> {
    let serving = context.description.serving.clone().unwrap_or_default();
    let limit = request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let sql = build_sql("{table}", request, limit, !context.delete_files.is_empty())?;
    let declared_sort_order_match = sort_order_matches_request(&context.sort_order, request);
    let matched_pattern = serving
        .patterns
        .iter()
        .find(|pattern| request_matches_pattern(request, pattern, limit))
        .map(|pattern| pattern.name.clone());
    let artifacts_considered = applicable_artifacts_for_request(
        request,
        &serving.patterns,
        &context.access_artifacts,
        declared_sort_order_match,
    );

    if !request_supported(request) {
        return Ok(ServingPlan {
            classification: ServingQueryClassification::Rejected,
            matched_pattern: None,
            reason: Some("Unsupported serving query shape".to_string()),
            limit,
            sql,
            selected_files: vec![],
            files_considered: context.files.len() + context.speedboat_files.len(),
            files_selected: 0,
            row_groups_considered: 0,
            row_groups_selected: 0,
            delete_files_considered: context.delete_files.len(),
            estimated_bytes: 0,
            partition_fields_available: context.partition_spec.len(),
            sort_fields_available: context.sort_order.len(),
            declared_sort_order_match,
            metadata_snapshot_cached: context.metadata_snapshot_cached,
            metadata_files_cached: 0,
            metadata_row_groups_cached: 0,
            page_index_row_groups_selected: 0,
            bloom_filter_row_groups_selected: 0,
            artifacts_considered,
            artifacts_used: vec![],
        });
    }

    let pruned = prune_candidate_files(&context.files, &context.file_stats, request);
    let mut artifacts_used = pruned.artifacts_used.clone();
    if declared_sort_order_match {
        for artifact in artifacts_considered
            .iter()
            .filter(|artifact| artifact.starts_with(ACCESS_ARTIFACT_KIND_SORT_ORDER))
        {
            push_unique_string(&mut artifacts_used, artifact.clone());
        }
    }
    let (classification, reason) = classify_request_with_admission(
        context,
        request,
        matched_pattern.as_deref(),
        declared_sort_order_match,
        &artifacts_considered,
        &artifacts_used,
        &pruned,
    );

    Ok(ServingPlan {
        classification,
        matched_pattern,
        reason,
        limit,
        sql,
        selected_files: pruned.selected_files.clone(),
        files_considered: context.files.len() + context.speedboat_files.len(),
        files_selected: pruned.files_selected + context.speedboat_files.len(),
        row_groups_considered: pruned.row_groups_considered,
        row_groups_selected: pruned.row_groups_selected,
        delete_files_considered: context.delete_files.len(),
        estimated_bytes: pruned.estimated_bytes
            + context
                .speedboat_files
                .iter()
                .map(|file| file.size)
                .sum::<u64>(),
        partition_fields_available: context.partition_spec.len(),
        sort_fields_available: context.sort_order.len(),
        declared_sort_order_match,
        metadata_snapshot_cached: context.metadata_snapshot_cached,
        metadata_files_cached: pruned.metadata_files_cached,
        metadata_row_groups_cached: pruned.metadata_row_groups_cached,
        page_index_row_groups_selected: pruned.page_index_row_groups_selected,
        bloom_filter_row_groups_selected: pruned.bloom_filter_row_groups_selected,
        artifacts_considered,
        artifacts_used,
    })
}

async fn execute_plan(
    files: &[FileDescriptor],
    speedboat_files: &[FileDescriptor],
    delete_files: &[String],
    file_stats: &HashMap<String, IcebergFileStats>,
    extension_files: &HashMap<String, Vec<ExtensionFile>>,
    sort_order: &[IcebergSortField],
    request: &ServingRequestPlan,
    sql: &str,
    limit: usize,
) -> Result<ServingExecutionResult, ServingQueryError> {
    if limit == 0 {
        return Ok(ServingExecutionResult::default());
    }
    let (files, mut execution_artifacts_used) =
        prune_execution_files_with_exact_artifact(files, extension_files, request).await?;
    let (speedboat_files, speedboat_artifacts_used) =
        prune_execution_files_with_exact_artifact(speedboat_files, extension_files, request)
            .await?;
    for artifact in speedboat_artifacts_used {
        push_unique_string(&mut execution_artifacts_used, artifact);
    }
    let execution_estimated_bytes = files
        .iter()
        .chain(speedboat_files.iter())
        .map(|file| file.size)
        .sum();

    if request.aggregate.is_some() {
        let rows =
            execute_aggregate_plan(&files, &speedboat_files, delete_files, request, limit).await?;
        return Ok(ServingExecutionResult {
            rows,
            files_selected: files.len() + speedboat_files.len(),
            estimated_bytes: execution_estimated_bytes,
            artifacts_used: execution_artifacts_used,
        });
    }

    if speedboat_files.is_empty() {
        if let Some(sort) = request.order_by.first() {
            if let Some(ordered_groups) = ordered_file_groups_for_top_k(
                &files,
                file_stats,
                sort_order,
                request,
                &sort.field,
                sort.descending,
            ) {
                let rows =
                    execute_ordered_top_k_plan(ordered_groups, delete_files, request, sql, limit)
                        .await?;
                return Ok(ServingExecutionResult {
                    rows,
                    files_selected: files.len(),
                    estimated_bytes: execution_estimated_bytes,
                    artifacts_used: execution_artifacts_used,
                });
            }
        }
    }

    let rows =
        execute_parallel_plan(&files, &speedboat_files, delete_files, request, sql, limit).await?;
    Ok(ServingExecutionResult {
        rows,
        files_selected: files.len() + speedboat_files.len(),
        estimated_bytes: execution_estimated_bytes,
        artifacts_used: execution_artifacts_used,
    })
}

async fn execute_parallel_plan(
    files: &[FileDescriptor],
    speedboat_files: &[FileDescriptor],
    delete_files: &[String],
    request: &ServingRequestPlan,
    sql: &str,
    limit: usize,
) -> Result<Vec<Value>, ServingQueryError> {
    let mut rows = vec![];
    let sql_template = sql.to_string();
    let query_input_groups = serving_query_input_groups(files, speedboat_files);
    let concurrency = query_input_groups
        .len()
        .clamp(1, serving_file_parallelism());
    let delete_files = delete_files.to_vec();
    let mut results = stream::iter(query_input_groups.into_iter().map(|query_files| {
        let local_sql_template = sql_template.clone();
        let local_delete_files = delete_files.clone();
        async move {
            execute_query_input_group_plan(query_files, &local_sql_template, &local_delete_files)
                .await
        }
    }))
    .buffer_unordered(concurrency);

    while let Some(result) = results.next().await {
        merge_rows(&mut rows, result?, request, limit);
    }

    Ok(rows)
}

async fn execute_aggregate_plan(
    files: &[FileDescriptor],
    speedboat_files: &[FileDescriptor],
    delete_files: &[String],
    request: &ServingRequestPlan,
    limit: usize,
) -> Result<Vec<Value>, ServingQueryError> {
    let aggregate = request.aggregate.as_ref().ok_or_else(|| {
        ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            "Aggregate serving query is missing aggregate specification",
        )
    })?;
    let measure_plans = aggregate_measure_plans(aggregate)?;
    let query_input_groups = serving_query_input_groups(files, speedboat_files);
    if query_input_groups.is_empty() {
        return Ok(empty_aggregate_rows(aggregate, &measure_plans));
    }

    let partial_sql =
        build_aggregate_sql("{table}", request, limit, !delete_files.is_empty(), true)?;
    let delete_files = delete_files.to_vec();
    let concurrency = query_input_groups
        .len()
        .clamp(1, serving_file_parallelism());
    let mut results = stream::iter(query_input_groups.into_iter().map(|query_files| {
        let local_partial_sql = partial_sql.clone();
        let local_delete_files = delete_files.clone();
        async move {
            execute_query_input_group_plan(query_files, &local_partial_sql, &local_delete_files)
                .await
        }
    }))
    .buffer_unordered(concurrency);

    let mut merged = HashMap::<String, serde_json::Map<String, Value>>::new();
    while let Some(result) = results.next().await {
        merge_partial_aggregate_rows(&mut merged, result?, aggregate, &measure_plans)?;
    }

    finalize_aggregate_rows(merged, aggregate, &measure_plans, limit)
}

async fn execute_ordered_top_k_plan(
    file_groups: Vec<OrderedFileGroup>,
    delete_files: &[String],
    request: &ServingRequestPlan,
    sql: &str,
    limit: usize,
) -> Result<Vec<Value>, ServingQueryError> {
    let mut rows = vec![];
    let Some(sort) = request.order_by.first() else {
        return Ok(rows);
    };
    let mut file_groups = file_groups.into_iter().peekable();
    let delete_files = delete_files.to_vec();

    while let Some(file_group) = file_groups.next() {
        let new_rows = execute_query_input_group_plan(
            iceberg_query_inputs(file_group.files),
            sql,
            &delete_files,
        )
        .await?;
        merge_rows(&mut rows, new_rows, request, limit);

        if let Some(next_group) = file_groups.peek() {
            if remaining_groups_cannot_beat_kth_row(
                &rows,
                &next_group.best_case_sort_value,
                &sort.field,
                sort.descending,
                limit,
            ) {
                break;
            }
        }
    }

    Ok(rows)
}

fn merge_partial_aggregate_rows(
    merged: &mut HashMap<String, serde_json::Map<String, Value>>,
    rows: Vec<Value>,
    aggregate: &ServingAggregateSpec,
    measure_plans: &[AggregateMeasurePlan],
) -> Result<(), ServingQueryError> {
    for row in rows {
        let Some(object) = row.as_object() else {
            continue;
        };
        let key = aggregate_group_key(object, &aggregate.group_by);
        let entry = merged.entry(key).or_insert_with(|| {
            let mut map = serde_json::Map::new();
            for field in aggregate.group_by.iter() {
                map.insert(
                    field.clone(),
                    object.get(field).cloned().unwrap_or(Value::Null),
                );
            }
            map
        });

        for plan in measure_plans.iter() {
            match plan.function {
                AggregateFunction::Count => {
                    let partial_value = object
                        .get(&plan.partial_value_alias)
                        .and_then(value_as_f64)
                        .unwrap_or(0.0);
                    let existing = entry.get(&plan.alias).and_then(value_as_f64).unwrap_or(0.0);
                    entry.insert(
                        plan.alias.clone(),
                        json_number_value(existing + partial_value),
                    );
                }
                AggregateFunction::Sum => {
                    if let Some(partial_value) =
                        object.get(&plan.partial_value_alias).and_then(value_as_f64)
                    {
                        let existing = entry.get(&plan.alias).and_then(value_as_f64).unwrap_or(0.0);
                        entry.insert(
                            plan.alias.clone(),
                            json_number_value(existing + partial_value),
                        );
                    } else {
                        entry.entry(plan.alias.clone()).or_insert(Value::Null);
                    }
                }
                AggregateFunction::Avg => {
                    let partial_sum = object
                        .get(&plan.partial_value_alias)
                        .and_then(value_as_f64)
                        .unwrap_or(0.0);
                    let partial_count = object
                        .get(plan.partial_count_alias.as_ref().unwrap())
                        .and_then(value_as_f64)
                        .unwrap_or(0.0);
                    let sum_key = format!("__avg_sum_{}", plan.alias);
                    let count_key = format!("__avg_count_{}", plan.alias);
                    let existing_sum = entry.get(&sum_key).and_then(value_as_f64).unwrap_or(0.0);
                    let existing_count =
                        entry.get(&count_key).and_then(value_as_f64).unwrap_or(0.0);
                    entry.insert(sum_key, json_number_value(existing_sum + partial_sum));
                    entry.insert(count_key, json_number_value(existing_count + partial_count));
                }
                AggregateFunction::Min => {
                    merge_extreme_value(
                        entry,
                        &plan.alias,
                        object.get(&plan.partial_value_alias),
                        true,
                    );
                }
                AggregateFunction::Max => {
                    merge_extreme_value(
                        entry,
                        &plan.alias,
                        object.get(&plan.partial_value_alias),
                        false,
                    );
                }
            }
        }
    }

    Ok(())
}

fn merge_extreme_value(
    entry: &mut serde_json::Map<String, Value>,
    alias: &str,
    candidate: Option<&Value>,
    is_min: bool,
) {
    let Some(candidate) = candidate else {
        return;
    };
    if candidate.is_null() {
        entry.entry(alias.to_string()).or_insert(Value::Null);
        return;
    }
    match entry.get(alias) {
        Some(existing) => {
            if existing.is_null() {
                entry.insert(alias.to_string(), candidate.clone());
                return;
            }
            let ordering = compare_values(candidate, existing);
            if (is_min && ordering == Ordering::Less) || (!is_min && ordering == Ordering::Greater)
            {
                entry.insert(alias.to_string(), candidate.clone());
            }
        }
        None => {
            entry.insert(alias.to_string(), candidate.clone());
        }
    }
}

fn finalize_aggregate_rows(
    merged: HashMap<String, serde_json::Map<String, Value>>,
    aggregate: &ServingAggregateSpec,
    measure_plans: &[AggregateMeasurePlan],
    limit: usize,
) -> Result<Vec<Value>, ServingQueryError> {
    if merged.is_empty() {
        return Ok(empty_aggregate_rows(aggregate, measure_plans));
    }

    let mut rows = merged
        .into_values()
        .map(|mut row| {
            for plan in measure_plans.iter() {
                match plan.function {
                    AggregateFunction::Avg => {
                        let sum_key = format!("__avg_sum_{}", plan.alias);
                        let count_key = format!("__avg_count_{}", plan.alias);
                        let sum = row.get(&sum_key).and_then(value_as_f64).unwrap_or(0.0);
                        let count = row.get(&count_key).and_then(value_as_f64).unwrap_or(0.0);
                        row.remove(&sum_key);
                        row.remove(&count_key);
                        row.insert(
                            plan.alias.clone(),
                            if count == 0.0 {
                                Value::Null
                            } else {
                                json_number_value(sum / count)
                            },
                        );
                    }
                    AggregateFunction::Count => {
                        row.entry(plan.alias.clone())
                            .or_insert_with(|| json_number_value(0.0));
                    }
                    AggregateFunction::Sum | AggregateFunction::Min | AggregateFunction::Max => {
                        row.entry(plan.alias.clone()).or_insert(Value::Null);
                    }
                }
            }
            Value::Object(row)
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| compare_grouped_rows(left, right, &aggregate.group_by));
    if !aggregate.group_by.is_empty() && rows.len() > limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

fn empty_aggregate_rows(
    aggregate: &ServingAggregateSpec,
    measure_plans: &[AggregateMeasurePlan],
) -> Vec<Value> {
    if !aggregate.group_by.is_empty() {
        return vec![];
    }

    let mut row = serde_json::Map::new();
    for plan in measure_plans.iter() {
        let value = match plan.function {
            AggregateFunction::Count => json_number_value(0.0),
            AggregateFunction::Sum
            | AggregateFunction::Avg
            | AggregateFunction::Min
            | AggregateFunction::Max => Value::Null,
        };
        row.insert(plan.alias.clone(), value);
    }
    vec![Value::Object(row)]
}

fn aggregate_group_key(object: &serde_json::Map<String, Value>, group_by: &[String]) -> String {
    let values = group_by
        .iter()
        .map(|field| object.get(field).cloned().unwrap_or(Value::Null))
        .collect::<Vec<_>>();
    serde_json::to_string(&values).unwrap_or_default()
}

fn compare_grouped_rows(left: &Value, right: &Value, group_by: &[String]) -> Ordering {
    for field in group_by {
        let ordering = compare_row_values(left.get(field), right.get(field), false);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|numeric| numeric as f64))
        .or_else(|| value.as_u64().map(|numeric| numeric as f64))
}

fn json_number_value(value: f64) -> Value {
    if value.fract() == 0.0
        && value.is_finite()
        && value >= i64::MIN as f64
        && value <= i64::MAX as f64
    {
        Value::Number(serde_json::Number::from(value as i64))
    } else {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

fn merge_rows(
    rows: &mut Vec<Value>,
    new_rows: Vec<Value>,
    request: &ServingRequestPlan,
    limit: usize,
) {
    rows.extend(new_rows);

    if let Some(sort) = request.order_by.first() {
        rows.sort_by(|left, right| {
            compare_row_values(
                left.get(&sort.field),
                right.get(&sort.field),
                sort.descending,
            )
        });
        rows.truncate(limit);
        return;
    }

    // Bound unsorted fan-in while preserving enough slack for stable comparisons.
    let unsorted_cap = limit.saturating_mul(4).max(limit);
    if rows.len() > unsorted_cap {
        rows.truncate(unsorted_cap);
    }
}

fn serving_file_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get().clamp(1, 8))
        .unwrap_or(4)
}

fn ordered_file_groups_for_top_k(
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
    sort_order: &[IcebergSortField],
    request: &ServingRequestPlan,
    sort_field: &str,
    descending: bool,
) -> Option<Vec<OrderedFileGroup>> {
    let mut groups = group_files_by_schema(files)
        .into_iter()
        .map(|group| {
            file_group_sort_bound(
                &group, file_stats, sort_order, request, sort_field, descending,
            )
            .map(|bound| OrderedFileGroup {
                files: group,
                best_case_sort_value: bound,
            })
        })
        .collect::<Option<Vec<_>>>()?;

    groups.sort_by(|left, right| {
        compare_sort_values(
            &left.best_case_sort_value,
            &right.best_case_sort_value,
            descending,
        )
    });
    Some(groups)
}

#[cfg(test)]
pub(crate) fn select_serving_warmup_files(
    serving: &ServingTableConfig,
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
    sort_order: &[IcebergSortField],
) -> Vec<FileDescriptor> {
    build_serving_warmup_plan(serving, files, file_stats, sort_order)
        .map(|plan| plan.selected_files)
        .unwrap_or_default()
}

pub(crate) fn default_serving_cache_manager_request(
    serving: &ServingTableConfig,
) -> ServingCacheManagerRequest {
    let mut warm_targets = vec![ServingCacheWarmTarget::Metadata];
    warm_targets.extend(
        serving
            .patterns
            .iter()
            .map(|pattern| ServingCacheWarmTarget::Pattern(pattern.name.clone())),
    );
    ServingCacheManagerRequest {
        warm_targets,
        evict_targets: vec![],
    }
}

pub(crate) fn build_serving_warmup_plan(
    serving: &ServingTableConfig,
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
    sort_order: &[IcebergSortField],
) -> Option<ServingWarmupPlan> {
    let mut selected = vec![];
    let mut seen_paths = HashSet::new();
    let mut matched_patterns = vec![];
    let mut estimated_bytes = 0;
    let mut steps = vec![];

    for pattern in serving.patterns.iter() {
        let Some((request, limit)) = warmup_request_for_pattern(pattern) else {
            continue;
        };
        if !request_matches_pattern(&request, pattern, limit) {
            continue;
        }
        let Some(step) =
            build_warmup_step_for_request(&pattern.name, request, files, file_stats, sort_order)
        else {
            continue;
        };
        let mut capped_step_files = vec![];
        for file in step.selected_files.iter().cloned() {
            capped_step_files.push(file.clone());
            if seen_paths.insert(file.file_path.clone()) {
                estimated_bytes += file.size;
                selected.push(file);
                if selected.len() >= MAX_WARMUP_FILES {
                    matched_patterns.push(pattern.name.clone());
                    steps.push(ServingWarmupStep {
                        pattern_name: step.pattern_name,
                        request: step.request,
                        selected_files: capped_step_files,
                        estimated_bytes: step.estimated_bytes,
                    });
                    return Some(ServingWarmupPlan {
                        matched_patterns,
                        selected_files: selected,
                        estimated_bytes,
                        steps,
                    });
                }
            }
        }
        matched_patterns.push(pattern.name.clone());
        steps.push(step);
    }

    if selected.is_empty() {
        None
    } else {
        Some(ServingWarmupPlan {
            matched_patterns,
            selected_files: selected,
            estimated_bytes,
            steps,
        })
    }
}

pub(crate) fn build_serving_cache_manager_plan(
    request: &ServingCacheManagerRequest,
    serving: &ServingTableConfig,
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
    sort_order: &[IcebergSortField],
    access_artifacts: &[IcebergAccessArtifact],
) -> ServingCacheManagerPlan {
    let mut plan = ServingCacheManagerPlan::default();
    let mut seen_warm_files = HashSet::new();
    let mut seen_evict_files = HashSet::new();

    for target in request.warm_targets.iter() {
        match target {
            ServingCacheWarmTarget::Metadata => {
                plan.metadata_refreshed = true;
            }
            ServingCacheWarmTarget::Pattern(pattern_name) => {
                let Some(pattern) = serving
                    .patterns
                    .iter()
                    .find(|pattern| &pattern.name == pattern_name)
                else {
                    continue;
                };
                let Some((warm_request, limit)) = warmup_request_for_pattern(pattern) else {
                    continue;
                };
                if !request_matches_pattern(&warm_request, pattern, limit) {
                    continue;
                }
                let Some(step) = build_warmup_step_for_request(
                    &pattern.name,
                    warm_request,
                    files,
                    file_stats,
                    sort_order,
                ) else {
                    continue;
                };
                push_unique_string(&mut plan.matched_patterns, pattern.name.clone());
                for artifact in applicable_artifacts_for_request(
                    &step.request,
                    &serving.patterns,
                    access_artifacts,
                    sort_order_matches_request(sort_order, &step.request),
                ) {
                    push_unique_string(&mut plan.matched_artifacts, artifact);
                }
                for file in step.selected_files.iter().cloned() {
                    if seen_warm_files.insert(file.file_path.clone()) {
                        plan.estimated_warm_bytes += file.size;
                        plan.warm_files.push(file);
                    }
                }
                plan.warmup_steps.push(step);
            }
            ServingCacheWarmTarget::Files(file_paths) => {
                for file in files
                    .iter()
                    .filter(|file| file_paths.contains(&file.file_path))
                {
                    if seen_warm_files.insert(file.file_path.clone()) {
                        plan.estimated_warm_bytes += file.size;
                        plan.warm_files.push(file.clone());
                    }
                }
            }
            ServingCacheWarmTarget::Range(range_target) => {
                let warm_request = request_for_cache_range_target(range_target);
                if let Some(step) = build_warmup_step_for_request(
                    &format!("range:{}", range_target.field),
                    warm_request,
                    files,
                    file_stats,
                    sort_order,
                ) {
                    plan.targeted_ranges += 1;
                    for artifact in applicable_artifacts_for_request(
                        &step.request,
                        &serving.patterns,
                        access_artifacts,
                        sort_order_matches_request(sort_order, &step.request),
                    ) {
                        push_unique_string(&mut plan.matched_artifacts, artifact);
                    }
                    for file in step.selected_files.iter().cloned() {
                        if seen_warm_files.insert(file.file_path.clone()) {
                            plan.estimated_warm_bytes += file.size;
                            plan.warm_files.push(file);
                        }
                    }
                    plan.warmup_steps.push(step);
                }
            }
        }
    }

    for target in request.evict_targets.iter() {
        match target {
            ServingCacheEvictTarget::Files(file_paths) => {
                for file_path in file_paths {
                    if seen_evict_files.insert(file_path.clone()) {
                        plan.files_to_evict.push(file_path.clone());
                    }
                }
            }
        }
    }

    plan
}

pub(crate) async fn execute_serving_warmup_plan(
    plan: &ServingWarmupPlan,
    delete_files: &[String],
) -> Result<(), ServingQueryError> {
    for step in plan.steps.iter() {
        tracing::info!(
            pattern_name = step.pattern_name,
            estimated_bytes = step.estimated_bytes,
            files_selected = step.selected_files.len(),
            "Executing serving-shaped warmup query"
        );
        execute_serving_warmup_step(step, delete_files).await?;
    }

    Ok(())
}

pub(crate) async fn execute_serving_cache_manager_plan(
    plan: &ServingCacheManagerPlan,
    delete_files: &[String],
) -> Result<(), ServingQueryError> {
    if !plan.files_to_evict.is_empty() {
        data_access::evict_serving_metadata_for_files(&plan.files_to_evict);
    }
    if !plan.warmup_steps.is_empty() {
        execute_serving_warmup_plan(
            &ServingWarmupPlan {
                matched_patterns: plan.matched_patterns.clone(),
                selected_files: plan.warm_files.clone(),
                estimated_bytes: plan.estimated_warm_bytes,
                steps: plan.warmup_steps.clone(),
            },
            delete_files,
        )
        .await?;
    }
    Ok(())
}

fn build_warmup_step_for_request(
    pattern_name: &str,
    request: ServingRequestPlan,
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
    sort_order: &[IcebergSortField],
) -> Option<ServingWarmupStep> {
    let pruned = prune_candidate_files(files, file_stats, &request);
    if pruned.selected_files.is_empty() {
        return None;
    }

    let (selected_files, estimated_bytes) = if let Some(sort) = request.order_by.first() {
        let ordered_groups = ordered_file_groups_for_top_k(
            &pruned.selected_files,
            file_stats,
            sort_order,
            &request,
            &sort.field,
            sort.descending,
        )?;
        let mut selected_files = vec![];
        let mut estimated_bytes = 0;
        for group in ordered_groups
            .into_iter()
            .take(MAX_WARMUP_FILE_GROUPS_PER_PATTERN)
        {
            for file in group.files {
                estimated_bytes += file.size;
                selected_files.push(file);
            }
        }
        if selected_files.is_empty() {
            return None;
        }
        (selected_files, estimated_bytes)
    } else {
        let selected_files = pruned
            .selected_files
            .iter()
            .take(MAX_WARMUP_FILES)
            .cloned()
            .collect::<Vec<_>>();
        let estimated_bytes = selected_files.iter().map(|file| file.size).sum();
        if selected_files.is_empty() {
            return None;
        }
        (selected_files, estimated_bytes)
    };

    Some(ServingWarmupStep {
        pattern_name: pattern_name.to_string(),
        request,
        selected_files,
        estimated_bytes,
    })
}

async fn execute_serving_warmup_step(
    step: &ServingWarmupStep,
    delete_files: &[String],
) -> Result<(), ServingQueryError> {
    let limit = step.request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let sql = build_sql("{table}", &step.request, limit, !delete_files.is_empty())?;
    let file_groups = group_files_by_schema(&step.selected_files);
    let concurrency = file_groups.len().clamp(1, serving_file_parallelism());
    let delete_files = delete_files.to_vec();
    let mut results = stream::iter(file_groups.into_iter().map(|files| {
        let local_sql = sql.clone();
        let local_delete_files = delete_files.clone();
        async move { execute_file_group_warmup(files, &local_sql, &local_delete_files).await }
    }))
    .buffer_unordered(concurrency);

    while let Some(result) = results.next().await {
        result?;
    }

    Ok(())
}

fn file_group_sort_bound(
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
    sort_order: &[IcebergSortField],
    request: &ServingRequestPlan,
    sort_field: &str,
    descending: bool,
) -> Option<Value> {
    let mut best_bound: Option<Value> = None;

    for file in files {
        let candidate = file_sort_bound(
            file, file_stats, sort_order, request, sort_field, descending,
        )?;
        if best_bound
            .as_ref()
            .map(|best| compare_sort_values(&candidate, best, descending) == Ordering::Less)
            .unwrap_or(true)
        {
            best_bound = Some(candidate);
        }
    }

    best_bound
}

fn file_sort_bound(
    file: &FileDescriptor,
    file_stats: &HashMap<String, IcebergFileStats>,
    sort_order: &[IcebergSortField],
    request: &ServingRequestPlan,
    sort_field: &str,
    descending: bool,
) -> Option<Value> {
    let stats = file_stats.get(&file.file_path)?;
    if !stats.row_groups.is_empty() {
        let matching_row_groups = stats
            .row_groups
            .iter()
            .filter(|row_group| row_group_may_match_request(row_group, request))
            .collect::<Vec<_>>();
        if matching_row_groups.is_empty() {
            return None;
        }

        let mut best_row_group_bound: Option<Value> = None;
        for row_group in matching_row_groups.iter() {
            let Some(candidate) = row_group_sort_bound(row_group, sort_field, descending) else {
                return file_level_sort_bound(stats, sort_order, sort_field, descending);
            };
            if best_row_group_bound
                .as_ref()
                .map(|best| compare_sort_values(&candidate, best, descending) == Ordering::Less)
                .unwrap_or(true)
            {
                best_row_group_bound = Some(candidate);
            }
        }
        return best_row_group_bound;
    }

    file_level_sort_bound(stats, sort_order, sort_field, descending)
}

fn file_level_sort_bound(
    stats: &IcebergFileStats,
    sort_order: &[IcebergSortField],
    sort_field: &str,
    descending: bool,
) -> Option<Value> {
    let column_stats = stats
        .columns
        .iter()
        .find(|stats| stats.field_name == sort_field);
    if column_stats.is_none() && sort_order_supports_field(sort_order, sort_field) {
        return partition_value_sort_bound(stats, sort_field);
    }
    let column_stats = column_stats?;

    if column_is_all_null(column_stats, stats.record_count) {
        return Some(Value::Null);
    }

    if descending {
        column_stats.upper_bound.clone()
    } else {
        column_stats.lower_bound.clone()
    }
}

fn partition_value_sort_bound(stats: &IcebergFileStats, sort_field: &str) -> Option<Value> {
    stats
        .partition_values
        .iter()
        .find(|partition_value| {
            partition_value.source_field_name == sort_field
                && partition_value.transform == "identity"
        })
        .and_then(|partition_value| partition_value.value.clone())
}

fn sort_order_supports_field(sort_order: &[IcebergSortField], sort_field: &str) -> bool {
    sort_order
        .first()
        .map(|field| field.source_field_name == sort_field && field.transform == "identity")
        .unwrap_or(false)
}

fn row_group_sort_bound(
    row_group: &IcebergRowGroupStats,
    sort_field: &str,
    descending: bool,
) -> Option<Value> {
    let column_stats = row_group
        .columns
        .iter()
        .find(|stats| stats.field_name == sort_field)?;

    if column_is_all_null(column_stats, row_group.record_count) {
        return Some(Value::Null);
    }

    if descending {
        column_stats.upper_bound.clone()
    } else {
        column_stats.lower_bound.clone()
    }
}

fn remaining_groups_cannot_beat_kth_row(
    rows: &[Value],
    next_best_sort_value: &Value,
    sort_field: &str,
    descending: bool,
    limit: usize,
) -> bool {
    if limit == 0 || rows.len() < limit {
        return false;
    }

    let Some(kth_row_value) = rows.get(limit - 1).and_then(|row| row.get(sort_field)) else {
        return false;
    };

    compare_sort_values(next_best_sort_value, kth_row_value, descending) == Ordering::Greater
}

async fn execute_file_group_warmup(
    files: Vec<FileDescriptor>,
    sql_template: &str,
    delete_files: &[String],
) -> Result<(), ServingQueryError> {
    let query_files = iceberg_query_inputs(files);
    let _ = execute_query_plan_batches(QueryExecutionPlan {
        sql: QuerySqlTemplate::Built(sql_template.replace("{table}", "{target_table}")),
        files: query_files,
        delete_files: delete_files.to_vec(),
        extension_suffixes: None,
        use_cpu_threadpool: true,
    })
    .await
    .map_err(|error| {
        ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.to_string())
    })?;
    Ok(())
}

fn iceberg_query_inputs(files: Vec<FileDescriptor>) -> Vec<QueryInputFile> {
    files
        .into_iter()
        .map(|file| QueryInputFile {
            file,
            storage: QueryStorageKind::Iceberg,
            extensions: vec![],
        })
        .collect()
}

fn speedboat_query_inputs(files: &[FileDescriptor]) -> Vec<QueryInputFile> {
    files
        .iter()
        .cloned()
        .map(|file| QueryInputFile {
            file,
            storage: QueryStorageKind::Speedboat,
            extensions: vec![],
        })
        .collect()
}

fn serving_query_input_groups(
    files: &[FileDescriptor],
    speedboat_files: &[FileDescriptor],
) -> Vec<Vec<QueryInputFile>> {
    let mut query_files = iceberg_query_inputs(files.to_vec());
    query_files.extend(speedboat_query_inputs(speedboat_files));
    group_query_input_files_by_schema(query_files)
}

async fn execute_query_input_group_plan(
    query_files: Vec<QueryInputFile>,
    sql_template: &str,
    delete_files: &[String],
) -> Result<Vec<Value>, ServingQueryError> {
    let batches = execute_query_plan_batches(QueryExecutionPlan {
        sql: QuerySqlTemplate::Built(sql_template.replace("{table}", "{target_table}")),
        files: query_files,
        delete_files: delete_files.to_vec(),
        extension_suffixes: None,
        use_cpu_threadpool: true,
    })
    .await
    .map_err(|error| {
        ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.to_string())
    })?;
    let serde_result = batches_to_serde_value(&batches).await.map_err(|error| {
        ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.message)
    })?;
    Ok(serde_result.values)
}

fn file_group_table_name(files: &[FileDescriptor]) -> String {
    let mut file_paths = files
        .iter()
        .map(|file| file.file_path.clone())
        .collect::<Vec<_>>();
    file_paths.sort();

    let mut hasher = DefaultHasher::new();
    for file_path in file_paths.iter() {
        file_path.hash(&mut hasher);
    }

    format!("table_group_{:016x}", hasher.finish())
}

fn validate_request(
    request: &ServingRequestPlan,
    schema: &PowdrrSchema,
) -> Result<(), ServingQueryError> {
    let schema_map = schema.to_map();
    let mut seen_filter_fields = HashSet::new();

    if request.order_by.len() > 1 {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            "Only one ORDER BY field is supported in the serving MVP",
        ));
    }

    if let Some(select_fields) = normalized_select(request.select.clone()) {
        for field in select_fields.iter() {
            if !schema_map.contains_key(field) {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Unknown select field {}", field),
                ));
            }
        }
    }

    if request.aggregate.is_some() {
        if request.select.is_some() {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                "Aggregate serving queries do not support select",
            ));
        }
        if !request.order_by.is_empty() {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                "Aggregate serving queries do not support ORDER BY yet",
            ));
        }
    }

    for sort in request.order_by.iter() {
        if !schema_map.contains_key(&sort.field) {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!("Unknown sort field {}", sort.field),
            ));
        }
    }

    for filter in request.filters.iter() {
        if !seen_filter_fields.insert(filter.field.clone()) {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                "Duplicate filters on the same field are not supported",
            ));
        }
        let field = match schema_map.get(&filter.field) {
            Some(field) => field,
            None => {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Unknown filter field {}", filter.field),
                ));
            }
        };
        validate_filter(filter, &field.data_type)?;
    }

    if let Some(aggregate) = request.aggregate.as_ref() {
        validate_aggregate_request(aggregate, &schema_map)?;
    }

    if request.limit.unwrap_or(DEFAULT_LIMIT) > MAX_LIMIT {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            &format!("Limit must be <= {}", MAX_LIMIT),
        ));
    }

    Ok(())
}

fn validate_filter(
    filter: &ServingPredicate,
    data_type: &PowdrrDataType,
) -> Result<(), ServingQueryError> {
    let eq_like = usize::from(filter.eq.is_some()) + usize::from(filter.in_values.is_some());
    let range_like = usize::from(filter.gt.is_some())
        + usize::from(filter.gte.is_some())
        + usize::from(filter.lt.is_some())
        + usize::from(filter.lte.is_some());

    if eq_like == 0 && range_like == 0 {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            &format!("Filter for {} has no operator", filter.field),
        ));
    }
    if eq_like > 1 || (eq_like > 0 && range_like > 0) {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            &format!("Filter for {} mixes incompatible operators", filter.field),
        ));
    }
    if let Some(values) = filter.in_values.as_ref() {
        if values.is_empty() || values.len() > MAX_IN_VALUES {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!(
                    "IN filter for {} must have 1-{} values",
                    filter.field, MAX_IN_VALUES
                ),
            ));
        }
        for value in values.iter() {
            validate_literal(value, data_type, &filter.field)?;
        }
    }
    if let Some(value) = filter.eq.as_ref() {
        validate_literal(value, data_type, &filter.field)?;
    }
    for range_value in [&filter.gt, &filter.gte, &filter.lt, &filter.lte] {
        if let Some(value) = range_value.as_ref() {
            validate_literal(value, data_type, &filter.field)?;
        }
    }

    Ok(())
}

fn validate_literal(
    value: &Value,
    data_type: &PowdrrDataType,
    field_name: &str,
) -> Result<(), ServingQueryError> {
    match data_type {
        PowdrrDataType::Boolean => {
            if !value.is_boolean() {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Field {} expects a boolean literal", field_name),
                ));
            }
        }
        PowdrrDataType::Float | PowdrrDataType::Integer => {
            if !value.is_number() {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Field {} expects a numeric literal", field_name),
                ));
            }
        }
        PowdrrDataType::String => {
            if !value.is_string() {
                return Err(ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Field {} expects a string literal", field_name),
                ));
            }
        }
        PowdrrDataType::Object(_) | PowdrrDataType::Array(_) | PowdrrDataType::Null => {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!("Field {} is not supported by the serving MVP", field_name),
            ));
        }
    }
    Ok(())
}

fn validate_aggregate_request(
    aggregate: &ServingAggregateSpec,
    schema_map: &HashMap<String, crate::schema_massager::PowdrrField>,
) -> Result<(), ServingQueryError> {
    if aggregate.measures.is_empty() {
        return Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            "Aggregate serving queries must declare at least one measure",
        ));
    }

    let mut seen_group_by = HashSet::new();
    for field_name in aggregate.group_by.iter() {
        if !seen_group_by.insert(field_name.clone()) {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!("Duplicate aggregate GROUP BY field {}", field_name),
            ));
        }
        let field = schema_map.get(field_name).ok_or_else(|| {
            ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!("Unknown aggregate GROUP BY field {}", field_name),
            )
        })?;
        validate_scalar_serving_field_type(&field.data_type, field_name)?;
    }

    let mut seen_aliases = HashSet::new();
    let mut seen_signatures = HashSet::new();
    for measure in aggregate.measures.iter() {
        let function = normalized_aggregate_function(&measure.function).ok_or_else(|| {
            ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!("Unsupported aggregate function {}", measure.function),
            )
        })?;
        let alias = aggregate_measure_alias_with_function(measure, function)?;
        if !seen_aliases.insert(alias) {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                "Aggregate measure aliases must be unique",
            ));
        }
        let signature = aggregate_measure_signature_with_function(measure, function)?;
        if !seen_signatures.insert(signature) {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                "Duplicate aggregate measures are not supported",
            ));
        }

        match function {
            "count" => {
                if measure.field.is_some() {
                    return Err(ServingQueryError::new(
                        StatusCode::BAD_REQUEST,
                        "COUNT serving measures do not take a field",
                    ));
                }
            }
            "sum" | "avg" | "min" | "max" => {
                let field_name = measure.field.as_ref().ok_or_else(|| {
                    ServingQueryError::new(
                        StatusCode::BAD_REQUEST,
                        &format!(
                            "{} serving measures require a field",
                            function.to_uppercase()
                        ),
                    )
                })?;
                let field = schema_map.get(field_name).ok_or_else(|| {
                    ServingQueryError::new(
                        StatusCode::BAD_REQUEST,
                        &format!("Unknown aggregate field {}", field_name),
                    )
                })?;
                validate_scalar_serving_field_type(&field.data_type, field_name)?;
                if matches!(function, "sum" | "avg")
                    && !matches!(
                        field.data_type,
                        PowdrrDataType::Float | PowdrrDataType::Integer
                    )
                {
                    return Err(ServingQueryError::new(
                        StatusCode::BAD_REQUEST,
                        &format!(
                            "{} aggregate field {} must be numeric",
                            function.to_uppercase(),
                            field_name
                        ),
                    ));
                }
            }
            _ => unreachable!(),
        }
    }

    Ok(())
}

fn validate_scalar_serving_field_type(
    data_type: &PowdrrDataType,
    field_name: &str,
) -> Result<(), ServingQueryError> {
    match data_type {
        PowdrrDataType::Boolean
        | PowdrrDataType::Float
        | PowdrrDataType::Integer
        | PowdrrDataType::String => Ok(()),
        PowdrrDataType::Object(_) | PowdrrDataType::Array(_) | PowdrrDataType::Null => {
            Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                &format!("Field {} is not supported by the serving MVP", field_name),
            ))
        }
    }
}

fn normalized_aggregate_function(function: &str) -> Option<&'static str> {
    match function.to_ascii_lowercase().as_str() {
        "count" => Some("count"),
        "sum" => Some("sum"),
        "avg" => Some("avg"),
        "min" => Some("min"),
        "max" => Some("max"),
        _ => None,
    }
}

fn aggregate_measure_signature_with_function(
    measure: &ServingAggregateMeasure,
    function: &str,
) -> Result<(String, Option<String>), ServingQueryError> {
    Ok((function.to_string(), measure.field.clone()))
}

fn aggregate_measure_alias_with_function(
    measure: &ServingAggregateMeasure,
    function: &str,
) -> Result<String, ServingQueryError> {
    if let Some(alias) = measure.alias.as_ref() {
        if alias.is_empty() {
            return Err(ServingQueryError::new(
                StatusCode::BAD_REQUEST,
                "Aggregate measure aliases must not be empty",
            ));
        }
        return Ok(alias.clone());
    }

    match function {
        "count" => Ok("count".to_string()),
        "sum" | "avg" | "min" | "max" => {
            let field = measure.field.as_ref().ok_or_else(|| {
                ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!(
                        "{} serving measures require a field",
                        function.to_uppercase()
                    ),
                )
            })?;
            Ok(format!("{function}_{field}"))
        }
        _ => Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            &format!("Unsupported aggregate function {}", measure.function),
        )),
    }
}

fn aggregate_specs_match(
    request: Option<&ServingAggregateSpec>,
    pattern: Option<&ServingAggregateSpec>,
) -> bool {
    match (request, pattern) {
        (None, None) => true,
        (Some(request), Some(pattern)) => {
            request.group_by == pattern.group_by
                && request.measures.len() == pattern.measures.len()
                && request
                    .measures
                    .iter()
                    .zip(pattern.measures.iter())
                    .all(|(left, right)| {
                        match (
                            normalized_aggregate_function(&left.function),
                            normalized_aggregate_function(&right.function),
                        ) {
                            (Some(left_function), Some(right_function)) => {
                                left_function == right_function && left.field == right.field
                            }
                            _ => false,
                        }
                    })
        }
        _ => false,
    }
}

fn request_supported(request: &ServingRequestPlan) -> bool {
    if request.aggregate.is_some() {
        request.order_by.is_empty()
    } else {
        request.order_by.len() <= 1
    }
}

fn sort_order_matches_request(
    sort_order: &[IcebergSortField],
    request: &ServingRequestPlan,
) -> bool {
    let Some(request_sort) = request.order_by.first() else {
        return false;
    };
    let Some(sort_field) = sort_order.first() else {
        return false;
    };
    sort_field.transform == "identity"
        && sort_field.source_field_name == request_sort.field
        && sort_field.descending == request_sort.descending
}

fn request_matches_pattern(
    request: &ServingRequestPlan,
    pattern: &ServingPattern,
    limit: usize,
) -> bool {
    if pattern
        .max_limit
        .map(|max_limit| limit > max_limit as usize)
        .unwrap_or(false)
    {
        return false;
    }

    let select_fields = normalized_select(request.select.clone());
    if let (Some(select_fields), Some(projection)) =
        (select_fields.as_ref(), pattern.projection.as_ref())
    {
        if !select_fields.iter().all(|field| projection.contains(field)) {
            return false;
        }
    } else if request.select.is_none() && pattern.projection.is_some() {
        return false;
    }

    if !aggregate_specs_match(request.aggregate.as_ref(), pattern.aggregate.as_ref()) {
        return false;
    }

    if let Some(order_field) = pattern.order_field.as_ref() {
        match request.order_by.first() {
            Some(sort) if &sort.field == order_field && sort.descending == pattern.descending => {}
            _ => return false,
        }
    } else if !request.order_by.is_empty() {
        return false;
    }

    let eq_field_set = pattern.eq_fields.iter().cloned().collect::<HashSet<_>>();
    let mut seen_fields = HashSet::new();
    for filter in request.filters.iter() {
        seen_fields.insert(filter.field.clone());
        let is_eq = filter.eq.is_some() || filter.in_values.is_some();
        let is_range = filter.gt.is_some()
            || filter.gte.is_some()
            || filter.lt.is_some()
            || filter.lte.is_some();
        if is_eq && !eq_field_set.contains(&filter.field) {
            return false;
        }
        if is_range && pattern.range_field.as_deref() != Some(filter.field.as_str()) {
            return false;
        }
    }

    if !pattern
        .eq_fields
        .iter()
        .all(|field| seen_fields.contains(field))
    {
        return false;
    }
    if let Some(range_field) = pattern.range_field.as_ref() {
        if !seen_fields.contains(range_field) {
            return false;
        }
    }

    true
}

fn warmup_request_for_pattern(pattern: &ServingPattern) -> Option<(ServingRequestPlan, usize)> {
    let order_field = pattern.order_field.as_ref()?;
    if !pattern.eq_fields.is_empty() || pattern.range_field.is_some() || pattern.aggregate.is_some()
    {
        return None;
    }

    let limit = pattern
        .max_limit
        .unwrap_or(DEFAULT_LIMIT as u64)
        .clamp(1, MAX_LIMIT as u64) as usize;
    Some((
        ServingRequestPlan {
            select: pattern.projection.clone(),
            filters: vec![],
            aggregate: None,
            order_by: vec![crate::serving_plan::ServingSort {
                field: order_field.clone(),
                descending: pattern.descending,
            }],
            limit: Some(limit),
            allow_slow_path: false,
            explain: false,
        },
        limit,
    ))
}

fn request_for_cache_range_target(target: &ServingCacheRangeTarget) -> ServingRequestPlan {
    ServingRequestPlan {
        select: target.projection.clone(),
        filters: vec![ServingPredicate {
            field: target.field.clone(),
            eq: None,
            in_values: None,
            gt: target.gt.clone(),
            gte: target.gte.clone(),
            lt: target.lt.clone(),
            lte: target.lte.clone(),
        }],
        aggregate: None,
        order_by: target
            .limit
            .map(|_| {
                vec![crate::serving_plan::ServingSort {
                    field: target.field.clone(),
                    descending: target.order_descending,
                }]
            })
            .unwrap_or_default(),
        limit: target.limit,
        allow_slow_path: false,
        explain: false,
    }
}

fn push_unique_string(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
        values.sort();
    }
}

fn applicable_artifacts_for_request(
    request: &ServingRequestPlan,
    patterns: &[ServingPattern],
    artifacts: &[IcebergAccessArtifact],
    declared_sort_order_match: bool,
) -> Vec<String> {
    let mut matched = vec![];
    let limit = request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);

    for artifact in artifacts {
        if artifact.kind == ACCESS_ARTIFACT_KIND_SECONDARY_PATTERN {
            let Some(pattern_name) = artifact
                .name
                .strip_prefix(&format!("{}:", ACCESS_ARTIFACT_KIND_SECONDARY_PATTERN))
            else {
                continue;
            };
            if patterns.iter().any(|pattern| {
                pattern.name == pattern_name && request_matches_pattern(request, pattern, limit)
            }) {
                push_unique_string(&mut matched, artifact.name.clone());
            }
            continue;
        }
        let matches_filter = request.filters.iter().any(|predicate| {
            if !artifact.fields.contains(&predicate.field) {
                return false;
            }
            let uses_eq = predicate.eq.is_some() || predicate.in_values.is_some();
            let uses_range = predicate.gt.is_some()
                || predicate.gte.is_some()
                || predicate.lt.is_some()
                || predicate.lte.is_some();
            (uses_eq && artifact.supports_eq) || (uses_range && artifact.supports_range)
        });
        let matches_sort = request
            .order_by
            .first()
            .map(|sort| {
                artifact.supports_order
                    && declared_sort_order_match
                    && artifact.fields.contains(&sort.field)
            })
            .unwrap_or(false);
        let matches_aggregate = request
            .aggregate
            .as_ref()
            .map(|aggregate| {
                aggregate
                    .group_by
                    .iter()
                    .any(|field| artifact.fields.contains(field))
                    || aggregate.measures.iter().any(|measure| {
                        measure
                            .field
                            .as_ref()
                            .is_some_and(|field| artifact.fields.contains(field))
                    })
            })
            .unwrap_or(false);
        if matches_filter || matches_sort || matches_aggregate {
            push_unique_string(&mut matched, artifact.name.clone());
        }
    }

    matched
}

fn classify_request_with_admission(
    context: &ServingExecutionContext,
    request: &ServingRequestPlan,
    matched_pattern: Option<&str>,
    declared_sort_order_match: bool,
    artifacts_considered: &[String],
    artifacts_used: &[String],
    pruned: &PrunedFileSelection,
) -> (ServingQueryClassification, Option<String>) {
    let speedboat_files_selected = context.speedboat_files.len();
    let speedboat_estimated_bytes = context
        .speedboat_files
        .iter()
        .map(|file| file.size)
        .sum::<u64>();
    let exceeds_fast_budget = pruned.estimated_bytes + speedboat_estimated_bytes
        > DEFAULT_FAST_PATH_MAX_BYTES
        || pruned.files_selected + speedboat_files_selected > DEFAULT_FAST_PATH_MAX_FILES
        || pruned.row_groups_selected > DEFAULT_FAST_PATH_MAX_ROW_GROUPS
        || context.delete_files.len() > DEFAULT_FAST_PATH_MAX_DELETE_FILES;
    let exceeds_slow_budget = pruned.estimated_bytes + speedboat_estimated_bytes
        > DEFAULT_SLOW_PATH_MAX_BYTES
        || pruned.files_selected + speedboat_files_selected > DEFAULT_SLOW_PATH_MAX_FILES
        || pruned.row_groups_selected > DEFAULT_SLOW_PATH_MAX_ROW_GROUPS
        || context.delete_files.len() > DEFAULT_SLOW_PATH_MAX_DELETE_FILES;

    if request.aggregate.is_some() && matched_pattern.is_none() {
        return (
            ServingQueryClassification::Rejected,
            Some(format_admission_reason(
                "No declared aggregate serving pattern matched this query",
                context,
                request,
                declared_sort_order_match,
                artifacts_considered,
                artifacts_used,
                pruned,
            )),
        );
    }

    if exceeds_slow_budget {
        return (
            ServingQueryClassification::Rejected,
            Some(format_admission_reason(
                "Query exceeds serving budget",
                context,
                request,
                declared_sort_order_match,
                artifacts_considered,
                artifacts_used,
                pruned,
            )),
        );
    }

    if matched_pattern.is_some() && !exceeds_fast_budget {
        return (ServingQueryClassification::FastPath, None);
    }

    if matched_pattern.is_some() {
        return (
            ServingQueryClassification::SlowPath,
            Some(format_admission_reason(
                "Matched serving pattern but exceeds fast-path budget",
                context,
                request,
                declared_sort_order_match,
                artifacts_considered,
                artifacts_used,
                pruned,
            )),
        );
    }

    (
        ServingQueryClassification::SlowPath,
        Some(format_admission_reason(
            "No declared serving pattern matched this query",
            context,
            request,
            declared_sort_order_match,
            artifacts_considered,
            artifacts_used,
            pruned,
        )),
    )
}

fn format_admission_reason(
    prefix: &str,
    context: &ServingExecutionContext,
    request: &ServingRequestPlan,
    declared_sort_order_match: bool,
    artifacts_considered: &[String],
    artifacts_used: &[String],
    pruned: &PrunedFileSelection,
) -> String {
    let mut reason = format!(
        "{prefix}: estimated {} bytes across {} files and {} row groups with {} delete files.",
        pruned.estimated_bytes,
        pruned.files_selected,
        pruned.row_groups_selected,
        context.delete_files.len(),
    );
    let suggestions = suggested_actions_for_request(
        context,
        request,
        declared_sort_order_match,
        artifacts_considered,
        artifacts_used,
    );
    if !suggestions.is_empty() {
        reason.push_str(" Suggested action: ");
        reason.push_str(&suggestions.join("; "));
        reason.push('.');
    }
    reason
}

fn suggested_actions_for_request(
    context: &ServingExecutionContext,
    request: &ServingRequestPlan,
    declared_sort_order_match: bool,
    artifacts_considered: &[String],
    _artifacts_used: &[String],
) -> Vec<String> {
    let mut suggestions = vec![];
    let identity_partition_fields = identity_partition_fields(&context.partition_spec);
    let missing_partition_fields = request
        .filters
        .iter()
        .filter(|predicate| predicate.eq.is_some() || predicate.in_values.is_some())
        .map(|predicate| predicate.field.clone())
        .filter(|field| !identity_partition_fields.contains(field))
        .collect::<Vec<_>>();
    if !missing_partition_fields.is_empty() {
        suggestions.push(format!(
            "cluster or partition hot data on {}",
            missing_partition_fields.join(", ")
        ));
    }
    if request_uses_exact_filters(request) && !has_exact_artifact(artifacts_considered) {
        suggestions.push(
            "publish exact_pruning or exact_index sidecars for exact-match fields".to_string(),
        );
    }
    if request.aggregate.is_some() {
        suggestions.push(
            "declare aggregate fragments or a bitmap/rollup artifact for this aggregate pattern"
                .to_string(),
        );
    }
    if let Some(pattern_name) = context.description.serving.as_ref().and_then(|serving| {
        serving
            .patterns
            .iter()
            .find(|pattern| {
                request_matches_pattern(
                    request,
                    pattern,
                    request.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT),
                )
            })
            .map(|pattern| pattern.name.clone())
    }) {
        let secondary_artifact = secondary_pattern_artifact_name(&pattern_name);
        if !artifacts_considered.contains(&secondary_artifact) && !request.aggregate.is_some() {
            suggestions.push(format!(
                "build a declared secondary serving artifact for pattern {}",
                pattern_name
            ));
        }
    }
    if let Some(sort) = request.order_by.first() {
        if !declared_sort_order_match {
            suggestions.push(format!(
                "rewrite or compact files with Iceberg sort order on {}",
                sort.field
            ));
        }
    }
    if context.delete_files.len() > DEFAULT_FAST_PATH_MAX_DELETE_FILES {
        suggestions
            .push("compact or apply delete files to reduce delete-join overhead".to_string());
    }
    suggestions
}

fn identity_partition_fields(partition_spec: &[IcebergPartitionField]) -> HashSet<String> {
    partition_spec
        .iter()
        .filter(|field| field.transform == "identity")
        .map(|field| field.source_field_name.clone())
        .collect()
}

fn has_exact_artifact(artifacts: &[String]) -> bool {
    artifacts.iter().any(|artifact| {
        artifact == ACCESS_ARTIFACT_KIND_EXACT_PRUNING
            || artifact == ACCESS_ARTIFACT_KIND_EXACT_INDEX
    })
}

fn exact_artifact_fields(schema: &PowdrrSchema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter_map(|field| {
            if matches!(
                field.data_type,
                PowdrrDataType::Boolean
                    | PowdrrDataType::Float
                    | PowdrrDataType::Integer
                    | PowdrrDataType::String
            ) {
                Some(field.name.clone())
            } else {
                None
            }
        })
        .collect()
}

fn flatten_extension_files(
    checkpoint: &TableMetadataCheckpoint,
) -> HashMap<String, Vec<ExtensionFile>> {
    let mut flattened: HashMap<String, Vec<ExtensionFile>> = HashMap::new();
    for files_by_path in checkpoint.extension_metadata.values() {
        for (file_path, extension_files) in files_by_path {
            let entry = flattened.entry(file_path.clone()).or_insert_with(Vec::new);
            for extension_file in extension_files.iter().cloned() {
                if !entry.iter().any(|existing| {
                    existing.suffix == extension_file.suffix
                        && existing.location == extension_file.location
                }) {
                    entry.push(extension_file);
                }
            }
            entry.sort_by(|left, right| left.suffix.cmp(&right.suffix));
        }
    }
    flattened
}

fn append_exact_sidecar_artifacts(
    access_artifacts: &mut Vec<IcebergAccessArtifact>,
    files: &[FileDescriptor],
    extension_files: &HashMap<String, Vec<ExtensionFile>>,
    fields: &[String],
) {
    if fields.is_empty() {
        return;
    }

    let has_complete_exact_pruning = files.iter().all(|file| {
        extension_files.get(&file.file_path).is_some_and(|files| {
            files
                .iter()
                .any(|extension| extension.suffix == "exact_pruning")
        })
    });
    if has_complete_exact_pruning
        && !access_artifacts
            .iter()
            .any(|artifact| artifact.name == ACCESS_ARTIFACT_KIND_EXACT_PRUNING)
    {
        access_artifacts.push(IcebergAccessArtifact {
            name: ACCESS_ARTIFACT_KIND_EXACT_PRUNING.to_string(),
            kind: ACCESS_ARTIFACT_KIND_EXACT_PRUNING.to_string(),
            fields: fields.to_vec(),
            exact: true,
            supports_eq: true,
            supports_range: false,
            supports_order: false,
        });
    }

    let has_complete_exact_index = files.iter().all(|file| {
        extension_files.get(&file.file_path).is_some_and(|files| {
            files
                .iter()
                .any(|extension| extension.suffix == "exact_index")
        })
    });
    if has_complete_exact_index
        && !access_artifacts
            .iter()
            .any(|artifact| artifact.name == ACCESS_ARTIFACT_KIND_EXACT_INDEX)
    {
        access_artifacts.push(IcebergAccessArtifact {
            name: ACCESS_ARTIFACT_KIND_EXACT_INDEX.to_string(),
            kind: ACCESS_ARTIFACT_KIND_EXACT_INDEX.to_string(),
            fields: fields.to_vec(),
            exact: true,
            supports_eq: true,
            supports_range: false,
            supports_order: false,
        });
    }

    access_artifacts.sort_by(|left, right| left.name.cmp(&right.name));
}

fn append_secondary_pattern_artifacts(
    access_artifacts: &mut Vec<IcebergAccessArtifact>,
    serving: &ServingTableConfig,
    sort_order: &[IcebergSortField],
) {
    for pattern in serving.patterns.iter() {
        if !pattern_is_secondary(pattern) {
            continue;
        }
        if !secondary_pattern_is_supported(pattern, access_artifacts, sort_order) {
            continue;
        }
        let name = secondary_pattern_artifact_name(&pattern.name);
        if access_artifacts
            .iter()
            .any(|artifact| artifact.name == name)
        {
            continue;
        }
        let mut fields = pattern.eq_fields.clone();
        if let Some(range_field) = pattern.range_field.as_ref() {
            push_unique_string(&mut fields, range_field.clone());
        }
        if let Some(order_field) = pattern.order_field.as_ref() {
            push_unique_string(&mut fields, order_field.clone());
        }
        access_artifacts.push(IcebergAccessArtifact {
            name,
            kind: ACCESS_ARTIFACT_KIND_SECONDARY_PATTERN.to_string(),
            fields,
            exact: false,
            supports_eq: !pattern.eq_fields.is_empty(),
            supports_range: pattern.range_field.is_some(),
            supports_order: pattern.order_field.is_some(),
        });
    }
    access_artifacts.sort_by(|left, right| left.name.cmp(&right.name));
}

fn pattern_is_secondary(pattern: &ServingPattern) -> bool {
    pattern.aggregate.is_none()
        && (pattern.eq_fields.len() > 1
            || pattern.range_field.is_some()
            || (!pattern.eq_fields.is_empty() && pattern.order_field.is_some()))
}

fn secondary_pattern_is_supported(
    pattern: &ServingPattern,
    access_artifacts: &[IcebergAccessArtifact],
    sort_order: &[IcebergSortField],
) -> bool {
    pattern.eq_fields.iter().all(|field| {
        artifact_supports_field_for_eq(access_artifacts, field)
            || artifact_supports_field_for_range(access_artifacts, field)
    }) && pattern
        .range_field
        .as_ref()
        .map(|field| artifact_supports_field_for_range(access_artifacts, field))
        .unwrap_or(true)
        && pattern
            .order_field
            .as_ref()
            .map(|field| sort_order_supports_field(sort_order, field))
            .unwrap_or(true)
}

fn artifact_supports_field_for_eq(access_artifacts: &[IcebergAccessArtifact], field: &str) -> bool {
    access_artifacts.iter().any(|artifact| {
        artifact.supports_eq
            && artifact.fields.iter().any(|candidate| candidate == field)
            && matches!(
                artifact.kind.as_str(),
                ACCESS_ARTIFACT_KIND_EXACT_INDEX
                    | ACCESS_ARTIFACT_KIND_EXACT_PRUNING
                    | ACCESS_ARTIFACT_KIND_PARTITION_SPEC
            )
    })
}

fn artifact_supports_field_for_range(
    access_artifacts: &[IcebergAccessArtifact],
    field: &str,
) -> bool {
    access_artifacts.iter().any(|artifact| {
        artifact.supports_range && artifact.fields.iter().any(|candidate| candidate == field)
    })
}

fn secondary_pattern_artifact_name(pattern_name: &str) -> String {
    format!(
        "{}:{}",
        ACCESS_ARTIFACT_KIND_SECONDARY_PATTERN, pattern_name
    )
}

fn request_uses_exact_filters(request: &ServingRequestPlan) -> bool {
    request
        .filters
        .iter()
        .any(|predicate| predicate.eq.is_some() || predicate.in_values.is_some())
}

fn exact_pruning_extension_file(extension_files: &[ExtensionFile]) -> Option<&ExtensionFile> {
    extension_files
        .iter()
        .find(|extension| extension.suffix == "exact_pruning")
}

async fn prune_execution_files_with_exact_artifact(
    files: &[FileDescriptor],
    extension_files: &HashMap<String, Vec<ExtensionFile>>,
    request: &ServingRequestPlan,
) -> Result<(Vec<FileDescriptor>, Vec<String>), ServingQueryError> {
    if files.is_empty() || !request_uses_exact_filters(request) {
        return Ok((files.to_vec(), vec![]));
    }
    if !files.iter().all(|file| {
        extension_files
            .get(&file.file_path)
            .is_some_and(|extensions| exact_pruning_extension_file(extensions).is_some())
    }) {
        return Ok((files.to_vec(), vec![]));
    }

    let mut retained = vec![];
    let mut used_exact_pruning = false;
    for file in files.iter().cloned() {
        let Some(extensions) = extension_files.get(&file.file_path) else {
            retained.push(file);
            continue;
        };
        let Some(summary) =
            load_exact_pruning_summary_for_serving(&file.file_path, extensions).await?
        else {
            retained.push(file);
            continue;
        };
        used_exact_pruning = true;
        if exact_pruning_summary_may_match_request(&summary, request) {
            retained.push(file);
        }
    }

    Ok((
        retained,
        if used_exact_pruning {
            vec![ACCESS_ARTIFACT_KIND_EXACT_PRUNING.to_string()]
        } else {
            vec![]
        },
    ))
}

async fn load_exact_pruning_summary_for_serving(
    base_file_path: &String,
    extension_files: &[ExtensionFile],
) -> Result<Option<ExactPruningSummary>, ServingQueryError> {
    let Some(extension_file) = exact_pruning_extension_file(extension_files) else {
        return Ok(None);
    };
    if let Some(cached) = EXACT_PRUNING_SUMMARY_CACHE
        .lock()
        .unwrap()
        .get(&extension_file.location)
        .cloned()
    {
        return Ok(Some(cached));
    }

    let local_name = format!("serving_exact_pruning_{}", IdInstance::next_id());
    data_access::reserve(&local_name, 1, vec![]).await;
    let result = async {
        data_access::load_file_as_table(&local_name, &extension_file.location, true, None)
            .await
            .map_err(|error| {
                ServingQueryError::new(StatusCode::SERVICE_UNAVAILABLE, &error.to_string())
            })?;
        let sql = format!("SELECT field_name, field_value, complete FROM {local_name}");
        let batches = execute_sql_async(&sql).await.map_err(|error| {
            ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.to_string())
        })?;
        let serde_result = batches_to_serde_value(&batches).await.map_err(|error| {
            ServingQueryError::new(StatusCode::UNPROCESSABLE_ENTITY, &error.message)
        })?;
        Ok::<ExactPruningSummary, ServingQueryError>(exact_pruning_summary_from_rows(
            serde_result.values,
        ))
    }
    .await;
    data_access::release(&local_name).await;
    let summary = result?;
    EXACT_PRUNING_SUMMARY_CACHE
        .lock()
        .unwrap()
        .insert(extension_file.location.clone(), summary.clone());
    let _ = base_file_path;
    Ok(Some(summary))
}

fn exact_pruning_summary_from_rows(rows: Vec<Value>) -> ExactPruningSummary {
    let mut summary = ExactPruningSummary::new();
    for row in rows {
        let Some(field_name) = row.get("field_name").and_then(|value| value.as_str()) else {
            continue;
        };
        let complete = row
            .get("complete")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let entry = summary
            .entry(field_name.to_string())
            .or_insert_with(ExactPruningFieldSummary::default);
        if entry.values.is_empty() && !entry.complete {
            entry.complete = complete;
        } else {
            entry.complete &= complete;
        }
        if let Some(field_value) = row.get("field_value").and_then(|value| value.as_str()) {
            entry.values.insert(field_value.to_string());
        }
    }
    summary
}

fn exact_pruning_summary_may_match_request(
    summary: &ExactPruningSummary,
    request: &ServingRequestPlan,
) -> bool {
    for predicate in request.filters.iter() {
        let Some(field_summary) = summary.get(&predicate.field) else {
            continue;
        };
        if let Some(eq) = predicate.eq.as_ref() {
            if field_summary.complete && !exact_pruning_value_matches(field_summary, eq) {
                return false;
            }
        }
        if let Some(in_values) = predicate.in_values.as_ref() {
            if field_summary.complete
                && !in_values
                    .iter()
                    .any(|value| exact_pruning_value_matches(field_summary, value))
            {
                return false;
            }
        }
    }
    true
}

fn exact_pruning_value_matches(summary: &ExactPruningFieldSummary, value: &Value) -> bool {
    render_exact_pruning_value(value)
        .map(|candidate| summary.values.contains(&candidate))
        .unwrap_or(true)
}

fn render_exact_pruning_value(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(|text| text.to_string())
        .or_else(|| value.as_i64().map(|numeric| numeric.to_string()))
        .or_else(|| value.as_u64().map(|numeric| numeric.to_string()))
        .or_else(|| value.as_f64().map(|numeric| numeric.to_string()))
        .or_else(|| value.as_bool().map(|boolean| boolean.to_string()))
}

fn prune_candidate_files(
    files: &[FileDescriptor],
    file_stats: &HashMap<String, IcebergFileStats>,
    request: &ServingRequestPlan,
) -> PrunedFileSelection {
    let mut pruned = PrunedFileSelection::default();

    for file in files.iter().cloned() {
        let Some(stats) = file_stats.get(&file.file_path) else {
            pruned.estimated_bytes += file.size;
            pruned.files_selected += 1;
            record_metadata_cache_coverage(&mut pruned, &file.file_path);
            pruned.selected_files.push(file);
            continue;
        };

        if !partition_values_may_match_request(stats, request, &mut pruned) {
            continue;
        }

        if stats.row_groups.is_empty() {
            record_file_stats_artifact_usage(&mut pruned, stats, request);
            if file_may_match_request(stats, request) {
                pruned.estimated_bytes += file.size;
                pruned.files_selected += 1;
                record_metadata_cache_coverage(&mut pruned, &file.file_path);
                pruned.selected_files.push(file);
            }
            continue;
        }

        pruned.row_groups_considered += stats.row_groups.len();
        let matching_row_groups = stats
            .row_groups
            .iter()
            .filter(|row_group| row_group_may_match_request(row_group, request))
            .collect::<Vec<_>>();
        if matching_row_groups.is_empty() {
            continue;
        }

        if !request.filters.is_empty() {
            record_artifact_usage(&mut pruned, ACCESS_ARTIFACT_KIND_ROW_GROUP_STATS);
        }
        pruned.files_selected += 1;
        pruned.row_groups_selected += matching_row_groups.len();
        pruned.estimated_bytes += matching_row_groups
            .iter()
            .map(|row_group| row_group.compressed_bytes)
            .sum::<u64>();
        record_page_pruning_coverage(&mut pruned, &matching_row_groups, request);
        record_metadata_cache_coverage(&mut pruned, &file.file_path);
        pruned.selected_files.push(file);
    }

    pruned
}

fn record_artifact_usage(pruned: &mut PrunedFileSelection, artifact_name: &str) {
    push_unique_string(&mut pruned.artifacts_used, artifact_name.to_string());
}

fn record_metadata_cache_coverage(pruned: &mut PrunedFileSelection, file_path: &str) {
    let coverage = data_access::cached_parquet_row_group_stats_coverage(&[file_path.to_string()]);
    pruned.metadata_files_cached += coverage.files_cached;
    pruned.metadata_row_groups_cached += coverage.row_groups_cached;
}

fn record_file_stats_artifact_usage(
    pruned: &mut PrunedFileSelection,
    file_stats: &IcebergFileStats,
    request: &ServingRequestPlan,
) {
    if request.filters.is_empty() {
        return;
    }
    if request.filters.iter().any(|predicate| {
        file_stats
            .columns
            .iter()
            .any(|column| column.field_name == predicate.field)
    }) {
        record_artifact_usage(pruned, ACCESS_ARTIFACT_KIND_FILE_STATS);
    }
}

fn record_page_pruning_coverage(
    pruned: &mut PrunedFileSelection,
    matching_row_groups: &[&IcebergRowGroupStats],
    request: &ServingRequestPlan,
) {
    if request.filters.is_empty() {
        return;
    }

    pruned.page_index_row_groups_selected += matching_row_groups
        .iter()
        .filter(|row_group| row_group.page_index_present)
        .count();
    if pruned.page_index_row_groups_selected > 0 {
        record_artifact_usage(pruned, ACCESS_ARTIFACT_KIND_PAGE_INDEX);
    }
    pruned.bloom_filter_row_groups_selected += matching_row_groups
        .iter()
        .filter(|row_group| row_group.bloom_filter_present)
        .count();
    if pruned.bloom_filter_row_groups_selected > 0 {
        record_artifact_usage(pruned, ACCESS_ARTIFACT_KIND_BLOOM_FILTER);
    }
}

fn partition_values_may_match_request(
    file_stats: &IcebergFileStats,
    request: &ServingRequestPlan,
    pruned: &mut PrunedFileSelection,
) -> bool {
    for predicate in request.filters.iter() {
        let Some((may_match, artifact_name)) =
            partition_predicate_may_match_file(file_stats, predicate)
        else {
            continue;
        };
        record_artifact_usage(pruned, &artifact_name);
        if !may_match {
            return false;
        }
    }

    true
}

fn partition_predicate_may_match_file(
    file_stats: &IcebergFileStats,
    predicate: &ServingPredicate,
) -> Option<(bool, String)> {
    let partition_value = file_stats.partition_values.iter().find(|partition_value| {
        partition_value.source_field_name == predicate.field
            && partition_value.transform == "identity"
    })?;
    let value = partition_value.value.as_ref()?;
    let may_match = if let Some(eq) = predicate.eq.as_ref() {
        compare_scalar_values(eq, value) == Some(Ordering::Equal)
    } else if let Some(in_values) = predicate.in_values.as_ref() {
        in_values
            .iter()
            .any(|candidate| compare_scalar_values(candidate, value) == Some(Ordering::Equal))
    } else {
        partition_range_may_match(value, predicate)
    };
    Some((
        may_match,
        format!(
            "{}:{}",
            ACCESS_ARTIFACT_KIND_PARTITION_SPEC, partition_value.field_name
        ),
    ))
}

fn partition_range_may_match(value: &Value, predicate: &ServingPredicate) -> bool {
    if let Some(candidate) = predicate.gt.as_ref() {
        if matches!(
            compare_scalar_values(value, candidate),
            Some(Ordering::Less | Ordering::Equal)
        ) {
            return false;
        }
    }
    if let Some(candidate) = predicate.gte.as_ref() {
        if matches!(
            compare_scalar_values(value, candidate),
            Some(Ordering::Less)
        ) {
            return false;
        }
    }
    if let Some(candidate) = predicate.lt.as_ref() {
        if matches!(
            compare_scalar_values(value, candidate),
            Some(Ordering::Greater | Ordering::Equal)
        ) {
            return false;
        }
    }
    if let Some(candidate) = predicate.lte.as_ref() {
        if matches!(
            compare_scalar_values(value, candidate),
            Some(Ordering::Greater)
        ) {
            return false;
        }
    }
    true
}

fn file_may_match_request(file_stats: &IcebergFileStats, request: &ServingRequestPlan) -> bool {
    file_may_match_predicates(file_stats, &serving_request_predicates(request))
}

fn row_group_may_match_request(
    row_group_stats: &IcebergRowGroupStats,
    request: &ServingRequestPlan,
) -> bool {
    row_group_may_match_predicates(row_group_stats, &serving_request_predicates(request))
}

fn serving_request_predicates(request: &ServingRequestPlan) -> Vec<QueryPredicate> {
    request
        .filters
        .iter()
        .map(|predicate| QueryPredicate {
            field: predicate.field.clone(),
            eq: predicate.eq.clone(),
            in_values: predicate.in_values.clone(),
            gt: predicate.gt.clone(),
            gte: predicate.gte.clone(),
            lt: predicate.lt.clone(),
            lte: predicate.lte.clone(),
        })
        .collect()
}

fn build_sql(
    table_name: &str,
    request: &ServingRequestPlan,
    limit: usize,
    include_delete_filter: bool,
) -> Result<String, ServingQueryError> {
    if request.aggregate.is_some() {
        return build_aggregate_sql(table_name, request, limit, include_delete_filter, false);
    }

    let select = match normalized_select(request.select.clone()) {
        Some(select_fields) => select_fields
            .iter()
            .map(|field| format!("\"{}\"", escape_identifier(field)))
            .collect::<Vec<_>>()
            .join(", "),
        None => {
            if include_delete_filter {
                "t.*".to_string()
            } else {
                "*".to_string()
            }
        }
    };

    let mut where_clauses = request
        .filters
        .iter()
        .map(sql_for_filter)
        .collect::<Result<Vec<_>, _>>()?;
    let mut sql = format!("SELECT {} FROM {} t", select, table_name);
    if include_delete_filter {
        sql.push_str(" LEFT JOIN {deletes_table} dt ON dt._id_seq_no = t.\"_id_seq_no\"");
        where_clauses.push("dt._id_seq_no IS NULL".to_string());
    }
    if !where_clauses.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_clauses.join(" AND ")));
    }
    if let Some(sort) = request.order_by.first() {
        sql.push_str(&format!(
            " ORDER BY \"{}\" {}",
            escape_identifier(&sort.field),
            if sort.descending { "DESC" } else { "ASC" }
        ));
    }
    sql.push_str(&format!(" LIMIT {}", limit));
    Ok(sql)
}

fn build_aggregate_sql(
    table_name: &str,
    request: &ServingRequestPlan,
    limit: usize,
    include_delete_filter: bool,
    partial: bool,
) -> Result<String, ServingQueryError> {
    let aggregate = request.aggregate.as_ref().ok_or_else(|| {
        ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            "Aggregate serving query is missing aggregate specification",
        )
    })?;
    let measure_plans = aggregate_measure_plans(aggregate)?;
    let mut select_parts = aggregate
        .group_by
        .iter()
        .map(|field| format!("\"{}\"", escape_identifier(field)))
        .collect::<Vec<_>>();
    for plan in measure_plans.iter() {
        match (plan.function, partial) {
            (AggregateFunction::Count, false) => select_parts.push(format!(
                "COUNT(*) AS \"{}\"",
                escape_identifier(&plan.alias)
            )),
            (AggregateFunction::Count, true) => select_parts.push(format!(
                "COUNT(*) AS \"{}\"",
                escape_identifier(&plan.partial_value_alias)
            )),
            (AggregateFunction::Sum, false) => select_parts.push(format!(
                "SUM(\"{}\") AS \"{}\"",
                escape_identifier(plan.field.as_ref().unwrap()),
                escape_identifier(&plan.alias)
            )),
            (AggregateFunction::Sum, true) => select_parts.push(format!(
                "SUM(\"{}\") AS \"{}\"",
                escape_identifier(plan.field.as_ref().unwrap()),
                escape_identifier(&plan.partial_value_alias)
            )),
            (AggregateFunction::Avg, false) => select_parts.push(format!(
                "AVG(\"{}\") AS \"{}\"",
                escape_identifier(plan.field.as_ref().unwrap()),
                escape_identifier(&plan.alias)
            )),
            (AggregateFunction::Avg, true) => {
                select_parts.push(format!(
                    "SUM(\"{}\") AS \"{}\"",
                    escape_identifier(plan.field.as_ref().unwrap()),
                    escape_identifier(&plan.partial_value_alias)
                ));
                select_parts.push(format!(
                    "COUNT(\"{}\") AS \"{}\"",
                    escape_identifier(plan.field.as_ref().unwrap()),
                    escape_identifier(plan.partial_count_alias.as_ref().unwrap())
                ));
            }
            (AggregateFunction::Min, false) => select_parts.push(format!(
                "MIN(\"{}\") AS \"{}\"",
                escape_identifier(plan.field.as_ref().unwrap()),
                escape_identifier(&plan.alias)
            )),
            (AggregateFunction::Min, true) => select_parts.push(format!(
                "MIN(\"{}\") AS \"{}\"",
                escape_identifier(plan.field.as_ref().unwrap()),
                escape_identifier(&plan.partial_value_alias)
            )),
            (AggregateFunction::Max, false) => select_parts.push(format!(
                "MAX(\"{}\") AS \"{}\"",
                escape_identifier(plan.field.as_ref().unwrap()),
                escape_identifier(&plan.alias)
            )),
            (AggregateFunction::Max, true) => select_parts.push(format!(
                "MAX(\"{}\") AS \"{}\"",
                escape_identifier(plan.field.as_ref().unwrap()),
                escape_identifier(&plan.partial_value_alias)
            )),
        }
    }

    let mut where_clauses = request
        .filters
        .iter()
        .map(sql_for_filter)
        .collect::<Result<Vec<_>, _>>()?;
    let mut sql = format!("SELECT {} FROM {} t", select_parts.join(", "), table_name);
    if include_delete_filter {
        sql.push_str(" LEFT JOIN {deletes_table} dt ON dt._id_seq_no = t.\"_id_seq_no\"");
        where_clauses.push("dt._id_seq_no IS NULL".to_string());
    }
    if !where_clauses.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_clauses.join(" AND ")));
    }
    if !aggregate.group_by.is_empty() {
        sql.push_str(&format!(
            " GROUP BY {}",
            aggregate
                .group_by
                .iter()
                .map(|field| format!("\"{}\"", escape_identifier(field)))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    sql.push_str(&format!(
        " LIMIT {}",
        if aggregate.group_by.is_empty() {
            1
        } else {
            limit
        }
    ));
    Ok(sql)
}

fn aggregate_measure_plans(
    aggregate: &ServingAggregateSpec,
) -> Result<Vec<AggregateMeasurePlan>, ServingQueryError> {
    aggregate
        .measures
        .iter()
        .map(|measure| {
            let function = normalized_aggregate_function(&measure.function).ok_or_else(|| {
                ServingQueryError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("Unsupported aggregate function {}", measure.function),
                )
            })?;
            let alias = aggregate_measure_alias_with_function(measure, function)?;
            let function = match function {
                "count" => AggregateFunction::Count,
                "sum" => AggregateFunction::Sum,
                "avg" => AggregateFunction::Avg,
                "min" => AggregateFunction::Min,
                "max" => AggregateFunction::Max,
                _ => unreachable!(),
            };
            Ok(AggregateMeasurePlan {
                function,
                field: measure.field.clone(),
                alias: alias.clone(),
                partial_value_alias: format!("__partial_value_{}", alias),
                partial_count_alias: if function == AggregateFunction::Avg {
                    Some(format!("__partial_count_{}", alias))
                } else {
                    None
                },
            })
        })
        .collect()
}

fn sql_for_filter(filter: &ServingPredicate) -> Result<String, ServingQueryError> {
    let field = format!("\"{}\"", escape_identifier(&filter.field));
    if let Some(value) = filter.eq.as_ref() {
        return Ok(format!("{} = {}", field, sql_literal(value)?));
    }
    if let Some(values) = filter.in_values.as_ref() {
        let sql_values = values
            .iter()
            .map(sql_literal)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(format!("{} IN ({})", field, sql_values.join(", ")));
    }

    let mut parts = vec![];
    if let Some(value) = filter.gt.as_ref() {
        parts.push(format!("{} > {}", field, sql_literal(value)?));
    }
    if let Some(value) = filter.gte.as_ref() {
        parts.push(format!("{} >= {}", field, sql_literal(value)?));
    }
    if let Some(value) = filter.lt.as_ref() {
        parts.push(format!("{} < {}", field, sql_literal(value)?));
    }
    if let Some(value) = filter.lte.as_ref() {
        parts.push(format!("{} <= {}", field, sql_literal(value)?));
    }
    Ok(parts.join(" AND "))
}

fn sql_literal(value: &Value) -> Result<String, ServingQueryError> {
    match value {
        Value::String(text) => Ok(format!("'{}'", text.replace('\'', "''"))),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(boolean) => Ok(if *boolean {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        }),
        _ => Err(ServingQueryError::new(
            StatusCode::BAD_REQUEST,
            "Only scalar literals are supported in serving queries",
        )),
    }
}

fn normalized_select(select: Option<Vec<String>>) -> Option<Vec<String>> {
    match select {
        Some(fields) if fields.len() == 1 && fields[0] == "*" => None,
        Some(fields) => Some(fields),
        None => None,
    }
}

fn compare_row_values(left: Option<&Value>, right: Option<&Value>, descending: bool) -> Ordering {
    let ordering = compare_values(left.unwrap_or(&Value::Null), right.unwrap_or(&Value::Null));
    if descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn compare_sort_values(left: &Value, right: &Value, descending: bool) -> Ordering {
    compare_row_values(Some(left), Some(right), descending)
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        _ => {
            if let (Some(left_number), Some(right_number)) = (left.as_f64(), right.as_f64()) {
                return left_number
                    .partial_cmp(&right_number)
                    .unwrap_or(Ordering::Equal);
            }
            if let (Some(left_string), Some(right_string)) = (left.as_str(), right.as_str()) {
                return left_string.cmp(right_string);
            }
            if let (Some(left_bool), Some(right_bool)) = (left.as_bool(), right.as_bool()) {
                return left_bool.cmp(&right_bool);
            }
            left.to_string().cmp(&right.to_string())
        }
    }
}

fn escape_identifier(identifier: &str) -> String {
    identifier.replace('"', "\"\"")
}

async fn parse_json_body<T: for<'de> Deserialize<'de>>(state: &mut State) -> Result<T, String> {
    let valid_body = body::to_bytes(Body::take_from(state))
        .await
        .map_err(|error| error.to_string())?;
    serde_json::from_slice::<T>(&valid_body).map_err(|error| error.to_string())
}

fn json_response<T: Serialize>(
    state: &State,
    status: StatusCode,
    body: &T,
) -> gotham::hyper::Response<Body> {
    create_response(
        state,
        status,
        mime::APPLICATION_JSON,
        serde_json::to_string(body).unwrap(),
    )
}

fn json_error(message: &str) -> Value {
    serde_json::json!({ "error": message })
}

#[derive(Debug)]
pub struct ServingQueryError {
    pub status: StatusCode,
    pub message: String,
}

impl ServingQueryError {
    fn new(status: StatusCode, message: &str) -> Self {
        Self {
            status,
            message: message.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ACCESS_ARTIFACT_KIND_EXACT_PRUNING, ACCESS_ARTIFACT_KIND_ROW_GROUP_STATS,
        DEFAULT_FAST_PATH_MAX_DELETE_FILES, DEFAULT_SLOW_PATH_MAX_BYTES, ExactPruningFieldSummary,
        ServingExecutionContext, aggregate_measure_plans, append_secondary_pattern_artifacts,
        build_serving_layout_advice, build_serving_warmup_plan, build_sql, exact_artifact_fields,
        exact_pruning_summary_may_match_request, file_group_table_name, finalize_aggregate_rows,
        group_files_by_schema, merge_partial_aggregate_rows, ordered_file_groups_for_top_k,
        plan_request, prune_candidate_files, remaining_groups_cannot_beat_kth_row,
        request_matches_pattern, secondary_pattern_artifact_name, select_serving_warmup_files,
    };
    use crate::data_access::{
        prime_parquet_row_group_stats_cache_for_test, reset_serving_metadata_caches_for_test,
    };
    use crate::data_contract::{
        FileDescriptor, IcebergAccessArtifact, IcebergColumnStats, IcebergFileStats,
        IcebergRowGroupStats, IcebergSortField, ServingAggregateMeasure, ServingAggregateSpec,
        ServingPattern, ServingTableConfig, TableDescription,
    };
    use crate::peers::CheckpointDescriptor;
    use crate::schema_massager::{PowdrrDataType, PowdrrField, PowdrrSchema};
    use crate::serving_plan::{
        ServingPredicate, ServingQueryClassification, ServingRequestPlan, ServingSort,
    };
    use serde_json::json;
    use std::collections::{HashMap, HashSet};

    fn test_schema() -> PowdrrSchema {
        PowdrrSchema::from(&vec![
            PowdrrField {
                name: "score".to_string(),
                data_type: PowdrrDataType::Integer,
            },
            PowdrrField {
                name: "tenant".to_string(),
                data_type: PowdrrDataType::String,
            },
        ])
    }

    fn alternate_schema() -> PowdrrSchema {
        PowdrrSchema::from(&vec![
            PowdrrField {
                name: "score".to_string(),
                data_type: PowdrrDataType::Integer,
            },
            PowdrrField {
                name: "region".to_string(),
                data_type: PowdrrDataType::String,
            },
            PowdrrField {
                name: "tenant".to_string(),
                data_type: PowdrrDataType::String,
            },
        ])
    }

    fn test_files(schema: &PowdrrSchema) -> Vec<FileDescriptor> {
        vec![
            FileDescriptor {
                file_path: "file://first.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://second.parquet".to_string(),
                schema: schema.clone(),
                size: 200,
            },
        ]
    }

    fn test_context(
        serving: ServingTableConfig,
        file_stats: Vec<IcebergFileStats>,
    ) -> ServingExecutionContext {
        let schema = test_schema();
        ServingExecutionContext {
            description: TableDescription {
                name: "events".to_string(),
                tags: HashMap::new(),
                serving: Some(serving),
                dynamodb: None,
                mongodb: None,
            },
            checkpoint: CheckpointDescriptor::new("events".to_string(), "checkpoint_1".to_string()),
            schema: schema.clone(),
            files: test_files(&schema),
            speedboat_files: vec![],
            delete_files: vec![],
            file_stats: file_stats
                .into_iter()
                .map(|stats| (stats.file_path.clone(), stats))
                .collect(),
            partition_spec: vec![],
            sort_order: vec![],
            access_artifacts: vec![],
            extension_files: HashMap::new(),
            exact_artifact_fields: exact_artifact_fields(&schema),
            snapshot_id: Some("snapshot_1".to_string()),
            metadata_snapshot_cached: false,
        }
    }

    fn column_stats(
        field_name: &str,
        null_count: Option<u64>,
        lower_bound: Option<serde_json::Value>,
        upper_bound: Option<serde_json::Value>,
    ) -> IcebergColumnStats {
        IcebergColumnStats {
            field_id: if field_name == "tenant" { 1 } else { 2 },
            field_name: field_name.to_string(),
            null_count,
            lower_bound,
            upper_bound,
        }
    }

    fn file_stats(
        file_path: &str,
        record_count: Option<u64>,
        columns: Vec<IcebergColumnStats>,
    ) -> IcebergFileStats {
        IcebergFileStats {
            file_path: file_path.to_string(),
            record_count,
            columns,
            partition_values: vec![],
            row_groups: vec![],
        }
    }

    fn row_group_stats(
        row_group_index: usize,
        record_count: Option<u64>,
        compressed_bytes: u64,
        columns: Vec<IcebergColumnStats>,
    ) -> IcebergRowGroupStats {
        IcebergRowGroupStats {
            row_group_index,
            record_count,
            compressed_bytes,
            page_index_present: false,
            bloom_filter_present: false,
            columns,
        }
    }

    fn aggregate_spec() -> ServingAggregateSpec {
        ServingAggregateSpec {
            group_by: vec!["tenant".to_string()],
            measures: vec![
                ServingAggregateMeasure {
                    function: "count".to_string(),
                    field: None,
                    alias: None,
                },
                ServingAggregateMeasure {
                    function: "sum".to_string(),
                    field: Some("score".to_string()),
                    alias: None,
                },
                ServingAggregateMeasure {
                    function: "avg".to_string(),
                    field: Some("score".to_string()),
                    alias: None,
                },
                ServingAggregateMeasure {
                    function: "min".to_string(),
                    field: Some("score".to_string()),
                    alias: None,
                },
                ServingAggregateMeasure {
                    function: "max".to_string(),
                    field: Some("score".to_string()),
                    alias: None,
                },
            ],
        }
    }

    fn test_file_stats_for_aggregate() -> Vec<IcebergFileStats> {
        vec![file_stats(
            "file://first.parquet",
            Some(10),
            vec![
                column_stats("tenant", Some(0), Some(json!("acme")), Some(json!("omega"))),
                column_stats("score", Some(0), Some(json!(0)), Some(json!(100))),
            ],
        )]
    }

    #[test]
    fn test_build_sql_for_range_top_n() {
        let sql = build_sql(
            "{table}",
            &ServingRequestPlan {
                select: Some(vec!["title".to_string()]),
                filters: vec![ServingPredicate {
                    field: "snippet".to_string(),
                    eq: Some(json!("hello")),
                    in_values: None,
                    gt: None,
                    gte: None,
                    lt: None,
                    lte: None,
                }],
                aggregate: None,
                order_by: vec![ServingSort {
                    field: "title".to_string(),
                    descending: false,
                }],
                limit: Some(5),
                allow_slow_path: false,
                explain: false,
            },
            5,
            false,
        )
        .unwrap();

        assert_eq!(
            sql,
            "SELECT \"title\" FROM {table} t WHERE \"snippet\" = 'hello' ORDER BY \"title\" ASC LIMIT 5"
        );
    }

    #[test]
    fn test_request_matches_pattern() {
        let request = ServingRequestPlan {
            select: Some(vec!["title".to_string()]),
            filters: vec![],
            aggregate: None,
            order_by: vec![ServingSort {
                field: "title".to_string(),
                descending: false,
            }],
            limit: Some(3),
            allow_slow_path: false,
            explain: false,
        };
        let pattern = ServingPattern {
            name: "recent_titles".to_string(),
            eq_fields: vec![],
            range_field: None,
            order_field: Some("title".to_string()),
            descending: false,
            max_limit: Some(10),
            projection: Some(vec!["title".to_string()]),
            aggregate: None,
        };

        assert!(request_matches_pattern(&request, &pattern, 3));
    }

    #[test]
    fn test_request_matches_aggregate_pattern() {
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            aggregate: Some(aggregate_spec()),
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };
        let pattern = ServingPattern {
            name: "tenant_status_counts".to_string(),
            eq_fields: vec!["tenant".to_string()],
            range_field: None,
            order_field: None,
            descending: false,
            max_limit: Some(10),
            projection: None,
            aggregate: Some(aggregate_spec()),
        };

        assert!(request_matches_pattern(&request, &pattern, 10));
    }

    #[test]
    fn test_build_sql_for_grouped_aggregate() {
        let sql = build_sql(
            "{table}",
            &ServingRequestPlan {
                select: None,
                filters: vec![ServingPredicate {
                    field: "tenant".to_string(),
                    eq: Some(json!("acme")),
                    in_values: None,
                    gt: None,
                    gte: None,
                    lt: None,
                    lte: None,
                }],
                aggregate: Some(aggregate_spec()),
                order_by: vec![],
                limit: Some(10),
                allow_slow_path: false,
                explain: false,
            },
            10,
            false,
        )
        .unwrap();

        assert_eq!(
            sql,
            "SELECT \"tenant\", COUNT(*) AS \"count\", SUM(\"score\") AS \"sum_score\", AVG(\"score\") AS \"avg_score\", MIN(\"score\") AS \"min_score\", MAX(\"score\") AS \"max_score\" FROM {table} t WHERE \"tenant\" = 'acme' GROUP BY \"tenant\" LIMIT 10"
        );
    }

    #[test]
    fn test_plan_request_prunes_files_for_fast_path_eq_query() {
        let context = test_context(
            ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "tenant_lookup".to_string(),
                    eq_fields: vec!["tenant".to_string()],
                    range_field: None,
                    order_field: None,
                    descending: false,
                    max_limit: Some(25),
                    projection: None,
                    aggregate: None,
                }],
            },
            vec![
                file_stats(
                    "file://first.parquet",
                    Some(10),
                    vec![column_stats(
                        "tenant",
                        Some(0),
                        Some(json!("acme")),
                        Some(json!("acme")),
                    )],
                ),
                file_stats(
                    "file://second.parquet",
                    Some(10),
                    vec![column_stats(
                        "tenant",
                        Some(0),
                        Some(json!("omega")),
                        Some(json!("omega")),
                    )],
                ),
            ],
        );
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            aggregate: None,
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let plan = plan_request(&context, &request).unwrap();

        assert_eq!(plan.classification, ServingQueryClassification::FastPath);
        assert_eq!(plan.files_considered, 2);
        assert_eq!(plan.files_selected, 1);
        assert_eq!(plan.row_groups_considered, 0);
        assert_eq!(plan.row_groups_selected, 0);
        assert_eq!(plan.estimated_bytes, 100);
        assert_eq!(
            plan.selected_files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://first.parquet"]
        );
    }

    #[test]
    fn test_plan_request_prunes_files_for_range_query() {
        let mut first = file_stats(
            "file://first.parquet",
            Some(20),
            vec![column_stats(
                "score",
                Some(0),
                Some(json!(0)),
                Some(json!(100)),
            )],
        );
        first.row_groups = vec![
            row_group_stats(
                0,
                Some(10),
                25,
                vec![column_stats(
                    "score",
                    Some(0),
                    Some(json!(0)),
                    Some(json!(10)),
                )],
            ),
            row_group_stats(
                1,
                Some(10),
                25,
                vec![column_stats(
                    "score",
                    Some(0),
                    Some(json!(20)),
                    Some(json!(30)),
                )],
            ),
        ];
        let mut second = file_stats(
            "file://second.parquet",
            Some(10),
            vec![column_stats(
                "score",
                Some(0),
                Some(json!(60)),
                Some(json!(100)),
            )],
        );
        second.row_groups = vec![row_group_stats(
            0,
            Some(10),
            40,
            vec![column_stats(
                "score",
                Some(0),
                Some(json!(60)),
                Some(json!(100)),
            )],
        )];
        let context = test_context(
            ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "score_scan".to_string(),
                    eq_fields: vec![],
                    range_field: Some("score".to_string()),
                    order_field: None,
                    descending: false,
                    max_limit: None,
                    projection: None,
                    aggregate: None,
                }],
            },
            vec![first, second],
        );
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "score".to_string(),
                eq: None,
                in_values: None,
                gt: None,
                gte: Some(json!(50)),
                lt: None,
                lte: None,
            }],
            aggregate: None,
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let plan = plan_request(&context, &request).unwrap();

        assert_eq!(plan.classification, ServingQueryClassification::FastPath);
        assert_eq!(plan.files_selected, 1);
        assert_eq!(plan.row_groups_considered, 3);
        assert_eq!(plan.row_groups_selected, 1);
        assert_eq!(plan.estimated_bytes, 40);
        assert_eq!(
            plan.selected_files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://second.parquet"]
        );
    }

    #[test]
    fn test_plan_request_reports_cached_row_group_metadata_coverage() {
        reset_serving_metadata_caches_for_test();

        let cached_row_groups = vec![row_group_stats(
            0,
            Some(10),
            25,
            vec![column_stats(
                "tenant",
                Some(0),
                Some(json!("acme")),
                Some(json!("acme")),
            )],
        )];
        prime_parquet_row_group_stats_cache_for_test("file://first.parquet", &cached_row_groups);

        let mut first = file_stats(
            "file://first.parquet",
            Some(10),
            vec![column_stats(
                "tenant",
                Some(0),
                Some(json!("acme")),
                Some(json!("acme")),
            )],
        );
        first.row_groups = cached_row_groups;
        let context = test_context(
            ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "tenant_lookup".to_string(),
                    eq_fields: vec!["tenant".to_string()],
                    range_field: None,
                    order_field: None,
                    descending: false,
                    max_limit: Some(25),
                    projection: None,
                    aggregate: None,
                }],
            },
            vec![
                first,
                file_stats(
                    "file://second.parquet",
                    Some(10),
                    vec![column_stats(
                        "tenant",
                        Some(0),
                        Some(json!("omega")),
                        Some(json!("omega")),
                    )],
                ),
            ],
        );
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            aggregate: None,
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let plan = plan_request(&context, &request).unwrap();

        assert_eq!(plan.metadata_files_cached, 1);
        assert_eq!(plan.metadata_row_groups_cached, 1);

        reset_serving_metadata_caches_for_test();
    }

    #[test]
    fn test_plan_request_reports_page_and_bloom_coverage_for_selected_row_groups() {
        let mut first = file_stats(
            "file://first.parquet",
            Some(20),
            vec![column_stats(
                "tenant",
                Some(0),
                Some(json!("acme")),
                Some(json!("omega")),
            )],
        );
        first.row_groups = vec![
            IcebergRowGroupStats {
                row_group_index: 0,
                record_count: Some(10),
                compressed_bytes: 25,
                page_index_present: true,
                bloom_filter_present: true,
                columns: vec![column_stats(
                    "tenant",
                    Some(0),
                    Some(json!("acme")),
                    Some(json!("acme")),
                )],
            },
            IcebergRowGroupStats {
                row_group_index: 1,
                record_count: Some(10),
                compressed_bytes: 25,
                page_index_present: true,
                bloom_filter_present: false,
                columns: vec![column_stats(
                    "tenant",
                    Some(0),
                    Some(json!("omega")),
                    Some(json!("omega")),
                )],
            },
        ];
        let context = test_context(
            ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "tenant_lookup".to_string(),
                    eq_fields: vec!["tenant".to_string()],
                    range_field: None,
                    order_field: None,
                    descending: false,
                    max_limit: Some(25),
                    projection: None,
                    aggregate: None,
                }],
            },
            vec![first],
        );
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            aggregate: None,
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let plan = plan_request(&context, &request).unwrap();

        assert_eq!(plan.row_groups_selected, 1);
        assert_eq!(plan.page_index_row_groups_selected, 1);
        assert_eq!(plan.bloom_filter_row_groups_selected, 1);
    }

    #[test]
    fn test_plan_request_rejects_query_over_serving_budget() {
        let schema = test_schema();
        let context = ServingExecutionContext {
            description: TableDescription {
                name: "events".to_string(),
                tags: HashMap::new(),
                serving: Some(ServingTableConfig::default()),
                dynamodb: None,
                mongodb: None,
            },
            checkpoint: CheckpointDescriptor::new("events".to_string(), "checkpoint_1".to_string()),
            schema: schema.clone(),
            files: vec![FileDescriptor {
                file_path: "file://huge.parquet".to_string(),
                schema: schema.clone(),
                size: DEFAULT_SLOW_PATH_MAX_BYTES + 1,
            }],
            speedboat_files: vec![],
            delete_files: vec![],
            file_stats: HashMap::from([(
                "file://huge.parquet".to_string(),
                file_stats(
                    "file://huge.parquet",
                    Some(10),
                    vec![column_stats(
                        "tenant",
                        Some(0),
                        Some(json!("acme")),
                        Some(json!("acme")),
                    )],
                ),
            )]),
            partition_spec: vec![],
            sort_order: vec![],
            access_artifacts: vec![],
            extension_files: HashMap::new(),
            exact_artifact_fields: exact_artifact_fields(&schema),
            snapshot_id: Some("snapshot_1".to_string()),
            metadata_snapshot_cached: false,
        };
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            aggregate: None,
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let plan = plan_request(&context, &request).unwrap();

        assert_eq!(plan.classification, ServingQueryClassification::Rejected);
        assert!(
            plan.reason
                .as_ref()
                .is_some_and(|reason| reason.contains("exceeds serving budget"))
        );
    }

    #[test]
    fn test_plan_request_rejects_aggregate_without_declared_pattern() {
        let context = test_context(
            ServingTableConfig::default(),
            test_file_stats_for_aggregate(),
        );
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            aggregate: Some(aggregate_spec()),
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let plan = plan_request(&context, &request).unwrap();

        assert_eq!(plan.classification, ServingQueryClassification::Rejected);
        assert!(
            plan.reason
                .as_ref()
                .is_some_and(|reason| reason.contains("aggregate serving pattern"))
        );
    }

    #[test]
    fn test_merge_partial_aggregate_rows_merges_groups_correctly() {
        let aggregate = aggregate_spec();
        let measure_plans = aggregate_measure_plans(&aggregate).unwrap();
        let mut merged = HashMap::new();

        merge_partial_aggregate_rows(
            &mut merged,
            vec![
                json!({
                    "tenant": "acme",
                    "__partial_value_count": 2,
                    "__partial_value_sum_score": 30,
                    "__partial_value_avg_score": 30,
                    "__partial_count_avg_score": 2,
                    "__partial_value_min_score": 10,
                    "__partial_value_max_score": 20
                }),
                json!({
                    "tenant": "omega",
                    "__partial_value_count": 1,
                    "__partial_value_sum_score": 5,
                    "__partial_value_avg_score": 5,
                    "__partial_count_avg_score": 1,
                    "__partial_value_min_score": 5,
                    "__partial_value_max_score": 5
                }),
            ],
            &aggregate,
            &measure_plans,
        )
        .unwrap();
        merge_partial_aggregate_rows(
            &mut merged,
            vec![json!({
                "tenant": "acme",
                "__partial_value_count": 1,
                "__partial_value_sum_score": 30,
                "__partial_value_avg_score": 30,
                "__partial_count_avg_score": 1,
                "__partial_value_min_score": 30,
                "__partial_value_max_score": 30
            })],
            &aggregate,
            &measure_plans,
        )
        .unwrap();

        let rows = finalize_aggregate_rows(merged, &aggregate, &measure_plans, 10).unwrap();

        assert_eq!(
            rows,
            vec![
                json!({
                    "tenant": "acme",
                    "count": 3,
                    "sum_score": 60,
                    "avg_score": 20,
                    "min_score": 10,
                    "max_score": 30
                }),
                json!({
                    "tenant": "omega",
                    "count": 1,
                    "sum_score": 5,
                    "avg_score": 5,
                    "min_score": 5,
                    "max_score": 5
                })
            ]
        );
    }

    #[test]
    fn test_append_secondary_pattern_artifacts_adds_supported_pattern() {
        let serving = ServingTableConfig {
            patterns: vec![ServingPattern {
                name: "tenant_recent".to_string(),
                eq_fields: vec!["tenant".to_string()],
                range_field: Some("score".to_string()),
                order_field: Some("score".to_string()),
                descending: true,
                max_limit: Some(10),
                projection: None,
                aggregate: None,
            }],
        };
        let mut access_artifacts = vec![
            IcebergAccessArtifact {
                name: ACCESS_ARTIFACT_KIND_EXACT_PRUNING.to_string(),
                kind: ACCESS_ARTIFACT_KIND_EXACT_PRUNING.to_string(),
                fields: vec!["tenant".to_string(), "score".to_string()],
                exact: true,
                supports_eq: true,
                supports_range: false,
                supports_order: false,
            },
            IcebergAccessArtifact {
                name: ACCESS_ARTIFACT_KIND_ROW_GROUP_STATS.to_string(),
                kind: ACCESS_ARTIFACT_KIND_ROW_GROUP_STATS.to_string(),
                fields: vec!["score".to_string()],
                exact: false,
                supports_eq: true,
                supports_range: true,
                supports_order: false,
            },
        ];

        append_secondary_pattern_artifacts(
            &mut access_artifacts,
            &serving,
            &[IcebergSortField {
                source_field_id: 1,
                source_field_name: "score".to_string(),
                transform: "identity".to_string(),
                descending: true,
                nulls_first: false,
            }],
        );

        assert!(
            access_artifacts.iter().any(|artifact| {
                artifact.name == secondary_pattern_artifact_name("tenant_recent")
            })
        );
    }

    #[test]
    fn test_plan_request_demotes_fast_path_when_delete_files_are_high() {
        let mut context = test_context(
            ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "tenant_lookup".to_string(),
                    eq_fields: vec!["tenant".to_string()],
                    range_field: None,
                    order_field: None,
                    descending: false,
                    max_limit: Some(25),
                    projection: None,
                    aggregate: None,
                }],
            },
            vec![file_stats(
                "file://first.parquet",
                Some(10),
                vec![column_stats(
                    "tenant",
                    Some(0),
                    Some(json!("acme")),
                    Some(json!("acme")),
                )],
            )],
        );
        context.delete_files = (0..(DEFAULT_FAST_PATH_MAX_DELETE_FILES + 1))
            .map(|index| format!("file://delete-{index}.parquet"))
            .collect();
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            aggregate: None,
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let plan = plan_request(&context, &request).unwrap();

        assert_eq!(plan.classification, ServingQueryClassification::SlowPath);
        assert!(
            plan.reason
                .as_ref()
                .is_some_and(|reason| reason.contains("delete files"))
        );
    }

    #[test]
    fn test_exact_pruning_summary_may_match_request_rejects_missing_value() {
        let summary = HashMap::from([(
            "tenant".to_string(),
            ExactPruningFieldSummary {
                complete: true,
                values: HashSet::from(["acme".to_string()]),
            },
        )]);
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("omega")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            aggregate: None,
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        assert!(!exact_pruning_summary_may_match_request(&summary, &request));
    }

    #[test]
    fn test_build_serving_layout_advice_recommends_partition_sort_and_exact_artifacts() {
        let context = test_context(
            ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "tenant_scores".to_string(),
                    eq_fields: vec!["tenant".to_string()],
                    range_field: None,
                    order_field: Some("score".to_string()),
                    descending: true,
                    max_limit: Some(10),
                    projection: None,
                    aggregate: None,
                }],
            },
            vec![file_stats(
                "file://first.parquet",
                Some(10),
                vec![column_stats(
                    "tenant",
                    Some(0),
                    Some(json!("acme")),
                    Some(json!("omega")),
                )],
            )],
        );

        let advice = build_serving_layout_advice(&context);

        assert_eq!(advice.patterns.len(), 1);
        assert!(!advice.issues.is_empty());
        assert!(
            advice.patterns[0]
                .recommendation
                .as_ref()
                .is_some_and(|recommendation| recommendation.contains("Cluster or partition"))
        );
    }

    #[test]
    fn test_prune_candidate_files_keeps_unknown_stats_and_drops_all_nulls() {
        let schema = test_schema();
        let files = test_files(&schema);
        let file_stats = HashMap::from([(
            "file://first.parquet".to_string(),
            file_stats(
                "file://first.parquet",
                Some(10),
                vec![column_stats("tenant", Some(10), None, None)],
            ),
        )]);
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            aggregate: None,
            order_by: vec![],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let selected_files = prune_candidate_files(&files, &file_stats, &request);

        assert_eq!(
            selected_files
                .selected_files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://second.parquet"]
        );
        assert_eq!(selected_files.files_selected, 1);
        assert_eq!(selected_files.row_groups_considered, 0);
        assert_eq!(selected_files.row_groups_selected, 0);
        assert_eq!(selected_files.estimated_bytes, 200);
    }

    #[test]
    fn test_group_files_by_schema_batches_compatible_files() {
        let schema = test_schema();
        let other_schema = alternate_schema();
        let files = vec![
            FileDescriptor {
                file_path: "file://first.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://second.parquet".to_string(),
                schema: schema.clone(),
                size: 200,
            },
            FileDescriptor {
                file_path: "file://third.parquet".to_string(),
                schema: other_schema,
                size: 300,
            },
        ];

        let groups = group_files_by_schema(&files);

        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0]
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://first.parquet", "file://second.parquet"]
        );
        assert_eq!(
            groups[1]
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://third.parquet"]
        );
    }

    #[test]
    fn test_ordered_file_groups_for_top_k_sorts_groups_by_descending_upper_bound() {
        let schema = test_schema();
        let other_schema = alternate_schema();
        let files = vec![
            FileDescriptor {
                file_path: "file://first.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://second.parquet".to_string(),
                schema: schema.clone(),
                size: 200,
            },
            FileDescriptor {
                file_path: "file://third.parquet".to_string(),
                schema: other_schema,
                size: 300,
            },
        ];
        let mut first = file_stats(
            "file://first.parquet",
            Some(20),
            vec![column_stats(
                "score",
                Some(0),
                Some(json!(0)),
                Some(json!(100)),
            )],
        );
        first.row_groups = vec![
            row_group_stats(
                0,
                Some(10),
                25,
                vec![
                    column_stats("tenant", Some(0), Some(json!("acme")), Some(json!("acme"))),
                    column_stats("score", Some(0), Some(json!(0)), Some(json!(20))),
                ],
            ),
            row_group_stats(
                1,
                Some(10),
                25,
                vec![
                    column_stats(
                        "tenant",
                        Some(0),
                        Some(json!("omega")),
                        Some(json!("omega")),
                    ),
                    column_stats("score", Some(0), Some(json!(90)), Some(json!(100))),
                ],
            ),
        ];
        let mut second = file_stats(
            "file://second.parquet",
            Some(10),
            vec![column_stats(
                "score",
                Some(0),
                Some(json!(10)),
                Some(json!(80)),
            )],
        );
        second.row_groups = vec![row_group_stats(
            0,
            Some(10),
            40,
            vec![
                column_stats("tenant", Some(0), Some(json!("acme")), Some(json!("acme"))),
                column_stats("score", Some(0), Some(json!(10)), Some(json!(80))),
            ],
        )];
        let file_stats = HashMap::from([
            ("file://first.parquet".to_string(), first),
            ("file://second.parquet".to_string(), second),
            (
                "file://third.parquet".to_string(),
                file_stats(
                    "file://third.parquet",
                    Some(10),
                    vec![
                        column_stats("tenant", Some(0), Some(json!("acme")), Some(json!("acme"))),
                        column_stats("score", Some(0), Some(json!(70)), Some(json!(75))),
                    ],
                ),
            ),
        ]);
        let request = ServingRequestPlan {
            select: None,
            filters: vec![ServingPredicate {
                field: "tenant".to_string(),
                eq: Some(json!("acme")),
                in_values: None,
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }],
            aggregate: None,
            order_by: vec![ServingSort {
                field: "score".to_string(),
                descending: true,
            }],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        let ordered_groups =
            ordered_file_groups_for_top_k(&files, &file_stats, &[], &request, "score", true)
                .expect("score bounds should enable ordered top-k");

        assert_eq!(
            ordered_groups[0]
                .files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://first.parquet", "file://second.parquet"]
        );
        assert_eq!(ordered_groups[0].best_case_sort_value, json!(80));
        assert_eq!(
            ordered_groups[1]
                .files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://third.parquet"]
        );
        assert_eq!(ordered_groups[1].best_case_sort_value, json!(75));
    }

    #[test]
    fn test_ordered_file_groups_for_top_k_returns_none_when_sort_bounds_are_missing() {
        let schema = test_schema();
        let files = test_files(&schema);
        let file_stats = HashMap::from([
            (
                "file://first.parquet".to_string(),
                file_stats(
                    "file://first.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(0)),
                        Some(json!(40)),
                    )],
                ),
            ),
            (
                "file://second.parquet".to_string(),
                file_stats("file://second.parquet", Some(10), vec![]),
            ),
        ]);
        let request = ServingRequestPlan {
            select: None,
            filters: vec![],
            aggregate: None,
            order_by: vec![ServingSort {
                field: "score".to_string(),
                descending: true,
            }],
            limit: Some(10),
            allow_slow_path: false,
            explain: false,
        };

        assert!(
            ordered_file_groups_for_top_k(&files, &file_stats, &[], &request, "score", true)
                .is_none(),
            "missing score bounds should fall back to the existing parallel path"
        );
    }

    #[test]
    fn test_select_serving_warmup_files_prefers_order_only_patterns() {
        let schema = test_schema();
        let files = vec![
            FileDescriptor {
                file_path: "file://first.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://second.parquet".to_string(),
                schema,
                size: 200,
            },
        ];
        let file_stats = HashMap::from([
            (
                "file://first.parquet".to_string(),
                file_stats(
                    "file://first.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(0)),
                        Some(json!(50)),
                    )],
                ),
            ),
            (
                "file://second.parquet".to_string(),
                file_stats(
                    "file://second.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(60)),
                        Some(json!(100)),
                    )],
                ),
            ),
        ]);
        let selected = select_serving_warmup_files(
            &ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "top_scores".to_string(),
                    eq_fields: vec![],
                    range_field: None,
                    order_field: Some("score".to_string()),
                    descending: true,
                    max_limit: Some(10),
                    projection: None,
                    aggregate: None,
                }],
            },
            &files,
            &file_stats,
            &[],
        );

        assert_eq!(
            selected
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://first.parquet", "file://second.parquet"]
        );
    }

    #[test]
    fn test_select_serving_warmup_files_skips_patterns_that_need_filters() {
        let schema = test_schema();
        let files = test_files(&schema);
        let file_stats = HashMap::from([
            (
                "file://first.parquet".to_string(),
                file_stats(
                    "file://first.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(0)),
                        Some(json!(40)),
                    )],
                ),
            ),
            (
                "file://second.parquet".to_string(),
                file_stats(
                    "file://second.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(60)),
                        Some(json!(100)),
                    )],
                ),
            ),
        ]);
        let selected = select_serving_warmup_files(
            &ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "tenant_scores".to_string(),
                    eq_fields: vec!["tenant".to_string()],
                    range_field: None,
                    order_field: Some("score".to_string()),
                    descending: true,
                    max_limit: Some(10),
                    projection: None,
                    aggregate: None,
                }],
            },
            &files,
            &file_stats,
            &[],
        );

        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_serving_warmup_files_skips_missing_sort_bounds() {
        let schema = test_schema();
        let files = test_files(&schema);
        let file_stats = HashMap::from([(
            "file://first.parquet".to_string(),
            file_stats(
                "file://first.parquet",
                Some(10),
                vec![column_stats(
                    "score",
                    Some(0),
                    Some(json!(0)),
                    Some(json!(40)),
                )],
            ),
        )]);
        let selected = select_serving_warmup_files(
            &ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "top_scores".to_string(),
                    eq_fields: vec![],
                    range_field: None,
                    order_field: Some("score".to_string()),
                    descending: true,
                    max_limit: Some(10),
                    projection: None,
                    aggregate: None,
                }],
            },
            &files,
            &file_stats,
            &[],
        );

        assert!(selected.is_empty());
    }

    #[test]
    fn test_build_serving_warmup_plan_reports_patterns_and_estimated_bytes() {
        let schema = test_schema();
        let files = vec![
            FileDescriptor {
                file_path: "file://first.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://second.parquet".to_string(),
                schema,
                size: 200,
            },
        ];
        let file_stats = HashMap::from([
            (
                "file://first.parquet".to_string(),
                file_stats(
                    "file://first.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(0)),
                        Some(json!(50)),
                    )],
                ),
            ),
            (
                "file://second.parquet".to_string(),
                file_stats(
                    "file://second.parquet",
                    Some(10),
                    vec![column_stats(
                        "score",
                        Some(0),
                        Some(json!(60)),
                        Some(json!(100)),
                    )],
                ),
            ),
        ]);

        let plan = build_serving_warmup_plan(
            &ServingTableConfig {
                patterns: vec![ServingPattern {
                    name: "top_scores".to_string(),
                    eq_fields: vec![],
                    range_field: None,
                    order_field: Some("score".to_string()),
                    descending: true,
                    max_limit: Some(10),
                    projection: None,
                    aggregate: None,
                }],
            },
            &files,
            &file_stats,
            &[],
        )
        .expect("order-only pattern should produce a warmup plan");

        assert_eq!(plan.matched_patterns, vec!["top_scores"]);
        assert_eq!(plan.estimated_bytes, 300);
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].pattern_name, "top_scores");
        assert_eq!(plan.steps[0].estimated_bytes, 300);
        assert_eq!(plan.steps[0].request.order_by[0].field, "score".to_string());
        assert_eq!(
            plan.selected_files
                .iter()
                .map(|file| file.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["file://first.parquet", "file://second.parquet"]
        );
    }

    #[test]
    fn test_remaining_groups_cannot_beat_kth_row_descending() {
        let rows = vec![json!({ "score": 100 }), json!({ "score": 90 })];

        assert!(remaining_groups_cannot_beat_kth_row(
            &rows,
            &json!(89),
            "score",
            true,
            2,
        ));
        assert!(!remaining_groups_cannot_beat_kth_row(
            &rows,
            &json!(90),
            "score",
            true,
            2,
        ));
    }

    #[test]
    fn test_remaining_groups_cannot_beat_kth_row_ignores_zero_limit() {
        let rows = vec![json!({ "score": 100 })];

        assert!(!remaining_groups_cannot_beat_kth_row(
            &rows,
            &json!(99),
            "score",
            true,
            0,
        ));
    }

    #[test]
    fn test_file_group_table_name_is_order_independent() {
        let schema = test_schema();
        let forward = vec![
            FileDescriptor {
                file_path: "file://alpha.parquet".to_string(),
                schema: schema.clone(),
                size: 100,
            },
            FileDescriptor {
                file_path: "file://beta.parquet".to_string(),
                schema: schema.clone(),
                size: 200,
            },
        ];
        let reverse = vec![forward[1].clone(), forward[0].clone()];

        assert_eq!(
            file_group_table_name(&forward),
            file_group_table_name(&reverse)
        );
    }
}
